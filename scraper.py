import time
import json
import os
import logging
import re
import concurrent.futures
import random
import requests
import feedparser
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# --- 3rd Party Libs ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from duckduckgo_search import DDGS

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS FILTER (The Brain) ---
class LCDSFilter:
    def __init__(self):
        self.NOISE = [
            "industrial", "construction", "retail", "producer price", "turnover", 
            "trade", "gdp", "tourism", "transport", "business", "output", "electricity", "ppi", "hicp"
        ]
        self.MAP = {
            "mortal": "Mortality", "death": "Mortality", "suicide": "Mortality", "life expect": "Mortality",
            "birth": "Fertility", "fertil": "Fertility", "baby": "Fertility",
            "migra": "Migration", "asylum": "Migration", "visa": "Migration",
            "pop": "Population", "census": "Population", "resident": "Population",
            "health": "Health", "medic": "Health", "covid": "Health", "cancer": "Health",
            "inequal": "Inequality", "poverty": "Inequality", "income": "Inequality",
            "environ": "Environment", "climate": "Environment"
        }

    def classify(self, title):
        t = title.lower()
        if any(x in t for x in self.NOISE): return None
        for key, topic in self.MAP.items():
            if key in t: return topic
        return "General Demography"

    def normalize_date(self, date_str):
        try:
            return date_parser.parse(date_str, fuzzy=True).strftime("%Y-%m-%d")
        except: return None

# --- 2. PRIMARY: TEXT SCANNER (The Working Method) ---
class TextScanner:
    def __init__(self):
        self.filter = LCDSFilter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.options = options

    def scan_url(self, target):
        driver = None
        events = []
        name = target['name']
        url = target['url']
        
        try:
            try: driver = webdriver.Chrome(options=self.options)
            except: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
            
            logging.info(f"🌐 {name}: Scanning {url}")
            driver.get(url)
            time.sleep(5) 
            
            # --- BRUTE FORCE TEXT DUMP ---
            body_text = driver.find_element("tag name", "body").text
            lines = body_text.split('\n')
            
            for i, line in enumerate(lines):
                # Matches: "12 March 2026", "March 12, 2026", "2026-03-12"
                match = re.search(r'(\d{1,2} [A-Z][a-z]+ \d{4})|([A-Z][a-z]+ \d{1,2}, \d{4})|(\d{4}-\d{2}-\d{2})', line)
                
                if match:
                    date_str = match.group(0)
                    # Look at current line AND previous line for title
                    candidates = [line.replace(date_str, ""), lines[i-1] if i>0 else ""]
                    
                    for text_chunk in candidates:
                        clean_title = text_chunk.strip(" -:")
                        topic = self.filter.classify(clean_title)
                        
                        if topic and len(clean_title) > 10 and len(clean_title) < 150:
                            norm_date = self.filter.normalize_date(date_str)
                            if norm_date:
                                events.append({
                                    "title": clean_title, "start": norm_date,
                                    "country": target['country'], "source": name,
                                    "topic": topic, "url": url
                                })
                                break 
            
            logging.info(f"✅ {name}: Found {len(events)} items (Text Scan).")

        except Exception as e:
            logging.error(f"❌ {name} Scan Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- 3. SECONDARY: SEARCH & RESCUE (Self-Healing) ---
class SearchRescue:
    """If Primary fails (0 items), finds a new URL and retries."""
    def find_and_scan(self, target):
        logging.info(f"🚑 {target['name']}: Primary failed. Searching for fresh URL...")
        try:
            with DDGS() as ddgs:
                # e.g. "site:ons.gov.uk release calendar 2026"
                query = f"site:{target.get('domain', '')} {target.get('search_term', 'release calendar')}"
                results = list(ddgs.text(query, max_results=1))
                
                if results:
                    new_url = results[0]['href']
                    logging.info(f"🎯 Found New URL: {new_url}")
                    target['url'] = new_url # Update target
                    
                    # Retry Scan with new URL
                    scanner = TextScanner()
                    return scanner.scan_url(target)
        except Exception as e:
            logging.warning(f"⚠️ Search Rescue failed: {e}")
        return []

# --- 4. TERTIARY: RSS/XML FEED (Fast Backup) ---
class RSSAgent:
    def __init__(self):
        self.filter = LCDSFilter()

    def fetch(self, target):
        events = []
        name = target['name']
        # Window: Future + Past 30 Days (Fixed the "0 items" issue)
        start_window = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        try:
            if not target.get('feed_url'): return []
            
            logging.info(f"📡 {name}: Checking Feed...")
            feed = feedparser.parse(target['feed_url'])
            
            for entry in feed.entries:
                topic = self.filter.classify(entry.title)
                if topic:
                    d = self.filter.normalize_date(entry.published)
                    if d and d >= start_window: 
                        events.append({
                            "title": entry.title, "start": d, 
                            "country": target['country'], "source": name, 
                            "topic": topic, "url": entry.link
                        })
            logging.info(f"✅ {name}: Found {len(events)} items (Feed).")
        except: pass
        return events

# --- ORCHESTRATOR ---
def run():
    print("🚀 Starting Defense-in-Depth Engine...")
    all_data = []
    
    # TARGET DEFINITIONS
    targets = [
        # UK
        {
            "name": "ONS (UK)", "country": "UK",
            "url": "https://www.ons.gov.uk/releasecalendar", 
            "domain": "ons.gov.uk", "search_term": "release calendar upcoming",
            "feed_url": "https://www.ons.gov.uk/releasecalendar/rss"
        },
        # EU
        {
            "name": "Eurostat", "country": "EU",
            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
            "domain": "ec.europa.eu", "search_term": "eurostat release calendar",
            "feed_url": "https://ec.europa.eu/eurostat/cache/RSS/rss.xml"
        },
        # USA (Census has no feed, CDC has feed)
        {
            "name": "US Census", "country": "USA",
            "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html",
            "domain": "census.gov", "search_term": "upcoming releases calendar"
        },
        {
            "name": "CDC (Vital Stats)", "country": "USA",
            "url": "https://www.cdc.gov/nchs/nvss/deaths.htm", # For text scan
            "feed_url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss"
        },
        # NETHERLANDS
        {
            "name": "CBS (Netherlands)", "country": "Netherlands",
            "url": "https://www.cbs.nl/en-gb/publication-calendar",
            "domain": "cbs.nl", "search_term": "publication calendar english"
        },
        # CANADA (New Addition)
        {
            "name": "StatCan", "country": "Canada",
            "url": "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm",
            "domain": "statcan.gc.ca", "search_term": "the daily release schedule"
        }
    ]

    scanner = TextScanner()
    rescuer = SearchRescue()
    rss = RSSAgent()

    # SEQUENTIAL EXECUTION (Safest for Memory)
    for t in targets:
        # 1. TRY PRIMARY (Text Scan)
        results = scanner.scan_url(t)
        
        # 2. IF FAILED (0 items), TRY RESCUE (Search & Scan)
        if not results:
            results = rescuer.find_and_scan(t)
            
        # 3. IF STILL EMPTY, TRY FEED (RSS)
        if not results and t.get('feed_url'):
            results = rss.fetch(t)
            
        # 4. IF STILL EMPTY, TRY FEED ANYWAY (Data Enrichment)
        # Even if scan worked, feed might have *recent* data the calendar hides
        if t.get('feed_url'):
            feed_results = rss.fetch(t)
            results.extend(feed_results)

        all_data.extend(results)

    # Save
    if all_data:
        unique = {f"{x['start']}_{x['title'][:15]}": x for x in all_data}.values()
        final_list = list(unique)
        final_list.sort(key=lambda x: x['start'])
        
        with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
        print(f"💾 Saved {len(final_list)} unique datasets.")
    else:
        print("⚠️ No data collected.")

if __name__ == "__main__":
    run()
