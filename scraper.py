import time
import json
import os
import logging
import re
import concurrent.futures
import random
import requests
import feedparser
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# --- 3rd Party Libs ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS FILTER ---
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

# --- 2. BRUTE FORCE SCANNER (Selenium) ---
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

    def scan_page(self, target):
        driver = None
        events = []
        name = target['name']
        url = target['url']
        
        try:
            # Init Driver
            try: driver = webdriver.Chrome(options=self.options)
            except: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
            
            logging.info(f"🌐 {name}: scanning {url}")
            driver.get(url)
            time.sleep(5) # Wait for JS load
            
            # --- STRATEGY: DUMP ALL TEXT ---
            # We get the full body text and search for date patterns.
            # This bypasses complex HTML structure entirely.
            body_text = driver.find_element("tag name", "body").text
            lines = body_text.split('\n')
            
            for i, line in enumerate(lines):
                # Pattern: "12 March 2026" or "March 12, 2026"
                match = re.search(r'(\d{1,2} [A-Z][a-z]+ \d{4})|([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                
                if match:
                    date_str = match.group(0)
                    # The title is usually on the SAME line or the line BEFORE
                    # We check both
                    candidates = [line.replace(date_str, ""), lines[i-1] if i>0 else ""]
                    
                    for text_chunk in candidates:
                        clean_title = text_chunk.strip(" -:")
                        topic = self.filter.classify(clean_title)
                        
                        # Valid Title Criteria
                        if topic and len(clean_title) > 10 and len(clean_title) < 150:
                            norm_date = self.filter.normalize_date(date_str)
                            if norm_date:
                                events.append({
                                    "title": clean_title,
                                    "start": norm_date,
                                    "country": target['country'],
                                    "source": name,
                                    "topic": topic,
                                    "url": url
                                })
                                break # Found a valid title for this date
            
            logging.info(f"✅ {name}: Found {len(events)} items.")

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- 3. RSS AGENT (Filtered) ---
class RSSAgent:
    def __init__(self):
        self.filter = LCDSFilter()

    def fetch(self, target):
        events = []
        name = target['name']
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            logging.info(f"📡 {name}: Checking Feed...")
            feed = feedparser.parse(target['url'])
            
            for entry in feed.entries:
                topic = self.filter.classify(entry.title)
                if topic:
                    d = self.filter.normalize_date(entry.published)
                    # FILTER: Only Future or Recent (last 7 days)
                    if d and d >= today: 
                        events.append({
                            "title": entry.title, "start": d, 
                            "country": target['country'], "source": name, 
                            "topic": topic, "url": entry.link
                        })
            logging.info(f"✅ {name}: Found {len(events)} items (Future Only).")
        except: pass
        return events

# --- ORCHESTRATOR ---
def run():
    print("🚀 Starting Brute Force Engine...")
    all_data = []
    
    # 1. RSS TARGETS (Fast)
    rss_agent = RSSAgent()
    rss_targets = [
        {"name": "CDC (Vital Stats)", "url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss", "country": "USA"},
    ]
    all_data.extend(rss_agent.fetch(rss_targets[0]))

    # 2. TEXT SCAN TARGETS (Robust)
    # We scan the visual calendar pages directly for text patterns
    scan_targets = [
        {"name": "ONS (UK)", "url": "https://www.ons.gov.uk/releasecalendar", "country": "UK"},
        {"name": "Eurostat", "url": "https://ec.europa.eu/eurostat/news/release-calendar", "country": "EU"},
        {"name": "US Census", "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html", "country": "USA"},
        {"name": "CBS (Netherlands)", "url": "https://www.cbs.nl/en-gb/publication-calendar", "country": "Netherlands"}
    ]

    scanner = TextScanner()
    # Run Selenium scans sequentially to avoid memory crash
    for t in scan_targets:
        all_data.extend(scanner.scan_page(t))

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
