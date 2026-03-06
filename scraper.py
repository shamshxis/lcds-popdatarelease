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
from duckduckgo_search import DDGS

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json")
os.makedirs(DATA_DIR, exist_ok=True)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS THEME FILTER ---
class LCDSFilter:
    def __init__(self):
        # Noise to strictly ignore
        self.NOISE = [
            "industrial", "construction", "retail", "producer price", "turnover", 
            "trade", "gdp", "tourism", "transport", "business", "output", "electricity", "ppi"
        ]
        # Core Mapping
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

# --- 2. SPECIAL AGENT: ONS (UK) JSON ---
class ONSJsonAgent:
    """Hits the ONS hidden data endpoint directly."""
    def __init__(self):
        self.filter = LCDSFilter()
        # Mimic a browser to avoid 403
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
        }

    def scrape(self):
        # ONS uses this endpoint to render their page. We can read it directly!
        url = "https://www.ons.gov.uk/releasecalendar/data"
        events = []
        try:
            logging.info("🇬🇧 ONS: Fetching JSON Data...")
            resp = requests.get(url, headers=self.headers, timeout=15)
            data = resp.json()
            
            # The JSON structure usually has a 'result' or 'sections' list
            # We iterate through the raw list of releases
            results = data.get('result', {}).get('results', [])
            
            for item in results:
                title = item.get('description', {}).get('title') or item.get('title')
                date_str = item.get('description', {}).get('releaseDate')
                uri = item.get('uri')
                
                if title and date_str:
                    topic = self.filter.classify(title)
                    if topic:
                        # Date is often ISO or close to it
                        dt = date_parser.parse(date_str).strftime("%Y-%m-%d")
                        link = "https://www.ons.gov.uk" + uri
                        events.append({"title": title, "start": dt, "country": "UK", "source": "ONS", "topic": topic, "url": link})
            
            logging.info(f"✅ ONS: Found {len(events)} items (via JSON).")
            return events
        except Exception as e:
            logging.error(f"❌ ONS JSON Failed: {e}")
            return []

# --- 3. SELENIUM AGENT (Fallback for JS Sites) ---
class SeleniumAgent:
    def __init__(self):
        self.filter = LCDSFilter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        # Anti-Detection Flags
        options.add_argument("--disable-blink-features=AutomationControlled") 
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.options = options

    def normalize_date(self, date_str):
        try:
            return date_parser.parse(date_str, fuzzy=True).strftime("%Y-%m-%d")
        except: return None

    def get_driver(self):
        try:
            return webdriver.Chrome(options=self.options)
        except:
            service = Service(ChromeDriverManager().install())
            return webdriver.Chrome(service=service, options=self.options)

    def scrape(self, target):
        driver = None
        events = []
        name = target['name']
        
        try:
            driver = self.get_driver()
            logging.info(f"🌐 {name}: Visiting {target['url']}")
            driver.get(target['url'])
            
            # Random wait for JS to execute
            time.sleep(random.uniform(4, 7))
            
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            
            # --- EUROSTAT (HTML Table) ---
            if "Eurostat" in name:
                for row in soup.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) > 1:
                        t = cols[-1].get_text(strip=True)
                        topic = self.filter.classify(t)
                        if topic:
                            d_str = self.normalize_date(row.get_text())
                            if d_str: 
                                events.append({"title": t, "start": d_str, "country": "EU", "source": "Eurostat", "topic": topic, "url": target['url']})

            # --- CBS (Netherlands) ---
            elif "CBS" in name:
                # Look for cards/thumbnails
                for item in soup.select('.thumbnail, .overview-item'):
                    t_elem = item.select_one('h3, h2')
                    if t_elem:
                        t = t_elem.get_text(strip=True)
                        topic = self.filter.classify(t)
                        if topic:
                            time_tag = item.select_one('time')
                            if time_tag:
                                d_str = self.normalize_date(time_tag.get('datetime'))
                                link = "https://www.cbs.nl" + item.select_one('a')['href']
                                events.append({"title": t, "start": d_str, "country": "Netherlands", "source": "CBS", "topic": topic, "url": link})

            # --- US Census ---
            elif "Census" in name:
                text = soup.get_text("\n")
                for line in text.split("\n"):
                    match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                    if match:
                        t = line.replace(match.group(0), "").strip(" -:")
                        topic = self.filter.classify(t)
                        if topic and len(t) > 10:
                            events.append({"title": t, "start": self.normalize_date(match.group(0)), "country": "USA", "source": "US Census", "topic": topic, "url": target['url']})

            logging.info(f"✅ {name}: Found {len(events)} items.")

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- 4. RSS AGENT (Feeds) ---
class RSSAgent:
    def __init__(self):
        self.filter = LCDSFilter()

    def scrape(self, target):
        events = []
        name = target['name']
        try:
            logging.info(f"📡 {name}: Checking Feed...")
            feed = feedparser.parse(target['url'])
            for entry in feed.entries:
                topic = self.filter.classify(entry.title)
                if topic:
                    d = date_parser.parse(entry.published).strftime("%Y-%m-%d")
                    events.append({
                        "title": entry.title, "start": d, 
                        "country": target['country'], "source": name, 
                        "topic": topic, "url": entry.link
                    })
            logging.info(f"✅ {name}: Found {len(events)} items.")
        except Exception as e:
            logging.error(f"❌ {name} RSS Failed: {e}")
        return events

# --- ORCHESTRATOR ---
def run_scraper_system():
    print("🚀 Starting API/Hybrid System...")
    
    # 1. SPECIAL AGENTS
    ons_agent = ONSJsonAgent()
    
    # 2. SELENIUM TARGETS
    selenium_targets = [
        {"name": "Eurostat", "url": "https://ec.europa.eu/eurostat/news/release-calendar"},
        {"name": "CBS (Netherlands)", "url": "https://www.cbs.nl/en-gb/publication-calendar"},
        {"name": "US Census", "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"}
    ]
    
    # 3. RSS TARGETS
    rss_targets = [
        {"name": "CDC (Vital Stats)", "url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss", "country": "USA"}
    ]

    all_data = []

    # EXECUTION
    # ONS runs on main thread (fast)
    all_data.extend(ons_agent.scrape())

    # Parallelize the rest
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        sel_agent = SeleniumAgent()
        sel_futures = [executor.submit(sel_agent.scrape, t) for t in selenium_targets]
        
        rss_agent = RSSAgent()
        rss_futures = [executor.submit(rss_agent.scrape, t) for t in rss_targets]
        
        for future in concurrent.futures.as_completed(sel_futures + rss_futures):
            all_data.extend(future.result())

    # SAVE
    if all_data:
        unique = {f"{x['start']}_{x['title'][:15]}": x for x in all_data}.values()
        final_list = list(unique)
        final_list.sort(key=lambda x: x['start'])
        
        with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
        print(f"💾 Saved {len(final_list)} unique datasets.")
    else:
        print("⚠️ No data collected.")

if __name__ == "__main__":
    run_scraper_system()
