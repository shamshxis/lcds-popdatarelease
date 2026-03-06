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

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS FILTER (Removes Economic Noise) ---
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

# --- 2. FEED AGENT (Reliable & Fast) ---
class FeedAgent:
    def __init__(self):
        self.filter = LCDSFilter()
        # Header to mimic a browser (avoids ONS 403 on RSS)
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

    def normalize_date(self, date_str):
        try:
            return date_parser.parse(date_str, fuzzy=True).strftime("%Y-%m-%d")
        except: return None

    def fetch(self, target):
        events = []
        name = target['name']
        try:
            logging.info(f"📡 {name}: Fetching Feed...")
            
            # Fetch content with headers (critical for ONS)
            resp = requests.get(target['url'], headers=self.headers, timeout=15)
            
            # Parse
            feed = feedparser.parse(resp.content)
            
            # If standard RSS parsing fails (empty entries), try manual XML
            if not feed.entries and resp.status_code == 200:
                logging.warning(f"⚠️ {name}: Feedparser found 0. Trying XML fallback...")
                soup = BeautifulSoup(resp.content, 'xml')
                for item in soup.find_all('item'):
                    t = item.title.text
                    topic = self.filter.classify(t)
                    if topic:
                        d = item.pubDate.text if item.pubDate else ""
                        link = item.link.text if item.link else target['url']
                        events.append({"title": t, "start": self.normalize_date(d), "country": target['country'], "source": name, "topic": topic, "url": link})
            
            # Standard RSS processing
            for entry in feed.entries:
                t = entry.title
                topic = self.filter.classify(t)
                if topic:
                    d = self.normalize_date(entry.published)
                    events.append({"title": t, "start": d, "country": target['country'], "source": name, "topic": topic, "url": entry.link})

            logging.info(f"✅ {name}: Found {len(events)} items.")

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        return events

# --- 3. SELENIUM AGENT (Fallback for US Census) ---
class SeleniumAgent:
    def __init__(self):
        self.filter = LCDSFilter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.options = options

    def scrape_text_scan(self, url):
        """Dumps text and searches for dates. Robust against layout changes."""
        driver = None
        events = []
        try:
            # Driver Loader
            try: driver = webdriver.Chrome(options=self.options)
            except: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
            
            logging.info(f"🌐 US Census: Scanning {url}")
            driver.get(url)
            time.sleep(3)
            
            # Get raw text
            text = driver.find_element(By.TAG_NAME, "body").text
            
            for line in text.split('\n'):
                # Look for "March 12, 2026" pattern
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                if match:
                    title = line.replace(match.group(0), "").strip(" -:")
                    topic = self.filter.classify(title)
                    # Filter short garbage lines
                    if topic and len(title) > 10 and len(title) < 150:
                        d_str = date_parser.parse(match.group(0)).strftime("%Y-%m-%d")
                        events.append({
                            "title": title, "start": d_str, 
                            "country": "USA", "source": "US Census", 
                            "topic": topic, "url": url
                        })
            
            logging.info(f"✅ US Census: Found {len(events)} items.")
            
        except Exception as e:
            logging.error(f"❌ US Census Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- ORCHESTRATOR ---
def run():
    print("🚀 Starting Feed-First Engine...")
    
    # 1. RSS TARGETS (The Reliable Ones)
    feed_targets = [
        # ONS: The RSS feed is the most stable entry point (if headers are used)
        {"name": "ONS (UK)", "url": "https://www.ons.gov.uk/releasecalendar/rss", "country": "UK"},
        
        # CDC: Your logs proved this works perfectly
        {"name": "CDC (Vital Stats)", "url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss", "country": "USA"},
        
        # CBS: Use their English News RSS. It contains release notifications.
        {"name": "CBS (Netherlands)", "url": "https://www.cbs.nl/en-gb/service/news-releases-rss", "country": "Netherlands"},
        
        # Eurostat: Use their General News RSS. It lists major releases.
        {"name": "Eurostat", "url": "https://ec.europa.eu/eurostat/cache/RSS/rss.xml", "country": "EU"}
    ]

    all_data = []

    # Run Feeds
    agent = FeedAgent()
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(agent.fetch, t) for t in feed_targets]
        for f in concurrent.futures.as_completed(futures):
            all_data.extend(f.result())

    # Run Selenium (Only for US Census which has no feed)
    sel_agent = SeleniumAgent()
    census_data = sel_agent.scrape_text_scan("https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html")
    all_data.extend(census_data)

    # Save
    if all_data:
        unique = {f"{x['start']}_{x['title'][:15]}": x for x in all_data}.values()
        final_list = list(unique)
        final_list.sort(key=lambda x: x['start'])
        
        with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
        print(f"💾 Saved {len(final_list)} datasets.")
    else:
        print("⚠️ No data collected.")

if __name__ == "__main__":
    run()
