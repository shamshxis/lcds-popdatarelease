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
from selenium.webdriver.common.by import By  # <--- FIXED MISSING IMPORT
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

# --- 2. SPECIAL AGENTS (API/XML) ---
class APIScraper:
    def __init__(self):
        self.filter = LCDSFilter()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'
        }

    def scrape_ons_json(self):
        """
        ONS requires 'X-Requested-With: XMLHttpRequest' to return JSON.
        Without it, they return the full HTML page (causing JSON errors).
        """
        url = "https://www.ons.gov.uk/releasecalendar/data?view=upcoming&size=50"
        headers = self.headers.copy()
        headers['X-Requested-With'] = 'XMLHttpRequest' # <--- THE SECRET KEY
        
        events = []
        try:
            logging.info("🇬🇧 ONS: Hitting Hidden JSON API...")
            resp = requests.get(url, headers=headers, timeout=15)
            
            if resp.status_code != 200:
                logging.error(f"❌ ONS Failed: Status {resp.status_code}")
                return []
                
            data = resp.json()
            # ONS JSON structure: {'result': {'results': [...]}}
            results = data.get('result', {}).get('results', [])
            
            for item in results:
                title = item.get('description', {}).get('title')
                date_raw = item.get('description', {}).get('releaseDate')
                uri = item.get('uri')
                
                if title and date_raw:
                    topic = self.filter.classify(title)
                    if topic:
                        link = "https://www.ons.gov.uk" + uri
                        events.append({
                            "title": title, "start": self.filter.normalize_date(date_raw), 
                            "country": "UK", "source": "ONS", "topic": topic, "url": link
                        })
            logging.info(f"✅ ONS: Found {len(events)} items.")
            return events
        except Exception as e:
            logging.error(f"❌ ONS JSON Error: {e}")
            return []

    def scrape_eurostat_xml(self):
        """Uses the Official XML Calendar (Not RSS)"""
        url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
        events = []
        try:
            logging.info("🇪🇺 Eurostat: Fetching XML Calendar...")
            resp = requests.get(url, headers=self.headers, timeout=20)
            soup = BeautifulSoup(resp.content, 'xml') # XML Parser
            
            # Eurostat XML structure: <release_calendar> ... <release> ... </release>
            items = soup.find_all('release')
            
            for item in items:
                # Iterate children to find title/date (structure varies slightly)
                title = item.find('title').text if item.find('title') else ""
                date_str = item.find('release_date').text if item.find('release_date') else ""
                
                topic = self.filter.classify(title)
                if topic and date_str:
                    events.append({
                        "title": title, "start": self.filter.normalize_date(date_str), 
                        "country": "EU", "source": "Eurostat", "topic": topic, 
                        "url": "https://ec.europa.eu/eurostat/news/release-calendar"
                    })
            logging.info(f"✅ Eurostat: Found {len(events)} items.")
            return events
        except Exception as e:
            logging.error(f"❌ Eurostat XML Error: {e}")
            return []

# --- 3. RSS AGENT ---
class RSSAgent:
    def __init__(self):
        self.filter = LCDSFilter()

    def scrape_cdc(self):
        url = "https://tools.cdc.gov/api/v2/resources/media/132608.rss"
        events = []
        try:
            logging.info("🇺🇸 CDC: Checking Feed...")
            feed = feedparser.parse(url)
            for entry in feed.entries:
                topic = self.filter.classify(entry.title)
                if topic:
                    d = self.filter.normalize_date(entry.published)
                    events.append({
                        "title": entry.title, "start": d, 
                        "country": "USA", "source": "CDC", "topic": topic, "url": entry.link
                    })
            logging.info(f"✅ CDC: Found {len(events)} items.")
            return events
        except: return []

# --- 4. SELENIUM AGENT (US Census Only) ---
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

    def scrape_census_text(self):
        """Scans US Census Page Text"""
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        driver = None
        events = []
        try:
            # Robust Driver Loading
            try: driver = webdriver.Chrome(options=self.options)
            except: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
            
            logging.info(f"🌐 US Census: Scanning {url}")
            driver.get(url)
            time.sleep(4)
            
            # Get clean text
            text = driver.find_element(By.TAG_NAME, "body").text
            
            for line in text.split('\n'):
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                if match:
                    title = line.replace(match.group(0), "").strip(" -:")
                    topic = self.filter.classify(title)
                    if topic and len(title) > 10 and len(title) < 150:
                        d_str = self.filter.normalize_date(match.group(0))
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

# --- MAIN ---
def run():
    print("🚀 Starting Logic-Based Engine...")
    all_data = []
    
    # 1. API & XML Sources (Fast & Reliable)
    api = APIScraper()
    all_data.extend(api.scrape_ons_json())       # Fixed ONS
    all_data.extend(api.scrape_eurostat_xml())   # Fixed Eurostat
    
    # 2. RSS Sources
    rss = RSSAgent()
    all_data.extend(rss.scrape_cdc())
    
    # 3. Selenium Sources (Hard to scrape)
    sel = SeleniumAgent()
    all_data.extend(sel.scrape_census_text())    # Fixed Import Error

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
