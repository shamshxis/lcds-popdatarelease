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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from duckduckgo_search import DDGS

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json")
os.makedirs(DATA_DIR, exist_ok=True)

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS THEME FILTER (The Brain) ---
class LCDSFilter:
    def __init__(self):
        # Noise to strictly ignore
        self.NOISE = [
            "industrial", "construction", "retail", "producer price", "turnover", 
            "trade", "gdp", "tourism", "transport", "business", "output", "electricity"
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

# --- 2. SEARCH SCOUT (DDGS) ---
class SearchScout:
    """Finds fresh URLs if hardcoded ones fail."""
    def find_calendar_url(self, domain, query):
        try:
            time.sleep(random.uniform(2, 4))
            with DDGS() as ddgs:
                dork = f"site:{domain} {query}"
                results = list(ddgs.text(dork, max_results=2))
                if results:
                    return results[0]['href']
        except Exception as e:
            logging.warning(f"⚠️ Scout failed for {domain}: {e}")
        return None

# --- 3. SELENIUM AGENT (The Browser) ---
class SeleniumAgent:
    def __init__(self):
        self.filter = LCDSFilter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
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
            
            url = target.get('url')
            if not url:
                logging.info(f"🔎 {name}: Scouting for URL...")
                scout = SearchScout()
                url = scout.find_calendar_url(target['domain'], target['search_query'])
            
            if not url:
                logging.error(f"❌ {name}: No URL found.")
                return []

            logging.info(f"🌐 {name}: Visiting {url}")
            driver.get(url)
            
            # --- ROBUST WAIT STRATEGY ---
            wait = WebDriverWait(driver, 20) # Wait up to 20 seconds
            
            # 1. ONS (UK) Logic
            if "ONS" in name:
                # Wait for ANY item to load
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".release__item, .list__item, h3")))
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                # Try generic selectors if specific ones fail
                items = soup.select('.release__item') or soup.select('.list__item')
                
                for item in items:
                    t_elem = item.select_one('h3') or item.select_one('a')
                    if t_elem:
                        t = t_elem.get_text(strip=True)
                        topic = self.filter.classify(t)
                        if topic:
                            d_elem = item.select_one('.release__date')
                            if d_elem:
                                d_txt = d_elem.get_text(strip=True).replace("Release date:", "")
                                d_str = self.normalize_date(d_txt)
                                link_tag = item.select_one('a')
                                link = "https://www.ons.gov.uk" + link_tag['href'] if link_tag else url
                                if d_str:
                                    events.append({"title": t, "start": d_str, "country": "UK", "source": "ONS", "topic": topic, "url": link})

            # 2. Eurostat Logic
            elif "Eurostat" in name:
                # Wait for TABLE to load (crucial for JS sites)
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/3);")
                time.sleep(2)
                
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                for row in soup.find_all('tr'):
                    cols = row.find_all('td')
                    if len(cols) > 1:
                        t = cols[-1].get_text(strip=True)
                        topic = self.filter.classify(t)
                        if topic:
                            d_str = self.normalize_date(row.get_text())
                            if d_str: 
                                events.append({"title": t, "start": d_str, "country": "EU", "source": "Eurostat", "topic": topic, "url": url})

            # 3. CBS (Netherlands) Logic - MOVED TO SELENIUM
            elif "CBS" in name:
                # Wait for thumbnails or calendar items
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".thumbnail, .overview-item")))
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                
                for item in soup.select('.thumbnail') + soup.select('.overview-item'):
                    t_elem = item.select_one('h3') or item.select_one('h2')
                    if t_elem:
                        t = t_elem.get_text(strip=True)
                        topic = self.filter.classify(t)
                        if topic:
                            time_tag = item.select_one('time')
                            if time_tag:
                                d_str = self.normalize_date(time_tag.get('datetime')) or self.normalize_date(time_tag.get_text())
                                link_tag = item.select_one('a')
                                link = "https://www.cbs.nl" + link_tag['href'] if link_tag else url
                                if d_str:
                                    events.append({"title": t, "start": d_str, "country": "Netherlands", "source": "CBS", "topic": topic, "url": link})

            # 4. US Census Logic
            elif "Census" in name:
                # Simple text extraction, no complex JS usually
                time.sleep(3)
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                text = soup.get_text("\n")
                for line in text.split("\n"):
                    match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                    if match:
                        t = line.replace(match.group(0), "").strip(" -:")
                        topic = self.filter.classify(t)
                        if topic and len(t) > 10:
                            events.append({"title": t, "start": self.normalize_date(match.group(0)), "country": "USA", "source": "US Census", "topic": topic, "url": url})

            logging.info(f"✅ {name}: Found {len(events)} items.")

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- 4. RSS AGENT (Lightweight) ---
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
def run_parallel_scraper():
    print("🚀 Starting Multi-Agent System...")
    
    # 1. SELENIUM TARGETS (JS Sites)
    selenium_targets = [
        {"name": "ONS (UK)", "domain": "ons.gov.uk", "search_query": "\"release calendar\" demography", "url": "https://www.ons.gov.uk/releasecalendar"},
        {"name": "Eurostat", "domain": "ec.europa.eu", "search_query": "release calendar", "url": "https://ec.europa.eu/eurostat/news/release-calendar"},
        {"name": "CBS (Netherlands)", "domain": "cbs.nl", "search_query": "publication calendar", "url": "https://www.cbs.nl/en-gb/publication-calendar"},
        {"name": "US Census", "domain": "census.gov", "search_query": "upcoming releases", "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"}
    ]
    
    # 2. RSS TARGETS (Feed Sites)
    rss_targets = [
        {"name": "CDC (Vital Stats)", "url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss", "country": "USA"}
    ]

    all_data = []

    # 3. EXECUTION
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        sel_agent = SeleniumAgent()
        sel_futures = [executor.submit(sel_agent.scrape, t) for t in selenium_targets]
        
        rss_agent = RSSAgent()
        rss_futures = [executor.submit(rss_agent.scrape, t) for t in rss_targets]
        
        for future in concurrent.futures.as_completed(sel_futures + rss_futures):
            all_data.extend(future.result())

    # 4. SAVE
    if all_data:
        unique = {f"{x['start']}_{x['title'][:15]}": x for x in all_data}.values()
        final_list = list(unique)
        final_list.sort(key=lambda x: x['start'])
        
        with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
        print(f"💾 Saved {len(final_list)} unique datasets.")
    else:
        print("⚠️ No data collected.")

if __name__ == "__main__":
    run_parallel_scraper()
