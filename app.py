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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. LCDS FILTER (Context Aware) ---
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

# --- 2. SMART ELEMENT WALKER (Selenium) ---
class ElementWalker:
    def __init__(self):
        self.filter = LCDSFilter()
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        self.options = options

    def scrape_site(self, target):
        driver = None
        events = []
        name = target['name']
        today = datetime.now().strftime("%Y-%m-%d")
        
        try:
            # Init Driver
            try: driver = webdriver.Chrome(options=self.options)
            except: driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=self.options)
            
            logging.info(f"🌐 {name}: Visiting {target['url']}")
            driver.get(target['url'])
            
            # Universal Wait
            wait = WebDriverWait(driver, 15)
            
            # --- ONS STRATEGY (Specific Classes) ---
            if "ONS" in name:
                # Wait for items
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".release__item, .list__item")))
                
                # Expand specific link extraction
                elements = driver.find_elements(By.CSS_SELECTOR, ".release__item, .list__item")
                for el in elements:
                    try:
                        title_el = el.find_element(By.TAG_NAME, "h3")
                        link_el = el.find_element(By.TAG_NAME, "a")
                        date_el = el.find_element(By.CLASS_NAME, "release__date")
                        
                        title = title_el.text.strip()
                        link = link_el.get_attribute("href")
                        date_txt = date_el.text.replace("Release date:", "").strip()
                        
                        topic = self.filter.classify(title)
                        if topic:
                            d_str = self.filter.normalize_date(date_txt)
                            # STRICT FUTURE CHECK
                            if d_str and d_str >= today:
                                events.append({"title": title, "start": d_str, "country": "UK", "source": "ONS", "topic": topic, "url": link})
                    except: continue

            # --- EUROSTAT STRATEGY (Table Row Parsing) ---
            elif "Eurostat" in name:
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
                rows = driver.find_elements(By.TAG_NAME, "tr")
                for row in rows:
                    try:
                        # Eurostat structure: Date | ... | Title (Link)
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) > 1:
                            # Date is often in a specific cell or header. We check text.
                            row_text = row.text
                            # Extract Date Regex
                            date_match = re.search(r'\d{2}-\d{2}-\d{4}', row_text)
                            
                            if date_match:
                                link_el = row.find_element(By.TAG_NAME, "a")
                                title = link_el.text.strip()
                                link = link_el.get_attribute("href")
                                
                                topic = self.filter.classify(title)
                                if topic:
                                    d_str = self.filter.normalize_date(date_match.group(0))
                                    if d_str and d_str >= today:
                                        events.append({"title": title, "start": d_str, "country": "EU", "source": "Eurostat", "topic": topic, "url": link})
                    except: continue

            # --- US CENSUS STRATEGY (Text Scan + Base URL) ---
            elif "Census" in name:
                # Census list is just text, they don't link individual future releases easily.
                # We keep the base URL but filter strict future dates.
                text = driver.find_element(By.TAG_NAME, "body").text
                for line in text.split('\n'):
                    match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                    if match:
                        title = line.replace(match.group(0), "").strip(" -:")
                        topic = self.filter.classify(title)
                        if topic and len(title) > 10:
                            d_str = self.filter.normalize_date(match.group(0))
                            if d_str and d_str >= today:
                                events.append({"title": title, "start": d_str, "country": "USA", "source": "US Census", "topic": topic, "url": target['url']})

            # --- STATCAN STRATEGY (List Parsing) ---
            elif "StatCan" in name:
                # StatCan uses <li> with date headers. 
                # Complex structure, simplified: Scan links
                links = driver.find_elements(By.CSS_SELECTOR, "main li a")
                for a in links:
                    try:
                        title = a.text
                        link = a.get_attribute("href")
                        # Look for date in parent or preceding sibling (approximated by text scan of parent)
                        parent = a.find_element(By.XPATH, "..")
                        parent_text = parent.text 
                        
                        # Find date in text
                        match = re.search(r'([A-Z][a-z]+ \d{1,2})', parent_text)
                        if match and self.filter.classify(title):
                            # StatCan often omits year, assume current/next
                            d_str = self.filter.normalize_date(f"{match.group(0)} {datetime.now().year}")
                            if d_str and d_str >= today:
                                events.append({"title": title, "start": d_str, "country": "Canada", "source": "StatCan", "topic": self.filter.classify(title), "url": link})
                    except: continue

            logging.info(f"✅ {name}: Found {len(events)} Future items.")

        except Exception as e:
            logging.error(f"❌ {name} Failed: {e}")
        finally:
            if driver: driver.quit()
        return events

# --- 3. RSS AGENT (Backup) ---
class RSSAgent:
    def __init__(self):
        self.filter = LCDSFilter()

    def fetch(self, target):
        events = []
        name = target['name']
        today = datetime.now().strftime("%Y-%m-%d")
        try:
            feed = feedparser.parse(target['feed_url'])
            for entry in feed.entries:
                topic = self.filter.classify(entry.title)
                d = self.filter.normalize_date(entry.published)
                # STRICT FUTURE CHECK
                if topic and d and d >= today:
                    events.append({"title": entry.title, "start": d, "country": target['country'], "source": name, "topic": topic, "url": entry.link})
        except: pass
        return events

# --- ORCHESTRATOR ---
def run():
    print("🚀 Starting Future-Only Deep-Link Engine...")
    all_data = []
    
    # TARGETS
    targets = [
        # ONS: Pointing to UPCOMING view to get future data
        {"name": "ONS (UK)", "country": "UK", "url": "https://www.ons.gov.uk/releasecalendar?view=upcoming"},
        {"name": "Eurostat", "country": "EU", "url": "https://ec.europa.eu/eurostat/news/release-calendar"},
        {"name": "US Census", "country": "USA", "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"},
        {"name": "StatCan", "country": "Canada", "url": "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm"}
    ]
    
    # RSS TARGET (CDC)
    rss_target = {"name": "CDC", "country": "USA", "feed_url": "https://tools.cdc.gov/api/v2/resources/media/132608.rss"}

    walker = ElementWalker()
    rss = RSSAgent()

    # Run Selenium sequentially (Stability)
    for t in targets:
        all_data.extend(walker.scrape_site(t))
    
    # Run RSS
    all_data.extend(rss.fetch(rss_target))

    # Save
    if all_data:
        # Deduplicate by URL to ensure uniqueness
        unique = {x['url']: x for x in all_data}.values()
        final_list = list(unique)
        final_list.sort(key=lambda x: x['start'])
        
        with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
        print(f"💾 Saved {len(final_list)} Future Datasets with Direct Links.")
    else:
        print("⚠️ No future data found.")

if __name__ == "__main__":
    run()
