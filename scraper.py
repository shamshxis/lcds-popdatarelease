import time
import json
import os
import logging
import re
import concurrent.futures
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# API/Feeds
import eurostat
import cbsodata
import feedparser
from duckduckgo_search import DDGS

# Browser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# --- CONFIG ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
os.makedirs(DATA_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class LCDSFilter:
    def __init__(self):
        self.NOISE = ["industrial", "retail", "trade", "gdp", "energy", "construction", "ppi", "hicp"]
        self.CORE = ["mortal", "death", "birth", "fertil", "migra", "pop", "census", "health", "suicide"]

    def is_relevant(self, text):
        t = str(text).lower()
        if any(n in t for n in self.NOISE): return False
        return any(c in t for c in self.CORE)

class WaterfallScraper:
    def __init__(self):
        self.filter = LCDSFilter()
        self.options = Options()
        self.options.add_argument("--headless=new")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36")

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True).strftime("%Y-%m-%d")
        except: return None

    # --- TIER 1: OFFICIAL APIs ---
    def agent_eurostat(self):
        try:
            logging.info("🇪🇺 Tier 1: Eurostat API")
            toc = eurostat.get_toc_df()
            # Fix column name error by searching for 'update' in columns
            date_col = [c for c in toc.columns if 'update' in c.lower()][0]
            relevant = toc[toc['title'].apply(self.filter.is_relevant)].copy()
            
            events = []
            for _, row in relevant.head(20).iterrows():
                events.append({
                    "title": row['title'], "start": self.normalize_date(row[date_col]),
                    "country": "EU", "source": "Eurostat API", "topic": "Population",
                    "url": "https://ec.europa.eu/eurostat/data/database"
                })
            return events
        except Exception as e:
            logging.error(f"Eurostat API failed: {e}")
            return []

    def agent_cbs_nl(self):
        try:
            logging.info("🇳🇱 Tier 1: CBS Netherlands API")
            toc = pd.DataFrame(cbsodata.get_table_list())
            relevant = toc[toc['Title'].apply(self.filter.is_relevant)]
            return [{
                "title": r['Title'], "start": self.normalize_date(r['Modified']),
                "country": "Netherlands", "source": "CBS API", "topic": "Demography",
                "url": "https://opendata.cbs.nl/statline"
            } for _, r in relevant.head(10).iterrows()]
        except Exception as e:
            logging.error(f"CBS API failed: {e}")
            return []

    # --- TIER 2: RSS & XML FEEDS ---
    def agent_ons_uk(self):
        try:
            logging.info("🇬🇧 Tier 2: ONS UK (Feed)")
            url = "https://www.ons.gov.uk/releasecalendar/rss"
            # ONS requires headers even for RSS
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            feed = feedparser.parse(resp.content)
            events = []
            for entry in feed.entries:
                if self.filter.is_relevant(entry.title):
                    events.append({
                        "title": entry.title, "start": self.normalize_date(entry.published),
                        "country": "UK", "source": "ONS Feed", "topic": "Vital Stats",
                        "url": entry.link
                    })
            return events
        except: return []

    # --- TIER 3: SELENIUM TEXT SCAN (The Brute Force) ---
    def agent_census_usa(self):
        driver = None
        try:
            logging.info("🇺🇸 Tier 3: US Census (Selenium)")
            driver = webdriver.Chrome(options=self.options)
            url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
            driver.get(url)
            time.sleep(5)
            body = driver.find_element("tag name", "body").text
            events = []
            for line in body.split('\n'):
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                if match:
                    title = line.replace(match.group(0), "").strip(" -:")
                    if self.filter.is_relevant(title):
                        events.append({
                            "title": title, "start": self.normalize_date(match.group(0)),
                            "country": "USA", "source": "US Census", "topic": "Population", "url": url
                        })
            return events
        except Exception as e:
            logging.error(f"US Census Failed: {e}")
            return []
        finally:
            if driver: driver.quit()

    # --- TIER 4: SEARCH RESCUE (Self-Healing) ---
    def agent_search_rescue(self, query, country):
        try:
            logging.info(f"🚑 Tier 4: Search Rescue for {country}")
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=3))
                events = []
                for r in results:
                    if self.filter.is_relevant(r['title']):
                        events.append({
                            "title": r['title'], "start": datetime.now().strftime("%Y-%m-%d"),
                            "country": country, "source": "Search Discovery", "topic": "Discovery", "url": r['href']
                        })
                return events
        except: return []

    def run(self):
        all_results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            tasks = [
                executor.submit(self.agent_eurostat),
                executor.submit(self.agent_cbs_nl),
                executor.submit(self.agent_ons_uk),
                executor.submit(self.agent_census_usa),
                executor.submit(self.agent_search_rescue, "site:statcan.gc.ca demography release 2026", "Canada")
            ]
            for future in concurrent.futures.as_completed(tasks):
                all_results.extend(future.result())

        if all_results:
            # Deduplicate by Title
            df = pd.DataFrame(all_results).drop_duplicates(subset=['title'])
            df = df.sort_values(by='start', ascending=False)
            df.to_json(JSON_FILE, orient='records', indent=4)
            logging.info(f"💾 Successfully saved {len(df)} LCDS relevant records.")
        else:
            logging.warning("No data found across any tiers.")

if __name__ == "__main__":
    WaterfallScraper().run()
