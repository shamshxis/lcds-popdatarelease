import pandas as pd
import requests
import feedparser
import re
import os
import json
import logging
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from bs4 import BeautifulSoup

# --- CONFIG ---
DATA_FILE = "data/releases.json"
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class WaterfallScraper:
    def __init__(self):
        # Header rotation to avoid 403 blocks
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.today = datetime.now()
        self.results = []
        
        # KEYWORDS: The "LCDS Filter"
        self.KEYWORDS = ['death', 'mortal', 'birth', 'fertil', 'popul', 'migra', 'census', 'health', 'life exp']

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True)
        except: return None

    def add_row(self, title, date_obj, country, source, url, note="", status_override=None):
        if not date_obj: return
        
        diff = (date_obj - self.today).days
        # LOGIC: Future (diff >= 0) OR Recent Past (-14 days)
        if diff >= -14:
            is_new = (date_obj.date() == self.today.date())
            
            # Status Logic
            if status_override:
                status = status_override
            elif diff >= 0:
                status = "🟢 CONFIRMED"
            else:
                status = "🔴 RELEASED"
            
            # Yellow Tinge Logic (Freshness)
            freshness = "NEW" if is_new else "EXISTING"

            self.results.append({
                "title": title.strip(),
                "start": date_obj.strftime("%Y-%m-%d"),
                "country": country,
                "source": source,
                "url": url,
                "status": status,
                "days_diff": diff,
                "commentary": note,
                "is_new": is_new,
                "freshness": freshness
            })

    # --- ONS (The Problem Child) ---
    def scrape_ons(self):
        # LAYER 1: JSON API (Best)
        try:
            logging.info("🇬🇧 ONS: Trying Layer 1 (JSON API)...")
            url = "https://www.ons.gov.uk/releasecalendar/data?view=upcoming&size=50"
            resp = requests.get(url, headers=self.headers, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                items = data.get('result', {}).get('results', [])
                if items:
                    for item in items:
                        t = item.get('description', {}).get('title', '')
                        d = item.get('description', {}).get('releaseDate', '')
                        if any(k in t.lower() for k in self.KEYWORDS):
                             self.add_row(t, self.normalize_date(d), "UK", "ONS", "https://www.ons.gov.uk"+item['uri'], note="Source: Official API")
                    return # Success! Exit function.
        except Exception as e:
            logging.warning(f"ONS Layer 1 Failed: {e}")

        # LAYER 2: RSS FEED (Fallback)
        try:
            logging.info("🇬🇧 ONS: Trying Layer 2 (RSS Feed)...")
            feed = feedparser.parse("https://www.ons.gov.uk/releasecalendar/rss")
            if feed.entries:
                for entry in feed.entries:
                    if any(k in entry.title.lower() for k in self.KEYWORDS):
                        self.add_row(entry.title, self.normalize_date(entry.published), "UK", "ONS", entry.link, note="Source: RSS Feed")
                return # Success!
        except: pass

        # LAYER 3: HTML SCRAPE (Last Resort)
        try:
            logging.info("🇬🇧 ONS: Trying Layer 3 (HTML Scrape)...")
            url = "https://www.ons.gov.uk/releasecalendar?view=upcoming"
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            for item in soup.select('.release__item'):
                t = item.select_one('h3').get_text(strip=True)
                d = item.select_one('.release__date').get_text(strip=True).replace("Release date:", "").strip()
                if any(k in t.lower() for k in self.KEYWORDS):
                     self.add_row(t, self.normalize_date(d), "UK", "ONS", url, note="Source: HTML Scrape")
        except: pass

    # --- EUROSTAT (XML + Fallback) ---
    def scrape_eurostat(self):
        # LAYER 1: XML Calendar
        try:
            logging.info("🇪🇺 Eurostat: Trying Layer 1 (XML)...")
            url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'xml')
            for item in soup.find_all('release'):
                t = item.find('title').text
                d = item.find('release_date').text
                if any(k in t.lower() for k in self.KEYWORDS):
                    self.add_row(t, self.normalize_date(d), "EU", "Eurostat", "https://ec.europa.eu/eurostat/news/release-calendar", note="Source: XML Calendar")
        except:
             # LAYER 2: RSS Fallback
             try:
                logging.info("🇪🇺 Eurostat: Trying Layer 2 (RSS)...")
                feed = feedparser.parse("https://ec.europa.eu/eurostat/cache/RSS/rss.xml")
                for entry in feed.entries:
                    if any(k in entry.title.lower() for k in self.KEYWORDS):
                        self.add_row(entry.title, self.normalize_date(entry.published), "EU", "Eurostat", entry.link, note="Source: RSS Feed")
             except: pass

    # --- ICELAND (Simple Table) ---
    def scrape_iceland(self):
        try:
            url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            for row in soup.select("table tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    d = cols[0].text.strip()
                    t = cols[1].text.strip()
                    if any(k in t.lower() for k in self.KEYWORDS):
                        self.add_row(t, self.normalize_date(d), "Iceland", "Statice", url, note="Source: Official Table")
        except: pass

    # --- GDELT (Intelligence Signal) ---
    def scrape_gdelt(self):
        try:
            logging.info("📡 GDELT: Scanning News Signals...")
            query = "Office%20for%20National%20Statistics%20release"
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=10&format=json"
            resp = requests.get(url, timeout=5)
            data = resp.json()
            for art in data.get('articles', []):
                t = art.get('title', '')
                u = art.get('url', '')
                d = art.get('seendate', '')[:8]
                if any(k in t.lower() for k in self.KEYWORDS):
                    self.add_row(t, self.normalize_date(d), "Global", "Media Signal", u, note="Detected by GDELT", status_override="🔵 NEWS SIGNAL")
        except: pass

    def run(self):
        print("🚀 Starting Waterfall Scraper...")
        self.scrape_ons()      # 3 Layers
        self.scrape_eurostat() # 2 Layers
        self.scrape_iceland()  # 1 Layer
        self.scrape_gdelt()    # Intelligence Layer
        
        # Save
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values(by='start')
            df.to_json(DATA_FILE, orient="records", indent=4)
            print(f"✅ Saved {len(df)} Records (with fallbacks).")
        else:
            print("⚠️ No data found (All layers failed).")

if __name__ == "__main__":
    WaterfallScraper().run()
