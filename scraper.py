import pandas as pd
import requests
import feedparser
import re
import os
import json
import logging
from datetime import datetime, timedelta
from dateutil import parser as date_parser

# --- CONFIG ---
DATA_FILE = "data/releases.json"
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class IntelligenceScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (ResearchBot/2.0; LCDS)'}
        self.today = datetime.now()
        self.results = []
        
        # KEYWORDS: Broaden slightly to ensure we catch data
        self.KEYWORDS = ['death', 'mortal', 'birth', 'fertil', 'popul', 'migra', 'census', 'health', 'life exp']

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True)
        except: return None

    def add_row(self, title, date_obj, country, source, url, note=""):
        if not date_obj: return
        
        diff = (date_obj - self.today).days
        # LOGIC: Future (diff >= 0) OR Recent Past (-14 days)
        if diff >= -14:
            is_new = (date_obj.date() == self.today.date()) # Flag for Yellow Tinge
            
            status = "🟢 CONFIRMED" if diff >= 0 else "🔴 RELEASED"
            if "Est" in note: status = "🟡 EXPECTED"
            
            self.results.append({
                "title": title.strip(),
                "start": date_obj.strftime("%Y-%m-%d"),
                "country": country,
                "source": source,
                "url": url,
                "status": status,
                "days_diff": diff,
                "commentary": note or f"Official {source} release.",
                "is_new": is_new,
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M")
            })

    # --- 1. ONS (UK) - VIA JSON API (Fail-Safe) ---
    def scrape_ons(self):
        try:
            # OFFICIAL JSON ENDPOINT (No HTML parsing needed)
            url = "https://www.ons.gov.uk/releasecalendar/data?view=upcoming&size=100"
            resp = requests.get(url, headers=self.headers, timeout=15)
            data = resp.json()
            
            releases = data.get('result', {}).get('results', [])
            for item in releases:
                title = item.get('description', {}).get('title', '')
                date_raw = item.get('description', {}).get('releaseDate', '')
                
                # Check Keywords
                if any(k in title.lower() for k in self.KEYWORDS):
                    link = "https://www.ons.gov.uk" + item.get('uri', '')
                    d_obj = self.normalize_date(date_raw)
                    self.add_row(title, d_obj, "UK", "ONS", link, note="Confirmed via ONS API")
        except Exception as e:
            logging.error(f"ONS API Failed: {e}")

    # --- 2. EUROSTAT - XML CALENDAR ---
    def scrape_eurostat(self):
        try:
            # XML is the most stable feed for EU
            url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = requests.get(url, headers=self.headers, timeout=15)
            # Simple string search to avoid lxml complexity if it fails
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.content, 'xml')
            
            for item in soup.find_all('release'):
                title = item.find('title').text
                d_str = item.find('release_date').text
                
                if any(k in title.lower() for k in self.KEYWORDS):
                    self.add_row(title, self.normalize_date(d_str), "EU", "Eurostat", "https://ec.europa.eu/eurostat/news/release-calendar", note="EU Standard Release")
        except Exception as e:
            logging.error(f"Eurostat Failed: {e}")

    # --- 3. GDELT (INTELLIGENCE LAYER) ---
    def scrape_gdelt(self):
        """
        Queries GDELT for major news signals about releases.
        This finds things the calendars miss.
        """
        try:
            # Query: "Office for National Statistics" AND "Release" (Last 24 hours)
            # URL constructs a JSON query
            query = "Office%20for%20National%20Statistics%20release"
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=5&format=json"
            
            resp = requests.get(url, timeout=10)
            data = resp.json()
            
            for art in data.get('articles', []):
                title = art.get('title', '')
                url = art.get('url', '')
                date_str = art.get('seendate', '') # Format: 20260306T...
                
                # If news mentions our keywords, flag it
                if any(k in title.lower() for k in self.KEYWORDS):
                    d_obj = self.normalize_date(date_str[:8]) # Extract YYYYMMDD
                    self.add_row(f"NEWS SIGNAL: {title}", d_obj, "Global", "GDELT Intelligence", url, note="Detected by GDELT News Scan")
        except: 
            pass # GDELT fails often, don't crash

    # --- 4. ICELAND (Keeping what worked) ---
    def scrape_iceland(self):
        try:
            url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
            resp = requests.get(url, headers=self.headers)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.content, 'html.parser')
            for row in soup.select("table tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    d_txt = cols[0].text.strip()
                    title = cols[1].text.strip()
                    if any(k in title.lower() for k in self.KEYWORDS):
                        self.add_row(title, self.normalize_date(d_txt), "Iceland", "Statice", url, note="Statice Official Table")
        except: pass

    def run(self):
        print("🚀 Starting Intelligence Scraper...")
        self.scrape_ons()      # JSON API
        self.scrape_eurostat() # XML
        self.scrape_iceland()  # HTML
        self.scrape_gdelt()    # News API
        
        # Save
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values(by='start')
            df.to_json(DATA_FILE, orient="records", indent=4)
            print(f"✅ Saved {len(df)} Intelligence Records.")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    IntelligenceScraper().run()
