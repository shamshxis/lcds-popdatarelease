import pandas as pd
import requests
import feedparser
import re
import os
import json
import logging
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as date_parser

# --- CONFIG ---
DATA_FILE = "data/releases.json"
os.makedirs("data", exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class PrecisionScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        self.today = datetime.now()
        self.results = []

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True)
        except: return None

    def add_result(self, title, date_obj, country, source, url, status="🟢 CONFIRMED"):
        if not date_obj: return
        
        # LOGIC: Only add if Future OR Recent Past (7 days)
        diff = (date_obj - self.today).days
        if diff >= 0 or (diff < 0 and diff > -7):
            self.results.append({
                "title": title.strip(),
                "start": date_obj.strftime("%Y-%m-%d"),
                "country": country,
                "source": source,
                "url": url,
                "status": status if diff >= 0 else "🔵 NEWS"
            })

    # --- 1. EUROSTAT (XML Calendar) ---
    def scrape_eurostat(self):
        try:
            logging.info("🇪🇺 Eurostat: Fetching XML Calendar...")
            url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'xml')
            
            for item in soup.find_all('release'):
                title = item.find('title').text
                d_str = item.find('release_date').text
                
                # Filter: Demography/Health only
                if any(x in title.lower() for x in ['mortality', 'death', 'population', 'health', 'life', 'fertility']):
                    self.add_result(title, self.normalize_date(d_str), "EU", "Eurostat", "https://ec.europa.eu/eurostat/news/release-calendar")
        except Exception as e:
            logging.error(f"Eurostat failed: {e}")

    # --- 2. ONS (UK) - UPCOMING VIEW ---
    def scrape_ons(self):
        try:
            logging.info("🇬🇧 ONS: Fetching Upcoming Releases...")
            # This URL forces the 'upcoming' list
            url = "https://www.ons.gov.uk/releasecalendar?view=upcoming&size=50"
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for item in soup.select('.release__item'):
                title_el = item.select_one('h3')
                date_el = item.select_one('.release__date')
                
                if title_el and date_el:
                    title = title_el.get_text(strip=True)
                    date_text = date_el.get_text(strip=True).replace("Release date:", "").strip()
                    
                    if any(x in title.lower() for x in ['death', 'birth', 'census', 'population', 'migration']):
                        link = "https://www.ons.gov.uk" + item.select_one('a')['href']
                        self.add_result(title, self.normalize_date(date_text), "UK", "ONS", link)
        except Exception as e:
            logging.error(f"ONS failed: {e}")

    # --- 3. INSEE (FRANCE) ---
    def scrape_insee(self):
        try:
            logging.info("🇫🇷 INSEE: Fetching Calendar...")
            url = "https://www.insee.fr/en/information/2107811"
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # INSEE lists are complex, we look for 'li' containing dates
            for li in soup.select('li'):
                text = li.get_text(" ", strip=True)
                # Regex to find DD/MM/YYYY
                match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                if match and any(x in text.lower() for x in ['birth', 'death', 'population']):
                    title = text.replace(match.group(0), "").strip()
                    self.add_result(title, self.normalize_date(match.group(0)), "France", "INSEE", url)
        except Exception as e:
            logging.error(f"INSEE failed: {e}")

    # --- 4. STATICE (ICELAND) ---
    def scrape_statice(self):
        try:
            logging.info("🇮🇸 Statice: Fetching Table...")
            url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for row in soup.select("tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    date_txt = cols[0].get_text(strip=True)
                    title = cols[1].get_text(strip=True)
                    
                    if any(x in title.lower() for x in ['population', 'death', 'migration']):
                        self.add_result(title, self.normalize_date(date_txt), "Iceland", "Statice", url)
        except Exception as e:
            logging.error(f"Statice failed: {e}")

    # --- 5. FINDATA (RSS) ---
    def scrape_findata(self):
        try:
            logging.info("🇫🇮 FinData: Fetching News...")
            feed = feedparser.parse("https://findata.fi/en/feed/")
            for entry in feed.entries:
                self.add_result(entry.title, self.normalize_date(entry.published), "Finland", "FinData", entry.link, status="🔵 NEWS")
        except: pass

    def run(self):
        print("🚀 Starting Precision Scraper...")
        self.scrape_eurostat()
        self.scrape_ons()
        self.scrape_insee()
        self.scrape_statice()
        self.scrape_findata()
        
        # Save
        if self.results:
            df = pd.DataFrame(self.results)
            # Sort: Soonest first
            df = df.sort_values(by='start')
            df.to_json(DATA_FILE, orient="records", indent=4)
            print(f"✅ Saved {len(df)} High-Quality Future Datasets.")
        else:
            print("⚠️ No future data found.")

if __name__ == "__main__":
    PrecisionScraper().run()
