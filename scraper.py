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
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'}
        self.today = datetime.now()
        self.results = []

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True)
        except: return None

    def add_result(self, title, date_obj, country, source, url):
        """
        STRICT FILTER: Only add if Future OR Recent Past (last 7 days).
        This eliminates 'trash' from 2011.
        """
        if not date_obj: return
        
        diff = (date_obj - self.today).days
        
        # LOGIC: Future (diff >= 0) OR Recent Release (diff > -7)
        if diff >= -7:
            status = "🟢 CONFIRMED" if diff >= 0 else "🔴 RELEASED"
            self.results.append({
                "title": title.strip(),
                "start": date_obj.strftime("%Y-%m-%d"),
                "country": country,
                "source": source,
                "url": url,
                "status": status,
                "days_diff": diff
            })

    # --- 1. ONS (UK) - DEATHS REGISTERED WEEKLY ---
    def scrape_ons(self):
        try:
            # We target the 'upcoming' view directly
            url = "https://www.ons.gov.uk/releasecalendar?view=upcoming&size=50"
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for item in soup.select('.release__item'):
                title_el = item.select_one('h3')
                date_el = item.select_one('.release__date')
                
                if title_el and date_el:
                    title = title_el.get_text(strip=True)
                    # TARGET FILTER: Only pick up Vital Stats
                    if any(x in title.lower() for x in ['death', 'birth', 'conception', 'life expectancy']):
                        date_text = date_el.get_text(strip=True).replace("Release date:", "").strip()
                        link = "https://www.ons.gov.uk" + item.select_one('a')['href']
                        self.add_result(title, self.normalize_date(date_text), "UK", "ONS", link)
        except Exception as e:
            logging.error(f"ONS failed: {e}")

    # --- 2. EUROSTAT - EXCESS MORTALITY & POPULATION ---
    def scrape_eurostat(self):
        try:
            # XML Calendar is the most reliable source for Eurostat
            url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'xml')
            
            for item in soup.find_all('release'):
                title = item.find('title').text
                d_str = item.find('release_date').text
                
                # TARGET FILTER: High Value Datasets Only
                if any(x in title.lower() for x in ['excess mortality', 'population', 'fertility', 'life expectancy']):
                    self.add_result(title, self.normalize_date(d_str), "EU", "Eurostat", "https://ec.europa.eu/eurostat/news/release-calendar")
        except Exception as e:
            logging.error(f"Eurostat failed: {e}")

    # --- 3. INSEE (FRANCE) - DEMOGRAPHY ---
    def scrape_insee(self):
        try:
            url = "https://www.insee.fr/en/information/2107811"
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # INSEE lists dates in 'li' tags, often mixed with text
            for li in soup.select('#consulter li'):
                text = li.get_text(" ", strip=True)
                # Regex to extract "dd/mm/yyyy"
                match = re.search(r'(\d{2}/\d{2}/\d{4})', text)
                
                if match:
                    # TARGET FILTER
                    if any(x in text.lower() for x in ['birth', 'death', 'mortality', 'population']):
                        date_obj = self.normalize_date(match.group(0))
                        title = text.replace(match.group(0), "").strip("- ")
                        self.add_result(title, date_obj, "France", "INSEE", url)
        except Exception as e:
            logging.error(f"INSEE failed: {e}")

    # --- 4. STATICE (ICELAND) - POPULATION ---
    def scrape_statice(self):
        try:
            url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for row in soup.select("table tr"):
                cols = row.find_all("td")
                if len(cols) >= 2:
                    date_txt = cols[0].get_text(strip=True)
                    title = cols[1].get_text(strip=True)
                    
                    # TARGET FILTER
                    if any(x in title.lower() for x in ['population', 'death', 'migration', 'census']):
                        self.add_result(title, self.normalize_date(date_txt), "Iceland", "Statice", url)
        except Exception as e:
            logging.error(f"Statice failed: {e}")

    # --- 5. FINDATA (FINLAND) - NEWS ---
    def scrape_findata(self):
        try:
            # FinData is a permit authority, they announce updates via News RSS
            feed = feedparser.parse("https://findata.fi/en/feed/")
            for entry in feed.entries:
                self.add_result(entry.title, self.normalize_date(entry.published), "Finland", "FinData", entry.link)
        except: pass

    def run(self):
        print("🚀 Starting Precision Scraper...")
        self.scrape_ons()
        self.scrape_eurostat()
        self.scrape_insee()
        self.scrape_statice()
        self.scrape_findata()
        
        # Save & Sort
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values(by='start')
            df.to_json(DATA_FILE, orient="records", indent=4)
            print(f"✅ Saved {len(df)} High-Quality Datasets.")
        else:
            print("⚠️ No relevant data found.")

if __name__ == "__main__":
    PrecisionScraper().run()
