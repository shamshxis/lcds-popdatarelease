import pandas as pd
import requests
import time
import os
import logging
import random
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as date_parser

# --- CONFIG ---
DATA_FILE = "data/releases.json"
os.makedirs("data", exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class SlowScraper:
    def __init__(self):
        # 1. PERSISTENT SESSION (Like a real browser window)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://google.com'
        })
        self.today = datetime.now()
        self.results = []
        self.logs = [] # Store logs for the UI

    def log(self, msg):
        print(msg)
        self.logs.append(msg)

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True)
        except: return None

    def sleep(self):
        # 2. RANDOM DELAY (Human behavior)
        delay = random.uniform(2.0, 4.0)
        self.log(f"💤 Waiting {delay:.1f}s...")
        time.sleep(delay)

    def add_result(self, title, date_obj, country, source, url):
        if not date_obj: return
        
        diff = (date_obj - self.today).days
        
        # 3. STRICT FILTER: Future (>=0) or Very Recent (-7)
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

    # --- ONS (UK) ---
    def scrape_ons(self):
        self.log("🇬🇧 Connecting to ONS...")
        try:
            # We use the JSON API but with a specific header that mimics AJAX
            url = "https://www.ons.gov.uk/releasecalendar/data?view=upcoming&size=50"
            self.session.headers.update({'X-Requested-With': 'XMLHttpRequest'}) # Critical for ONS
            
            resp = self.session.get(url, timeout=15)
            self.log(f"   Status: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                items = data.get('result', {}).get('results', [])
                for item in items:
                    title = item.get('description', {}).get('title', '')
                    date_raw = item.get('description', {}).get('releaseDate', '')
                    
                    # FILTER: Demography Only
                    if any(k in title.lower() for k in ['death', 'birth', 'population', 'migration', 'census', 'life expect']):
                        link = "https://www.ons.gov.uk" + item.get('uri', '')
                        self.add_result(title, self.normalize_date(date_raw), "UK", "ONS", link)
        except Exception as e:
            self.log(f"❌ ONS Failed: {e}")
        self.sleep() # Wait before next site

    # --- EUROSTAT (EU) ---
    def scrape_eurostat(self):
        self.log("🇪🇺 Connecting to Eurostat...")
        try:
            url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = self.session.get(url, timeout=15)
            self.log(f"   Status: {resp.status_code}")
            
            soup = BeautifulSoup(resp.content, 'xml')
            for item in soup.find_all('release'):
                title = item.find('title').text
                d_str = item.find('release_date').text
                
                if any(k in title.lower() for k in ['mortality', 'population', 'fertility', 'health']):
                    self.add_result(title, self.normalize_date(d_str), "EU", "Eurostat", "https://ec.europa.eu/eurostat/news/release-calendar")
        except Exception as e:
            self.log(f"❌ Eurostat Failed: {e}")
        self.sleep()

    # --- STATICE (ICELAND) ---
    def scrape_statice(self):
        self.log("🇮🇸 Connecting to Statice...")
        try:
            url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
            resp = self.session.get(url, timeout=15)
            self.log(f"   Status: {resp.status_code}")
            
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Strict selector: Only the main calendar table
            rows = soup.select(".table-responsive table tr") 
            
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    d_txt = cols[0].get_text(strip=True)
                    title = cols[1].get_text(strip=True)
                    
                    if any(k in title.lower() for k in ['population', 'death', 'migration']):
                        self.add_result(title, self.normalize_date(d_txt), "Iceland", "Statice", url)
        except Exception as e:
            self.log(f"❌ Statice Failed: {e}")
        self.sleep()

    def run(self):
        self.log("🚀 Starting Slow & Steady Scraper...")
        self.scrape_ons()
        self.scrape_eurostat()
        self.scrape_statice()
        
        # Save Results
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values(by='start')
            df.to_json(DATA_FILE, orient="records", indent=4)
            self.log(f"✅ Saved {len(df)} High-Quality Records.")
        else:
            self.log("⚠️ No data found.")
        
        # Save Logs for UI
        with open("data/scraper.log", "w") as f:
            f.write("\n".join(self.logs))

if __name__ == "__main__":
    SlowScraper().run()
