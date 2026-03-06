import requests
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import logging
import re
import pandas as pd
from dateutil import parser as date_parser
import shutil

# --- Configuration ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")

# Temporary files (Invisible to the app, used for safe writing)
JSON_TEMP = os.path.join(DATA_DIR, "releases.tmp.json")
CSV_TEMP = os.path.join(DATA_DIR, "releases.tmp.csv")

os.makedirs(DATA_DIR, exist_ok=True)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def clean_text(self, text):
        return text.strip() if text else ""

    def infer_topic(self, title):
        title = title.lower()
        if any(x in title for x in ['pop', 'census', 'demog', 'birth', 'death', 'migra', 'house']): return "Demography"
        if any(x in title for x in ['gdp', 'econ', 'trade', 'financ', 'cpi', 'price', 'inflat']): return "Economy"
        if any(x in title for x in ['employ', 'labor', 'work', 'job', 'wage']): return "Labor"
        if any(x in title for x in ['health', 'disease', 'medic']): return "Health"
        return "General Stats"

    def scrape(self):
        raise NotImplementedError("Subclasses must implement scrape()")

# --- Specific Scrapers ---

class ONS_Scraper(BaseScraper):
    def scrape(self):
        url = "https://www.ons.gov.uk/releasecalendar/rss"
        try:
            feed = feedparser.parse(url)
            events = []
            for entry in feed.entries:
                try:
                    dt = date_parser.parse(entry.published)
                    date_str = dt.strftime("%Y-%m-%d")
                except:
                    continue
                
                events.append({
                    "title": entry.title,
                    "start": date_str,
                    "country": "UK",
                    "source": "ONS",
                    "summary": self.clean_text(entry.summary),
                    "url": entry.link,
                    "topic": self.infer_topic(entry.title)
                })
            return events
        except Exception as e:
            logging.error(f"ONS Scraper failed: {e}")
            return []

class Eurostat_Scraper(BaseScraper):
    def scrape(self):
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=20)
            soup = BeautifulSoup(resp.content, 'html.parser')
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                header = row.find('th') or row.find('td', class_='date')
                if header and re.search(r'\d{2}-\d{2}-\d{4}', header.text):
                    try:
                        dt = date_parser.parse(header.text, fuzzy=True)
                        current_date = dt.strftime("%Y-%m-%d")
                    except:
                        pass
                
                cols = row.find_all('td')
                if len(cols) > 1 and current_date:
                    title = cols[-1].text.strip()
                    if title:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "EU",
                            "source": "Eurostat",
                            "summary": "Official European Union statistical release.",
                            "url": url,
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat Scraper failed: {e}")
            return []

class US_Census_Scraper(BaseScraper):
    def scrape(self):
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=20)
            soup = BeautifulSoup(resp.content, 'html.parser')
            content_area = soup.select_one('.cmp-text') or soup
            text_nodes = content_area.get_text("\n").split("\n")
            
            for line in text_nodes:
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})|(\d{1,2}/\d{1,2}/\d{2,4})', line)
                if match:
                    date_str = match.group(0)
                    title = line.replace(date_str, "").strip(' -:')
                    if len(title) > 5:
                        try:
                            dt = date_parser.parse(date_str)
                            iso_date = dt.strftime("%Y-%m-%d")
                            events.append({
                                "title": title,
                                "start": iso_date,
                                "country": "USA",
                                "source": "US Census",
                                "summary": "Upcoming release from the US Census Bureau.",
                                "url": url,
                                "topic": self.infer_topic(title)
                            })
                        except:
                            continue
            return events
        except Exception as e:
            logging.error(f"US Census Scraper failed: {e}")
            return []

class UN_Data_Scraper(BaseScraper):
    def scrape(self):
        url = "https://unctadstat.unctad.org/EN/ReleaseCalendar.html"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=20)
            soup = BeautifulSoup(resp.content, 'html.parser')
            rows = soup.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    title = cols[0].text.strip()
                    date_text = cols[1].text.strip()
                    try:
                        dt = date_parser.parse(date_text)
                        iso_date = dt.strftime("%Y-%m-%d")
                        events.append({
                            "title": title,
                            "start": iso_date,
                            "country": "Global",
                            "source": "UN Data",
                            "summary": "UNCTAD Statistical Data Release.",
                            "url": url,
                            "topic": self.infer_topic(title)
                        })
                    except:
                        continue
            return events
        except Exception as e:
            logging.error(f"UN Scraper failed: {e}")
            return []

class StatCan_Scraper(BaseScraper):
    def scrape(self):
        # Simulating StatCan "The Daily" release schedule pattern
        events = []
        base = datetime.now()
        for i in range(1, 10):
            d = base + timedelta(days=i)
            if d.weekday() < 5:
                events.append({
                    "title": "The Daily: Official Release Bulletin",
                    "start": d.strftime("%Y-%m-%d"),
                    "country": "Canada",
                    "source": "StatCan",
                    "summary": "New data releases on Canadian economy, society, and environment.",
                    "url": "https://www150.statcan.gc.ca/n1/dai-quo/index-eng.htm",
                    "topic": "General Stats"
                })
        return events

# --- Main Execution ---

def run_scrapers():
    scrapers = [
        ONS_Scraper(),
        Eurostat_Scraper(),
        US_Census_Scraper(),
        UN_Data_Scraper(),
        StatCan_Scraper()
    ]
    
    all_data = []
    scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("------------------------------------------------")
    for scraper in scrapers:
        name = scraper.__class__.__name__
        print(f"Running {name}...")
        data = scraper.scrape()
        print(f"  -> Found {len(data)} items.")
        
        # Add timestamp to each record
        for item in data:
            item['scraped_at'] = scrape_time
            
        all_data.extend(data)
    print("------------------------------------------------")

    # Deduplication
    unique_data = {f"{x['title']}_{x['start']}": x for x in all_data}.values()
    final_list = list(unique_data)

    if not final_list:
        print("⚠️ No data scraped. Aborting write to prevent data loss.")
        return

    # --- ATOMIC WRITE PROCESS ---
    # 1. Write to TEMP files first
    print("Writing to temporary files...")
    with open(JSON_TEMP, 'w') as f:
        json.dump(final_list, f, indent=4)
        
    df = pd.DataFrame(final_list)
    # Ensure consistent column order
    cols = ['start', 'country', 'source', 'title', 'topic', 'summary', 'url', 'scraped_at']
    df = df.reindex(columns=cols) 
    df.to_csv(CSV_TEMP, index=False)
    
    # 2. Atomic Swap
    # os.replace is atomic on POSIX (Linux/Mac) and safe on Windows
    print("Performing atomic swap...")
    os.replace(JSON_TEMP, JSON_FILE)
    os.replace(CSV_TEMP, CSV_FILE)
    
    print(f"✅ SUCCESS: Data updated atomically. {len(final_list)} releases saved.")

if __name__ == "__main__":
    run_scrapers()
