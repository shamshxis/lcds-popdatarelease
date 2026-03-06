import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import logging
import re
import pandas as pd
from dateutil import parser as date_parser
import concurrent.futures
import time

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json") # Memory file

# Temporary files for atomic writes
JSON_TEMP = os.path.join(DATA_DIR, "releases.tmp.json")
CSV_TEMP = os.path.join(DATA_DIR, "releases.tmp.csv")

os.makedirs(DATA_DIR, exist_ok=True)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScraperEngine:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        })
        self.memory = self.load_memory()

    def load_memory(self):
        if os.path.exists(HEALTH_FILE):
            with open(HEALTH_FILE, 'r') as f: return json.load(f)
        return {}

    def save_memory(self):
        with open(HEALTH_FILE, 'w') as f: json.dump(self.memory, f, indent=4)

    def clean_text(self, text):
        return " ".join(text.split()) if text else ""

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except: return None

    def infer_topic(self, title):
        t = title.lower()
        # PRIORITY 1: CORE DEMOGRAPHY
        if any(x in t for x in ['mortal', 'death', 'life expect', 'suicide']): return "Mortality"
        if any(x in t for x in ['birth', 'fertil', 'natal', 'baby']): return "Births"
        if any(x in t for x in ['migra', 'immigration', 'asylum', 'border']): return "Migration"
        if any(x in t for x in ['pop', 'census', 'demog', 'household']): return "Population"
        
        # PRIORITY 2: LINKED SECTORS
        if any(x in t for x in ['health', 'disease', 'hospital']): return "Health"
        if any(x in t for x in ['employ', 'labor', 'job', 'wage', 'earn']): return "Labor"
        if any(x in t for x in ['gdp', 'econ', 'trade', 'cpi', 'price', 'inflat']): return "Economy"
        
        return "General Stats"

    # --- INDIVIDUAL SCRAPERS ---

    def scrape_ons(self):
        """Scrapes UK ONS - Pages 1 to 5 to get future dates"""
        events = []
        base_url = "https://www.ons.gov.uk/releasecalendar"
        
        for page in range(1, 6): # Scrape deeper (5 pages)
            try:
                resp = self.session.get(f"{base_url}?page={page}", timeout=10)
                soup = BeautifulSoup(resp.content, 'html.parser')
                items = soup.select('.release__item')
                if not items: items = soup.select('li.list__item') # Fallback

                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    
                    if title_elem and date_elem:
                        title = self.clean_text(title_elem.text)
                        date_raw = self.clean_text(date_elem.text).replace("Release date:", "")
                        date_str = self.normalize_date(date_raw)
                        
                        if date_str:
                            events.append({
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "source": "ONS",
                                "url": "https://www.ons.gov.uk" + title_elem['href'],
                                "topic": self.infer_topic(title),
                                "summary": f"Official ONS Release: {title}"
                            })
            except Exception as e:
                logging.warning(f"ONS Page {page} warning: {e}")
        return events

    def scrape_eurostat(self):
        """Scrapes Eurostat General Calendar"""
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                header = row.find(['th', 'td'])
                if header and re.search(r'\d{2}-\d{2}-\d{4}', header.text):
                    current_date = self.normalize_date(header.text)
                
                cols = row.find_all('td')
                if current_date and len(cols) >= 1:
                    title = self.clean_text(cols[-1].text)
                    topic = self.infer_topic(title)
                    
                    country = "EU (Eurostat)"
                    for c in ["Germany", "France", "Spain", "Italy", "Poland"]:
                        if c in title: country = c

                    if title:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": country,
                            "source": "Eurostat",
                            "url": url,
                            "topic": topic,
                            "summary": "Eurostat Official Release"
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat failed: {e}")
            return []

    def scrape_cdc(self):
        """Scrapes CDC NVSS 'What's New' for Vital Stats"""
        url = "https://www.cdc.gov/nchs/nvss/new_nvss.htm"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            # Extract list items which often contain dates
            items = soup.find_all('li')
            
            for item in items:
                text = self.clean_text(item.get_text())
                # Look for dates like (12/19/2026) or (Jan 2026)
                date_match = re.search(r'\((\d{1,2}/\d{1,2}/\d{4})\)', text)
                
                if date_match:
                    date_str = self.normalize_date(date_match.group(1))
                    title = re.sub(r'\(.*?\)', '', text).strip() # Remove date from title
                    
                    if date_str and len(title) > 10 and "Release" not in title:
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "CDC",
                            "url": url,
                            "topic": self.infer_topic(title), # Will likely hit Mortality/Births
                            "summary": "CDC Vital Statistics Release"
                        })
            return events
        except Exception as e:
            logging.error(f"CDC failed: {e}")
            return []

    def scrape_statcan(self):
        """Scrapes StatCan 'The Daily' Schedule"""
        url = "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            main = soup.find('main') or soup
            
            current_date = None
            year = datetime.now().year
            
            for tag in main.find_all(['h2', 'h3', 'li']):
                txt = self.clean_text(tag.get_text())
                # Match "March 12"
                if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}$', txt):
                    current_date = self.normalize_date(f"{txt} {year}")
                
                elif current_date and tag.name == 'li':
                    title = txt
                    link = tag.find('a')
                    url_link = "https://www150.statcan.gc.ca" + link['href'] if link else url
                    
                    if len(title) > 5:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "Canada",
                            "source": "StatCan",
                            "url": url_link,
                            "topic": self.infer_topic(title),
                            "summary": "The Daily Release"
                        })
            return events
        except Exception as e:
            logging.error(f"StatCan failed: {e}")
            return []

    def run(self):
        print("🚀 Starting Parallel Scraper Engine...")
        
        # Define tasks
        tasks = {
            "ONS": self.scrape_ons,
            "Eurostat": self.scrape_eurostat,
            "CDC": self.scrape_cdc,
            "StatCan": self.scrape_statcan
        }
        
        all_data = []
        scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # THREADPOOL EXECUTION (Speed!)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_source = {executor.submit(func): source for source, func in tasks.items()}
            
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    data = future.result()
                    count = len(data)
                    
                    if count > 0:
                        print(f"✅ {source}: Found {count} items.")
                        # Update Memory: Success
                        self.memory[source] = {"status": "ok", "last_run": scrape_time}
                        all_data.extend(data)
                    else:
                        print(f"⚠️ {source}: Found 0 items (Check selector?)")
                        
                except Exception as exc:
                    print(f"❌ {source} Generated Exception: {exc}")
                    self.memory[source] = {"status": "error", "error": str(exc), "last_run": scrape_time}

        self.save_memory()

        # Add timestamp
        for item in all_data: item['scraped_at'] = scrape_time

        # PRIORITY SORTING (Demography First)
        # We assign a rank: Mortality/Births/Pop = 0, Health = 1, Labor = 2, Economy = 3
        rank_map = {
            "Mortality": 0, "Births": 0, "Migration": 0, "Population": 0,
            "Health": 1, 
            "Labor": 2, 
            "Economy": 3, "General Stats": 4
        }
        
        # Deduplication
        unique_map = {f"{x['start']}_{x['title']}": x for x in all_data}
        final_list = list(unique_map.values())
        
        # Sort by Rank (Topic) then Date
        final_list.sort(key=lambda x: (rank_map.get(x['topic'], 99), x['start']))
        
        print(f"📊 Total Unique Datasets: {len(final_list)}")

        # Atomic Save
        with open(JSON_TEMP, 'w') as f: json.dump(final_list, f, indent=4)
        pd.DataFrame(final_list).to_csv(CSV_TEMP, index=False)
        os.replace(JSON_TEMP, JSON_FILE)
        os.replace(CSV_TEMP, CSV_FILE)
        print("💾 Database Updated.")

if __name__ == "__main__":
    engine = ScraperEngine()
    engine.run()
