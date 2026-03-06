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
import random

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json")

# Temp files for atomic writes
JSON_TEMP = os.path.join(DATA_DIR, "releases.tmp.json")
CSV_TEMP = os.path.join(DATA_DIR, "releases.tmp.csv")

os.makedirs(DATA_DIR, exist_ok=True)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScraperEngine:
    def __init__(self):
        self.session = requests.Session()
        # STEALTH HEADERS: Crucial for ONS and Destatis to avoid "403 Forbidden"
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/'
        })
        self.memory = self.load_memory()

    def load_memory(self):
        if os.path.exists(HEALTH_FILE):
            try:
                with open(HEALTH_FILE, 'r') as f: return json.load(f)
            except: return {}
        return {}

    def save_memory(self):
        with open(HEALTH_FILE, 'w') as f: json.dump(self.memory, f, indent=4)

    def clean_text(self, text):
        return " ".join(text.split()) if text else ""

    def normalize_date(self, date_str):
        try:
            # Handles "12 March 2026", "2026-03-12", "March 12"
            dt = date_parser.parse(date_str, fuzzy=True)
            # If date is in past (e.g. "March 12" interpreted as last year), fix year if needed
            # For this context, we assume dates without year are FUTURE if closer to now
            return dt.strftime("%Y-%m-%d")
        except: return None

    def infer_topic(self, title):
        t = title.lower()
        # PRIORITY 1: VITAL STATISTICS (The "Pop" Data)
        if any(x in t for x in ['mortal', 'death', 'life expect', 'suicide', 'homicide']): return "Mortality"
        if any(x in t for x in ['birth', 'fertil', 'natal', 'baby', 'conception']): return "Births"
        if any(x in t for x in ['migra', 'immigration', 'emigration', 'asylum', 'visa', 'border']): return "Migration"
        if any(x in t for x in ['pop', 'census', 'demog', 'household', 'family', 'resident']): return "Population"
        
        # PRIORITY 2: RELATED
        if any(x in t for x in ['health', 'disease', 'hospital', 'cancer']): return "Health"
        if any(x in t for x in ['employ', 'labor', 'work', 'job', 'wage', 'pay', 'vacanc']): return "Labor"
        if any(x in t for x in ['gdp', 'econ', 'trade', 'financ', 'cpi', 'price', 'inflat']): return "Economy"
        
        return "General Stats"

    # --- SCRAPER MODULES ---

    def scrape_ons_uk(self):
        """🇬🇧 ONS (UK) - Iterates Pages 1-5 for Vital Stats"""
        events = []
        base_url = "https://www.ons.gov.uk/releasecalendar"
        
        for page in range(1, 6): # Deep scrape (approx 3-5 months out)
            try:
                url = f"{base_url}?page={page}"
                resp = self.session.get(url, timeout=10)
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # ONS List Items
                items = soup.select('.release__item')
                if not items: items = soup.select('li.list__item')

                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    
                    if title_elem and date_elem:
                        title = self.clean_text(title_elem.text)
                        # Text: "Release date: 18 March 2026"
                        date_raw = self.clean_text(date_elem.text).replace("Release date:", "")
                        date_str = self.normalize_date(date_raw)
                        
                        topic = self.infer_topic(title)
                        
                        if date_str:
                            events.append({
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "source": "ONS",
                                "url": "https://www.ons.gov.uk" + title_elem['href'],
                                "topic": topic,
                                "summary": f"Official ONS Release ({topic})"
                            })
                time.sleep(0.5) # Be polite to ONS server
            except Exception as e:
                logging.warning(f"ONS Page {page} Error: {e}")
        return events

    def scrape_destatis_germany(self):
        """🇩🇪 Destatis (Germany) - Press Release Calendar"""
        url = "https://www.destatis.de/EN/Press/Calendar/calendar.html"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            # They use a table structure
            rows = soup.find_all('tr')
            
            current_date = None
            for row in rows:
                # Header often contains the date
                th = row.find('th')
                if th:
                    txt = self.clean_text(th.text)
                    if re.search(r'\d', txt): # Has number
                        current_date = self.normalize_date(txt)
                
                # Cells contain the release
                td = row.find('td')
                if td and current_date:
                    title = self.clean_text(td.text)
                    if title:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "Germany",
                            "source": "Destatis",
                            "url": url,
                            "topic": self.infer_topic(title),
                            "summary": "Federal Statistical Office of Germany"
                        })
            return events
        except Exception as e:
            logging.error(f"Destatis Error: {e}")
            return []

    def scrape_abs_australia(self):
        """🇦🇺 ABS (Australia) - Future Release Calendar"""
        url = "https://www.abs.gov.au/release-calendar/future-releases"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # ABS lists dates in headers or cards
            # We look for the general layout
            main = soup.find('main') or soup
            
            # Simple heuristic: Look for blocks with Date + Title
            # This varies, but usually date is in a <time> or <h3>
            items = main.find_all(['div', 'li'], class_=re.compile(r'card|item|row'))
            
            for item in items:
                date_tag = item.find('time') or item.find(['h3', 'h4'])
                link_tag = item.find('a')
                
                if date_tag and link_tag:
                    date_str = self.normalize_date(self.clean_text(date_tag.text))
                    title = self.clean_text(link_tag.text)
                    
                    if date_str and title:
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "Australia",
                            "source": "ABS",
                            "url": "https://www.abs.gov.au" + link_tag['href'],
                            "topic": self.infer_topic(title),
                            "summary": "Australian Bureau of Statistics"
                        })
            return events
        except Exception as e:
            logging.error(f"ABS Error: {e}")
            return []

    def scrape_cdc_vital(self):
        """🇺🇸 CDC (USA) - Vital Statistics Rapid Release"""
        url = "https://www.cdc.gov/nchs/nvss/new_nvss.htm"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # CDC lists are often just text with dates in parenthesis
            # We scan list items
            items = soup.find_all('li')
            for item in items:
                text = self.clean_text(item.get_text())
                # Look for future dates: e.g. "Q4 2026 (December 2026)"
                # Regex for Month Year
                match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December) \d{4}', text)
                
                if match:
                    date_str = self.normalize_date(match.group(0))
                    title = re.split(r'\(|-', text)[0].strip() # Clean title
                    
                    if date_str and len(title) > 10 and "Release" not in title:
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "CDC",
                            "url": url,
                            "topic": self.infer_topic(title), # Usually Mortality/Births
                            "summary": "CDC Vital Statistics"
                        })
            return events
        except Exception as e:
            logging.error(f"CDC Error: {e}")
            return []

    def scrape_eurostat(self):
        """🇪🇺 Eurostat (EU) - General Calendar"""
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
                    
                    # Extract specific country if mentioned
                    country = "EU (Eurostat)"
                    for c in ["Germany", "France", "Spain", "Italy", "Poland", "Netherlands"]:
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
            logging.error(f"Eurostat Error: {e}")
            return []

    def scrape_statcan(self):
        """🇨🇦 StatCan (Canada) - The Daily"""
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
        except: return []

    def run(self):
        print("🚀 Starting Global Scraper Engine...")
        
        # Define the Task List
        tasks = {
            "ONS (UK)": self.scrape_ons_uk,
            "Destatis (Germany)": self.scrape_destatis_germany,
            "ABS (Australia)": self.scrape_abs_australia,
            "CDC (USA)": self.scrape_cdc_vital,
            "Eurostat (EU)": self.scrape_eurostat,
            "StatCan (Canada)": self.scrape_statcan
        }
        
        all_data = []
        scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # EXECUTE IN PARALLEL (Max 5 workers)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_source = {executor.submit(func): source for source, func in tasks.items()}
            
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    data = future.result()
                    count = len(data)
                    
                    if count > 0:
                        print(f"✅ {source}: Found {count} items.")
                        self.memory[source] = {"status": "ok", "last_run": scrape_time, "count": count}
                        all_data.extend(data)
                    else:
                        print(f"⚠️ {source}: Found 0 items (Check selector or site layout)")
                        self.memory[source] = {"status": "warning", "last_run": scrape_time, "error": "Zero items found"}
                        
                except Exception as exc:
                    print(f"❌ {source} Failed: {exc}")
                    self.memory[source] = {"status": "error", "last_run": scrape_time, "error": str(exc)}

        self.save_memory()

        # Add timestamps
        for item in all_data: item['scraped_at'] = scrape_time

        # PRIORITY SORTING
        # 0 = Critical Pop Data, 1 = Health, 2 = Labor, 3 = Economy
        rank_map = {
            "Mortality": 0, "Births": 0, "Migration": 0, "Population": 0,
            "Health": 1, "Labor": 2, "Economy": 3, "General Stats": 4
        }
        
        # Deduplicate
        unique_map = {f"{x['start']}_{x['title']}": x for x in all_data}
        final_list = list(unique_map.values())
        
        # Sort
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
