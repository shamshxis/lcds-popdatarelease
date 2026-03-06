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
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json")
os.makedirs(DATA_DIR, exist_ok=True)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class LCDSFilter:
    """
    Filters datasets based on Leverhulme Centre for Demographic Science (LCDS) research areas.
    """
    def __init__(self):
        # High Priority Keywords (LCDS Core)
        self.CORE_THEMES = [
            "mortality", "death", "life expectancy", "suicide", "excess deaths", # Mortality
            "fertility", "birth", "conception", "maternity", "natal", # Fertility
            "migration", "asylum", "refugee", "border", "population", "census", "demograph", # Migration/Pop
            "household", "family", "marriage", "divorce", # Family
            "health", "disease", "covid", "pandemic", "vaccin", "hospital", # Biosocial/Health
            "inequality", "poverty", "deprivation", "social mobility", "gender", # Inequality
            "climate", "environment", "emission", "pollution", # Environmental Demography
            "digital", "internet access", "broadband" # Digital Demography
        ]
        
        # Lower Priority / Economic Noise (to be filtered out or ranked low)
        self.NOISE_THEMES = [
            "industrial production", "construction output", "retail sales", 
            "producer price", "business sentiment", "tourism", "transport", "agriculture"
        ]

    def classify(self, title):
        t = title.lower()
        
        # Check for Noise First
        if any(x in t for x in self.NOISE_THEMES):
            return "Economy (Low Priority)"
            
        # Check Core Themes
        if any(x in t for x in ["mortal", "death", "suicide", "life expect"]): return "Mortality"
        if any(x in t for x in ["birth", "fertil", "baby"]): return "Fertility"
        if any(x in t for x in ["migra", "asylum", "visa"]): return "Migration"
        if any(x in t for x in ["pop", "census", "resident", "age"]): return "Population"
        if any(x in t for x in ["health", "medic", "cancer", "covid"]): return "Health"
        if any(x in t for x in ["household", "family", "gender"]): return "Family/Gender"
        if any(x in t for x in ["inequal", "poverty", "wage", "earn", "income"]): return "Inequality"
        if any(x in t for x in ["climate", "environment"]): return "Environment"
        
        # Default fallback
        if "gdp" in t or "cpi" in t or "inflat" in t: return "Economy"
        
        return "General Stats"

    def is_relevant(self, title):
        """Returns True if the dataset aligns with LCDS themes."""
        topic = self.classify(title)
        return topic != "Economy (Low Priority)"

class ScraperEngine:
    def __init__(self):
        self.filter = LCDSFilter()
        self.memory = {}
        
        # STEALTH HEADERS (Rotates to avoid blocks)
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
        ]

    def get_session(self):
        s = requests.Session()
        s.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.google.com/'
        })
        return s

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except: return None

    # --- SCRAPERS ---

    def scrape_ons(self):
        """🇬🇧 ONS - Uses deep pagination and stealth headers"""
        events = []
        base_url = "https://www.ons.gov.uk/releasecalendar"
        session = self.get_session()
        
        for page in range(1, 4): # Get next ~3 months
            try:
                resp = session.get(f"{base_url}?page={page}", timeout=10)
                if resp.status_code == 403:
                    logging.warning(f"ONS 403 Blocked on page {page}")
                    continue
                    
                soup = BeautifulSoup(resp.content, 'html.parser')
                items = soup.select('.release__item')
                if not items: items = soup.select('li.list__item')

                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    
                    if title_elem and date_elem:
                        title = title_elem.text.strip()
                        # Clean Title (Remove "Release date:")
                        date_txt = date_elem.text.replace("Release date:", "").strip()
                        date_str = self.normalize_date(date_txt)
                        
                        if date_str and self.filter.is_relevant(title):
                            events.append({
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "source": "ONS",
                                "url": "https://www.ons.gov.uk" + title_elem['href'],
                                "topic": self.filter.classify(title)
                            })
                time.sleep(1) # Polite delay
            except Exception as e:
                logging.error(f"ONS Error: {e}")
        return events

    def scrape_eurostat(self):
        """🇪🇺 Eurostat - Parsed via table structure"""
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        session = self.get_session()
        events = []
        
        try:
            resp = session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Eurostat uses standard tables. We look for rows.
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                # Header Check (Date)
                header = row.find(['th', 'td'])
                if header and re.search(r'\d{2}-\d{2}-\d{4}', header.text):
                    current_date = self.normalize_date(header.text)
                
                # Data Check
                cols = row.find_all('td')
                if current_date and len(cols) >= 1:
                    title = cols[-1].text.strip()
                    
                    if title and self.filter.is_relevant(title):
                        # Detect Country if listed
                        country = "EU"
                        for c in ["Germany", "France", "Spain", "Italy", "Poland"]:
                            if c in title: country = c
                        
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": country,
                            "source": "Eurostat",
                            "url": url,
                            "topic": self.filter.classify(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat Error: {e}")
            return []

    def scrape_destatis(self):
        """🇩🇪 Destatis - Weekly Preview (Reliable Source)"""
        url = "https://www.destatis.de/EN/Press/Dates/Weekly-Preview/preview.html"
        session = self.get_session()
        events = []
        
        try:
            resp = session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Look for the data table
            table = soup.find('table')
            if not table: return []
            
            rows = table.find_all('tr')[1:] # Skip header
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:
                    # Col 2 is Title, Last Col is Date
                    title = cols[2].text.strip()
                    date_txt = cols[-1].text.strip()
                    
                    date_str = self.normalize_date(date_txt)
                    
                    if date_str and self.filter.is_relevant(title):
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "Germany",
                            "source": "Destatis",
                            "url": "https://www.destatis.de/EN/Press/Dates/Weekly-Preview/_node.html",
                            "topic": self.filter.classify(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Destatis Error: {e}")
            return []

    def scrape_cdc(self):
        """🇺🇸 CDC - Vital Stats"""
        url = "https://www.cdc.gov/nchs/nvss/new_nvss.htm"
        session = self.get_session()
        events = []
        try:
            resp = session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for item in soup.find_all('li'):
                text = item.get_text().strip()
                # Find dates like (January 2026)
                match = re.search(r'\((January|February|March|April|May|June|July|August|September|October|November|December) \d{4}\)', text)
                if match:
                    date_str = self.normalize_date(match.group(1))
                    title = re.sub(r'\(.*?\)', '', text).strip()
                    
                    if date_str and self.filter.is_relevant(title):
                         events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "CDC",
                            "url": url,
                            "topic": "Mortality/Health"
                        })
            return events
        except: return []

    def scrape_statcan(self):
        """🇨🇦 StatCan - Official Release Schedule"""
        url = "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm"
        session = self.get_session()
        events = []
        try:
            resp = session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            main = soup.find('main') or soup
            
            current_date = None
            year = datetime.now().year
            
            for tag in main.find_all(['h2', 'h3', 'li']):
                txt = tag.get_text().strip()
                if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}$', txt):
                    current_date = self.normalize_date(f"{txt} {year}")
                elif current_date and tag.name == 'li':
                    title = txt
                    link = tag.find('a')
                    url_link = "https://www150.statcan.gc.ca" + link['href'] if link else url
                    
                    if self.filter.is_relevant(title):
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "Canada",
                            "source": "StatCan",
                            "url": url_link,
                            "topic": self.filter.classify(title)
                        })
            return events
        except: return []

    def run(self):
        print("🚀 Starting LCDS-Focused Scraper...")
        
        tasks = {
            "ONS (UK)": self.scrape_ons,
            "Eurostat (EU)": self.scrape_eurostat,
            "Destatis (Germany)": self.scrape_destatis,
            "CDC (USA)": self.scrape_cdc,
            "StatCan (Canada)": self.scrape_statcan
        }
        
        all_data = []
        health_report = {}
        scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_source = {executor.submit(func): source for source, func in tasks.items()}
            
            for future in concurrent.futures.as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    data = future.result()
                    if data:
                        print(f"✅ {source}: Found {len(data)} items.")
                        health_report[source] = {"status": "ok", "count": len(data), "last_run": scrape_time}
                        all_data.extend(data)
                    else:
                        print(f"⚠️ {source}: Found 0 items.")
                        health_report[source] = {"status": "warning", "error": "Zero items found", "last_run": scrape_time}
                except Exception as e:
                    print(f"❌ {source} Failed: {e}")
                    health_report[source] = {"status": "error", "error": str(e), "last_run": scrape_time}

        # Save Health Report
        with open(HEALTH_FILE, 'w') as f: json.dump(health_report, f, indent=4)

        # Save Data
        if all_data:
            # Add Timestamp
            for item in all_data: item['scraped_at'] = scrape_time
            
            # Deduplicate
            unique = {f"{x['start']}_{x['title']}": x for x in all_data}.values()
            final_list = list(unique)
            
            # Save
            with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
            print(f"💾 Saved {len(final_list)} datasets to {JSON_FILE}")
        else:
            print("⚠️ No data collected. Check internet connection or layout changes.")

if __name__ == "__main__":
    engine = ScraperEngine()
    engine.run()
