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

# --- Configuration ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")
JSON_TEMP = os.path.join(DATA_DIR, "releases.tmp.json")
CSV_TEMP = os.path.join(DATA_DIR, "releases.tmp.csv")

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        })

    def clean_text(self, text):
        if not text: return ""
        return " ".join(text.split())

    def infer_topic(self, title):
        t = title.lower()
        # PRIORITY 1: POPULATION / DEMOGRAPHY
        if any(x in t for x in ['mortal', 'death', 'life expect', 'suicide', 'homicide', 'cause of death']): return "Mortality"
        if any(x in t for x in ['birth', 'fertil', 'natal', 'baby', 'conception']): return "Births"
        if any(x in t for x in ['migra', 'immigration', 'emigration', 'asylum', 'visa', 'passenger', 'border']): return "Migration"
        if any(x in t for x in ['pop', 'census', 'demog', 'household', 'family', 'ageing', 'resident']): return "Population"
        if any(x in t for x in ['health', 'disease', 'hospital', 'cancer', 'medic', 'vaccin']): return "Health"
        
        # PRIORITY 2: LABOR / SOCIAL
        if any(x in t for x in ['employ', 'labor', 'work', 'job', 'wage', 'pay', 'vacanc', 'unemploy', 'earn']): return "Labor Market"
        if any(x in t for x in ['crime', 'justice', 'prison', 'police']): return "Crime"
        if any(x in t for x in ['educ', 'school', 'student', 'univers']): return "Education"
        
        # PRIORITY 3: ECONOMY (To be filtered out if needed)
        if any(x in t for x in ['gdp', 'econ', 'trade', 'financ', 'cpi', 'ppi', 'price', 'inflat', 'retail', 'money', 'business', 'output', 'construct', 'sales']): return "Economy"
        
        return "Other Stats"

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except:
            return None

    def scrape(self):
        raise NotImplementedError("Subclasses must implement scrape()")

# --- 1. UK ONS (Specific Focus) ---
class ONS_Scraper(BaseScraper):
    def scrape(self):
        # We scrape the main calendar but rely on our `infer_topic` to categorize strictly
        base_url = "https://www.ons.gov.uk/releasecalendar"
        events = []
        # Scrape 5 pages to capture enough future data
        for page in range(1, 6):
            try:
                resp = self.session.get(f"{base_url}?page={page}", timeout=10)
                soup = BeautifulSoup(resp.content, 'html.parser')
                items = soup.select('.release__item')
                if not items: items = soup.select('li.list__item')
                
                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    if title_elem and date_elem:
                        title = self.clean_text(title_elem.text)
                        date_str = self.normalize_date(self.clean_text(date_elem.text).replace("Release date:", ""))
                        
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
                logging.error(f"ONS Page {page} Error: {e}")
        return events

# --- 2. US CDC (Mortality Focus) ---
class CDC_Mortality_Scraper(BaseScraper):
    def scrape(self):
        # Target: NVSS Vital Statistics Rapid Release
        url = "https://www.cdc.gov/nchs/nvss/deaths.htm"
        events = []
        try:
            logging.info("🏥 Scraping CDC Vital Stats...")
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Look for "Upcoming Releases" or "Quarterly Provisional Estimates"
            # CDC pages are messy, so we look for dates in text
            text_blocks = soup.get_text("\n").split("\n")
            
            for line in text_blocks:
                if "release" in line.lower() or "scheduled" in line.lower():
                    # Find dates like "May 2026" or "Q1 2026"
                    date_match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December) \d{4}', line)
                    if date_match:
                        date_str = self.normalize_date(date_match.group(0))
                        title = line.strip()
                        if len(title) > 10 and len(title) < 100 and date_str:
                             events.append({
                                "title": title,
                                "start": date_str,
                                "country": "USA",
                                "source": "CDC",
                                "url": url,
                                "topic": "Mortality",
                                "summary": "CDC Vital Statistics Rapid Release"
                            })
            return events
        except Exception as e:
            logging.error(f"CDC Error: {e}")
            return []

# --- 3. Eurostat (Filtered for Pop) ---
class Eurostat_Scraper(BaseScraper):
    def scrape(self):
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
                    
                    # Extract country if present
                    country = "EU (Eurostat)"
                    for c in ["Germany", "France", "Spain", "Italy", "Poland", "Netherlands"]:
                        if c in title: country = c
                    
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
        except: return []

# --- 4. US Census (Demography Core) ---
class US_Census_Scraper(BaseScraper):
    def scrape(self):
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            content = soup.get_text("\n").split("\n")
            for line in content:
                line = self.clean_text(line)
                match = re.search(r'(\d{1,2}/\d{1,2}/\d{2,4})|([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                if match:
                    date_str = self.normalize_date(match.group(0))
                    title = line.replace(match.group(0), "").strip(" -:")
                    if date_str and len(title) > 10:
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "US Census",
                            "url": url,
                            "topic": self.infer_topic(title),
                            "summary": line
                        })
            return events
        except: return []

# --- EXECUTION ---
def run_scrapers():
    scrapers = [ONS_Scraper(), CDC_Mortality_Scraper(), Eurostat_Scraper(), US_Census_Scraper()]
    all_data = []
    scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("🚀 Starting Scrape...")
    for scraper in scrapers:
        try:
            data = scraper.scrape()
            print(f"✅ {scraper.__class__.__name__}: {len(data)} items")
            for item in data: item['scraped_at'] = scrape_time
            all_data.extend(data)
        except Exception as e:
            print(f"❌ {scraper.__class__.__name__} Failed: {e}")

    # Deduplicate & Filter
    unique = {f"{x['start']}_{x['title']}": x for x in all_data}.values()
    
    # Save
    with open(JSON_TEMP, 'w') as f: json.dump(list(unique), f, indent=4)
    pd.DataFrame(list(unique)).to_csv(CSV_TEMP, index=False)
    os.replace(JSON_TEMP, JSON_FILE)
    os.replace(CSV_TEMP, CSV_FILE)
    print("💾 Data Saved.")

if __name__ == "__main__":
    run_scrapers()
