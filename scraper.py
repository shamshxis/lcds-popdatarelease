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
import time

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
        if any(x in t for x in ['pop', 'census', 'demog', 'birth', 'death', 'migra', 'house', 'household']): return "Demography"
        if any(x in t for x in ['gdp', 'econ', 'trade', 'financ', 'cpi', 'price', 'inflat', 'retail']): return "Economy"
        if any(x in t for x in ['employ', 'labor', 'work', 'job', 'wage', 'pay', 'vacanc']): return "Labor"
        if any(x in t for x in ['health', 'disease', 'medic', 'life']): return "Health"
        return "General Stats"

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except:
            return None

    def scrape(self):
        raise NotImplementedError("Subclasses must implement scrape()")

# --- Specific Scrapers ---

class ONS_Scraper(BaseScraper):
    def scrape(self):
        # ONS Release Calendar - Iterating Pages for Future Releases
        base_url = "https://www.ons.gov.uk/releasecalendar"
        events = []
        
        # Scrape 5 pages to ensure we get ~2-3 months of future data
        for page in range(1, 6):
            url = f"{base_url}?page={page}"
            try:
                logging.info(f"Scraping ONS Page {page}...")
                resp = self.session.get(url, timeout=15)
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # ONS structure: List items with details
                # Look for 'release__item' or generally list items inside the container
                items = soup.select('.release__item')
                if not items: items = soup.select('li.list__item') # Fallback selector
                
                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    
                    if title_elem and date_elem:
                        title = self.clean_text(title_elem.text)
                        link = "https://www.ons.gov.uk" + title_elem['href']
                        
                        # Text: "Release date: 5 March 2026 9:30am"
                        date_text = self.clean_text(date_elem.text).replace("Release date:", "").strip()
                        date_str = self.normalize_date(date_text)
                        
                        if date_str:
                            events.append({
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "source": "ONS",
                                "summary": f"Official ONS Release: {title}",
                                "url": link,
                                "topic": self.infer_topic(title)
                            })
            except Exception as e:
                logging.error(f"ONS Page {page} Error: {e}")
        return events

class StatCan_Scraper(BaseScraper):
    def scrape(self):
        # "The Daily" Release Schedule (Official 2-week lookahead)
        url = "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm"
        events = []
        try:
            logging.info(f"Scraping StatCan Schedule: {url}")
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # They use h3 or similar headers for dates, followed by lists of releases
            # We need to find the main content area
            main_content = soup.find('main') or soup.find('div', {'role': 'main'}) or soup
            
            # Iterate through all elements to find Date Headers
            current_date = None
            current_year = datetime.now().year
            
            # Find all potential date headers (usually h3 or strong text)
            for element in main_content.find_all(['h2', 'h3', 'h4', 'li']):
                text = self.clean_text(element.get_text())
                
                # Check if this element is a Date Header (e.g., "March 2")
                # We assume current year for these dates
                if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}$', text):
                    try:
                        dt = date_parser.parse(f"{text} {current_year}")
                        current_date = dt.strftime("%Y-%m-%d")
                    except:
                        pass
                
                # If it's a list item and we have a date, it's a release
                elif current_date and element.name == 'li':
                    # Check if it has a link (usually the release title)
                    link_tag = element.find('a')
                    if link_tag:
                        title = self.clean_text(link_tag.text)
                        url_link = "https://www150.statcan.gc.ca" + link_tag['href']
                        
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "Canada",
                            "source": "StatCan",
                            "summary": "The Daily - Official Release",
                            "url": url_link,
                            "topic": self.infer_topic(title)
                        })
                    else:
                        # Sometimes text only
                        title = self.clean_text(element.text)
                        if len(title) > 10:
                             events.append({
                                "title": title,
                                "start": current_date,
                                "country": "Canada",
                                "source": "StatCan",
                                "summary": "The Daily - Official Release",
                                "url": "https://www150.statcan.gc.ca/n1/dai-quo/index-eng.htm",
                                "topic": self.infer_topic(title)
                            })
                            
            return events
        except Exception as e:
            logging.error(f"StatCan Error: {e}")
            return []

class US_Census_Scraper(BaseScraper):
    def scrape(self):
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            content = soup.get_text("\n")
            
            for line in content.split("\n"):
                line = self.clean_text(line)
                # Matches "2/26/2026" or "March 5, 2026"
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
                            "summary": line,
                            "url": url,
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"US Census Error: {e}")
            return []

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
                    if title:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "EU",
                            "source": "Eurostat",
                            "summary": "Eurostat Release",
                            "url": url,
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat Error: {e}")
            return []

# --- Main Execution ---

def run_scrapers():
    scrapers = [ONS_Scraper(), StatCan_Scraper(), US_Census_Scraper(), Eurostat_Scraper()]
    all_data = []
    scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("🚀 Starting Scrape Job...")
    
    for scraper in scrapers:
        try:
            data = scraper.scrape()
            print(f"✅ {scraper.__class__.__name__}: Found {len(data)} items.")
            for item in data: item['scraped_at'] = scrape_time
            all_data.extend(data)
        except Exception as e:
            print(f"❌ {scraper.__class__.__name__} Failed: {e}")

    # Deduplication & Date Filter (Today - 30 days to Today + 180 days)
    unique_data = {f"{x['start']}_{x['title']}": x for x in all_data}
    final_list = list(unique_data.values())
    
    today = datetime.now()
    min_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    max_date = (today + timedelta(days=180)).strftime("%Y-%m-%d")
    
    filtered = [x for x in final_list if min_date <= x['start'] <= max_date]

    # Save
    with open(JSON_TEMP, 'w') as f: json.dump(filtered, f, indent=4)
    pd.DataFrame(filtered).to_csv(CSV_TEMP, index=False)
    
    os.replace(JSON_TEMP, JSON_FILE)
    os.replace(CSV_TEMP, CSV_FILE)
    print(f"💾 Saved {len(filtered)} releases.")

if __name__ == "__main__":
    run_scrapers()
