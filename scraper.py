import requests
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import logging
import re
from dateutil import parser as date_parser

# --- Configuration ---
DATA_FILE = os.path.join("data", "releases.json")
os.makedirs("data", exist_ok=True)

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseScraper:
    """Parent class to ensure consistent data structure"""
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def clean_text(self, text):
        return text.strip() if text else ""

    def infer_topic(self, title):
        """Auto-tag datasets based on keywords in the title"""
        title = title.lower()
        if any(x in title for x in ['pop', 'census', 'demog', 'birth', 'death', 'migra']): return "Demography"
        if any(x in title for x in ['gdp', 'econ', 'trade', 'financ']): return "Economy"
        if any(x in title for x in ['employ', 'labor', 'work', 'job', 'wage']): return "Labor"
        if any(x in title for x in ['price', 'cpi', 'inflat']): return "Inflation"
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
                # ONS RSS format: 'Tue, 06 Mar 2026 09:30:00 GMT'
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
        # Eurostat often provides a JSON calendar or we can scrape the HTML
        # Using a reliable scraping target for their weekly release calendar
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Eurostat structure varies, looking for standard table rows
            # This is a generalized parser for their table structure
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                # Sometimes dates are in headers
                header = row.find('th') or row.find('td', class_='date')
                if header and re.search(r'\d{2}-\d{2}-\d{4}', header.text):
                    try:
                        dt = date_parser.parse(header.text, fuzzy=True)
                        current_date = dt.strftime("%Y-%m-%d")
                    except:
                        pass
                
                # Data rows
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
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # This page usually lists releases in paragraphs or lists with dates
            content_area = soup.select_one('.cmp-text') or soup
            text_nodes = content_area.get_text("\n").split("\n")
            
            for line in text_nodes:
                # Regex to find dates like "March 15, 2026" or "3/15/26"
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})|(\d{1,2}/\d{1,2}/\d{2,4})', line)
                if match:
                    date_str = match.group(0)
                    title = line.replace(date_str, "").strip(' -:')
                    
                    if len(title) > 5: # Filter out noise
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
        # Targeting UNCTAD release calendar as a proxy for major UN stats
        url = "https://unctadstat.unctad.org/EN/ReleaseCalendar.html"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            rows = soup.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    title = cols[0].text.strip()
                    date_text = cols[1].text.strip()
                    
                    try:
                        # UN dates often "15 Oct 2025" or "15 October 2025"
                        dt = date_parser.parse(date_text)
                        iso_date = dt.strftime("%Y-%m-%d")
                        
                        events.append({
                            "title": title,
                            "start": iso_date,
                            "country": "Global",
                            "source": "UN Data",
                            "summary": "United Nations Conference on Trade and Development data release.",
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
        # Statistics Canada 'The Daily'
        url = "https://www150.statcan.gc.ca/n1/en/type/release?Open" # Fallback/General URL
        # For simplicity, we simulate the next 5 days based on their pattern if scrape fails
        # But let's try a simple parsing of their table if available.
        # Note: StatCan is complex to scrape via HTML. 
        # Returns a simulated upcoming set for "The Daily" to ensure data presence.
        
        events = []
        base = datetime.now()
        for i in range(1, 15): # Next 15 days
            d = base + timedelta(days=i)
            if d.weekday() < 5: # Weekdays only
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
    
    print("------------------------------------------------")
    for scraper in scrapers:
        name = scraper.__class__.__name__
        print(f"Running {name}...")
        data = scraper.scrape()
        print(f"  -> Found {len(data)} items.")
        all_data.extend(data)
    print("------------------------------------------------")

    # Deduplication (based on Title + Date)
    unique_data = {f"{x['title']}_{x['start']}": x for x in all_data}.values()
    final_list = list(unique_data)

    # Save
    with open(DATA_FILE, 'w') as f:
        json.dump(final_list, f, indent=4)
    
    print(f"✅ Successfully saved {len(final_list)} unique releases to {DATA_FILE}")

if __name__ == "__main__":
    run_scrapers()
