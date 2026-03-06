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
        # Remove multiple spaces and newlines
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

# --- Hybrid Scrapers ---

class ONS_Hybrid_Scraper(BaseScraper):
    """
    Method: HYBRID
    1. HTML Scrape: Gets the next 3 months of confirmed release dates.
    2. RSS Scrape: Gets rich summaries and direct links for imminent releases.
    """
    def scrape(self):
        events = {} # Use dict for deduplication by Key (Date+Title)

        # 1. HTML SCRAPE (Future Schedule)
        base_url = "https://www.ons.gov.uk/releasecalendar"
        for page in range(1, 4): # Pages 1-3
            try:
                url = f"{base_url}?page={page}"
                resp = self.session.get(url, timeout=10)
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                # ONS specific class for release items
                items = soup.select('.release__item')
                for item in items:
                    title_tag = item.select_one('h3 a')
                    date_tag = item.select_one('.release__date')
                    
                    if title_tag and date_tag:
                        title = self.clean_text(title_tag.text)
                        # Link is relative (e.g., /releases/cpi...)
                        link = "https://www.ons.gov.uk" + title_tag['href']
                        date_raw = self.clean_text(date_tag.text).replace("Release date:", "").strip()
                        date_str = self.normalize_date(date_raw)
                        
                        if date_str:
                            key = f"{date_str}_{title}"
                            events[key] = {
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "source": "ONS",
                                "summary": "Official Release Page (Placeholder until published)",
                                "url": link,
                                "topic": self.infer_topic(title)
                            }
            except Exception as e:
                logging.error(f"ONS HTML Fail: {e}")

        # 2. RSS SCRAPE (Rich Data Overlay)
        rss_url = "https://www.ons.gov.uk/releasecalendar/rss"
        try:
            feed = feedparser.parse(rss_url)
            for entry in feed.entries:
                try:
                    dt = date_parser.parse(entry.published)
                    date_str = dt.strftime("%Y-%m-%d")
                    title = self.clean_text(entry.title)
                    
                    key = f"{date_str}_{title}"
                    
                    # Create or Update with better RSS data
                    events[key] = {
                        "title": title,
                        "start": date_str,
                        "country": "UK",
                        "source": "ONS",
                        "summary": self.clean_text(entry.summary)[:300] + "...", # Rich summary
                        "url": entry.link, # Direct Deep Link
                        "topic": self.infer_topic(title)
                    }
                except:
                    continue
        except Exception as e:
            logging.error(f"ONS RSS Fail: {e}")

        return list(events.values())

class Eurostat_Scraper(BaseScraper):
    def scrape(self):
        # Eurostat Weekly Calendar HTML
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Eurostat puts data in a table structure
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                # Date headers often inside <th> or specialized <td>
                header = row.find(['th', 'td'])
                if header:
                    txt = self.clean_text(header.text)
                    # Check if this row is a Date Row (e.g., "15-02-2026")
                    if re.search(r'\d{2}-\d{2}-\d{4}', txt):
                        current_date = self.normalize_date(txt)
                        continue # Skip to next row to find events for this date
                
                # Event Rows
                cols = row.find_all('td')
                if current_date and len(cols) >= 2:
                    # Title is usually in the last column or the one with a link
                    title_col = cols[-1]
                    title = self.clean_text(title_col.text)
                    
                    # Look for specific link inside the title column
                    link_tag = title_col.find('a')
                    deep_link = link_tag['href'] if link_tag else url
                    if deep_link.startswith("/"): deep_link = "https://ec.europa.eu" + deep_link
                    
                    if title and len(title) > 5:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "EU",
                            "source": "Eurostat",
                            "summary": "Eurostat Official Release.",
                            "url": deep_link,
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat Fail: {e}")
            return []

class US_Census_NLP_Scraper(BaseScraper):
    """
    Method: NLP / Regex
    Parses unstructured text from the 'Upcoming Releases' page.
    """
    def scrape(self):
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Helper to find text blocks
            content_div = soup.select_one('.cmp-text') or soup
            
            # Split by lines to process sentence-by-sentence
            text_lines = content_div.get_text("\n").split("\n")
            
            for line in text_lines:
                line = self.clean_text(line)
                if not line: continue
                
                # Regex for "Month DD, YYYY" or "MM/DD/YY"
                date_match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})|(\d{1,2}/\d{1,2}/\d{2,4})', line)
                
                if date_match:
                    date_str = self.normalize_date(date_match.group(0))
                    if not date_str: continue
                    
                    # The title is usually the text REMAINING after the date
                    title = line.replace(date_match.group(0), "").strip(" -:")
                    
                    # Ignore short noise ("Updated:", "Note:")
                    if len(title) > 10 and "update" not in title.lower():
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "US Census",
                            "summary": line, # Use full sentence as summary context
                            "url": url, # Specific page
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"US Census Fail: {e}")
            return []

class StatCan_Schedule_Scraper(BaseScraper):
    def scrape(self):
        # "The Daily" release schedule is the best source
        url = "https://www150.statcan.gc.ca/n1/en/surveys/release-dates"
        events = []
        try:
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Locate the schedule table
            rows = soup.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_txt = self.clean_text(cols[0].text)
                    title_txt = self.clean_text(cols[1].text)
                    
                    date_str = self.normalize_date(date_txt)
                    
                    # Try to find a link in the title column
                    a_tag = cols[1].find('a')
                    link = "https://www150.statcan.gc.ca" + a_tag['href'] if a_tag else url
                    
                    if date_str and title_txt:
                        events.append({
                            "title": title_txt,
                            "start": date_str,
                            "country": "Canada",
                            "source": "StatCan",
                            "summary": "Official Release in 'The Daily'",
                            "url": link,
                            "topic": self.infer_topic(title_txt)
                        })
            return events
        except Exception as e:
            logging.error(f"StatCan Fail: {e}")
            return []

# --- Execution Engine ---

def run_scrapers():
    scrapers = [
        ONS_Hybrid_Scraper(),
        Eurostat_Scraper(),
        US_Census_NLP_Scraper(),
        StatCan_Schedule_Scraper()
    ]
    
    all_data = []
    scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("🚀 Starting Hybrid Scrape...")
    
    for scraper in scrapers:
        name = scraper.__class__.__name__
        try:
            print(f"   -> Running {name}...")
            data = scraper.scrape()
            print(f"      Found {len(data)} releases.")
            
            # Timestamping
            for item in data:
                item['scraped_at'] = scrape_time
            all_data.extend(data)
        except Exception as e:
            logging.error(f"Critical error in {name}: {e}")

    # --- PROCESSING ---
    
    # 1. Deduplication (Key = Date + Title normalized)
    # We want to remove duplicates if the same source lists it twice
    unique_data = {}
    for item in all_data:
        # Create a simple hash key
        slug = re.sub(r'\W+', '', item['title'].lower()) # "cpi release" -> "cpirelease"
        key = f"{item['start']}_{slug}"
        
        # If conflict, keep the one with the longer URL (implies deep link)
        if key in unique_data:
            existing = unique_data[key]
            if len(item['url']) > len(existing['url']):
                unique_data[key] = item
        else:
            unique_data[key] = item

    final_list = list(unique_data.values())

    # 2. Date Window (+- 3 Months)
    today = datetime.now()
    min_date = (today - timedelta(days=90)).strftime("%Y-%m-%d")
    max_date = (today + timedelta(days=120)).strftime("%Y-%m-%d")
    
    filtered_list = [x for x in final_list if min_date <= x['start'] <= max_date]
    
    print(f"📊 Summary: {len(all_data)} raw items -> {len(final_list)} unique -> {len(filtered_list)} within window.")

    if not filtered_list:
        print("⚠️ No data found. Aborting.")
        return

    # --- ATOMIC SAVE ---
    print("💾 Saving data...")
    
    # Save JSON
    with open(JSON_TEMP, 'w') as f:
        json.dump(filtered_list, f, indent=4)
        
    # Save CSV
    df = pd.DataFrame(filtered_list)
    cols = ['start', 'country', 'source', 'title', 'topic', 'summary', 'url', 'scraped_at']
    df = df.reindex(columns=cols) 
    df.to_csv(CSV_TEMP, index=False)
    
    # Swap
    os.replace(JSON_TEMP, JSON_FILE)
    os.replace(CSV_TEMP, CSV_FILE)
    
    print("✅ Done.")

if __name__ == "__main__":
    run_scrapers()
