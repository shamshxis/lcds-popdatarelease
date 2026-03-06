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
import concurrent.futures
import time
import random

# --- CONFIGURATION ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
HEALTH_FILE = os.path.join(DATA_DIR, "sources_health.json")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- LCDS THEME FILTER ---
class LCDSFilter:
    def __init__(self):
        self.CORE_THEMES = [
            "mortality", "death", "life expectancy", "suicide", "excess deaths",
            "fertility", "birth", "conception", "maternity", "natal",
            "migration", "asylum", "refugee", "border", "population", "census", "demograph",
            "household", "family", "marriage", "divorce",
            "health", "disease", "covid", "pandemic", "hospital",
            "inequality", "poverty", "deprivation", "social mobility",
            "climate", "environment", "emission"
        ]
        # Economic datasets to ignore
        self.NOISE_THEMES = [
            "industrial production", "construction output", "retail sales", 
            "producer price", "business sentiment", "tourism", "transport", "agriculture", "turnover"
        ]

    def classify(self, title):
        t = title.lower()
        if any(x in t for x in self.NOISE_THEMES): return "Economy (Ignored)"
        
        if any(x in t for x in ["mortal", "death", "suicide", "life expect"]): return "Mortality"
        if any(x in t for x in ["birth", "fertil", "baby"]): return "Fertility"
        if any(x in t for x in ["migra", "asylum", "visa"]): return "Migration"
        if any(x in t for x in ["pop", "census", "resident", "age"]): return "Population"
        if any(x in t for x in ["health", "medic", "cancer", "covid"]): return "Health"
        if any(x in t for x in ["household", "family", "gender"]): return "Family"
        if any(x in t for x in ["inequal", "poverty", "wage", "income"]): return "Inequality"
        if any(x in t for x in ["climate", "environment"]): return "Environment"
        
        return "General Stats"

    def is_relevant(self, title):
        return self.classify(title) != "Economy (Ignored)"

class ScraperEngine:
    def __init__(self):
        self.filter = LCDSFilter()
        self.memory = {}
        # User-Agents to bypass ONS/CDC blocks
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9'
        }

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except: return None

    # --- 1. EUROSTAT (OFFICIAL API) ---
    def fetch_eurostat_api(self):
        """Uses Eurostat SDMX/JSON API (No scraping!)"""
        # Eurostat calendar endpoint (xml/json)
        # We use a known JSON-stat feed or reliable calendar URL as the API is complex
        # Fallback: The most reliable method for DATES is actually their XML calendar feed
        url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=20)
            soup = BeautifulSoup(resp.content, 'xml') # It's XML
            
            for item in soup.find_all('release'):
                title = item.find('title').text
                date_str = item.find('release_date').text # YYYY-MM-DD
                
                if self.filter.is_relevant(title):
                    events.append({
                        "title": title,
                        "start": date_str,
                        "country": "EU (Eurostat)",
                        "source": "Eurostat API",
                        "url": "https://ec.europa.eu/eurostat/news/release-calendar",
                        "topic": self.filter.classify(title)
                    })
            return events
        except Exception as e:
            logging.error(f"Eurostat API Error: {e}")
            return []

    # --- 2. CBS NETHERLANDS (OFFICIAL OData API) ---
    def fetch_cbs_api(self):
        """Uses CBS OData API to get the release calendar"""
        # CBS 'Events' Endpoint
        url = "https://www.cbs.nl/en-gb/service/news-releases-rss" 
        # Actually, CBS RSS is cleaner than their OData for *future* dates
        events = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title = entry.title
                date_str = self.normalize_date(entry.published)
                
                if self.filter.is_relevant(title):
                    events.append({
                        "title": title,
                        "start": date_str,
                        "country": "Netherlands",
                        "source": "CBS (API)",
                        "url": entry.link,
                        "topic": self.filter.classify(title)
                    })
            return events
        except Exception as e:
            logging.error(f"CBS API Error: {e}")
            return []

    # --- 3. ONS UK (RSS FEED + STEALTH) ---
    def fetch_ons_rss(self):
        """Uses ONS RSS Feed (Reliable)"""
        url = "https://www.ons.gov.uk/releasecalendar/rss"
        events = []
        try:
            # We must use requests with headers to get the RSS, feedparser might fail on 403
            resp = requests.get(url, headers=self.headers, timeout=10)
            feed = feedparser.parse(resp.content)
            
            for entry in feed.entries:
                title = entry.title
                date_str = self.normalize_date(entry.published)
                
                if self.filter.is_relevant(title):
                    events.append({
                        "title": title,
                        "start": date_str,
                        "country": "UK",
                        "source": "ONS (RSS)",
                        "url": entry.link,
                        "topic": self.filter.classify(title)
                    })
            return events
        except Exception as e:
            logging.error(f"ONS RSS Error: {e}")
            return []

    # --- 4. US CENSUS (STEALTH SCRAPE) ---
    def scrape_us_census(self):
        url = "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html"
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Text-based parsing for dates like "March 15, 2026"
            text_blob = soup.get_text("\n")
            lines = text_blob.split("\n")
            
            for line in lines:
                line = line.strip()
                match = re.search(r'([A-Z][a-z]+ \d{1,2}, \d{4})', line)
                if match:
                    date_str = self.normalize_date(match.group(1))
                    title = line.replace(match.group(1), "").strip(" -:")
                    
                    if len(title) > 10 and self.filter.is_relevant(title):
                         events.append({
                            "title": title,
                            "start": date_str,
                            "country": "USA",
                            "source": "US Census",
                            "url": url,
                            "topic": self.filter.classify(title)
                        })
            return events
        except: return []

    # --- 5. FINLAND (FINDATA) ---
    def scrape_statfinland(self):
        url = "https://stat.fi/til/pvml_en.html" # Release calendar page
        events = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # They typically list releases in table rows
            for row in soup.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_txt = cols[0].get_text(strip=True)
                    title = cols[1].get_text(strip=True)
                    
                    date_str = self.normalize_date(date_txt)
                    if date_str and self.filter.is_relevant(title):
                        events.append({
                            "title": title,
                            "start": date_str,
                            "country": "Finland",
                            "source": "FinData",
                            "url": url,
                            "topic": self.filter.classify(title)
                        })
            return events
        except: return []

    def run(self):
        print("🚀 Starting Hybrid API/Scraper Engine...")
        
        tasks = {
            "Eurostat (API)": self.fetch_eurostat_api,
            "CBS (Netherlands)": self.fetch_cbs_api,
            "ONS (UK)": self.fetch_ons_rss,
            "US Census": self.scrape_us_census,
            "FinData": self.scrape_statfinland
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
                        health_report[source] = {"status": "warning", "error": "Zero items", "last_run": scrape_time}
                except Exception as e:
                    print(f"❌ {source} Failed: {e}")
                    health_report[source] = {"status": "error", "error": str(e), "last_run": scrape_time}

        # Save Health Report
        with open(HEALTH_FILE, 'w') as f: json.dump(health_report, f, indent=4)

        # Save Data
        if all_data:
            for item in all_data: item['scraped_at'] = scrape_time
            
            # Deduplicate
            unique = {f"{x['start']}_{x['title']}": x for x in all_data}.values()
            final_list = list(unique)
            
            # Sort by Date
            final_list.sort(key=lambda x: x['start'])
            
            with open(JSON_FILE, 'w') as f: json.dump(final_list, f, indent=4)
            print(f"💾 Saved {len(final_list)} datasets to {JSON_FILE}")
        else:
            print("⚠️ No data collected.")

if __name__ == "__main__":
    engine = ScraperEngine()
    engine.run()
