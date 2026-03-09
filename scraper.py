import yaml
import json
import os
import requests
import re
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as date_parser
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIG ---
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
WATCHLIST_FILE = "watchlist.yml"
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")

class EnterpriseScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/xml, application/xml, */*'
        }
        self.today = datetime.now()
        self.data = []

    def normalize_date(self, date_str):
        if not date_str: return None
        try:
            return date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
        except: return None

    # ---------------------------------------------------------
    # 1. SPECIALIZED PARSERS (The "Best Method" for each)
    # ---------------------------------------------------------

    def parser_ons_api(self, source):
        """Handler for ONS JSON API (Fastest)"""
        results = []
        try:
            # ONS requires a specific header to return JSON
            headers = self.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest' 
            
            resp = requests.get(source['url'], headers=headers, timeout=10)
            data = resp.json()
            
            items = data.get('result', {}).get('results', [])
            for item in items:
                title = item.get('description', {}).get('title', 'Unknown')
                date_raw = item.get('description', {}).get('releaseDate', '')
                
                # Filter for Demography Keywords
                if any(k in title.lower() for k in ['birth', 'death', 'population', 'migration', 'census']):
                    d_obj = self.normalize_date(date_raw)
                    if d_obj:
                        status = "📅 Scheduled" if d_obj > self.today else "✅ Published"
                        link = "https://www.ons.gov.uk" + item.get('uri', '')
                        results.append({
                            "dataset_title": title,
                            "source": "ONS (API)",
                            "action_date": d_obj,
                            "status": status,
                            "url": link,
                            "priority": "High"
                        })
        except Exception as e:
            print(f"   ❌ ONS API Error: {e}")
        return results

    def parser_eurostat_xml(self, source):
        """Handler for Eurostat XML Feed (Structured)"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=10)
            soup = BeautifulSoup(resp.content, 'xml')
            
            for item in soup.find_all('release'):
                title = item.find('title').text
                date_str = item.find('release_date').text
                
                if any(k in title.lower() for k in ['mortality', 'population', 'fertility', 'migration']):
                    d_obj = self.normalize_date(date_str)
                    if d_obj:
                        results.append({
                            "dataset_title": title,
                            "source": "Eurostat (XML)",
                            "action_date": d_obj,
                            "status": "📅 Scheduled", # XML is usually upcoming
                            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
                            "priority": "High"
                        })
        except Exception as e:
            print(f"   ❌ Eurostat Error: {e}")
        return results

    def parser_rss(self, source):
        """Handler for RSS Feeds (DHS, etc.)"""
        results = []
        try:
            feed = feedparser.parse(source['url'])
            for entry in feed.entries[:5]:
                 if any(k in entry.title.lower() for k in ['data', 'release', 'population', 'health', 'survey']):
                    d_obj = self.normalize_date(entry.published)
                    if d_obj:
                        results.append({
                            "dataset_title": entry.title,
                            "source": source['name'],
                            "action_date": d_obj,
                            "status": "📢 Announcement",
                            "url": entry.link,
                            "priority": "Medium"
                        })
        except Exception as e:
            print(f"   ❌ RSS Error: {e}")
        return results

    def parser_html_deep_scan(self, source):
        """Fallback: Aggressive Regex Hunter for HTML pages"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            text = soup.get_text(" ", strip=True)
            
            found = False
            
            # STRATEGY 1: TABLE ROW SCANNER (Best for Nordic/Dutch Calendars)
            # Many of these sites (CBS, SCB, DST) use <tr> with Date in one col and Title in another
            rows = soup.find_all('tr')
            for row in rows:
                row_text = row.get_text(" ", strip=True)
                # Regex to find a date at start of row
                date_match = re.search(r'([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2})', row_text)
                if date_match:
                    d_obj = self.normalize_date(date_match.group(1))
                    # Check if row has relevant keywords
                    if d_obj and any(k in row_text.lower() for k in ['population', 'census', 'death', 'birth', 'migration']):
                        results.append({
                            "dataset_title": row_text.replace(date_match.group(1), "").strip()[:100], # Clean title
                            "source": source['country'],
                            "action_date": d_obj,
                            "status": "📅 Scheduled" if d_obj > self.today else "✅ Published",
                            "url": source['url'],
                            "priority": "Medium"
                        })
                        found = True

            # STRATEGY 2: IF NO TABLE FOUND, USE REGEX HUNTER
            if not found:
                future_regex = r'(?:Next|Upcoming|Expected|Schedule)[^0-9]{1,30}?([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})'
                match = re.search(future_regex, text, re.IGNORECASE)
                if match:
                    d_obj = self.normalize_date(match.group(1))
                    if d_obj:
                        results.append({
                            "dataset_title": source['name'],
                            "source": source['country'],
                            "action_date": d_obj,
                            "status": "📅 Scheduled",
                            "url": source['url'],
                            "priority": "Medium"
                        })
                else:
                     # Add as monitoring even if no date found
                    results.append({
                        "dataset_title": source['name'],
                        "source": source['country'],
                        "action_date": self.today, # Placeholder
                        "status": "⚠️ Monitoring",
                        "url": source['url'],
                        "priority": "Low"
                    })
        except Exception as e:
            print(f"   ❌ HTML Error {source['name']}: {e}")
        return results

    # ---------------------------------------------------------
    # 2. INTELLIGENCE LAYERS (GDELT)
    # ---------------------------------------------------------
    
    def fetch_gdelt_intelligence(self):
        """Queries GDELT for global news signals about population data"""
        results = []
        try:
            # Query: "Statistical Office" + "Release" + "Population"
            query = "population%20data%20release"
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=10&format=json"
            
            resp = requests.get(url, timeout=10)
            data = resp.json()
            
            for art in data.get('articles', []):
                title = art.get('title', '')
                d_str = art.get('seendate', '')[:8] # YYYYMMDD
                d_obj = self.normalize_date(d_str)
                
                if d_obj and (self.today - d_obj).days < 7:
                    results.append({
                        "dataset_title": title,
                        "source": "GDELT Intelligence",
                        "action_date": d_obj,
                        "status": "🔵 News Signal",
                        "url": art.get('url'),
                        "priority": "Low"
                    })
        except: pass
        return results

    # ---------------------------------------------------------
    # 3. THREADED EXECUTOR
    # ---------------------------------------------------------

    def process_source(self, source):
        """Dispatcher function for threads"""
        method = source.get('parser', 'html_deep_scan')
        print(f"   👉 Starting {source['name']} (Method: {method})...")
        
        if method == 'ons_json_api':
            return self.parser_ons_api(source)
        elif method == 'eurostat_xml':
            return self.parser_eurostat_xml(source)
        elif method == 'rss_feed':
            return self.parser_rss(source)
        else:
            return self.parser_html_deep_scan(source)

    def run(self):
        print("🚀 Starting Enterprise Scraper (Threaded)...")
        
        # Load Watchlist
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)
        sources = config.get('sources', [])

        all_results = []

        # THREADPOOL: Run 5 scrapers in parallel
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 1. Submit Watchlist Tasks
            future_to_source = {executor.submit(self.process_source, s): s for s in sources}
            
            # 2. Submit GDELT Task (Intelligence)
            future_gdelt = executor.submit(self.fetch_gdelt_intelligence)

            # 3. Gather Results as they complete
            for future in as_completed(future_to_source):
                try:
                    data = future.result()
                    all_results.extend(data)
                except Exception as exc:
                    print(f"   ❌ Thread Exception: {exc}")

            # 4. Gather GDELT
            try:
                gdelt_data = future_gdelt.result()
                all_results.extend(gdelt_data)
            except: pass

        # Save
        if all_results:
            df = pd.DataFrame(all_results)
            # Filter ±1 Year
            df = df.sort_values(by='action_date', ascending=False)
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Saved {len(df)} High-Quality Assets.")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    EnterpriseScraper().run()
