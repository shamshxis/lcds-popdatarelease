import yaml
import json
import os
import requests
import re
import pandas as pd
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime
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
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.today = datetime.now().replace(tzinfo=None) # Ensure today is naive
        self.data = []

    def normalize_date(self, date_str):
        """
        CRITICAL FIX: Enforce Timezone-Naivety
        This prevents 'TypeError: can't compare offset-naive and offset-aware datetimes'
        """
        if not date_str: return None
        try:
            dt = date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
            return dt.replace(tzinfo=None) # STRIP TIMEZONE
        except: return None

    # ---------------------------------------------------------
    # 1. PARSERS
    # ---------------------------------------------------------

    def parser_ons_api(self, source):
        """Handler for ONS JSON API"""
        results = []
        try:
            # ONS requires X-Requested-With to return JSON
            headers = self.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest'
            
            resp = requests.get(source['url'], headers=headers, timeout=10)
            
            # Check for JSON content type or success
            if "json" not in resp.headers.get("Content-Type", "").lower():
                print(f"   ⚠️ ONS API returned HTML (Access blocked?). Skipping.")
                return results

            data = resp.json()
            items = data.get('result', {}).get('results', [])
            
            for item in items:
                title = item.get('description', {}).get('title', 'Unknown')
                date_raw = item.get('description', {}).get('releaseDate', '')
                
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
        """Handler for Eurostat XML"""
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
                            "status": "📅 Scheduled",
                            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
                            "priority": "High"
                        })
        except Exception as e:
            print(f"   ❌ Eurostat Error: {e}")
        return results

    def parser_rss(self, source):
        """Handler for RSS Feeds"""
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

    def parser_html_table_scan(self, source):
        """Handler for structured HTML Tables (CBS, Denmark)"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Find all table rows
            rows = soup.find_all('tr')
            for row in rows:
                text = row.get_text(" ", strip=True)
                # Regex for Date (DD MMM YYYY or YYYY-MM-DD)
                date_match = re.search(r'([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4}|[0-9]{4}-[0-9]{2}-[0-9]{2})', text)
                
                if date_match and any(k in text.lower() for k in ['population', 'census', 'death', 'birth', 'migration']):
                    d_obj = self.normalize_date(date_match.group(1))
                    if d_obj:
                        clean_title = text.replace(date_match.group(1), "").strip()[:100]
                        results.append({
                            "dataset_title": clean_title,
                            "source": source['country'],
                            "action_date": d_obj,
                            "status": "📅 Scheduled" if d_obj > self.today else "✅ Published",
                            "url": source['url'],
                            "priority": "Medium"
                        })
        except Exception as e:
            print(f"   ❌ Table Scan Error {source['name']}: {e}")
        return results

    def parser_html_deep_scan(self, source):
        """Fallback: Aggressive Regex Hunter"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            text = BeautifulSoup(resp.content, 'html.parser').get_text(" ", strip=True)
            
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
                 # Add 'Monitoring' entry even if no date found
                 results.append({
                    "dataset_title": source['name'],
                    "source": source['country'],
                    "action_date": self.today, 
                    "status": "⚠️ Monitoring",
                    "url": source['url'],
                    "priority": "Low"
                })
        except Exception as e:
            print(f"   ❌ Deep Scan Error {source['name']}: {e}")
        return results

    # ---------------------------------------------------------
    # 2. RUNNER
    # ---------------------------------------------------------

    def process_source(self, source):
        method = source.get('parser', 'html_deep_scan')
        print(f"   👉 Starting {source['name']} (Method: {method})...")
        
        if method == 'ons_json_api': return self.parser_ons_api(source)
        elif method == 'eurostat_xml': return self.parser_eurostat_xml(source)
        elif method == 'rss_feed': return self.parser_rss(source)
        elif method == 'html_table_scan': return self.parser_html_table_scan(source)
        else: return self.parser_html_deep_scan(source)

    def run(self):
        print("🚀 Starting Enterprise Scraper (Threaded)...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)
        sources = config.get('sources', [])

        all_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_source = {executor.submit(self.process_source, s): s for s in sources}
            for future in as_completed(future_to_source):
                try:
                    all_results.extend(future.result())
                except Exception as exc:
                    print(f"   ❌ Thread Exception: {exc}")

        if all_results:
            df = pd.DataFrame(all_results)
            # Safe Sort (No Timezone Errors)
            df = df.sort_values(by='action_date', ascending=False)
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Saved {len(df)} Assets.")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    EnterpriseScraper().run()
