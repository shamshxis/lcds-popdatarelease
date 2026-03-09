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

class SmartWatchdog:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.today = datetime.now().replace(tzinfo=None)
        
        # Load Memory (Existing Data)
        if os.path.exists(OUTPUT_FILE):
            self.memory = pd.read_csv(OUTPUT_FILE).to_dict('records')
        else:
            self.memory = []

    def normalize_date(self, date_str):
        if not date_str: return None
        try:
            dt = date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
            return dt.replace(tzinfo=None)
        except: return None

    # ---------------------------------------------------------
    # PARSERS
    # ---------------------------------------------------------

    def parser_ons_json_api(self, source):
        """Primary ONS Method: Hidden JSON API"""
        results = []
        try:
            # Fake AJAX request
            headers = self.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest'
            
            resp = requests.get(source['url'], headers=headers, timeout=10)
            
            # IF BLOCKED (Returns HTML instead of JSON) -> Trigger Fallback
            if "json" not in resp.headers.get("Content-Type", "").lower():
                print(f"   ⚠️ ONS API Blocked. Triggering Fallback...")
                # Change URL to HTML version and use fallback parser
                source['url'] = "https://www.ons.gov.uk/releasecalendar?view=upcoming"
                return self.parser_html_deep_scan(source)

            data = resp.json()
            items = data.get('result', {}).get('results', [])
            
            for item in items:
                title = item.get('description', {}).get('title', 'Unknown')
                date_raw = item.get('description', {}).get('releaseDate', '')
                
                if any(k in title.lower() for k in ['birth', 'death', 'population', 'migration', 'census']):
                    d_obj = self.normalize_date(date_raw)
                    if d_obj:
                        results.append({
                            "dataset_title": title,
                            "source": "ONS (UK)",
                            "action_date": d_obj,
                            "status": "📅 Scheduled" if d_obj > self.today else "✅ Published",
                            "url": "https://www.ons.gov.uk" + item.get('uri', ''),
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except Exception as e:
            print(f"   ❌ ONS Error: {e}")
        return results

    def parser_html_table_scan(self, source):
        """Scans <table> tags (Best for Registries like CBS/DST)"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            for row in soup.find_all('tr'):
                text = row.get_text(" ", strip=True)
                # Regex: Find YYYY-MM-DD or DD MMM YYYY
                date_match = re.search(r'([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})', text)
                
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
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except Exception as e:
            print(f"   ❌ Table Scan Error: {e}")
        return results

    def parser_html_deep_scan(self, source):
        """Fallback: Aggressive Regex Hunter"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            text = BeautifulSoup(resp.content, 'html.parser').get_text(" ", strip=True)
            
            # Look for "Next release: [Date]"
            match = re.search(r'(?:Next|Upcoming|Expected|Schedule)[^0-9]{1,30}?([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})', text, re.IGNORECASE)
            
            if match:
                d_obj = self.normalize_date(match.group(1))
                if d_obj:
                    results.append({
                        "dataset_title": source['name'],
                        "source": source['country'],
                        "action_date": d_obj,
                        "status": "📅 Scheduled",
                        "url": source['url'],
                        "last_checked": self.today.strftime("%Y-%m-%d")
                    })
            else:
                # If scraping fails, CHECK MEMORY. Do we have an old record for this?
                # If yes, keep it but mark as "Cached"
                pass 
        except: pass
        return results

    def parser_eurostat_xml(self, source):
        """Eurostat XML Parser"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'xml')
            for item in soup.find_all('release'):
                if any(k in item.find('title').text.lower() for k in ['population', 'mortality', 'migration']):
                    d_obj = self.normalize_date(item.find('release_date').text)
                    if d_obj:
                        results.append({
                            "dataset_title": item.find('title').text,
                            "source": "Eurostat",
                            "action_date": d_obj,
                            "status": "📅 Scheduled",
                            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except: pass
        return results

    def parser_rss(self, source):
        """RSS Feed Parser"""
        results = []
        try:
            feed = feedparser.parse(source['url'])
            for entry in feed.entries[:5]:
                if any(k in entry.title.lower() for k in ['population', 'data', 'release', 'health']):
                    d_obj = self.normalize_date(entry.published)
                    if d_obj:
                        results.append({
                            "dataset_title": entry.title,
                            "source": source['name'],
                            "action_date": d_obj,
                            "status": "📢 Announcement",
                            "url": entry.link,
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except: pass
        return results

    # ---------------------------------------------------------
    # ENGINE
    # ---------------------------------------------------------

    def process_source(self, source):
        method = source.get('parser', 'html_deep_scan')
        print(f"   👉 Checking {source['name']} ({method})...")
        
        if method == 'ons_json_api': return self.parser_ons_json_api(source)
        elif method == 'html_table_scan': return self.parser_html_table_scan(source)
        elif method == 'eurostat_xml': return self.parser_eurostat_xml(source)
        elif method == 'rss_feed': return self.parser_rss(source)
        else: return self.parser_html_deep_scan(source)

    def run(self):
        print("🚀 Starting Smart Watchdog...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)
        
        new_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.process_source, s): s for s in config['sources']}
            for future in as_completed(futures):
                try:
                    new_data.extend(future.result())
                except Exception as e:
                    print(f"   ❌ Thread Error: {e}")

        # --- MEMORY MERGE (The "Smart" Part) ---
        # 1. Convert new data to DataFrame
        if not new_data:
            print("⚠️ No new data found. Retaining memory.")
            return

        df_new = pd.DataFrame(new_data)
        
        # 2. Load old data (Memory)
        if self.memory:
            df_old = pd.DataFrame(self.memory)
            # Filter out old rows that correspond to sources we just successfully scraped
            # (We overwrite them with fresh data)
            scraped_sources = df_new['source'].unique()
            df_old_kept = df_old[~df_old['source'].isin(scraped_sources)]
            
            # Combine: Fresh Data + Unscraped Memory
            df_final = pd.concat([df_new, df_old_kept])
        else:
            df_final = df_new

        # 3. Save
        df_final = df_final.sort_values(by='action_date', ascending=False).drop_duplicates(subset=['dataset_title', 'action_date'])
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"💾 Database updated. Total Assets: {len(df_final)}")

if __name__ == "__main__":
    SmartWatchdog().run()
