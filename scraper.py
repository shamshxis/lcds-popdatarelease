import sys
import subprocess
import os

# --- 1. BOOTSTRAP: AUTO-INSTALL DEPENDENCIES ---
def install(package):
    print(f"📦 Installing missing package: {package}...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

try: import feedparser
except ImportError: install("feedparser"); import feedparser

try: import yaml
except ImportError: install("pyyaml"); import yaml

try: from dateutil import parser as date_parser
except ImportError: install("python-dateutil"); from dateutil import parser as date_parser

try: from bs4 import BeautifulSoup
except ImportError: install("beautifulsoup4"); from bs4 import BeautifulSoup

import requests
import re
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIG ---
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
WATCHLIST_FILE = "watchlist.yml"
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")

class LCDS_Data_Engine:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.today = datetime.now().replace(tzinfo=None)
        self.memory_df = self.load_memory()

    def load_memory(self):
        """Loads existing memory and ensures dates are type-safe."""
        if os.path.exists(OUTPUT_FILE):
            try:
                df = pd.read_csv(OUTPUT_FILE)
                df['action_date'] = pd.to_datetime(df['action_date'], errors='coerce')
                return df.dropna(subset=['action_date'])
            except: pass
        return pd.DataFrame()

    def normalize_date(self, date_str):
        """Standardizes dates to YYYY-MM-DD (Naive)."""
        if not date_str: return None
        try:
            dt = date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
            return dt.replace(tzinfo=None)
        except: return None

    def is_relevant(self, text, keywords=None):
        """Strict Noise Filter: Must match keywords."""
        text = text.lower()
        core_keys = ['population', 'census', 'migration', 'birth', 'death', 'fertility', 'mortality', 'demograph']
        
        # If specific keywords provided in YAML, use those + core
        if keywords:
            check_list = [k.lower() for k in keywords] + core_keys
        else:
            check_list = core_keys
            
        return any(k in text for k in check_list)

    # ---------------------------------------------------------
    # PARSERS
    # ---------------------------------------------------------

    def parser_ons_json_api(self, source):
        results = []
        try:
            headers = self.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest'
            resp = requests.get(source['url'], headers=headers, timeout=15)
            
            # Fallback if blocked
            if "json" not in resp.headers.get("Content-Type", "").lower():
                print(f"   ⚠️ ONS API Blocked. Switching to Deep Scan...")
                source['url'] = "https://www.ons.gov.uk/releasecalendar?view=upcoming"
                return self.parser_html_deep_scan(source)

            data = resp.json()
            for item in data.get('result', {}).get('results', []):
                title = item.get('description', {}).get('title', 'Unknown')
                if self.is_relevant(title):
                    d_obj = self.normalize_date(item.get('description', {}).get('releaseDate', ''))
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
        """
        Scans <tr> tags. Optimized for CBS, SCB, DST.
        Looks for: [Date] [Title] ...
        """
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            keywords = source.get('keywords', [])
            
            for row in soup.find_all('tr'):
                text = row.get_text(" ", strip=True)
                
                # Regex for Date (DD MMM YYYY or YYYY-MM-DD)
                date_match = re.search(r'([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})', text)
                
                if date_match:
                    clean_title = text.replace(date_match.group(1), "").strip()
                    # Filter by relevance
                    if self.is_relevant(clean_title, keywords):
                        d_obj = self.normalize_date(date_match.group(1))
                        if d_obj:
                            results.append({
                                "dataset_title": clean_title[:100], # Truncate long titles
                                "source": source['country'],
                                "action_date": d_obj,
                                "status": "📅 Scheduled" if d_obj > self.today else "✅ Published",
                                "url": source['url'],
                                "last_checked": self.today.strftime("%Y-%m-%d")
                            })
        except Exception as e:
            print(f"   ❌ Table Scan Error ({source['name']}): {e}")
        return results

    def parser_html_deep_scan(self, source):
        """Regex Hunter for non-standard sites"""
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
        except: pass
        return results

    def parser_eurostat_xml(self, source):
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'xml')
            for item in soup.find_all('release'):
                title = item.find('title').text
                if self.is_relevant(title):
                    d_obj = self.normalize_date(item.find('release_date').text)
                    if d_obj:
                        results.append({
                            "dataset_title": title,
                            "source": "Eurostat",
                            "action_date": d_obj,
                            "status": "📅 Scheduled",
                            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except: pass
        return results

    def parser_rss(self, source):
        results = []
        try:
            feed = feedparser.parse(source['url'])
            for entry in feed.entries[:10]: # Check last 10 items
                if self.is_relevant(entry.title):
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

    # --- INTELLIGENCE LAYER: GDELT ---
    def fetch_gdelt(self):
        results = []
        try:
            # Query: (population OR migration) AND (release OR data)
            query = "(population%20OR%20migration)%20AND%20(release%20OR%20data)"
            url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={query}&mode=artlist&maxrecords=10&format=json"
            resp = requests.get(url, timeout=10)
            data = resp.json()
            for art in data.get('articles', []):
                title = art.get('title', '')
                if self.is_relevant(title):
                    d_obj = self.normalize_date(art.get('seendate', '')[:8])
                    if d_obj:
                        results.append({
                            "dataset_title": title,
                            "source": "GDELT Intelligence",
                            "action_date": d_obj,
                            "status": "🔵 News Signal",
                            "url": art.get('url'),
                            "last_checked": self.today.strftime("%Y-%m-%d")
                        })
        except: pass
        return results

    # --- RUNNER ---
    def process(self, source):
        method = source.get('parser', 'html_deep_scan')
        print(f"   👉 Checking {source['name']} ({method})...")
        if method == 'ons_json_api': return self.parser_ons_json_api(source)
        elif method == 'html_table_scan': return self.parser_html_table_scan(source)
        elif method == 'eurostat_xml': return self.parser_eurostat_xml(source)
        elif method == 'rss_feed': return self.parser_rss(source)
        else: return self.parser_html_deep_scan(source)

    def run(self):
        print("🚀 Starting LCDS Data Engine...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)

        new_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # 1. Sources
            futures = {executor.submit(self.process, s): s for s in config['sources']}
            
            # 2. GDELT
            future_gdelt = executor.submit(self.fetch_gdelt)

            for future in as_completed(futures):
                try: new_data.extend(future.result())
                except Exception as e: print(f"   ❌ Error: {e}")
            
            try: new_data.extend(future_gdelt.result())
            except: pass

        # --- MEMORY MERGE ---
        if not new_data:
            print("⚠️ No new data. Retaining memory.")
            if not self.memory_df.empty: self.memory_df.to_csv(OUTPUT_FILE, index=False)
            return

        df_new = pd.DataFrame(new_data)
        if 'action_date' in df_new.columns:
            df_new['action_date'] = pd.to_datetime(df_new['action_date'])

        # Combine
        if not self.memory_df.empty:
            scraped_sources = df_new['source'].unique()
            df_old_kept = self.memory_df[~self.memory_df['source'].isin(scraped_sources)]
            df_final = pd.concat([df_new, df_old_kept])
        else:
            df_final = df_new

        # Sort & Save
        df_final = df_final.sort_values(by='action_date', ascending=False)
        df_final = df_final.drop_duplicates(subset=['dataset_title', 'action_date'])
        
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"💾 Database updated: {len(df_final)} assets.")

if __name__ == "__main__":
    LCDS_Data_Engine().run()
