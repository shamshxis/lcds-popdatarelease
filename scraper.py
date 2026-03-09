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
# Optional AI filtering (Uncomment if machine supports it, otherwise uses keywords)
from sentence_transformers import SentenceTransformer, util

# --- CONFIG ---
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
WATCHLIST_FILE = "watchlist.yml"
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")

class GlobalPopWatch:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.today = datetime.now().replace(tzinfo=None)
        
        # Load AI Model for Noise Filtering
        print("🧠 Loading AI Relevance Model...")
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.targets = ["population statistics", "migration data", "births and deaths", "census results", "demographic trends"]
        self.target_embeddings = self.model.encode(self.targets, convert_to_tensor=True)

    def is_relevant(self, text):
        """AI-Powered Noise Filter"""
        # 1. Quick Keyword Check (Fast Fail)
        if any(k in text.lower() for k in ['population', 'census', 'migra', 'birth', 'death', 'fertil', 'demog', 'life exp']):
            return True
        
        # 2. Semantic Check (Slow but Smart)
        # Checks if text is semantically similar to our targets
        embedding = self.model.encode(text, convert_to_tensor=True)
        scores = util.cos_sim(embedding, self.target_embeddings)
        return float(scores.max()) > 0.25 # Threshold for relevance

    def normalize_date(self, date_str):
        """CRITICAL: Returns UTC-Naive Datetime or None"""
        if not date_str: return None
        try:
            dt = date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
            return dt.replace(tzinfo=None)
        except: return None

    # --- PARSERS ---

    def parser_html_table_scan(self, source):
        """Handles Registries (CBS, Denmark, Norway, Sweden)"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Iterate all table rows
            for row in soup.find_all('tr'):
                text = row.get_text(" ", strip=True)
                
                # Regex: YYYY-MM-DD or DD Month YYYY
                date_match = re.search(r'([0-9]{4}-[0-9]{2}-[0-9]{2}|[0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})', text)
                
                if date_match:
                    title = text.replace(date_match.group(1), "").strip()
                    if len(title) > 10 and self.is_relevant(title):
                        d_obj = self.normalize_date(date_match.group(1))
                        if d_obj:
                             results.append({
                                "dataset_title": title[:100],
                                "source": source['name'],
                                "action_date": d_obj,
                                "status": "📅 Scheduled" if d_obj > self.today else "✅ Published",
                                "url": source['url']
                            })
        except Exception as e:
            print(f"   ❌ Table Error {source['name']}: {e}")
        return results

    def parser_ons_json_api(self, source):
        results = []
        try:
            headers = self.headers.copy()
            headers['X-Requested-With'] = 'XMLHttpRequest'
            resp = requests.get(source['url'], headers=headers, timeout=10)
            
            # Fallback if blocked
            if "json" not in resp.headers.get("Content-Type", "").lower():
                print("   ⚠️ ONS API Blocked. Switching to Deep Scan.")
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
                            "url": "https://www.ons.gov.uk" + item.get('uri', '')
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
                            "url": "https://ec.europa.eu/eurostat/news/release-calendar"
                        })
        except: pass
        return results

    def parser_rss(self, source):
        results = []
        try:
            feed = feedparser.parse(source['url'])
            for entry in feed.entries[:5]:
                if self.is_relevant(entry.title):
                    d_obj = self.normalize_date(entry.published)
                    if d_obj:
                        results.append({
                            "dataset_title": entry.title,
                            "source": source['name'],
                            "action_date": d_obj,
                            "status": "📢 Announcement",
                            "url": entry.link
                        })
        except: pass
        return results

    def parser_html_deep_scan(self, source):
        """Fallback Regex Hunter"""
        results = []
        try:
            resp = requests.get(source['url'], headers=self.headers, timeout=15)
            text = BeautifulSoup(resp.content, 'html.parser').get_text(" ", strip=True)
            match = re.search(r'(?:Next|Upcoming|Expected|Schedule)[^0-9]{1,30}?([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})', text, re.IGNORECASE)
            if match:
                d_obj = self.normalize_date(match.group(1))
                if d_obj:
                    results.append({
                        "dataset_title": source['name'],
                        "source": source['name'].split(" ")[0],
                        "action_date": d_obj,
                        "status": "📅 Scheduled",
                        "url": source['url']
                    })
        except: pass
        return results

    # --- ENGINE ---
    def process(self, source):
        method = source.get('parser', 'html_deep_scan')
        print(f"   👉 Checking {source['name']} ({method})...")
        if method == 'ons_json_api': return self.parser_ons_json_api(source)
        elif method == 'html_table_scan': return self.parser_html_table_scan(source)
        elif method == 'eurostat_xml': return self.parser_eurostat_xml(source)
        elif method == 'rss_feed': return self.parser_rss(source)
        else: return self.parser_html_deep_scan(source)

    def run(self):
        print("🚀 Starting GlobalPopWatch...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)
        
        # 1. Scrape
        new_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.process, s): s for s in config['sources']}
            for future in as_completed(futures):
                try: new_data.extend(future.result())
                except: pass

        # 2. Merge with Memory
        df_final = pd.DataFrame(new_data)
        if os.path.exists(OUTPUT_FILE):
            try:
                old_df = pd.read_csv(OUTPUT_FILE)
                old_df['action_date'] = pd.to_datetime(old_df['action_date']) # Force type
                if not df_final.empty:
                    df_final['action_date'] = pd.to_datetime(df_final['action_date'])
                    # Overwrite old data with fresh
                    df_final = pd.concat([df_final, old_df]).drop_duplicates(subset=['dataset_title', 'action_date'])
                else:
                    df_final = old_df
            except: pass
        
        # 3. Save
        if not df_final.empty:
            df_final = df_final.sort_values(by='action_date', ascending=False)
            df_final.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Database updated: {len(df_final)} assets.")
        else:
            print("⚠️ No data.")

if __name__ == "__main__":
    GlobalPopWatch().run()
