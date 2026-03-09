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

# --- CONFIG ---
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
WATCHLIST_FILE = "watchlist.yml"
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")

class LCDSScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        self.today = datetime.now()
        self.data = []

    def normalize_date(self, date_str):
        if not date_str: return None
        try:
            return date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
        except: return None

    # --- PRONG 1: WATCHLIST (Future Scanning) ---
    def process_watchlist(self):
        print("🔍 Scanning Watchlist Targets...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)

        for item in config.get('sources', []):
            try:
                print(f"   👉 Visiting: {item['name']}...")
                resp = requests.get(item['url'], headers=self.headers, timeout=15)
                soup = BeautifulSoup(resp.content, 'html.parser')
                text = soup.get_text()

                # STRATEGY: Look for "Next Release" or "Last Updated"
                next_date = None
                status = "⚠️ Monitoring"
                
                # Regex 1: "Next release: 15 May 2026"
                match_next = re.search(r'(?:Next release|Next update|Upcoming release)[^0-9]*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)
                
                # Regex 2: "Release date: 15 May 2025" (Past/Current)
                match_prev = re.search(r'(?:Release date|Published|Last updated)[^0-9]*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)

                date_found = None
                
                if match_next:
                    date_found = match_next.group(1)
                    status = "📅 Scheduled"
                elif match_prev:
                    date_found = match_prev.group(1)
                    # If date is very recent, mark as Published
                    d_obj = self.normalize_date(date_found)
                    if d_obj and (self.today - d_obj).days < 30:
                        status = "✅ Published"
                    else:
                        status = "✅ Published (Older)"

                if date_found:
                    self.data.append({
                        "dataset_title": item['name'],
                        "source": item.get('country', 'Global'),
                        "action_date": self.normalize_date(date_found),
                        "status": status,
                        "url": item['url'],
                        "priority": item.get('priority', 'Medium')
                    })
                    
            except Exception as e:
                print(f"   ❌ Failed {item['name']}: {e}")

    # --- PRONG 2: NEWS / RSS (Media Capture) ---
    def process_news(self):
        print("📡 Capturing Media Updates...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)

        for feed_meta in config.get('feeds', []):
            try:
                feed = feedparser.parse(feed_meta['url'])
                for entry in feed.entries[:5]: # Top 5 only
                    # Filter for Demography Keywords
                    if any(k in entry.title.lower() for k in ['birth', 'death', 'population', 'migration', 'census', 'life expect', 'fertility']):
                        
                        pub_date = self.normalize_date(entry.published)
                        # Only keep last 60 days
                        if pub_date and (self.today - pub_date).days < 60:
                            self.data.append({
                                "dataset_title": entry.title,
                                "source": feed_meta['name'],
                                "action_date": pub_date,
                                "status": "📢 Announcement",
                                "url": entry.link,
                                "priority": "High"
                            })
            except: pass

    def save(self):
        df = pd.DataFrame(self.data)
        if not df.empty:
            # Drop None dates
            df = df.dropna(subset=['action_date'])
            # Sort by Date
            df = df.sort_values(by='action_date', ascending=False)
            
            # Filter: ±1 Year Logic
            start_window = self.today - timedelta(days=365)
            end_window = self.today + timedelta(days=365)
            df = df[(df['action_date'] >= start_window) & (df['action_date'] <= end_window)]
            
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Saved {len(df)} LCDS Assets.")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    s = LCDSScraper()
    s.process_watchlist()
    s.process_news()
    s.save()
