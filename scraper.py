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

class NewsroomScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        self.today = datetime.now()
        self.data = []

    def normalize_date(self, date_str):
        if not date_str: return None
        try:
            # Handle "May 2026" -> "2026-05-01"
            return date_parser.parse(str(date_str), fuzzy=True, dayfirst=True)
        except: return None

    def clean_text(self, text):
        return re.sub(r'\s+', ' ', text).strip()

    # --- PARSERS ---

    def parse_ons(self, content, source_meta):
        """Special logic for ONS Release Calendar structure"""
        soup = BeautifulSoup(content, 'html.parser')
        results = []
        
        # ONS uses .release__item or similar lists
        items = soup.find_all(class_='release__item')
        if not items: # Fallback to generic text search
            return self.parse_generic(content, source_meta)

        for item in items:
            try:
                title_tag = item.find('h3')
                if not title_tag: continue
                title = self.clean_text(title_tag.get_text())
                
                # Check Keywords/Themes from YAML
                keywords = source_meta.get('keywords', []) + source_meta.get('themes', [])
                if keywords and not any(k.lower() in title.lower() for k in keywords):
                    continue

                date_tag = item.find(class_='release__date')
                date_str = self.clean_text(date_tag.get_text()) if date_tag else ""
                release_date = self.normalize_date(date_str)

                if release_date:
                    status = "📅 Scheduled" if release_date > self.today else "✅ Published"
                    link = item.find('a')['href']
                    if link.startswith('/'): link = f"https://www.ons.gov.uk{link}"
                    
                    results.append({
                        "dataset_title": title,
                        "source": source_meta['name'],
                        "action_date": release_date,
                        "status": status,
                        "url": link,
                        "priority": "High"
                    })
            except: continue
        return results

    def parse_generic(self, content, source_meta):
        """Regex Hunter for 'Next Release' on any website"""
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text()
        results = []
        
        # 1. Hunt for Future Dates (Next Release)
        # Regex: "Next release: [Date]" or "Upcoming: [Date]"
        match_future = re.search(r'(?:Next release|Next update|Upcoming|Expected)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}|[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)
        
        # 2. Hunt for Past Dates (Last Updated)
        match_past = re.search(r'(?:Last update|Published|Released)[:\s]+([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4}|[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)

        target_date = None
        status = "⚠️ Monitoring"

        if match_future:
            target_date = self.normalize_date(match_future.group(1))
            status = "📅 Scheduled"
        elif match_past:
            target_date = self.normalize_date(match_past.group(1))
            status = "✅ Published"

        if target_date:
             results.append({
                "dataset_title": source_meta['name'], # Use Source Name as Title for generic pages
                "source": source_meta['country'],     # Use Country as Source Column
                "action_date": target_date,
                "status": status,
                "url": source_meta['url'],
                "priority": "Medium"
            })
        
        return results

    # --- ENGINES ---

    def process_watchlist(self):
        print("🔍 Scanning Watchlist...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)

        for item in config.get('sources', []):
            try:
                print(f"   👉 Visiting: {item['name']}...")
                resp = requests.get(item['url'], headers=self.headers, timeout=15)
                
                # Switch Parser based on YAML 'parser' key
                parser_type = item.get('parser', 'simple_page')
                
                extracted = []
                if 'ons_release_calendar' in parser_type:
                    extracted = self.parse_ons(resp.content, item)
                else:
                    # Default to Generic Regex Hunter
                    extracted = self.parse_generic(resp.content, item)
                
                self.data.extend(extracted)

            except Exception as e:
                print(f"   ❌ Failed {item['name']}: {e}")

    def process_rss_news(self):
        """Scans RSS feeds for Announcements"""
        print("📡 Scanning Media Feeds...")
        # Hardcoded High-Value Feeds (You can add these to YAML later)
        feeds = [
            ("ONS Feed", "https://www.ons.gov.uk/releasecalendar/rss"),
            ("Eurostat", "https://ec.europa.eu/eurostat/cache/RSS/rss.xml"),
            ("DHS News", "https://dhsprogram.com/rss/news.cfm")
        ]

        for source_name, url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    # Filter for Population keywords
                    if any(k in entry.title.lower() for k in ['birth', 'death', 'census', 'population', 'migration', 'fertility']):
                        
                        pub_date = self.normalize_date(entry.published)
                        if pub_date and (self.today - pub_date).days < 30:
                            self.data.append({
                                "dataset_title": entry.title,
                                "source": source_name,
                                "action_date": pub_date,
                                "status": "📢 Announcement",
                                "url": entry.link,
                                "priority": "High"
                            })
            except: pass

    def save(self):
        df = pd.DataFrame(self.data)
        if not df.empty:
            df = df.dropna(subset=['action_date'])
            df = df.sort_values(by='action_date', ascending=False)
            
            # Filter: ±1 Year Logic
            start_window = self.today - timedelta(days=365)
            end_window = self.today + timedelta(days=365)
            df = df[(df['action_date'] >= start_window) & (df['action_date'] <= end_window)]
            
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Saved {len(df)} Assets to {OUTPUT_FILE}")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    bot = NewsroomScraper()
    bot.process_watchlist()
    bot.process_rss_news()
    bot.save()
