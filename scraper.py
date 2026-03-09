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
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        }
        self.today = datetime.now()
        self.data = []

    def normalize_date(self, date_str):
        if not date_str: return None
        try:
            # Clean up noise like "Released: 12 May"
            clean_str = re.sub(r'(Released|Updated|Published|Next|Date|:)', '', str(date_str), flags=re.IGNORECASE).strip()
            # Force current year if year is missing? (Risk of wrong year). Let fuzzy handle it.
            return date_parser.parse(clean_str, fuzzy=True, dayfirst=True)
        except: return None

    # --- ADVANCED PARSERS ---

    def parse_ons(self, content, source_meta):
        """Dedicated ONS Parser (High Precision)"""
        soup = BeautifulSoup(content, 'html.parser')
        results = []
        
        # 1. Look for specific ONS "Release date" / "Next release" blocks
        meta_box = soup.find(class_='meta__item') 
        # ONS pages usually have: <p>Release date: ...</p> <p>Next release: ...</p>
        
        extracted_date = None
        status = "⚠️ Monitoring"
        
        # Text scan specifically for ONS patterns
        text = soup.get_text()
        
        match_next = re.search(r'Next release:\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)
        match_prev = re.search(r'Release date:\s*([0-9]{1,2}\s+[A-Za-z]+\s+[0-9]{4})', text, re.IGNORECASE)

        if match_next:
            extracted_date = self.normalize_date(match_next.group(1))
            status = "📅 Scheduled"
        elif match_prev:
            extracted_date = self.normalize_date(match_prev.group(1))
            status = "✅ Published"

        if extracted_date:
            results.append({
                "dataset_title": source_meta['name'],
                "source": "ONS (UK)",
                "action_date": extracted_date,
                "status": status,
                "url": source_meta['url'],
                "priority": source_meta.get('priority', 'Medium')
            })
        return results

    def parse_generic_deep_scan(self, content, source_meta):
        """Aggressive Hunter for Any Dates"""
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text(" ", strip=True) # Flatten text
        
        found_date = None
        status = "⚠️ Monitoring"

        # STRATEGY 1: META TAGS (Hidden Gold)
        # Many sites (US Census, Eurostat) hide the date in <meta>
        meta_checks = [
            {'name': 'dcterms.issued'},
            {'name': 'date'},
            {'property': 'article:published_time'},
            {'name': 'last-modified'}
        ]
        for check in meta_checks:
            tag = soup.find('meta', check)
            if tag and tag.get('content'):
                d = self.normalize_date(tag['content'])
                if d:
                    found_date = d
                    status = "✅ Published" # Meta tags are usually "Published" dates
                    break

        # STRATEGY 2: REGEX PATTERN MATCHING (If Meta failed)
        if not found_date:
            # Look for explicit future labels first
            future_regex = r'(?:Next|Upcoming|Expected|Schedule)[^0-9]{1,30}?([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4}|[A-Za-z]{3,}\s+[0-9]{4})'
            match = re.search(future_regex, text, re.IGNORECASE)
            if match:
                found_date = self.normalize_date(match.group(1))
                status = "📅 Scheduled"

        # STRATEGY 3: FALLBACK TO "LAST UPDATED"
        if not found_date:
            past_regex = r'(?:Updated|Published|Release)[^0-9]{1,30}?([0-9]{1,2}\s+[A-Za-z]{3,}\s+[0-9]{4})'
            match = re.search(past_regex, text, re.IGNORECASE)
            if match:
                found_date = self.normalize_date(match.group(1))
                status = "✅ Published"

        if found_date:
            return [{
                "dataset_title": source_meta['name'],
                "source": source_meta.get('country', 'Global'),
                "action_date": found_date,
                "status": status,
                "url": source_meta['url'],
                "priority": source_meta.get('priority', 'Medium')
            }]
        
        # Even if NO date found, add it as "Monitoring" so we know the link works
        return [{
            "dataset_title": source_meta['name'],
            "source": source_meta.get('country', 'Global'),
            "action_date": self.today, # Placeholder
            "status": "⚠️ Date Not Found",
            "url": source_meta['url'],
            "priority": "Low"
        }]

    # --- ENGINES ---
    def process_watchlist(self):
        print("🔍 Deep Scanning Watchlist...")
        with open(WATCHLIST_FILE, 'r') as f:
            config = yaml.safe_load(f)

        for item in config.get('sources', []):
            try:
                print(f"   👉 Visiting: {item['name']}...")
                resp = requests.get(item['url'], headers=self.headers, timeout=15)
                
                # ONS has a specific structure, everything else gets Deep Scanned
                if 'ons.gov.uk' in item['url']:
                    extracted = self.parse_ons(resp.content, item)
                else:
                    extracted = self.parse_generic_deep_scan(resp.content, item)
                
                if extracted:
                    self.data.extend(extracted)
                else:
                    print(f"      ⚠️ No data extracted for {item['name']}")

            except Exception as e:
                print(f"   ❌ Failed {item['name']}: {e}")

    def process_rss_news(self):
        print("📡 Scanning Media Feeds...")
        feeds = [
            ("ONS Feed", "https://www.ons.gov.uk/releasecalendar/rss"),
            ("Eurostat", "https://ec.europa.eu/eurostat/cache/RSS/rss.xml"),
            ("DHS News", "https://dhsprogram.com/rss/news.cfm")
        ]
        for source_name, url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    if any(k in entry.title.lower() for k in ['birth', 'death', 'census', 'population', 'migration']):
                        pub_date = self.normalize_date(entry.published)
                        if pub_date and (self.today - pub_date).days < 45:
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
            # Sort: Future dates at the top? Or just Chronological?
            # Let's do descending (Newest/Future first)
            df = df.sort_values(by='action_date', ascending=False)
            df.to_csv(OUTPUT_FILE, index=False)
            print(f"💾 Saved {len(df)} Assets.")
        else:
            print("⚠️ No data found.")

if __name__ == "__main__":
    bot = NewsroomScraper()
    bot.process_watchlist()
    bot.process_rss_news()
    bot.save()
