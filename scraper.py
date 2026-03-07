import feedparser
import pandas as pd
import os
import logging
from datetime import datetime
from dateutil import parser as date_parser
from bs4 import BeautifulSoup

# --- CONFIG ---
DATA_FILE = "data/releases.json"
os.makedirs("data", exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class GlobalPulseScraper:
    def __init__(self):
        self.feeds = [
            # 1. ONS (UK) - The main release feed
            {
                "country": "UK", 
                "source": "ONS", 
                "url": "https://www.ons.gov.uk/releasecalendar/rss",
                "color": "#003399"
            },
            # 2. EUROSTAT - Official Data Update Feed
            {
                "country": "EU", 
                "source": "Eurostat", 
                "url": "https://ec.europa.eu/eurostat/api/dissemination/catalogue/rss/en/statistics-update.rss",
                "color": "#FFCC00"
            },
            # 3. INSEE (France) - News & Indicators
            {
                "country": "France", 
                "source": "INSEE", 
                "url": "https://www.insee.fr/en/rss/actualites",
                "color": "#002395"
            },
            # 4. STATICE (Iceland) - News Archive
            {
                "country": "Iceland", 
                "source": "Statice", 
                "url": "https://www.statice.is/publications/news-archive/rss/",
                "color": "#D72828"
            },
            # 5. FINDATA (Finland) - Health Data News
            {
                "country": "Finland", 
                "source": "FinData", 
                "url": "https://findata.fi/en/feed/",
                "color": "#002F6C"
            },
            # 6. USAID / DHS - Program News (Catches "Cuts" or "Changes")
            {
                "country": "Global", 
                "source": "USAID/DHS", 
                "url": "https://dhsprogram.com/rss/news.cfm",
                "color": "#BA0C2F"
            }
        ]
        self.results = []
        self.today = datetime.now().date()

    def normalize_date(self, date_str):
        try:
            return date_parser.parse(date_str, fuzzy=True)
        except:
            return datetime.now()

    def clean_html(self, html_text):
        try:
            return BeautifulSoup(html_text, "html.parser").get_text()[:200] + "..."
        except:
            return html_text[:200]

    def run(self):
        print("🚀 Starting Global Pulse RSS Scan...")
        
        for feed_meta in self.feeds:
            try:
                print(f"📡 Connecting to {feed_meta['source']} ({feed_meta['url']})...")
                # Parse Feed
                feed = feedparser.parse(feed_meta['url'])
                
                if not feed.entries:
                    print(f"   ⚠️ No entries found for {feed_meta['source']}.")
                    continue
                
                print(f"   ✅ Found {len(feed.entries)} items.")
                
                # Process Top 15 items per feed
                for entry in feed.entries[:15]:
                    pub_date = self.normalize_date(getattr(entry, 'published', str(datetime.now())))
                    
                    # Logic: Is this "Fresh" (Last 30 days)?
                    diff_days = (datetime.now() - pub_date).days
                    if diff_days > 60: continue # Skip very old news
                    
                    self.results.append({
                        "title": entry.title,
                        "date": pub_date.strftime("%Y-%m-%d"),
                        "datetime": pub_date, # For sorting
                        "country": feed_meta['country'],
                        "source": feed_meta['source'],
                        "url": entry.link,
                        "summary": self.clean_html(getattr(entry, 'summary', '')),
                        "is_today": (pub_date.date() == self.today),
                        "color": feed_meta['color']
                    })
                    
            except Exception as e:
                print(f"   ❌ Error scanning {feed_meta['source']}: {e}")

        # Sort by Date (Newest First)
        if self.results:
            df = pd.DataFrame(self.results)
            df = df.sort_values(by='datetime', ascending=False)
            # Drop datetime obj before saving json
            df = df.drop(columns=['datetime']) 
            df.to_json(DATA_FILE, orient="records", indent=4)
            print(f"💾 Saved {len(df)} Real-Time Updates.")
        else:
            print("⚠️ No data collected.")

if __name__ == "__main__":
    GlobalPulseScraper().run()
