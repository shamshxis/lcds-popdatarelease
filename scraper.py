import pandas as pd
import feedparser
import requests
import os
import json
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# --- CONFIG ---
DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "current_state.json")
HISTORY_FILE = os.path.join(DATA_DIR, "change_log.csv")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. THE AGENTS (Native APIs & Feeds Only) ---

class WatchtowerAgents:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (ResearchBot/1.0)'}

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True).strftime("%Y-%m-%d")
        except: return datetime.now().strftime("%Y-%m-%d")

    def fetch_usaid_dhs(self):
        """Catches major global health data news (like the cuts)"""
        url = "https://dhsprogram.com/rss/news.cfm"
        return self._parse_feed(url, "USAID/DHS", "Global Health")

    def fetch_insee_france(self):
        """French National Statistics (INSEE)"""
        url = "https://www.insee.fr/en/rss/actualites"
        return self._parse_feed(url, "INSEE (France)", "General Stats")

    def fetch_statice_iceland(self):
        """Statistics Iceland"""
        url = "https://www.statice.is/rss"
        return self._parse_feed(url, "Statice (Iceland)", "Demography")

    def fetch_findata_finland(self):
        """FinData (Social & Health Data Permit Authority)"""
        url = "https://findata.fi/en/feed/"
        return self._parse_feed(url, "FinData", "Health Registry")

    def fetch_ons_news(self):
        """ONS (UK) - Focusing on News/Announcements to catch policy changes"""
        url = "https://www.ons.gov.uk/news/rss"
        return self._parse_feed(url, "ONS (UK)", "Policy & Data")

    def fetch_eurostat_alerts(self):
        """Eurostat Alerts"""
        url = "https://ec.europa.eu/eurostat/cache/RSS/rss.xml"
        return self._parse_feed(url, "Eurostat", "EU Stats")

    def _parse_feed(self, url, source, default_type):
        items = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                # Basic Keyword Filter (can be expanded)
                title = entry.title
                link = entry.link
                desc = entry.summary if 'summary' in entry else ""
                date = self.normalize_date(entry.published if 'published' in entry else datetime.now())
                
                # Create a unique ID for tracking
                uid = f"{source}_{title[:30]}"
                
                items.append({
                    "id": uid,
                    "source": source,
                    "date": date,
                    "type": default_type,
                    "description": title, # Title often contains the core news
                    "link": link,
                    "raw_desc": desc
                })
        except Exception as e:
            logging.error(f"Failed {source}: {e}")
        return items

# --- 2. THE LOGIC (State Diffing) ---

def run_watchtower():
    print("🚀 Starting Watchtower...")
    agents = WatchtowerAgents()
    
    # 1. FETCH CURRENT STATE
    current_data = []
    current_data.extend(agents.fetch_usaid_dhs())
    current_data.extend(agents.fetch_insee_france())
    current_data.extend(agents.fetch_statice_iceland())
    current_data.extend(agents.fetch_findata_finland())
    current_data.extend(agents.fetch_ons_news())
    current_data.extend(agents.fetch_eurostat_alerts())
    
    print(f"📥 Fetched {len(current_data)} total items.")

    # 2. LOAD PREVIOUS STATE
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            prev_state = {item['id']: item for item in json.load(f)}
    else:
        prev_state = {}

    # 3. CALCULATE DIFF (The "What's Happening" Logic)
    updates = []
    current_ids = set(item['id'] for item in current_data)
    
    # Check for NEW items
    for item in current_data:
        if item['id'] not in prev_state:
            item['status'] = "🔴 RELEASE" # New Release
            updates.append(item)
        else:
            # Check for UPDATES (e.g. Link changed, Description changed)
            old = prev_state[item['id']]
            if old['link'] != item['link']:
                 item['status'] = "🟡 UPDATE"
                 updates.append(item)

    # Check for DELETIONS (Missing from feed)
    # Note: Feeds naturally expire items, so we must be careful calling it "Deletion"
    # But for an API, this would be a deletion. For RSS, we track "Archived".
    # We won't log deletions for RSS to avoid noise, but we UPDATE the state file.

    # 4. SAVE HISTORY
    if updates:
        df_new = pd.DataFrame(updates)
        # Columns: Status | Source | Date | Type | Description | Link
        df_new = df_new[['status', 'source', 'date', 'type', 'description', 'link']]
        
        # Append to CSV History
        if os.path.exists(HISTORY_FILE):
            df_new.to_csv(HISTORY_FILE, mode='a', header=False, index=False)
        else:
            df_new.to_csv(HISTORY_FILE, mode='w', header=True, index=False)
            
        print(f"✅ Added {len(updates)} new events to history.")
    else:
        print("💤 No changes detected.")

    # 5. UPDATE STATE FILE (Overwrite)
    with open(STATE_FILE, 'w') as f:
        json.dump(current_data, f, indent=4)

if __name__ == "__main__":
    run_watchtower()
