import pandas as pd
import feedparser
import requests
import os
import json
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# --- CONFIGURATION ---
DATA_DIR = "data"
STATE_FILE = os.path.join(DATA_DIR, "current_state.json")
HISTORY_FILE = os.path.join(DATA_DIR, "change_log.csv")
os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- 1. THE AGENTS (Native APIs & Feeds) ---
class WatchtowerAgents:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (ResearchBot/1.0)'}

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True).strftime("%Y-%m-%d")
        except: return datetime.now().strftime("%Y-%m-%d")

    # --- AGENT: USAID / DHS (News Feed) ---
    def fetch_usaid(self):
        # Catches major announcements like data cuts
        return self._parse_feed("https://dhsprogram.com/rss/news.cfm", "USAID/DHS", "Global Health")

    # --- AGENT: INSEE FRANCE (Official Feed) ---
    def fetch_insee(self):
        return self._parse_feed("https://www.insee.fr/en/rss/actualites", "INSEE (France)", "National Stats")

    # --- AGENT: STATISTICS ICELAND (Official Feed) ---
    def fetch_statice(self):
        return self._parse_feed("https://www.statice.is/rss", "Statice (Iceland)", "Demography")

    # --- AGENT: FINDATA (Finland Social/Health) ---
    def fetch_findata(self):
        return self._parse_feed("https://findata.fi/en/feed/", "FinData", "Health Registry")

    # --- AGENT: ONS (UK) ---
    def fetch_ons(self):
        # Tracking "News" to catch policy changes/cuts, not just data releases
        return self._parse_feed("https://www.ons.gov.uk/news/rss", "ONS (UK)", "Policy & Data")

    # --- AGENT: EUROSTAT (Alerts) ---
    def fetch_eurostat(self):
        return self._parse_feed("https://ec.europa.eu/eurostat/cache/RSS/rss.xml", "Eurostat", "EU Stats")

    # --- AGENT: CBS NETHERLANDS (News) ---
    def fetch_cbs(self):
        return self._parse_feed("https://www.cbs.nl/en-gb/service/news-releases-rss", "CBS (Netherlands)", "National Stats")

    # --- HELPER: FEED PARSER ---
    def _parse_feed(self, url, source, default_type):
        items = []
        try:
            feed = feedparser.parse(url)
            for entry in feed.entries:
                title = entry.title
                link = entry.link
                # Create a stable ID. For RSS, link is usually best.
                uid = link 
                
                items.append({
                    "id": uid,
                    "source": source,
                    "date": self.normalize_date(entry.published if 'published' in entry else datetime.now()),
                    "type": default_type,
                    "description": title,
                    "link": link
                })
        except Exception as e:
            logging.error(f"Failed {source}: {e}")
        return items

# --- 2. THE LOGIC (State Diffing) ---
def run_watchtower():
    print("🚀 Starting Watchtower Engine...")
    agents = WatchtowerAgents()
    
    # A. FETCH CURRENT STATE (The "Now")
    current_data = []
    current_data.extend(agents.fetch_usaid())
    current_data.extend(agents.fetch_insee())
    current_data.extend(agents.fetch_statice())
    current_data.extend(agents.fetch_findata())
    current_data.extend(agents.fetch_ons())
    current_data.extend(agents.fetch_eurostat())
    current_data.extend(agents.fetch_cbs())
    
    print(f"📥 Fetched {len(current_data)} total items from live feeds.")

    # B. LOAD PREVIOUS STATE (The "Then")
    prev_state = {}
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                loaded = json.load(f)
                # Map ID -> Item for fast lookup
                prev_state = {item['id']: item for item in loaded}
        except: pass

    # C. CALCULATE DIFF (The "What Changed")
    updates = []
    current_ids = set(item['id'] for item in current_data)
    
    # 1. Detect RELEASES (New ID)
    for item in current_data:
        if item['id'] not in prev_state:
            item['status'] = "🔴 RELEASE" 
            updates.append(item)
        else:
            # 2. Detect UPDATES (Metadata change)
            old = prev_state[item['id']]
            if old['description'] != item['description']:
                 item['status'] = "🟡 UPDATE"
                 updates.append(item)

    # 3. Detect DELETIONS (ID missing from source)
    # *Note: For RSS, items expire naturally. We only flag 'Deletion' if we were tracking an API list.
    # For now, we implicitly handle deletions by overwriting the state file.
    
    # D. SAVE HISTORY
    if updates:
        df_new = pd.DataFrame(updates)
        # Select clean columns
        df_new = df_new[['status', 'source', 'date', 'type', 'description', 'link']]
        
        # Append to CSV
        header = not os.path.exists(HISTORY_FILE)
        df_new.to_csv(HISTORY_FILE, mode='a', header=header, index=False)
            
        print(f"✅ Logged {len(updates)} new events to {HISTORY_FILE}")
    else:
        print("💤 No new changes detected.")

    # E. OVERWRITE STATE (Reset for next run)
    with open(STATE_FILE, 'w') as f:
        json.dump(current_data, f, indent=4)

if __name__ == "__main__":
    run_watchtower()
