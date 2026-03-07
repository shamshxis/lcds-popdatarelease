import pandas as pd
import requests
import feedparser
import eurostat
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser as date_parser
import logging

# --- CONFIG ---
DATA_FILE = "data/releases.json"
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

class PrecisionScraper:
    def __init__(self):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        self.results = []

    def normalize_date(self, d):
        try: return date_parser.parse(str(d), fuzzy=True).strftime("%Y-%m-%d")
        except: return None

    # --- 1. ONS (UK) - DEATHS WEEKLY ---
    def scrape_ons(self):
        # ONS often puts the "Next release" date in the meta metadata of the product page
        target = {
            "title": "Deaths registered weekly in England and Wales",
            "url": "https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/deaths/bulletins/deathsregisteredweeklyinenglandandwales/latest",
            "source": "ONS",
            "country": "UK"
        }
        try:
            resp = requests.get(target['url'], headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # ONS Structure: <span class="release__date">Next release: 12 March 2026</span>
            # Or text searching "Next release"
            meta_box = soup.find("div", class_="meta__item")
            if meta_box and "Next release" in meta_box.text:
                date_text = meta_box.text.replace("Next release:", "").strip()
                target['start'] = self.normalize_date(date_text)
                target['status'] = "🟢 CONFIRMED"
                self.results.append(target)
            else:
                # Fallback: Scrape the general calendar for this title
                self.results.append({**target, "start": "Check Site", "status": "⚠️ UNKNOWN"})
        except Exception as e:
            logging.error(f"ONS failed: {e}")

    # --- 2. EUROSTAT - EXCESS MORTALITY ---
    def scrape_eurostat(self):
        # Use the official API to find the next update for "demo_mexrt" (Excess Mortality)
        try:
            # Note: Eurostat API gives *last* update. For *next* release, we use the calendar XML.
            cal_url = "https://ec.europa.eu/eurostat/cache/RELEASE_CALENDAR/calendar_en.xml"
            resp = requests.get(cal_url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'xml')
            
            found = False
            for item in soup.find_all('release'):
                title = item.find('title').text
                if "excess mortality" in title.lower():
                    d = item.find('release_date').text
                    self.results.append({
                        "title": title,
                        "start": self.normalize_date(d),
                        "source": "Eurostat",
                        "country": "EU",
                        "url": "https://ec.europa.eu/eurostat/statistics-explained/index.php?title=Excess_mortality_statistics",
                        "status": "🟢 CONFIRMED"
                    })
                    found = True
            
            if not found:
                 self.results.append({
                    "title": "Excess Mortality (demo_mexrt)",
                    "start": "Mid-Month (Est)", # Eurostat releases mid-month usually
                    "source": "Eurostat",
                    "country": "EU",
                    "url": "https://ec.europa.eu/eurostat/web/main/data/database",
                    "status": "🟡 ESTIMATED"
                })
        except Exception as e:
            logging.error(f"Eurostat failed: {e}")

    # --- 3. INSEE (FRANCE) - DEMOGRAPHY ---
    def scrape_insee(self):
        url = "https://www.insee.fr/en/information/2107811" # Release calendar
        try:
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Iterate list items
            for item in soup.select(".liste-publications li"):
                text = item.text.strip()
                if any(x in text.lower() for x in ["births", "deaths", "mortality", "population"]):
                    # Extract date (usually at start or end)
                    # Regex for dd/mm/yyyy
                    match = re.search(r'\d{2}/\d{2}/\d{4}', text)
                    if match:
                        self.results.append({
                            "title": text.split("\n")[0][:50] + "...",
                            "start": self.normalize_date(match.group(0)),
                            "source": "INSEE",
                            "country": "France",
                            "url": url,
                            "status": "🟢 CONFIRMED"
                        })
        except Exception as e:
             logging.error(f"INSEE failed: {e}")

    # --- 4. STATICE (ICELAND) ---
    def scrape_statice(self):
        url = "https://www.statice.is/publications/news-archive/advance-release-calendar/"
        try:
            resp = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(resp.content, 'html.parser')
            
            # Statice uses a Table
            rows = soup.select("table tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 2:
                    date_txt = cols[0].text.strip()
                    title_txt = cols[1].text.strip()
                    
                    if any(x in title_txt.lower() for x in ["population", "death", "migration"]):
                         self.results.append({
                            "title": title_txt,
                            "start": self.normalize_date(date_txt),
                            "source": "Statice",
                            "country": "Iceland",
                            "url": url,
                            "status": "🟢 CONFIRMED"
                        })
        except Exception as e:
            logging.error(f"Statice failed: {e}")

    # --- 5. FINDATA (FINLAND) ---
    def scrape_findata(self):
        # FinData RSS is reliable
        try:
            feed = feedparser.parse("https://findata.fi/en/feed/")
            for entry in feed.entries:
                self.results.append({
                    "title": entry.title,
                    "start": self.normalize_date(entry.published),
                    "source": "FinData",
                    "country": "Finland",
                    "url": entry.link,
                    "status": "🔵 NEWS" # Blue for News items (not future releases)
                })
        except: pass

    def run(self):
        print("🚀 Starting Precision Scraper...")
        self.scrape_ons()
        self.scrape_eurostat()
        self.scrape_insee()
        self.scrape_statice()
        self.scrape_findata()
        
        # Save
        df = pd.DataFrame(self.results)
        df.to_json(DATA_FILE, orient="records", indent=4)
        print(f"✅ Saved {len(df)} LCDS-Relevant Datasets.")

if __name__ == "__main__":
    PrecisionScraper().run()
