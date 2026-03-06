import requests
import feedparser
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import logging
import re
import pandas as pd
from dateutil import parser as date_parser

# --- Configuration ---
DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")
JSON_TEMP = os.path.join(DATA_DIR, "releases.tmp.json")
CSV_TEMP = os.path.join(DATA_DIR, "releases.tmp.csv")

os.makedirs(DATA_DIR, exist_ok=True)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class BaseScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        })

    def clean_text(self, text):
        if not text: return ""
        return " ".join(text.split())

    def infer_topic(self, title):
        t = title.lower()
        if any(x in t for x in ['pop', 'census', 'demog', 'birth', 'death', 'migra', 'house', 'household']): return "Demography"
        if any(x in t for x in ['gdp', 'econ', 'trade', 'financ', 'cpi', 'price', 'inflat', 'retail', 'money']): return "Economy"
        if any(x in t for x in ['employ', 'labor', 'work', 'job', 'wage', 'pay', 'vacanc', 'unemploy']): return "Labor"
        if any(x in t for x in ['health', 'disease', 'medic', 'life', 'mortal']): return "Health"
        return "General Stats"

    def normalize_date(self, date_str):
        try:
            dt = date_parser.parse(date_str, fuzzy=True)
            return dt.strftime("%Y-%m-%d")
        except:
            return None

    def scrape(self):
        raise NotImplementedError("Subclasses must implement scrape()")

# --- EUROPE (Aggregated via Eurostat) ---
class Eurostat_Scraper(BaseScraper):
    """
    Covering: EU, Eurozone, Germany, France, Spain, Italy, etc.
    Source: Eurostat Release Calendar
    """
    def scrape(self):
        url = "https://ec.europa.eu/eurostat/news/release-calendar"
        events = []
        try:
            logging.info("🇪🇺 Scraping Eurostat (EU)...")
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            rows = soup.find_all('tr')
            current_date = None
            
            for row in rows:
                header = row.find(['th', 'td'])
                if header and re.search(r'\d{2}-\d{2}-\d{4}', header.text):
                    current_date = self.normalize_date(header.text)
                
                cols = row.find_all('td')
                if current_date and len(cols) >= 1:
                    title = self.clean_text(cols[-1].text)
                    # Eurostat often lists the country in the title, e.g., "GDP - Germany"
                    # If not, it defaults to "EU"
                    country = "EU (Eurostat)"
                    if "germany" in title.lower(): country = "Germany"
                    elif "france" in title.lower(): country = "France"
                    elif "spain" in title.lower(): country = "Spain"
                    elif "italy" in title.lower(): country = "Italy"
                    
                    if title:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": country,
                            "region": "Europe",
                            "source": "Eurostat",
                            "summary": "Official Eurostat Harmonized Release",
                            "url": url,
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"Eurostat Error: {e}")
            return []

# --- NORTH AMERICA ---
class US_BLS_Scraper(BaseScraper):
    """
    Covering: USA (Inflation, Employment)
    Source: Bureau of Labor Statistics Schedule
    """
    def scrape(self):
        url = "https://www.bls.gov/schedule/news_release/2026_sched.htm" # Fallback to current year logic needed usually
        # For demo reliability, we check the main schedule page
        url = "https://www.bls.gov/schedule/news_release/"
        events = []
        try:
            logging.info("🇺🇸 Scraping US BLS...")
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            # BLS is tricky, often best to just grab the "Upcoming" list if available
            # Or parse the main table
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    if len(cols) >= 3:
                        date_text = self.clean_text(cols[0].text) # e.g. "Feb. 12"
                        time_text = self.clean_text(cols[1].text)
                        title = self.clean_text(cols[2].text)
                        
                        # Handle Date (BLS often omits year in tables, assumes current/next)
                        if re.search(r'[A-Z][a-z]{2}\.?\s\d{1,2}', date_text):
                            # Append current year for parsing
                            full_date = f"{date_text} {datetime.now().year}"
                            date_str = self.normalize_date(full_date)
                            
                            if date_str and title:
                                events.append({
                                    "title": title,
                                    "start": date_str,
                                    "country": "USA",
                                    "region": "Americas",
                                    "source": "US BLS",
                                    "summary": f"US Bureau of Labor Statistics: {title}",
                                    "url": "https://www.bls.gov/schedule/news_release/",
                                    "topic": self.infer_topic(title)
                                })
            return events
        except Exception as e:
            logging.error(f"BLS Error: {e}")
            return []

class StatCan_Scraper(BaseScraper):
    def scrape(self):
        url = "https://www150.statcan.gc.ca/n1/dai-quo/cal2-eng.htm"
        events = []
        try:
            logging.info("🇨🇦 Scraping StatCan...")
            resp = self.session.get(url, timeout=15)
            soup = BeautifulSoup(resp.content, 'html.parser')
            main_content = soup.find('main') or soup
            
            current_date = None
            current_year = datetime.now().year
            
            for element in main_content.find_all(['h2', 'h3', 'li']):
                text = self.clean_text(element.get_text())
                if re.match(r'^(January|February|March|April|May|June|July|August|September|October|November|December) \d{1,2}$', text):
                    try:
                        dt = date_parser.parse(f"{text} {current_year}")
                        current_date = dt.strftime("%Y-%m-%d")
                    except: pass
                elif current_date and element.name == 'li':
                    title = self.clean_text(element.text)
                    if len(title) > 5:
                        events.append({
                            "title": title,
                            "start": current_date,
                            "country": "Canada",
                            "region": "Americas",
                            "source": "StatCan",
                            "summary": "The Daily Release",
                            "url": "https://www150.statcan.gc.ca/n1/dai-quo/index-eng.htm",
                            "topic": self.infer_topic(title)
                        })
            return events
        except Exception as e:
            logging.error(f"StatCan Error: {e}")
            return []

# --- SOUTH AMERICA ---
class Brazil_IBGE_Scraper(BaseScraper):
    def scrape(self):
        # Brazil IBGE Calendar (Simulated Logic as scraping IBGE's dynamic JS calendar is brittle)
        # In a production app, we would hit their API: https://agenciadenoticias.ibge.gov.br/
        # For this prototype, we create a placeholder based on known monthly schedules
        events = []
        logging.info("🇧🇷 Generating Brazil IBGE Schedule...")
        
        # Simulating standard monthly releases
        today = datetime.now()
        for i in range(3): # Next 3 months
            month_offset = today.month + i
            year_offset = today.year + (month_offset // 13)
            month_offset = month_offset % 12 or 12
            
            # Inflation (IPCA) usually around 10th
            d_ipca = datetime(year_offset, month_offset, 10)
            if d_ipca.weekday() > 4: d_ipca += timedelta(days=2) # Push to Monday if weekend
            
            events.append({
                "title": "Extended National Consumer Price Index (IPCA)",
                "start": d_ipca.strftime("%Y-%m-%d"),
                "country": "Brazil",
                "region": "Americas",
                "source": "IBGE",
                "summary": "Official inflation data for Brazil.",
                "url": "https://www.ibge.gov.br/en/statistics-release-calendar.html",
                "topic": "Economy"
            })
            
            # Unemployment (PNAD) usually end of month
            d_pnad = datetime(year_offset, month_offset, 28)
            if d_pnad.weekday() > 4: d_pnad -= timedelta(days=2)
            
            events.append({
                "title": "Continuous PNAD (Unemployment)",
                "start": d_pnad.strftime("%Y-%m-%d"),
                "country": "Brazil",
                "region": "Americas",
                "source": "IBGE",
                "summary": "National Household Sample Survey - Unemployment rate.",
                "url": "https://www.ibge.gov.br/en/statistics-release-calendar.html",
                "topic": "Labor"
            })
            
        return events

# --- ASIA ---
class Japan_Stat_Scraper(BaseScraper):
    def scrape(self):
        # Japan Statistics Bureau (Standard Monthly Pattern)
        # CPI is usually released on the Friday of the week containing the 19th
        events = []
        logging.info("🇯🇵 Generating Japan Stat Schedule...")
        
        today = datetime.now()
        for i in range(3):
            month = today.month + i
            year = today.year
            if month > 12: 
                month -= 12
                year += 1
            
            # Estimate CPI Release (approx 20th of month)
            cpi_date = datetime(year, month, 20)
            events.append({
                "title": "Consumer Price Index (CPI)",
                "start": cpi_date.strftime("%Y-%m-%d"),
                "country": "Japan",
                "region": "Asia",
                "source": "StatJapan",
                "summary": "Japan Nationwide CPI release.",
                "url": "https://www.stat.go.jp/english/data/cpi/1581.html",
                "topic": "Economy"
            })
            
             # Labor Force Survey (approx 30th)
            lab_date = datetime(year, month, 28)
            events.append({
                "title": "Labor Force Survey",
                "start": lab_date.strftime("%Y-%m-%d"),
                "country": "Japan",
                "region": "Asia",
                "source": "StatJapan",
                "summary": "Monthly employment and unemployment statistics.",
                "url": "https://www.stat.go.jp/english/data/roudou/index.html",
                "topic": "Labor"
            })
            
        return events

class China_NBS_Scraper(BaseScraper):
    def scrape(self):
        # NBS China - Release Calendar URL
        url = "http://www.stats.gov.cn/english/PressRelease/ReleaseCalendar/"
        # China's site is often static or PDF based. 
        # We will add a recurring "15th of month" logic which is their standard for major data (Retail, Industrial, GDP)
        events = []
        logging.info("🇨🇳 Generating China NBS Schedule...")
        
        today = datetime.now()
        for i in range(3):
            month = today.month + i
            year = today.year
            if month > 12: 
                month -= 12
                year += 1
                
            # Major Economic Data Bundle (usually 15th-18th)
            release_date = datetime(year, month, 15)
            if release_date.weekday() > 4: release_date += timedelta(days=2)
            
            events.append({
                "title": "National Economic Performance (Industrial, Retail, Investment)",
                "start": release_date.strftime("%Y-%m-%d"),
                "country": "China",
                "region": "Asia",
                "source": "NBS China",
                "summary": "Monthly release of Industrial Production, Retail Sales, and Fixed Asset Investment.",
                "url": "http://www.stats.gov.cn/english/",
                "topic": "Economy"
            })
            
            # CPI/PPI (usually 9th-10th)
            cpi_date = datetime(year, month, 9)
            if cpi_date.weekday() > 4: cpi_date += timedelta(days=2)
            
            events.append({
                "title": "Consumer Price Index (CPI) & PPI",
                "start": cpi_date.strftime("%Y-%m-%d"),
                "country": "China",
                "region": "Asia",
                "source": "NBS China",
                "summary": "Monthly inflation data.",
                "url": "http://www.stats.gov.cn/english/",
                "topic": "Economy"
            })

        return events

class ONS_Scraper(BaseScraper):
    def scrape(self):
        # UK ONS Scraper (Simplified for brevity, same logic as before)
        base_url = "https://www.ons.gov.uk/releasecalendar"
        events = []
        for page in range(1, 3):
            try:
                resp = self.session.get(f"{base_url}?page={page}", timeout=10)
                soup = BeautifulSoup(resp.content, 'html.parser')
                items = soup.select('.release__item')
                for item in items:
                    title_elem = item.select_one('h3 a')
                    date_elem = item.select_one('.release__date')
                    if title_elem and date_elem:
                        title = self.clean_text(title_elem.text)
                        date_str = self.normalize_date(self.clean_text(date_elem.text).replace("Release date:", ""))
                        if date_str:
                            events.append({
                                "title": title,
                                "start": date_str,
                                "country": "UK",
                                "region": "Europe",
                                "source": "ONS",
                                "summary": f"Official UK Release: {title}",
                                "url": "https://www.ons.gov.uk" + title_elem['href'],
                                "topic": self.infer_topic(title)
                            })
            except: pass
        return events

# --- EXECUTION ---
def run_scrapers():
    scrapers = [
        Eurostat_Scraper(),
        US_BLS_Scraper(),
        StatCan_Scraper(),
        Brazil_IBGE_Scraper(),
        Japan_Stat_Scraper(),
        China_NBS_Scraper(),
        ONS_Scraper()
    ]
    
    all_data = []
    scrape_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print("🚀 Starting Global Scrape...")
    for scraper in scrapers:
        try:
            data = scraper.scrape()
            print(f"✅ {scraper.__class__.__name__}: Found {len(data)} items.")
            for item in data: item['scraped_at'] = scrape_time
            all_data.extend(data)
        except Exception as e:
            print(f"❌ {scraper.__class__.__name__} Failed: {e}")

    # Deduplicate & Filter (30 days back, 180 days forward)
    unique = {f"{x['start']}_{x['title']}": x for x in all_data}.values()
    
    today = datetime.now()
    min_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    max_date = (today + timedelta(days=180)).strftime("%Y-%m-%d")
    
    filtered = [x for x in unique if min_date <= x['start'] <= max_date]

    # Atomic Save
    with open(JSON_TEMP, 'w') as f: json.dump(filtered, f, indent=4)
    pd.DataFrame(filtered).to_csv(CSV_TEMP, index=False)
    os.replace(JSON_TEMP, JSON_FILE)
    os.replace(CSV_TEMP, CSV_FILE)
    print(f"💾 Saved {len(filtered)} global releases.")

if __name__ == "__main__":
    run_scrapers()
