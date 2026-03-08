import requests
import pandas as pd
import yaml
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from dateutil import parser
from pathlib import Path

DATA_FILE = "data/dataset_tracker.csv"
HISTORY_FILE = "data/dataset_tracker_history.csv"

TODAY = datetime.utcnow()

six_months_back = TODAY - timedelta(days=180)

def load_sources():
    with open("watchlist.yml") as f:
        return yaml.safe_load(f)["sources"]


def scrape_page(source):

    results = []

    try:
        r = requests.get(source["url"], timeout=30)
        soup = BeautifulSoup(r.text, "html.parser")

        text = soup.get_text("\n")

        for line in text.split("\n"):

            if any(x in line.lower() for x in ["release", "update", "dataset"]):

                entry = {
                    "source": source["name"],
                    "country": source["country"],
                    "url": source["url"],
                    "summary": line.strip()[:200],
                    "announcement_date": TODAY.date(),
                    "action_date": None
                }

                try:
                    date = parser.parse(line, fuzzy=True)
                    entry["action_date"] = date.date()
                except:
                    pass

                results.append(entry)

    except:
        pass

    return results


def run_scraper():

    sources = load_sources()

    all_results = []

    for source in sources:
        rows = scrape_page(source)
        all_results.extend(rows)

    df = pd.DataFrame(all_results)

    if df.empty:
        return

    Path("data").mkdir(exist_ok=True)

    if Path(DATA_FILE).exists():
        existing = pd.read_csv(DATA_FILE)
    else:
        existing = pd.DataFrame()

    combined = pd.concat([existing, df]).drop_duplicates()

    combined["announcement_date"] = pd.to_datetime(combined["announcement_date"])

    combined = combined[
        combined["announcement_date"] > six_months_back
    ]

    combined.to_csv(DATA_FILE, index=False)

    history = combined.copy()
    history["scrape_date"] = TODAY

    if Path(HISTORY_FILE).exists():
        old_history = pd.read_csv(HISTORY_FILE)
        history = pd.concat([old_history, history])

    history.to_csv(HISTORY_FILE, index=False)


if __name__ == "__main__":
    run_scraper()
