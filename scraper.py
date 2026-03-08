import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

# --- CONSTANTS ---
DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/3.2; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- CONFIG ---
# Junk phrases to identify non-dataset links
BAD_TITLES = [
    "click here", "read more", "download", "pdf", "csv", "xlsx", 
    "view", "more info", "accessibility", "privacy policy", 
    "cookies", "contact us", "home", "search", "filter", "help",
    "terms and conditions", "about us", "careers"
]

# --- FUNCTIONS ---

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_watchlist() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with open("watchlist.yml", "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("settings", {}), raw.get("sources", [])

def get_headers(settings: dict[str, Any]) -> dict[str, str]:
    return {"User-Agent": settings.get("user_agent", DEFAULT_HEADERS["User-Agent"])}

def fetch_html(url: str, settings: dict[str, Any]) -> str:
    time.sleep(1.0)
    try:
        response = requests.get(
            url,
            headers=get_headers(settings),
            timeout=int(settings.get("request_timeout_seconds", 25)),
        )
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"  !! Failed to fetch {url}: {e}")
        return ""

def clean_text(value: str) -> str:
    if not value: return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ")).strip()

def extract_date(text: str):
    """Strict date extractor. Returns ISO string or None."""
    if not text: return None
    
    # Priority: "12 January 2026" or "Jan 2026"
    patterns = [
        r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b", 
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b",           
        r"\b\d{4}-\d{2}-\d{2}\b",                                                            
        r"\b\d{1,2}/\d{1,2}/\d{4}\b"                                                         
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                dt = date_parser.parse(match.group(0), fuzzy=True, dayfirst=True).date()
                
                # --- FRESHNESS FILTER ---
                # Ignore anything older than 30 days ago (Keep recent history + future)
                if dt < (TODAY - timedelta(days=30)):
                    continue 
                
                # Ignore anything too far in future (e.g. typos stating 2099)
                if dt > (TODAY + timedelta(days=730)):
                    continue

                return dt.isoformat()
            except: continue
    return None

def detect_status(text: str) -> str:
    t = text.lower()
    if any(x in t for x in ["removed", "withdrawn", "cancelled", "discontinued"]): return "Removed"
    if any(x in t for x in ["updated", "published", "released", "available"]): return "Released"
    if any(x in t for x in ["upcoming", "planned", "due", "schedule"]): return "Upcoming"
    return "Scheduled"

def is_junk(text: str) -> bool:
    lower = text.lower()
    if len(lower) < 4: return True 
    if any(b in lower for b in BAD_TITLES): return True
    if "copyright" in lower or "all rights reserved" in lower: return True
    return False

def parse_source(source: dict, settings: dict) -> list[dict]:
    html = fetch_html(source["url"], settings)
    if not html: return []
    
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    # Look for container elements
    targets = soup.find_all(["tr", "li", "article", "div", "h3", "h4"])
    
    for tag in targets:
        raw_text = clean_text(tag.get_text(" ", strip=True))
        
        # 1. STRICT FILTER: Must have a valid, recent/future Date
        date_str = extract_date(raw_text)
        if not date_str:
            continue 

        # 2. Extract Title
        link = tag.find("a", href=True)
        if link:
            title = clean_text(link.get_text())
            url = link["href"]
            if url.startswith("/"):
                parsed = urlparse(source["url"])
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            title = raw_text[:100]
            url = source["url"]

        if is_junk(title):
            continue

        # 3. Create One Liner
        one_liner = raw_text.replace(date_str, "").strip()
        one_liner = re.sub(r"\s+", " ", one_liner)
        
        # Key for deduplication
        key = (title, date_str)
        if key in seen: continue
        seen.add(key)

        rows.append({
            "source": source["name"],
            "country": source.get("country", ""),
            "dataset_title": title,
            "summary": one_liner[:300],
            "status": detect_status(raw_text),
            "action_date": date_str,
            "url": url,
            "source_id": source.get("id", "")
        })

    return rows

def main():
    ensure_dirs()
    settings, sources = load_watchlist()
    
    all_rows = []
    print("🚀 Starting Smart Scraper (Freshness Filter Active)...")

    for source in sources:
        print(f"Scanning {source['name']}...")
        try:
            rows = parse_source(source, settings)
            print(f"  -> Found {len(rows)} verified releases.")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  -> Error: {e}")

    df = pd.DataFrame(all_rows)
    
    if not df.empty:
        # Deduplicate
        df = df.drop_duplicates(subset=["source", "dataset_title", "action_date"])
        # Sort by Date
        df = df.sort_values(by="action_date", ascending=True)
    
    df.to_csv(CURRENT_CSV, index=False)
    
    # Meta
    with open(META_JSON, "w") as f:
        json.dump({"run_at": NOW.isoformat(), "count": len(df)}, f)

    print("✅ Done.")

if __name__ == "__main__":
    main()
