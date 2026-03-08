import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse, parse_qs, urlunparse

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
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/3.5; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- JUNK FILTER ---
# If a title matches these patterns, it is NOT a dataset.
JUNK_PATTERNS = [
    r"^clear all", r"^filter", r"^search", r"^release date", 
    r"^published", r"^time series", r"^correction", r"^notice",
    r"^\d{1,2}:\d{2}",  # "9:30am"
    r"^page \d", r"^next page", r"^previous page",
    r"^cookies", r"^accessibility", r"^privacy",
    r"^view all", r"^hide all", r"^download",
    r"^[0-9\/\-\. ]+$", # Titles that are just numbers/dates
]

# --- CORE FUNCTIONS ---

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

def is_content_junk(text: str) -> bool:
    """Returns True if the text is navigation noise."""
    lower = text.lower().strip()
    if len(lower) < 5: return True
    
    for pattern in JUNK_PATTERNS:
        if re.search(pattern, lower):
            return True
    return False

def extract_date(text: str):
    """Strict date extractor."""
    if not text: return None
    
    # Priority: "12 Jan 2026", "Jan 2026", "2026-01-01"
    patterns = [
        r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b", 
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b",           
        r"\b\d{4}-\d{2}-\d{2}\b"
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                dt = date_parser.parse(match.group(0), fuzzy=True, dayfirst=True).date()
                # Freshness Check: Keep recent (30 days ago) to future (2 years)
                if (TODAY - timedelta(days=30)) <= dt <= (TODAY + timedelta(days=730)):
                    return dt.isoformat()
            except: continue
    return None

def detect_status(text: str) -> str:
    t = text.lower()
    if any(x in t for x in ["removed", "withdrawn", "cancelled"]): return "Removed"
    if any(x in t for x in ["published", "released", "available"]): return "Released"
    return "Scheduled"

def parse_source(source: dict, settings: dict) -> list[dict]:
    html = fetch_html(source["url"], settings)
    if not html: return []
    
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    # Broad search for potential dataset containers
    targets = soup.find_all(["tr", "li", "article", "div", "h3", "h4"])
    
    for tag in targets:
        raw_text = clean_text(tag.get_text(" ", strip=True))
        
        # 1. MUST have a valid date
        date_str = extract_date(raw_text)
        if not date_str: continue

        # 2. Extract Title & Link
        link = tag.find("a", href=True)
        if link:
            title = clean_text(link.get_text())
            url = link["href"]
            if url.startswith("/"):
                parsed = urlparse(source["url"])
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            # Fallback: Use text before the date as title
            parts = raw_text.split(date_str)
            title = parts[0].strip() if parts else raw_text[:100]
            url = source["url"]

        # 3. CRITICAL: Junk Check
        if is_content_junk(title): continue

        # 4. Create "One Liner" Brief
        # Remove date and title from raw text to leave the description
        brief = raw_text.replace(date_str, "").replace(title, "").strip()
        brief = re.sub(r"\s+", " ", brief).strip(" -:.")
        if len(brief) < 5: brief = title # Fallback if brief is empty

        # Dedupe
        key = (title, date_str)
        if key in seen: continue
        seen.add(key)

        rows.append({
            "source": source["name"],
            "country": source.get("country", ""),
            "dataset_title": title,
            "summary": brief[:200], # Keep it short
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
    print("🚀 Starting Management-Ready Scraper...")

    for source in sources:
        print(f"Scanning {source['name']}...")
        try:
            rows = parse_source(source, settings)
            print(f"  -> Found {len(rows)} clean items.")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  -> Error: {e}")

    df = pd.DataFrame(all_rows)
    if not df.empty:
        # Global Dedupe
        df = df.drop_duplicates(subset=["source", "dataset_title", "action_date"])
        df = df.sort_values(by="action_date")
    
    df.to_csv(CURRENT_CSV, index=False)
    
    with open(META_JSON, "w") as f:
        json.dump({"run_at": NOW.isoformat(), "count": len(df)}, f)

    print("✅ Done.")

if __name__ == "__main__":
    main()
