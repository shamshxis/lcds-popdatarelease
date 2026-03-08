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
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/5.0; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- THE BLOCKLIST ---
# If a row contains ANY of these, it is deleted instantly.
JUNK_PHRASES = [
    "clear all", "filter", "search", "release date", "published", 
    "time series", "correction", "notice", "9:30am", "page", 
    "cookies", "accessibility", "privacy", "view all", "hide all", 
    "download", "microdata access", "top of section", "skip to content",
    "previous", "next", "beta", "help", "contact", "about us",
    "census.gov", "ons.gov.uk", "terms", "conditions"
]

# --- CORE FUNCTIONS ---

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_watchlist() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with open("watchlist.yml", "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("settings", {}), raw.get("sources", [])

def fetch_html(url: str) -> str:
    time.sleep(1.0)
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=20)
        response.raise_for_status()
        return response.text
    except Exception:
        return ""

def clean_whitespace(text: str) -> str:
    if not text: return ""
    return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()

def extract_date(text: str):
    """Strict date extractor. Returns ISO string or None."""
    if not text: return None
    
    # Matches: "12 Jan 2026", "Jan 2026", "2026-01-01"
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
                # Freshness Filter: Only keep data from last 30 days to 2 years in future
                if (TODAY - timedelta(days=30)) <= dt <= (TODAY + timedelta(days=730)):
                    return dt.isoformat()
            except: continue
    return None

def surgical_title_clean(raw_text: str, date_str: str) -> str:
    """
    Intelligently extracts the Real Title from the raw mess.
    """
    clean = raw_text
    
    # 1. Remove the Date string itself
    if date_str:
        clean = clean.replace(date_str, "")

    # 2. Cut off at common "Junk Starts"
    # (e.g. "School Finances API Download" -> "School Finances")
    cut_points = ["API", "Download", "http", "https", "Microdata", "View", "Release"]
    for cut in cut_points:
        if cut in clean:
            clean = clean.split(cut)[0]
    
    # 3. Remove stray junk words
    for junk in ["Public Sector:", "Current Population Survey", "Release:", "Updated:"]:
        clean = clean.replace(junk, "")

    # 4. Remove embedded dates (e.g. "May 2026")
    clean = re.sub(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b', '', clean, flags=re.IGNORECASE)
    
    # 5. Final polish
    clean = re.sub(r'\s+[:\-]\s+', ' ', clean) # Remove " : "
    clean = re.sub(r'\s+', ' ', clean).strip(" -:.")
    
    return clean

def is_junk_row(text: str, title: str) -> bool:
    """Returns True if this row should be trashed."""
    lower_text = text.lower()
    lower_title = title.lower()
    
    # Rule 1: Title too short?
    if len(title) < 5: return True
    
    # Rule 2: Title is just a number?
    if re.match(r"^[0-9\/\-\. ]+$", title): return True

    # Rule 3: Contains blocked phrases?
    if any(j in lower_text for j in JUNK_PHRASES): return True
    if any(j in lower_title for j in JUNK_PHRASES): return True
    
    return False

def parse_source(source: dict) -> list[dict]:
    html = fetch_html(source["url"])
    if not html: return []
    
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    # Broad search for rows
    targets = soup.find_all(["tr", "li", "article", "div", "h3", "h4"])
    
    for tag in targets:
        raw_text = clean_whitespace(tag.get_text(" ", strip=True))
        
        # 1. MUST HAVE DATE (The biggest filter)
        date_str = extract_date(raw_text)
        if not date_str: continue

        # 2. Get Title & Link
        link = tag.find("a", href=True)
        if link:
            # Prefer link text if available, usually cleaner
            title_candidate = clean_whitespace(link.get_text())
            url = link["href"]
            if url.startswith("/"):
                parsed = urlparse(source["url"])
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            title_candidate = raw_text
            url = source["url"]

        # 3. Clean the Title
        final_title = surgical_title_clean(title_candidate, date_str)
        
        # 4. JUNK CHECK
        if is_junk_row(raw_text, final_title): continue

        # 5. Create "One Liner" Summary
        # The summary is whatever is left in the text after removing the title and date
        summary = raw_text.replace(date_str, "").replace(final_title, "").strip()
        summary = re.sub(r"\s+", " ", summary).strip(" -:.")
        if len(summary) < 5: summary = final_title # Fallback

        # Dedupe
        key = (final_title, date_str)
        if key in seen: continue
        seen.add(key)

        rows.append({
            "source": source["name"],
            "country": source.get("country", ""),
            "dataset_title": final_title,
            "summary": summary[:200],
            "status": "Removed" if "remove" in raw_text.lower() else "Scheduled",
            "action_date": date_str,
            "url": url
        })

    return rows

def main():
    ensure_dirs()
    _, sources = load_watchlist()
    
    all_rows = []
    print("🚀 Starting Strict Scraper...")

    for source in sources:
        print(f"Scanning {source['name']}...")
        try:
            rows = parse_source(source)
            print(f"  -> Found {len(rows)} clean items.")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  -> Error: {e}")

    df = pd.DataFrame(all_rows)
    if not df.empty:
        # Final Dedupe
        df = df.drop_duplicates(subset=["source", "dataset_title", "action_date"])
        df = df.sort_values(by="action_date")
    
    df.to_csv(CURRENT_CSV, index=False)
    
    with open(META_JSON, "w") as f:
        json.dump({"run_at": NOW.isoformat(), "count": len(df)}, f)

    print("✅ Done.")

if __name__ == "__main__":
    main()
