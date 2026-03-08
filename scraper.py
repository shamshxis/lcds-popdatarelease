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
STATUS_CSV = DATA_DIR / "source_status.csv"
CANDIDATES_CSV = DATA_DIR / "candidate_sources.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/3.0; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- INTELLIGENCE CONFIG ---

# Keywords to auto-detect themes
THEME_MAP = {
    "💰 Economy": ["gdp", "inflation", "price", "spending", "finance", "pension", "debt", "economic", "trade"],
    "🏥 Health": ["health", "mortality", "death", "cancer", "hospital", "life expectancy", "disease", "covid"],
    "✈️ Migration": ["migration", "asylum", "refugee", "visa", "immigra", "border", "foreign"],
    "👶 Vital Stats": ["birth", "fertility", "baby", "maternity", "vital"],
    "💼 Labour": ["labour", "employ", "job", "work", "wage", "earning", "vacanc"],
    "📊 Census": ["census", "population count", "household", "demograph"],
}

# Junk phrases to strip from Titles
TITLE_NOISE = [
    "data.census.gov", "API", "Microdata Access", "Top of Section", 
    "release release", "updated updated", "upcoming release",
    "Public Sector:", "Current Population Survey", "Release:", 
    "View all", "Hide all", "Main figures", "Statistical release",
    "cookies on", "privacy policy"
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
    time.sleep(1.5) 
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

# --- INTELLIGENCE FUNCTIONS ---

def smart_clean_title(text: str) -> str:
    """Surgically removes dates and noise from the raw title string."""
    if not text: return ""
    clean = text
    
    # 1. Remove Hard-coded Junk
    for junk in TITLE_NOISE:
        clean = re.sub(re.escape(junk), "", clean, flags=re.IGNORECASE)

    # 2. Strip embedded dates (Start or End)
    #    matches: "January 2026", "2026-05-01", "12/05/2026"
    date_patterns = [
        r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b',
        r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', 
        r'\b\d{4}-\d{2}-\d{2}\b'
    ]
    for p in date_patterns:
        clean = re.sub(p, "", clean, flags=re.IGNORECASE)

    # 3. Clean Punctuation mess " : - "
    clean = re.sub(r'\s+[:\-]\s+', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    # 4. Fallback if over-cleaned
    if len(clean) < 5: return text[:100]
    return clean

def auto_tag_theme(text: str) -> str:
    """Returns an emoji theme based on keywords."""
    text = text.lower()
    for theme, keywords in THEME_MAP.items():
        if any(k in text for k in keywords):
            return theme
    return "📄 General"

def detect_status(text: str) -> str:
    t = text.lower()
    if any(x in t for x in ["removed", "withdrawn", "discontinued", "cancelled"]): return "warning"
    if any(x in t for x in ["updated", "published", "released", "available now"]): return "updated"
    if any(x in t for x in ["upcoming", "planned", "due", "calendar", "expected"]): return "upcoming"
    return "monitor"

def extract_date(text: str):
    if not text: return None
    # Prioritize "12 January 2026" formats
    patterns = [
        r"\b\d{1,2}\s+[A-Z][a-z]{2,}\s+\d{4}\b",
        r"\b[A-Z][a-z]{2,}\s+\d{4}\b",  # "May 2026"
        r"\b\d{4}-\d{2}-\d{2}\b"
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                dt = date_parser.parse(match.group(0), fuzzy=True, dayfirst=True)
                return dt.date().isoformat()
            except: continue
    return None

def is_blacklisted(text: str) -> bool:
    """Returns True if the text is UI noise (cookies, search bars)."""
    lower = text.lower()
    blacklist = [
        "cookie", "privacy policy", "settings", "javascript", 
        "filter results", "clear all", "search", "show only", 
        "day month year", "accessibility", "skip to main"
    ]
    return any(b in lower for b in blacklist)

# --- SCRAPER LOGIC ---

def add_row(rows: list, source: dict, settings: dict, raw_text: str, context: str):
    if len(raw_text) < 5 or is_blacklisted(raw_text): 
        return

    # Apply Intelligence
    clean_title = smart_clean_title(raw_text)
    theme = auto_tag_theme(clean_title + " " + context)
    status = detect_status(context)
    date_str = extract_date(context) or ""

    # Check Date Window
    if date_str:
        try:
            dt = date_parser.parse(date_str).date()
            # Default window: 180 days back, 365 forward
            if not (TODAY - timedelta(days=180) <= dt <= TODAY + timedelta(days=365)):
                return
        except: pass

    rows.append({
        "source_id": source.get("id", ""),
        "source": source.get("name", ""),
        "country": source.get("country", ""),
        "region": source.get("region", ""),
        "source_type": source.get("source_type", ""),
        "themes": theme,  # Using our smart theme instead of static
        "priority": source.get("priority", 5),
        "dataset_title": clean_title,
        "summary": clean_title, # Simple summary for now
        "status": status,
        "announcement_date": TODAY.isoformat(),
        "action_date": date_str,
        "url": source.get("url", ""),
        "notes": "",
        "last_seen": NOW.isoformat(),
    })

def parse_generic(source: dict, settings: dict) -> list[dict]:
    # Universal parser that works for ONS, Eurostat, Census, etc.
    html = fetch_html(source["url"], settings)
    if not html: return []
    
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    # Broad Tag Search
    targets = soup.find_all(["li", "tr", "article", "div", "h3", "h4", "a"])
    
    for tag in targets:
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 15 or text in seen: continue
        
        # Keyword Filter
        keywords = settings.get("discovery_keywords", []) + ["release", "publication"]
        if not any(k in text.lower() for k in keywords):
            continue

        seen.add(text)
        add_row(rows, source, settings, text, text)

    return rows

# --- MAIN EXECUTION ---

def main():
    ensure_dirs()
    settings, sources = load_watchlist()
    
    all_rows = []
    status_rows = []

    print(f"🚀 Starting Smart Scraper at {NOW.isoformat()}")

    for source in sources:
        print(f"Processing: {source['name']}...")
        start_time = datetime.now(timezone.utc)
        
        try:
            # Use one robust generic parser for everything
            rows = parse_generic(source, settings)
            
            # Deduplicate locally
            df_local = pd.DataFrame(rows)
            if not df_local.empty:
                # Keep row with Date if duplicates exist
                df_local = df_local.sort_values("action_date", ascending=False)
                df_local = df_local.drop_duplicates(subset=["dataset_title"])
                rows = df_local.to_dict(orient="records")

            print(f"  -> Found {len(rows)} valid items.")
            all_rows.extend(rows)
            
            status_rows.append({
                "source": source["name"], "ok": True, "count": len(rows), 
                "run_at": start_time.isoformat()
            })
            
        except Exception as e:
            print(f"  -> Error: {e}")
            status_rows.append({
                "source": source["name"], "ok": False, "count": 0, 
                "error": str(e), "run_at": start_time.isoformat()
            })

    # Global Deduplication & Save
    new_df = pd.DataFrame(all_rows)
    if not new_df.empty:
        new_df = new_df.sort_values("action_date", ascending=False)
        new_df = new_df.drop_duplicates(subset=["source", "dataset_title", "action_date"])
        
    new_df.to_csv(CURRENT_CSV, index=False)
    pd.DataFrame(status_rows).to_csv(STATUS_CSV, index=False)
    
    # Meta
    with open(META_JSON, "w") as f:
        json.dump({"run_at": NOW.isoformat(), "total": len(new_df)}, f)

    print("✅ Done.")

if __name__ == "__main__":
    main()
