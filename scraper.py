import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from sentence_transformers import SentenceTransformer, util

# --- CONSTANTS ---
DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/Management-1.0; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- AI & CONFIG ---
print("🧠 Loading AI Model...")
ai_model = SentenceTransformer('all-MiniLM-L6-v2')
RELEVANCE_TARGETS = [
    "population estimates", "migration statistics", "census results",
    "births deaths fertility", "labour market", "demographic trends"
]
target_embeddings = ai_model.encode(RELEVANCE_TARGETS, convert_to_tensor=True)
SIMILARITY_THRESHOLD = 0.35

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_watchlist():
    with open("watchlist.yml", "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("settings", {}), raw.get("sources", [])

def fetch_html(url: str) -> str:
    time.sleep(1.0)
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=25)
        response.raise_for_status()
        return response.text
    except Exception:
        return ""

def clean_whitespace(text: str) -> str:
    if not text: return ""
    return re.sub(r"\s+", " ", str(text).replace("\xa0", " ")).strip()

def extract_date(text: str):
    if not text: return None
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
                if (TODAY - timedelta(days=60)) <= dt <= (TODAY + timedelta(days=730)):
                    return dt.isoformat()
            except: continue
    return None

def detect_status(text: str, date_str: str) -> str:
    """
    Decides if the item is 'Released' or 'Scheduled'.
    """
    lower = text.lower()
    
    # 1. Explicit keywords
    if "removed" in lower or "withdrawn" in lower or "cancelled" in lower:
        return "⚠️ Withdrawn"
    if "published" in lower or "available now" in lower or "released on" in lower:
        return "✅ Published"
    
    # 2. Date Logic: If date is in the past, assume Released
    if date_str:
        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            if dt <= TODAY:
                return "✅ Published"
        except: pass

    return "📅 Scheduled"

def ai_is_relevant(text: str) -> bool:
    embedding = ai_model.encode(text, convert_to_tensor=True)
    scores = util.cos_sim(embedding, target_embeddings)
    return float(scores.max()) >= SIMILARITY_THRESHOLD

def parse_source(source: dict) -> list[dict]:
    html = fetch_html(source["url"])
    if not html: return []
    
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    targets = soup.find_all(["tr", "li", "article", "div", "h3", "h4"])
    
    for tag in targets:
        raw_text = clean_whitespace(tag.get_text(" ", strip=True))
        
        # 1. Date Check
        date_str = extract_date(raw_text)
        if not date_str: continue

        # 2. AI Check
        link = tag.find("a", href=True)
        title = clean_whitespace(link.get_text()) if link else raw_text
        if not ai_is_relevant(title + " " + source['name']):
            continue

        # 3. Build Row
        url = link["href"] if link else source["url"]
        if url.startswith("/"):
            parsed = urlparse(source["url"])
            url = f"{parsed.scheme}://{parsed.netloc}{url}"

        # Clean Summary
        brief = raw_text.replace(date_str, "").replace(title, "").strip(" -:.")
        brief = re.sub(r"\s+", " ", brief)
        if len(brief) < 5: brief = title

        # Smart Status
        status = detect_status(raw_text, date_str)

        key = (title, date_str)
        if key in seen: continue
        seen.add(key)

        rows.append({
            "source": source["name"],
            "dataset_title": title,
            "summary": brief[:250],
            "status": status,
            "action_date": date_str,
            "url": url
        })

    return rows

def main():
    ensure_dirs()
    _, sources = load_watchlist()
    all_rows = []
    print("🚀 Starting Smart Status Scraper...")

    for source in sources:
        print(f"Scanning {source['name']}...")
        try:
            rows = parse_source(source)
            print(f"  -> Found {len(rows)} items.")
            all_rows.extend(rows)
        except Exception as e:
            print(f"  -> Error: {e}")

    df = pd.DataFrame(all_rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["source", "dataset_title", "action_date"])
        df = df.sort_values(by="action_date")
    
    df.to_csv(CURRENT_CSV, index=False)
    with open(META_JSON, "w") as f:
        json.dump({"run_at": NOW.isoformat(), "count": len(df)}, f)
    print("✅ Done.")

if __name__ == "__main__":
    main()
