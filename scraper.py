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

# --- AI LIBRARY ---
# This runs locally and requires NO API KEY
from sentence_transformers import SentenceTransformer, util

# --- CONSTANTS ---
DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/AI-1.0; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

# --- AI CONFIGURATION ---
# The model will compare scraped titles against these "Gold Standard" phrases.
# If a title isn't mathematically similar to these, it gets dumped.
RELEVANCE_TARGETS = [
    "population estimates and projections",
    "international migration statistics",
    "census data and household survey",
    "births deaths and fertility rates",
    "employment and labour market participation",
    "demographic trends and ageing"
]

# Threshold (0.0 to 1.0). 
# 0.35 is a good balance. Higher = Stricter.
SIMILARITY_THRESHOLD = 0.35

# --- GLOBAL MODEL LOADER ---
# We load this once to avoid reloading it 100 times.
print("🧠 Loading AI Model (all-MiniLM-L6-v2)...")
ai_model = SentenceTransformer('all-MiniLM-L6-v2')
target_embeddings = ai_model.encode(RELEVANCE_TARGETS, convert_to_tensor=True)
print("✅ AI Model Loaded.")

# --- CORE FUNCTIONS ---

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
                # Freshness: -30 days to +2 years
                if (TODAY - timedelta(days=30)) <= dt <= (TODAY + timedelta(days=730)):
                    return dt.isoformat()
            except: continue
    return None

def ai_is_relevant(text: str) -> bool:
    """
    The AI Brain. Returns True if 'text' is semantically close to our targets.
    """
    # 1. Encode the candidate text
    candidate_embedding = ai_model.encode(text, convert_to_tensor=True)
    
    # 2. Compute Cosine Similarity against all targets
    # Returns a list of scores, we take the highest one.
    cosine_scores = util.cos_sim(candidate_embedding, target_embeddings)
    max_score = float(cosine_scores.max())
    
    # Debug print to help you tune it
    # print(f"  [AI Score: {max_score:.2f}] {text[:50]}...")
    
    return max_score >= SIMILARITY_THRESHOLD

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

        # 2. Link/Title Extraction
        link = tag.find("a", href=True)
        if link:
            title = clean_whitespace(link.get_text())
            url = link["href"]
            if url.startswith("/"):
                parsed = urlparse(source["url"])
                url = f"{parsed.scheme}://{parsed.netloc}{url}"
        else:
            title = raw_text
            url = source["url"]

        # 3. AI FILTER CHECK
        # We combine Title + Source Name to give the AI context
        # e.g. "ONS - UK Trade" vs "ONS - Population Estimates"
        context_string = f"{title} {source['name']}"
        
        if not ai_is_relevant(context_string):
            continue  # SKIP! This is the magic step.

        # 4. Cleanup & Summary
        brief = raw_text.replace(date_str, "").replace(title, "").strip(" -:.")
        brief = re.sub(r"\s+", " ", brief)
        if len(brief) < 5: brief = title

        key = (title, date_str)
        if key in seen: continue
        seen.add(key)

        rows.append({
            "source": source["name"],
            "dataset_title": title,
            "summary": brief[:200],
            "status": "🚀 Release" if "release" in raw_text.lower() else "📅 Scheduled",
            "action_date": date_str,
            "url": url
        })

    return rows

def main():
    ensure_dirs()
    _, sources = load_watchlist()
    
    all_rows = []
    print("🚀 Starting AI-Semantic Scraper...")

    for source in sources:
        print(f"Scanning {source['name']}...")
        try:
            rows = parse_source(source)
            print(f"  -> Found {len(rows)} relevant items.")
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
