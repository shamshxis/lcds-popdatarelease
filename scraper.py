import os
import re
import json
import hashlib
import time
import traceback
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from html import unescape
from urllib.parse import urljoin, quote_plus, urlparse, parse_qs, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from email.utils import parsedate_to_datetime
import random
import urllib.request

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from sentence_transformers import SentenceTransformer, util

# --- CONSTANTS ---
DATA_DIR = "data"
WATCHLIST_FILE = "watchlist.yml"
ORCID_FILE = os.path.join(DATA_DIR, "lcds_people_orcid_updated.csv")
DYNAMIC_PROFILE_CACHE = os.path.join(DATA_DIR, "lcds_dynamic_profile.json")
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "dataset_snapshot.json")
RUNLOG_FILE = os.path.join(DATA_DIR, "run_log.json")
SOURCE_HEALTH_FILE = os.path.join(DATA_DIR, "source_health.json")

DEFAULT_TIMEOUT = 25
MAX_ABS_DAYS = 730

THEME_KEYWORDS = {
    "Population": ["population", "demograph", "census", "resident", "ageing", "aging", "household", "people", "names", "surnames"],
    "Migration": ["migration", "migrant", "immigration", "emigration", "asylum", "refugee", "mobility", "visa"],
    "Fertility & Births": ["fertility", "birth", "newborn", "pregnan", "family formation", "maternity", "babies"],
    "Mortality & Health": ["mortality", "death", "life expectancy", "health", "cause of death", "suicide", "disease"],
    "Labour & Economy": ["labour", "labor", "employment", "earnings", "income", "poverty", "benefit", "workless", "workforce"],
    "Housing & Families": ["housing", "rent", "home", "family", "marriage", "divorce", "living conditions"],
    "Biobanks & Registries": ["biobank", "registry", "cohort", "genomic", "health data", "longitudinal", "clinical", "ehealth", "epidemiology"],
    "Methods & Infrastructure": ["method", "revision", "quality", "metadata", "api", "microdata", "archive", "discontinued", "decommission", "withdrawn", "access", "deleted"],
}

STATUS_PRIORITY = {"Deleted": 100, "Cancelled": 95, "Rescheduled": 90, "Restricted": 88, "Upcoming": 80, "Published": 60, "Announcement": 45, "Monitor": 30}
RED_FLAG_TERMS = ["discontinued", "deleted", "closure", "decommission", "retired", "withdrawn", "cancelled", "canceled", "removed", "archive", "archived", "end of series", "access change", "restricted access", "deprecation", "deprecated", "shutdown"]
STOPWORDS = {"about","above","across","after","against","along","among","around","before","behind","below","beneath","beside","between","during","except","from","inside","into","like","near","outside","over","past","since","through","throughout","toward","under","underneath","until","upon","with","within","without","although","because","since","unless","these","those","were","have","does","could","should","would","might","must","using","based","analysis","study","data","effects","impact","changes","patterns","trends","evidence","review","between","their","there","which","where","when","what","who","whom","whose","why","some","many","much","most","other","such","only","also","very","more","than","then","this","that","using","approach","method","methods","model","models","results","effect","among","associated","association","factors"}

DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b", r"\b\d{1,2}/\d{1,2}/\d{4}\b", 
    r"\b\d{1,2}(?:st|nd|rd|th)?\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[A-Za-z]*\s+\d{4}\b", 
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[A-Za-z]*\s+\d{1,2}(?:st|nd|rd|th)?(?:,)?\s*\d{4}\b", 
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[A-Za-z]*\s+\d{4}\b", r"\b(?:Q[1-4]|Spring|Summer|Autumn|Winter)\s+\d{4}\b"
]

print("🧠 Loading AI Semantic Model...")
ai_model = SentenceTransformer('all-MiniLM-L6-v2')
BASE_TARGETS = ["population estimates and demographic projections", "international migration asylum and refugee statistics", "national census results and household survey data", "births deaths mortality life expectancy and fertility rates", "employment labour market participation and workforce data", "housing living conditions and family structure statistics", "biobanks medical registries longitudinal cohorts and genomic health data"]
ANTI_TARGETS = ["agricultural crop production farming and livestock", "financial market stock exchange banking and corporate bonds", "weather climate change meteorology and environmental data", "software updates IT infrastructure and network maintenance", "manufacturing output industrial production and trade in goods"]
anti_embeddings = ai_model.encode(ANTI_TARGETS, convert_to_tensor=True)
SIMILARITY_THRESHOLD = 0.35 

def utcnow_naive() -> datetime: return datetime.now(timezone.utc).replace(tzinfo=None)

class DummyResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code
    def raise_for_status(self): pass
    def json(self): return json.loads(self.text)

@dataclass
class ParsedItem:
    dataset_title: str; source: str; source_group: str; source_type: str; event_type: str; action_date: datetime | None; status: str; url: str
    summary: str = ""; theme_primary: str = "General"; theme_secondary: str = ""; priority_score: int = 0; confidence: float = 0.0; red_flag: int = 0
    deleted_signal: int = 0; embargo: int = 0; tags: str = ""; raw_date: str = ""; last_checked: str = ""; source_page: str = ""
    fallback_hit: int = 0; source_quality: float = 0.0; media_relevance: int = 0; executive_flag: int = 0; academic_match: int = 0; record_key: str = ""
    def to_record(self) -> dict: return self.__dict__.copy()

class LCDSDataEngine:
    def __init__(self, watchlist_file: str = WATCHLIST_FILE):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.watchlist_file = watchlist_file
        self.config = self.load_watchlist()
        self.dynamic_terms = self.build_academic_profile()
        active_targets = list(BASE_TARGETS)
        if self.dynamic_terms: active_targets.extend([" ".join(self.dynamic_terms[i:i+4]) for i in range(0, min(len(self.dynamic_terms), 20), 4)])
        self.target_embeddings = ai_model.encode(active_targets, convert_to_tensor=True)

        settings = self.config.get("settings", {})
        self.timeout, self.max_workers, self.page_workers = int(settings.get("timeout", 30)), int(settings.get("max_workers", 16)), int(settings.get("page_workers", 4))
        self.max_abs_days = int(settings.get("max_abs_days", MAX_ABS_DAYS))
        self.today = utcnow_naive().replace(hour=0, minute=0, second=0, microsecond=0)
        self.session = requests.Session()
        self.snapshot, self.source_health, self.previous_df = self.load_json(SNAPSHOT_FILE), self.load_json(SOURCE_HEALTH_FILE), self.load_previous_df()

    def build_academic_profile(self) -> list:
        try:
            if os.path.exists(DYNAMIC_PROFILE_CACHE):
                with open(DYNAMIC_PROFILE_CACHE, "r") as f: cache = json.load(f)
                if time.time() - cache.get("timestamp", 0) < 604800: return cache.get("keywords", [])
            if not os.path.exists(ORCID_FILE): return []
            df = pd.read_csv(ORCID_FILE)
            if "ORCID" not in df.columns: return []
            orcids = [o for o in df["ORCID"].dropna().astype(str).str.strip().tolist() if re.match(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", o)]
            all_words = []
            def fetch_orcid(orcid):
                try:
                    resp = requests.get(f"https://api.crossref.org/works?filter=orcid:{orcid}&select=title,subject&rows=12", headers={"User-Agent": "LCDS-Scraper/2.0"}, timeout=10)
                    if resp.status_code == 200: return resp.json().get("message", {}).get("items", [])
                except: pass
                return []
            with ThreadPoolExecutor(max_workers=8) as ex:
                futures = {ex.submit(fetch_orcid, o): o for o in orcids}
                for future in as_completed(futures):
                    for item in future.result():
                        for subj in item.get("subject", []):
                            if s_clean := subj.lower().strip(): all_words.append(s_clean)
                        for title in item.get("title", []): all_words.extend([w for w in re.findall(r'\b[a-z]{5,}\b', title.lower()) if w not in STOPWORDS])
            top_terms = [term for term, _ in Counter(all_words).most_common(40)]
            os.makedirs(os.path.dirname(DYNAMIC_PROFILE_CACHE), exist_ok=True)
            with open(DYNAMIC_PROFILE_CACHE, "w") as f: json.dump({"timestamp": time.time(), "keywords": top_terms}, f)
            return top_terms
        except: return []

    def load_watchlist(self) -> dict:
        with open(self.watchlist_file, "r", encoding="utf-8") as f: return yaml.safe_load(f) or {}

    def load_json(self, path: str) -> dict:
        if not os.path.exists(path): return {}
        try:
            with open(path, "r", encoding="utf-8") as f: return json.load(f) or {}
        except: return {}

    def save_json(self, path: str, payload: dict) -> None:
        with open(path, "w", encoding="utf-8") as f: json.dump(payload, f, indent=2, ensure_ascii=False)

    def load_previous_df(self) -> pd.DataFrame:
        if not os.path.exists(OUTPUT_FILE): return pd.DataFrame()
        try:
            df = pd.read_csv(OUTPUT_FILE)
            if "action_date" in df.columns: df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
            return df
        except: return pd.DataFrame()

    def fetch(self, url: str, source: dict) -> requests.Response:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        headers.update(source.get("headers", {}))
        
        try:
            resp = self.session.get(url, headers=headers, timeout=self.timeout, verify=False)
            if resp.status_code in [403, 404, 406] or "cloudflare" in resp.text.lower() or "Just a moment..." in resp.text:
                raise requests.exceptions.HTTPError(response=resp)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            print(f"⚠️ [WAF BLOCKED] Rerouting {url} through public proxy...")
            try:
                proxy_url = f"https://api.allorigins.win/get?url={quote_plus(url)}"
                proxy_resp = self.session.get(proxy_url, timeout=self.timeout)
                if proxy_resp.status_code == 200:
                    data = proxy_resp.json()
                    if data and data.get("contents"):
                        return DummyResponse(data["contents"])
            except Exception as proxy_e:
                pass
            print(f"❌ [PERMA-BLOCKED/DEAD] {url}")
            raise e

    def strip_html_noise(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Aggressive pruning of headers, footers, and mega-menus to prevent scraping 'Jobs' and 'Privacy' links."""
        for tag in soup(["nav", "footer", "header", "aside", "style", "script", "button", "svg", "form"]):
            tag.decompose()
            
        def is_junk_class(classes):
            if isinstance(classes, list): classes = " ".join(classes)
            if not classes: return False
            c = classes.lower()
            return any(w in c for w in ["menu", "footer", "header", "sidebar", "cookie", "banner", "widget", "pagination", "breadcrumbs", "nav-container"])

        for tag in soup.find_all(["div", "section", "ul"], class_=is_junk_class):
            tag.decompose()
        return soup

    def normalize_whitespace(self, text: str) -> str: return re.sub(r"\s+", " ", unescape(text or "")).strip()

    def normalize_date(self, value) -> datetime | None:
        if value is None or (isinstance(value, float) and pd.isna(value)): return None
        if isinstance(value, pd.Timestamp): return value.to_pydatetime().replace(tzinfo=None)
        if isinstance(value, datetime): return value.replace(tzinfo=None)
        
        text = str(value).strip()
        if not text or text.lower() in {"nat", "nan", "none", "tbc", "tbd"}: return None
        
        text = re.sub(r"(?<=\d)(st|nd|rd|th)\b", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\|.*$", "", text).strip()
        text = text.replace("GMT", "").replace("Z", "").strip()

        try:
            fallback_default = datetime(self.today.year, self.today.month, 1)
            parsed = date_parser.parse(text, fuzzy=True, dayfirst=True, default=fallback_default)
            return parsed.replace(tzinfo=None)
        except: return None

    def in_time_window(self, dt: datetime | None) -> bool:
        if dt is None: return True
        return -90 <= (dt.date() - self.today.date()).days <= self.max_abs_days

    def canonical_key(self, title: str, source: str, date_obj) -> str:
        base = re.sub(r"[^a-z0-9]+", " ", str(title or "").lower()).strip()
        date_part = self.normalize_date(date_obj).strftime("%Y-%m-%d") if self.normalize_date(date_obj) else "nodate"
        return hashlib.md5(f"{source}|{base}|{date_part}".encode("utf-8")).hexdigest()

    def source_page_list(self, source: dict) -> list[tuple[str, int]]:
        pages = [(source.get("url"), 0)] if source.get("url") else []
        pages.extend([(u, 1) for u in source.get("fallback_urls", []) if u])
        seen = set()
        return [(u, h) for u, h in pages if not (u in seen or seen.add(u))]

    def classify_themes(self, text: str) -> tuple[str, str, list[str]]:
        text_l = (text or "").lower()
        scores = sorted([(theme, sum(1 for w in words if w in text_l)) for theme, words in THEME_KEYWORDS.items() if sum(1 for w in words if w in text_l)], key=lambda x: x[1], reverse=True)
        return scores[0][0] if scores else "General", scores[1][0] if len(scores) > 1 else "", [t for t, _ in scores[:3]]

    def compute_status(self, text: str, date_obj: datetime | None) -> tuple[str, str, int, int, int]:
        t = (text or "").lower()
        deleted_signal = int(any(term in t for term in ["deleted", "decommission", "discontinued", "withdrawn", "removed"]))
        if deleted_signal: return "Deleted", "Deletion", STATUS_PRIORITY["Deleted"], 1, 1
        if "cancelled" in t or "canceled" in t: return "Cancelled", "Cancellation", STATUS_PRIORITY["Cancelled"], 1, 0
        if "rescheduled" in t or "postponed" in t: return "Rescheduled", "Schedule Change", STATUS_PRIORITY["Rescheduled"], 1, 0
        if "restricted" in t or "access change" in t: return "Restricted", "Access Change", STATUS_PRIORITY["Restricted"], 1, 0
        if date_obj is not None:
            return ("Upcoming" if date_obj.date() >= self.today.date() else "Published", "Release", STATUS_PRIORITY["Upcoming"] if date_obj.date() >= self.today.date() else STATUS_PRIORITY["Published"], 0, 0)
        return "Announcement", "Announcement", STATUS_PRIORITY["Announcement"], 0, 0

    def is_junk_title(self, title: str) -> bool:
        t = title.lower().strip()
        if len(t) < 5 or re.fullmatch(r"^[0-9\/\-\. ]+$", t): return True
        if re.fullmatch(r"^(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}$", t): return True
        if re.fullmatch(r"^\d{1,3}(,\d{3})*\s+results$", t): return True
        
        exact_matches = {
            "information", "news bulletin", "jobs and vacancies", "registration", 
            "records and archives", "statistics and data", "database tables", 
            "publishing calendar", "data collection", "results", "search", 
            "contact us", "privacy", "cookie", "main contents start here", 
            "top of section", "skip to", "clear all", "search results", 
            "data.census.gov", "incremental developmental", "---", "about us",
            "news and press releases", "finding statistics", "menu", "read more",
            "view article", "click here", "find out more", "share this", "home"
        }
        
        if t in exact_matches: return True
        if t.startswith("skip to") or t.startswith("main contents"): return True
        return False

    def is_relevant(self, title: str, summary: str, source: dict) -> bool:
        combined = f"{title} {summary}".lower()
        exclude_kw = [x.lower() for x in (source.get("exclude_keywords") or [])]
        if exclude_kw and any(x in combined for x in exclude_kw): return False
        
        hard_rejects = [
            "economic census", "turnover", "producer price", "consumer price", "cpi", "inflation", 
            "gdp", "gross domestic product", "trade in goods", "export", "import", "retail sales", 
            "crop", "livestock", "agriculture", "fishery", "forestry", "manufacturing", 
            "industrial production", "financial market", "stock exchange", "interest rate", 
            "business insights", "company mergers", "acquisitions", "energy consumption", 
            "electricity", "emissions", "weather", "precipitation", "labour cost index", 
            "wage index", "price index", "construction index", "services index", "balance of payments", 
            "bovine", "cattle", "animal", "pig ", "poultry", "economic growth", "ict usage", "enterprises", 
            "spending", "consumer confidence", "producer confidence", "board of trustees", "governance", 
            "vacancies", "careers", "fundraising", "the access board", "the trading board"
        ]
        if any(x in combined for x in hard_rejects): return False

        strong_demographic = [
            "population estimates", "birth statistics", "fertility", "mortality", "life expectancy", 
            "census results", "international migration", "demographic trends", "eu population", 
            "babies' first names", "surnames in birth", "population projections", "death registrations", 
            "annual births data", "baby names", "europop", "data release", "new data available", 
            "cohort data", "researcher workbench", "whole genome sequencing", "genomic data", 
            "health records", "biobank data"
        ]
        if any(x in title.lower() for x in strong_demographic): return True

        context = f"{title} {summary} {source['name']}"
        if len(context) < 10: return False
        
        embedding = ai_model.encode(context, convert_to_tensor=True)
        target_score = float(util.cos_sim(embedding, self.target_embeddings).max())
        if float(util.cos_sim(embedding, anti_embeddings).max()) > target_score: return False
        if target_score >= SIMILARITY_THRESHOLD: return True

        keywords_any = [x.lower() for x in (source.get("keywords_any") or [])]
        if keywords_any and any(x in title.lower() for x in keywords_any): return True
        if self.dynamic_terms and any(t in title.lower() for t in self.dynamic_terms[:10]): return True
        
        return False

    def get_source_quality(self, source_name: str, page_url: str) -> float:
        state = self.source_health.get(source_name, {}).get("pages", {}).get(page_url, {})
        return 0.5 if state.get("attempts", 0) == 0 else round((float(state.get("successes", 0)) + 1.0) / (float(state.get("attempts", 0)) + 2.0), 3)

    def update_source_health(self, source_name: str, page_url: str, success: bool, item_count: int = 0) -> None:
        state = self.source_health.setdefault(source_name, {"pages": {}, "last_success": "", "last_failure": ""})
        page_state = state.setdefault("pages", {}).setdefault(page_url, {"attempts": 0, "successes": 0, "failures": 0, "items": 0})
        page_state["attempts"], page_state["items"] = page_state["attempts"] + 1, page_state["items"] + max(0, int(item_count))
        if success: page_state["successes"], state["last_success"] = page_state["successes"] + 1, utcnow_naive().isoformat()
        else: page_state["failures"], state["last_failure"] = page_state["failures"] + 1, utcnow_naive().isoformat()
            
    def compute_media_relevance(self, theme_primary: str, status: str, days_to_event, source_type: str, red_flag: int, title: str) -> int:
        score = 20 if theme_primary in {"Population", "Biobanks & Registries", "Mortality & Health", "Labour & Economy", "Housing & Families"} else 0
        score += 30 if status in {"Upcoming", "Cancelled", "Deleted", "Rescheduled", "Restricted"} else 0
        score += 25 if days_to_event is not None and pd.notna(days_to_event) and 0 <= int(days_to_event) <= 14 else 0
        score += 15 if source_type == "Official" else 0
        score += 10 if int(red_flag) == 1 else 0
        score += 10 if any(x in (title or "").lower() for x in ["population", "migration", "fertility", "death", "mortality", "census", "asylum", "biobank"]) else 0
        return int(score)

    def record_from_fields(self, source: dict, title: str, summary: str, date_value, url: str, extra_text: str = "", source_page: str = "", fallback_hit: int = 0) -> dict | None:
        title, summary = self.normalize_whitespace(re.sub(r"^\d+\.\s*", "", title or "").strip()), self.normalize_whitespace(summary)
        if not title or self.is_junk_title(title): return None
        if not self.is_relevant(title, summary, source): return None
        action_date = self.normalize_date(date_value)
        if action_date is not None and not self.in_time_window(action_date): return None

        combined = f"{title} {summary} {extra_text}"
        status, event_type, priority_score, red_flag, deleted_signal = self.compute_status(combined, action_date)
        theme_primary, theme_secondary, tags = self.classify_themes(combined)

        if any(term in combined.lower() for term in RED_FLAG_TERMS): red_flag = 1
        academic_match = 1 if self.dynamic_terms and any(t in title.lower() for t in self.dynamic_terms[:15]) else 0
        if academic_match: priority_score += 15; tags.append("Academic Priority")

        days_to_event = None if action_date is None else (action_date.date() - self.today.date()).days
        media_relevance = self.compute_media_relevance(theme_primary, status, days_to_event, source.get("source_type", "Official"), red_flag, title)
        
        bits = ["Deletion signal detected." if status == "Deleted" else ("Upcoming release." if status == "Upcoming" else ("Recently published." if status == "Published" else ""))]
        if days_to_event is not None: bits.append(f"Timing: {days_to_event} day(s) from now." if days_to_event >= 0 else f"Timing: {-days_to_event} day(s) ago.")
        if clean_summ := summary.replace(title, "").strip(" -:|"): bits.append(clean_summ[:220].rstrip(".") + ".")

        return ParsedItem(
            dataset_title=title, source=source["name"], source_group=source.get("group", "Other"), source_type=source.get("source_type", "Official"), event_type=event_type, action_date=action_date,
            status=status, url=url or source.get("url", ""), summary=" ".join(bits).strip(), theme_primary=theme_primary, theme_secondary=theme_secondary, priority_score=int(priority_score + int(source.get("priority_weight", 0))),
            confidence=round(min(0.48 + (0.15 if title else 0) + (0.10 if summary else 0) + (0.15 if action_date else 0), 0.99), 3), red_flag=int(red_flag), deleted_signal=int(deleted_signal), embargo=0, tags=", ".join(tags), raw_date=str(date_value or ""),
            last_checked=utcnow_naive().strftime("%Y-%m-%d %H:%M:%S"), source_page=source_page or url or source.get("url", ""), fallback_hit=int(fallback_hit), source_quality=float(self.get_source_quality(source["name"], source_page or url or source.get("url", ""))), media_relevance=int(media_relevance),
            executive_flag=int(priority_score >= 80 or red_flag == 1 or deleted_signal == 1), academic_match=academic_match, record_key=self.canonical_key(title, source["name"], action_date),
        ).to_record()

    # --- PARSERS ---
    
    def parser_ons_release_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        url_parts = list(urlparse(page_url))
        query = parse_qs(url_parts[4])
        target_queries = ["population", "migration", "health", "admin-based", "births", "deaths"] 
        if self.dynamic_terms: target_queries.append(self.dynamic_terms[0])
            
        results, seen_links = [], set()
        for kw in set(target_queries):
            query["keywords"] = kw
            url_parts[4] = urlencode(query, doseq=True)
            try: 
                resp = self.fetch(urlunparse(url_parts), source)
            except: continue
            
            soup = self.strip_html_noise(BeautifulSoup(resp.text, "html.parser"))
            for card in soup.find_all(["li", "div", "article"]):
                text = card.get_text(" ", strip=True)
                if "Release date:" not in text: continue
                link = card.find("a", href=True)
                if not link or (url := urljoin(page_url, link["href"])) in seen_links: continue
                title = self.normalize_whitespace(link.get_text())
                if not (m := re.search(r"Release date:\s*([^|]+)\|\s*([A-Za-z]+)", text)): continue
                date_text, label = m.group(1).strip(), m.group(2).strip()
                seen_links.add(url)
                if rec := self.record_from_fields(source, title, text.replace(title, "").replace(f"Release date: {date_text} | {label}", "").strip(), date_text, url, extra_text=text, source_page=page_url, fallback_hit=fallback_hit):
                    rec["status"] = {"Published": "Published", "Confirmed": "Upcoming", "Cancelled": "Cancelled"}.get(label, rec["status"])
                    results.append(rec)
        return results

    def parser_census_upcoming(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(page_url, source)
        except: return []
        soup = self.strip_html_noise(BeautifulSoup(resp.text, "html.parser"))
        lines = [self.normalize_whitespace(x) for x in soup.get_text("\n").splitlines() if self.normalize_whitespace(x)]
        results, current_date, current_channel = [], None, ""
        for line in lines:
            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", line): current_date, current_channel = line, ""
            elif line in {"Microdata Access & API", "data.census.gov & API", "API", "News Releases"}: current_channel = line
            elif current_date and len(line := line.lstrip("*• ").strip()) > 4:
                if rec := self.record_from_fields(source, line, current_channel, current_date, page_url, extra_text=f"{current_channel} {line}", source_page=page_url, fallback_hit=fallback_hit): results.append(rec)
        return results

    def parser_generic_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(page_url, source)
        except: return []
        soup = self.strip_html_noise(BeautifulSoup(resp.text, "html.parser"))
        results, seen_texts = [], set()
        
        for node in soup.find_all(["a", "h2", "h3", "h4", "article"]):
            text = self.normalize_whitespace(node.get_text(" ", strip=True))
            if len(text) < 10 or text in seen_texts: continue
            seen_texts.add(text)
            
            parent = node.find_parent()
            parent_text = self.normalize_whitespace(parent.get_text(" ", strip=True)) if parent else text
            date_text = next((m.group(0) for p in DATE_PATTERNS if (m := re.search(p, parent_text, re.IGNORECASE))), None)
            
            title, link = text, None
            if node.name == "a": link = node
            else:
                link = node.find("a", href=True)
                if link: title = self.normalize_whitespace(link.get_text(strip=True))
                
            url = urljoin(page_url, link["href"]) if link else page_url
            if rec := self.record_from_fields(source, title, parent_text.replace(title, "").replace(date_text or "", "").strip(" -:|"), date_text, url, extra_text=parent_text, source_page=page_url, fallback_hit=fallback_hit): 
                results.append(rec)
        return results

    def parser_rss(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(page_url, source)
        except: return []
        results = []
        
        try:
            root = ET.fromstring(resp.text)
            items = root.findall(".//item") + root.findall(".//{http://www.w3.org/2005/Atom}entry")
        except ET.ParseError:
            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all(["item", "entry"])
            
        for entry in items[:int(source.get("max_items", 50))]:
            def ft(names): 
                if hasattr(entry, 'find_all'): 
                    return next((x.get_text(strip=True) for n in names if (x := entry.find(n))), "")
                else:
                    return next((x.text.strip() for n in names if (x := entry.find(n)) is not None and (x.text or "").strip()), "")
                    
            title, summary, date_val = self.normalize_whitespace(ft(["title", "{http://www.w3.org/2005/Atom}title"])), self.normalize_whitespace(ft(["description", "summary", "{http://www.w3.org/2005/Atom}summary"])), ft(["pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}updated"])
            if "Dataset: updated data" in title or "Dataset: new data" in title:
                if len(summary) > 10: title, summary = f"{summary} ({title.split('-')[0].strip()})", "Data updated or added in Eurostat database."
            
            if hasattr(entry, 'find_all'):
                link_node = entry.find("link")
                url = link_node.get_text(strip=True) if link_node else page_url
            else:
                link = entry.find("link")
                url = link.text.strip() if link is not None and link.text else (link.attrib.get("href") if link is not None else page_url)
                
            if rec := self.record_from_fields(source, title, summary, date_val, url, extra_text=f"{title} {summary}", source_page=page_url, fallback_hit=fallback_hit): results.append(rec)
        return results

    def parser_xml_release(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(page_url, source)
        except: return []
        results = []
        for node in BeautifulSoup(resp.text, "xml").find_all(["release", "item", "entry"]):
            title = node.find(["title", "headline"]).get_text(" ", strip=True) if node.find(["title", "headline"]) else ""
            summary = node.find(["description", "summary"]).get_text(" ", strip=True) if node.find(["description", "summary"]) else ""
            date_val = node.find(["release_date", "pubDate", "updated", "published", "date"]).get_text(" ", strip=True) if node.find(["release_date", "pubDate", "updated", "published", "date"]) else ""
            url = node.find(["link", "id"]).get_text(" ", strip=True) if node.find(["link", "id"]) else page_url
            if rec := self.record_from_fields(source, title, summary, date_val, url, extra_text=f"{title} {summary}", source_page=page_url, fallback_hit=fallback_hit): results.append(rec)
        return results

    def parser_ics_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(page_url, source)
        except: return []
        results = []
        for block in resp.text.replace('\r', '').split("BEGIN:VEVENT")[1:]:
            unf = []
            for l in [x.strip() for x in block.split("END:VEVENT")[0].split('\n') if x.strip()]:
                if unf and l.startswith(" "): unf[-1] += l.strip()
                else: unf.append(l)
            def rf(prefixes): return next((u.split(":", 1)[-1].strip() for u in unf for p in prefixes if u.startswith(p)), "")
            if rec := self.record_from_fields(source, rf(["SUMMARY"]), rf(["DESCRIPTION"]), rf(["DTSTART", "DTSTART;VALUE=DATE"]), rf(["URL"]) or page_url, extra_text=block, source_page=page_url, fallback_hit=fallback_hit): results.append(rec)
        return results

    def parser_gdelt_jsonfeed(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        try: resp = self.fetch(f"https://api.gdeltproject.org/api/v2/doc/doc?query={quote_plus(source.get('gdelt_query', ''))}&mode=artlist&maxrecords={int(source.get('max_items', 75))}&format=jsonfeed&sort=datedesc&timespan={source.get('gdelt_timespan', '7d')}", source)
        except: return []
        if not hasattr(resp, 'json'): return []
        
        results, target_keywords = [], [k.lower() for k in source.get("keywords_any", [])]
        try: items = resp.json().get("items", [])
        except: items = []
        
        for item in items:
            title, summary = self.normalize_whitespace(item.get("title", "")), self.normalize_whitespace(item.get("summary", item.get("content_text", "")))
            if not any(k in title.lower() or summary.lower().count(k) >= 2 for k in target_keywords): continue
            if rec := self.record_from_fields(source, title, summary, item.get("date_published") or item.get("date_modified"), item.get("url", page_url), extra_text=f"{title} {summary}", source_page=page_url, fallback_hit=fallback_hit):
                if rec["status"] == "Published": rec["status"] = "Announcement"
                rec["event_type"] = "Media Signal"
                results.append(rec)
        return results

    def parse_page(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        aliases = {"ons_json_api": "ons_release_calendar", "html_deep_scan": "generic_calendar", "html_table_scan": "generic_calendar", "eurostat_xml": "xml_release", "rss_feed": "rss", "ics": "ics_calendar", "gdelt": "gdelt_jsonfeed", "cbs_calendar": "generic_calendar", "html_signal_scan": "generic_calendar"}
        parsers = {"ons_release_calendar": self.parser_ons_release_calendar, "census_upcoming": self.parser_census_upcoming, "generic_calendar": self.parser_generic_calendar, "xml_release": self.parser_xml_release, "rss": self.parser_rss, "ics_calendar": self.parser_ics_calendar, "gdelt_jsonfeed": self.parser_gdelt_jsonfeed}
        return parsers.get(aliases.get(source.get("parser", "generic_calendar"), source.get("parser", "generic_calendar")), self.parser_generic_calendar)(source, page_url, fallback_hit)

    def parse_source(self, source: dict) -> tuple[dict, list[dict], dict]:
        pages, all_items = self.source_page_list(source), []
        with ThreadPoolExecutor(max_workers=min(self.page_workers, max(1, len(pages)))) as ex:
            futures = {ex.submit(self.parse_page, source, pu, fh): (pu, fh) for pu, fh in pages}
            for future in as_completed(futures):
                pu, fh = futures[future]
                try:
                    items = future.result()
                    if items:
                        self.update_source_health(source["name"], pu, True, len(items))
                        all_items.extend(items)
                except Exception as e:
                    self.update_source_health(source["name"], pu, False, 0)
        
        current_keys = [self.canonical_key(x.get("dataset_title", ""), x.get("source", source["name"]), x.get("action_date")) for x in all_items if x.get("dataset_title")]
        current_snapshot = {k: True for k in current_keys}
        previous_snapshot = self.snapshot.get(source["name"], {})

        if source.get("track_missing_as_deleted", False) and previous_snapshot and not self.previous_df.empty and "source" in self.previous_df.columns:
            prev_lookup = self.previous_df[self.previous_df["source"] == source["name"]]
            if "record_key" in prev_lookup.columns:
                for key in set(previous_snapshot.keys()) - set(current_snapshot.keys()):
                    row = prev_lookup[prev_lookup["record_key"] == key]
                    if not row.empty:
                        r = row.iloc[0].to_dict()
                        r["status"], r["event_type"], r["priority_score"], r["red_flag"], r["deleted_signal"], r["last_checked"] = "Deleted", "Deletion", STATUS_PRIORITY["Deleted"], 1, 1, utcnow_naive().strftime("%Y-%m-%d %H:%M:%S")
                        all_items.append(r)
        return source, all_items, current_snapshot | previous_snapshot

    def postprocess(self, rows: list[dict]) -> pd.DataFrame:
        base_cols = ["dataset_title", "source", "source_group", "source_type", "event_type", "action_date", "status", "url", "summary", "theme_primary", "theme_secondary", "priority_score", "confidence", "red_flag", "deleted_signal", "embargo", "tags", "raw_date", "last_checked", "source_page", "fallback_hit", "source_quality", "media_relevance", "executive_flag", "academic_match", "record_key"]
        if not rows: return pd.DataFrame(columns=base_cols + ["days_to_event", "display_date", "sort_rank"])

        df = pd.DataFrame(rows)
        for col in base_cols: 
            if col not in df.columns: df[col] = 0 if col in ["priority_score", "red_flag", "deleted_signal", "embargo", "fallback_hit", "media_relevance", "executive_flag", "academic_match"] else ("" if col != "source_quality" else 0.5)

        df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
        for col in ["priority_score", "red_flag", "deleted_signal", "embargo", "fallback_hit", "media_relevance", "executive_flag", "academic_match"]: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["confidence", "source_quality"]: df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["dataset_title_norm"] = df["dataset_title"].fillna("").str.lower().str.replace(r"[^a-z0-9]+", " ", regex=True).str.strip()
        missing_key = df["record_key"].fillna("") == ""
        if missing_key.any(): df.loc[missing_key, "record_key"] = df.loc[missing_key].apply(lambda r: self.canonical_key(r.get("dataset_title", ""), r.get("source", ""), r.get("action_date")), axis=1)

        df = df.sort_values(["deleted_signal", "red_flag", "priority_score", "confidence", "source_quality", "fallback_hit", "action_date"], ascending=[False, False, False, False, False, True, True])
        df = df.drop_duplicates(subset=["record_key"], keep="first")
        df["date_key"] = df["action_date"].dt.strftime("%Y-%m-%d").fillna("nodate")
        df = df.drop_duplicates(subset=["dataset_title_norm", "date_key"], keep="first").reset_index(drop=True)
        df = df.drop(columns=["date_key"])

        df["days_to_event"] = (df["action_date"].dt.normalize() - pd.Timestamp(self.today)).dt.days
        df["display_date"] = df["action_date"].dt.strftime("%d %b %Y")
        df.loc[df["action_date"].isna(), "display_date"] = "Date TBC"
        df["executive_flag"] = ((df["priority_score"] >= 80) | (df["red_flag"] == 1) | (df["deleted_signal"] == 1)).astype(int)

        missing_media = df["media_relevance"] <= 0
        if missing_media.any(): df.loc[missing_media, "media_relevance"] = df.loc[missing_media].apply(lambda r: self.compute_media_relevance(r["theme_primary"], r["status"], r["days_to_event"], r["source_type"], r["red_flag"], r["dataset_title"]), axis=1)

        df = df[(df["action_date"].isna()) | ((df["days_to_event"] >= -60) & (df["days_to_event"] <= self.max_abs_days))]
        df["sort_rank"] = (df["deleted_signal"] * 1000 + df["red_flag"] * 500 + df["priority_score"] + df["media_relevance"] + (df["source_quality"] * 20).round().astype(int) - df["fallback_hit"] * 2 - df["days_to_event"].fillna(9999).clip(lower=-365, upper=365))

        return df[base_cols + ["days_to_event", "display_date", "sort_rank"]].sort_values(["sort_rank", "action_date", "source"], ascending=[False, True, True]).reset_index(drop=True)

    def build_metrics(self, df: pd.DataFrame) -> dict:
        if df.empty: return {"records": 0, "upcoming": 0, "red_flags": 0, "deletions": 0, "academic_matches": 0, "next_14_days": 0, "fallback_hits": 0, "generated_at": utcnow_naive().isoformat()}
        return {
            "records": int(len(df)), "upcoming": int(((df["status"] == "Upcoming") & (df["days_to_event"] >= 0)).sum()),
            "red_flags": int(df["red_flag"].sum()), "deletions": int(df["deleted_signal"].sum()),
            "academic_matches": int(df.get("academic_match", pd.Series([0])).sum()),
            "next_14_days": int(((df["days_to_event"] >= 0) & (df["days_to_event"] <= 14)).sum()),
            "fallback_hits": int(df["fallback_hit"].sum()), "generated_at": utcnow_naive().isoformat(),
        }

    def run(self) -> pd.DataFrame:
        sources, snapshots_out, all_rows, logs = self.config.get("sources", []), {}, [], []
        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(self.parse_source, source): source for source in sources}
            for future in as_completed(futures):
                source, items, snap = future.result()
                snapshots_out[source["name"]] = snap
                all_rows.extend(items)
                logs.append({"source": source["name"], "items": len(items)})

        df = self.postprocess(all_rows)
        df.to_csv(OUTPUT_FILE, index=False)
        self.save_json(SNAPSHOT_FILE, snapshots_out)
        self.save_json(SOURCE_HEALTH_FILE, self.source_health)
        self.save_json(RUNLOG_FILE, {"generated_at": utcnow_naive().isoformat(), "logs": logs, "metrics": self.build_metrics(df)})
        return df

if __name__ == "__main__":
    engine = LCDSDataEngine()
    frame = engine.run()
    print(f"✅ Saved {len(frame)} holistic records to {OUTPUT_FILE}")
