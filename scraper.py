import os
import re
import json
import hashlib
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from html import unescape
from urllib.parse import urljoin, quote_plus, urlparse, parse_qs, urlencode, urlunparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from email.utils import parsedate_to_datetime

DATA_DIR = "data"
WATCHLIST_FILE = "watchlist.yml"
OUTPUT_FILE = os.path.join(DATA_DIR, "dataset_tracker.csv")
SNAPSHOT_FILE = os.path.join(DATA_DIR, "dataset_snapshot.json")
RUNLOG_FILE = os.path.join(DATA_DIR, "run_log.json")
SOURCE_HEALTH_FILE = os.path.join(DATA_DIR, "source_health.json")

DEFAULT_TIMEOUT = 25
# Changed User-Agent to standard Chrome to prevent ONS/Census from blocking the scraper
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_ABS_DAYS = 366

THEME_KEYWORDS = {
    "Population": ["population", "demograph", "census", "resident", "ageing", "aging", "household", "people"],
    "Migration": ["migration", "migrant", "immigration", "emigration", "asylum", "refugee", "mobility"],
    "Fertility & Births": ["fertility", "birth", "newborn", "pregnan", "family formation"],
    "Mortality & Health": ["mortality", "death", "life expectancy", "health", "cause of death", "suicide"],
    "Labour & Economy": ["labour", "labor", "employment", "earnings", "income", "poverty", "benefit", "workless"],
    "Housing & Families": ["housing", "rent", "home", "family", "marriage", "divorce"],
    "Methods & Infrastructure": [
        "method", "revision", "quality", "metadata", "api", "microdata", "registry",
        "archive", "discontinued", "decommission", "withdrawn", "access", "deleted"
    ],
}

STATUS_PRIORITY = {
    "Deleted": 100,
    "Cancelled": 95,
    "Rescheduled": 90,
    "Restricted": 88,
    "Upcoming": 80,
    "Published": 60,
    "Announcement": 45,
    "Monitor": 30,
}

RED_FLAG_TERMS = [
    "discontinued", "deleted", "closure", "decommission", "retired", "withdrawn",
    "cancelled", "canceled", "removed", "archive", "archived", "end of series",
    "access change", "restricted access", "deprecation", "deprecated", "shutdown"
]

DATE_PATTERNS = [
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b",
    r"\b[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}\b",
]

def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)

@dataclass
class ParsedItem:
    dataset_title: str
    source: str
    source_group: str
    source_type: str
    event_type: str
    action_date: datetime | None
    status: str
    url: str
    summary: str = ""
    theme_primary: str = "General"
    theme_secondary: str = ""
    priority_score: int = 0
    confidence: float = 0.0
    red_flag: int = 0
    deleted_signal: int = 0
    embargo: int = 0
    tags: str = ""
    raw_date: str = ""
    last_checked: str = ""
    source_page: str = ""
    fallback_hit: int = 0
    source_quality: float = 0.0
    media_relevance: int = 0
    executive_flag: int = 0
    record_key: str = ""

    def to_record(self) -> dict:
        return self.__dict__.copy()

class LCDSDataEngine:
    def __init__(self, watchlist_file: str = WATCHLIST_FILE):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.watchlist_file = watchlist_file
        self.config = self.load_watchlist()
        settings = self.config.get("settings", {})
        self.timeout = int(settings.get("timeout", DEFAULT_TIMEOUT))
        self.max_workers = int(settings.get("max_workers", 10))
        self.page_workers = int(settings.get("page_workers", 4))
        self.user_agent = settings.get("user_agent", DEFAULT_USER_AGENT)
        self.max_abs_days = int(settings.get("max_abs_days", MAX_ABS_DAYS))
        self.today = utcnow_naive().replace(hour=0, minute=0, second=0, microsecond=0)
        self.headers = {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,*/*;q=0.8",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.snapshot = self.load_json(SNAPSHOT_FILE)
        self.source_health = self.load_json(SOURCE_HEALTH_FILE)
        self.previous_df = self.load_previous_df()

    def load_watchlist(self) -> dict:
        with open(self.watchlist_file, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def load_json(self, path: str) -> dict:
        if not os.path.exists(path): return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
                return obj if isinstance(obj, dict) else {}
        except: return {}

    def save_json(self, path: str, payload: dict) -> None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)

    def load_previous_df(self) -> pd.DataFrame:
        if not os.path.exists(OUTPUT_FILE): return pd.DataFrame()
        try:
            df = pd.read_csv(OUTPUT_FILE)
            if "action_date" in df.columns:
                df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
            return df
        except: return pd.DataFrame()

    def fetch(self, url: str, source: dict) -> requests.Response:
        headers = dict(self.headers)
        headers.update(source.get("headers", {}))
        return self.session.get(url, headers=headers, timeout=source.get("timeout", self.timeout))

    def normalize_whitespace(self, text: str) -> str:
        return re.sub(r"\s+", " ", unescape(text or "")).strip()

    def normalize_date(self, value) -> datetime | None:
        if value is None: return None
        try:
            if pd.isna(value): return None
        except: pass

        if isinstance(value, pd.Timestamp): return value.to_pydatetime().replace(tzinfo=None)
        if isinstance(value, datetime): return value.replace(tzinfo=None)

        text = str(value).strip()
        if not text or text.lower() in {"nat", "nan", "none"}: return None

        for fmt in ("%Y%m%d", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
            try: return datetime.strptime(text, fmt)
            except: pass
        try: return parsedate_to_datetime(text).replace(tzinfo=None)
        except: pass
        try: return date_parser.parse(text, fuzzy=True, dayfirst=False).replace(tzinfo=None)
        except: return None

    def in_time_window(self, dt: datetime | None) -> bool:
        if dt is None: return True
        delta = (dt.date() - self.today.date()).days
        return -60 <= delta <= self.max_abs_days

    def canonical_key(self, title: str, source: str, date_obj) -> str:
        base = re.sub(r"[^a-z0-9]+", " ", str(title or "").lower()).strip()
        norm_dt = self.normalize_date(date_obj)
        date_part = norm_dt.strftime("%Y-%m-%d") if norm_dt is not None else "nodate"
        return hashlib.md5(f"{source}|{base}|{date_part}".encode("utf-8")).hexdigest()

    def source_page_list(self, source: dict) -> list[tuple[str, int]]:
        pages = []
        main_url = source.get("url")
        if main_url: pages.append((main_url, 0))
        for url in source.get("fallback_urls", []):
            if url: pages.append((url, 1))
        seen = set()
        out = []
        for url, hit in pages:
            if url not in seen:
                seen.add(url)
                out.append((url, hit))
        return out

    def classify_themes(self, text: str) -> tuple[str, str, list[str]]:
        text_l = (text or "").lower()
        scores = []
        for theme, words in THEME_KEYWORDS.items():
            score = sum(1 for w in words if w in text_l)
            if score: scores.append((theme, score))
        scores.sort(key=lambda x: x[1], reverse=True)
        primary = scores[0][0] if scores else "General"
        secondary = scores[1][0] if len(scores) > 1 else ""
        tags = [theme for theme, _ in scores[:3]]
        return primary, secondary, tags

    def compute_status(self, text: str, date_obj: datetime | None) -> tuple[str, str, int, int, int]:
        t = (text or "").lower()
        deleted_signal = int(any(term in t for term in ["deleted", "decommission", "discontinued", "withdrawn", "removed"]))

        if deleted_signal: return "Deleted", "Deletion", STATUS_PRIORITY["Deleted"], 1, 1
        if "cancelled" in t or "canceled" in t: return "Cancelled", "Cancellation", STATUS_PRIORITY["Cancelled"], 1, 0
        if "rescheduled" in t or "postponed" in t: return "Rescheduled", "Schedule Change", STATUS_PRIORITY["Rescheduled"], 1, 0
        if "restricted" in t or "access change" in t: return "Restricted", "Access Change", STATUS_PRIORITY["Restricted"], 1, 0
        if date_obj is not None:
            return ("Upcoming" if date_obj.date() >= self.today.date() else "Published", "Release",
                STATUS_PRIORITY["Upcoming"] if date_obj.date() >= self.today.date() else STATUS_PRIORITY["Published"], 0, 0)
        return "Announcement", "Announcement", STATUS_PRIORITY["Announcement"], 0, 0

    def is_junk_title(self, title: str) -> bool:
        """Kills navigation menus, specific HTML structure bloat, and pure dates."""
        t = title.lower().strip()
        if len(t) < 5: return True
        if re.fullmatch(r"^[0-9\/\-\. ]+$", t): return True
        if re.fullmatch(r"^(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}$", t): return True
        
        junk_starts = [
            "clear all", "search results", "skip to", "contact us", "privacy policy",
            "microdata access", "top of section", "upcoming releases", "release date",
            "page not found", "next page", "previous page", "data.census.gov"
        ]
        for j in junk_starts:
            if t.startswith(j) or t == j: return True
        return False

    def is_relevant(self, title: str, summary: str, source: dict) -> bool:
        """Strict contextual filter relying ONLY on title and summary to prevent HTML bleed."""
        title_l = (title or "").lower()
        summary_l = (summary or "").lower()
        combined = f"{title_l} {summary_l}"
        
        exclude_keywords = [x.lower() for x in source.get("exclude_keywords", [])]
        if exclude_keywords and any(x in combined for x in exclude_keywords): return False
            
        # CRITICAL: The Anti-Targets list stops Crop Production and Financial junk
        anti_targets = [
            "crop production", "livestock", "agriculture", "fishery", "forestry",
            "financial market", "stock exchange", "manufacturing", "industrial production",
            "producer price", "exchange rate", "interest rate", "inflation rate",
            "economic census: finance", "economic census: accommodation", "economic census: utilities",
            "economic census: management"
        ]
        if any(x in combined for x in anti_targets): return False

        keywords_any = [x.lower() for x in source.get("keywords_any", source.get("keywords", []))]
        if keywords_any:
            # Verify the keyword is actually in the title, OR highly prevalent in the summary
            if any(x in title_l for x in keywords_any): return True
            if any(x in summary_l for x in keywords_any): return True
            return False
            
        theme_words = [w for words in THEME_KEYWORDS.values() for w in words]
        return any(x in title_l for x in theme_words) or any(x in summary_l for x in theme_words)

    def get_source_quality(self, source_name: str, page_url: str) -> float:
        page_state = self.source_health.get(source_name, {}).get("pages", {}).get(page_url, {})
        attempts = float(page_state.get("attempts", 0))
        successes = float(page_state.get("successes", 0))
        if attempts == 0: return 0.5
        return round((successes + 1.0) / (attempts + 2.0), 3)

    def update_source_health(self, source_name: str, page_url: str, success: bool, item_count: int = 0) -> None:
        state = self.source_health.setdefault(source_name, {"pages": {}, "last_success": "", "last_failure": ""})
        page_state = state.setdefault("pages", {}).setdefault(page_url, {"attempts": 0, "successes": 0, "failures": 0, "items": 0})
        page_state["attempts"] += 1
        page_state["items"] += max(0, int(item_count))
        if success:
            page_state["successes"] += 1
            state["last_success"] = utcnow_naive().isoformat()
        else:
            page_state["failures"] += 1
            state["last_failure"] = utcnow_naive().isoformat()

    def build_exec_summary(self, title: str, summary: str, source: dict, status: str, action_date: datetime | None) -> str:
        bits = []
        if status == "Deleted": bits.append("Deletion signal detected.")
        elif status == "Cancelled": bits.append("Cancellation signal detected.")
        elif status == "Rescheduled": bits.append("Schedule change detected.")
        elif status == "Upcoming": bits.append("Upcoming release.")
        elif status == "Published": bits.append("Recently published.")
        else: bits.append("Signal identified.")

        if action_date is not None:
            delta = (action_date.date() - self.today.date()).days
            bits.append(f"Timing: {delta} day(s) from now." if delta >= 0 else f"Timing: {-delta} day(s) ago.")

        clean_summ = summary.replace(title, "").strip(" -:|")
        if clean_summ: bits.append(clean_summ[:220].rstrip(".") + ".")
        return " ".join(bits)

    def compute_media_relevance(self, theme_primary: str, status: str, days_to_event, source_type: str, red_flag: int, title: str) -> int:
        score = 0
        if theme_primary in {"Population", "Migration", "Mortality & Health", "Labour & Economy", "Housing & Families"}: score += 20
        if status in {"Upcoming", "Cancelled", "Deleted", "Rescheduled", "Restricted"}: score += 30
        if days_to_event is not None and pd.notna(days_to_event) and 0 <= int(days_to_event) <= 14: score += 25
        if source_type == "Official": score += 15
        if int(red_flag) == 1: score += 10
        title_l = (title or "").lower()
        if any(x in title_l for x in ["population", "migration", "fertility", "death", "mortality", "census", "asylum"]): score += 10
        return int(score)

    def record_from_fields(self, source: dict, title: str, summary: str, date_value, url: str, extra_text: str = "", source_page: str = "", fallback_hit: int = 0) -> dict | None:
        title = self.normalize_whitespace(re.sub(r"^\d+\.\s*", "", title or "").strip())
        summary = self.normalize_whitespace(summary)
        
        # Guard against structural noise
        if not title or self.is_junk_title(title):
            return None

        if not self.is_relevant(title, summary, source):
            return None

        action_date = self.normalize_date(date_value)
        if action_date is not None and not self.in_time_window(action_date):
            return None

        combined = f"{title} {summary} {extra_text}"
        status, event_type, priority_score, red_flag, deleted_signal = self.compute_status(combined, action_date)
        theme_primary, theme_secondary, tags = self.classify_themes(combined)

        if any(term in combined.lower() for term in RED_FLAG_TERMS): red_flag = 1
        embargo = int("embargo" in combined.lower())
        source_quality = self.get_source_quality(source["name"], source_page or url or source.get("url", ""))

        confidence = 0.48 + (0.15 if title else 0) + (0.10 if summary else 0) + (0.15 if action_date else 0)
        
        exec_summary = self.build_exec_summary(title, summary, source, status, action_date)
        days_to_event = None if action_date is None else (action_date.date() - self.today.date()).days
        media_relevance = self.compute_media_relevance(theme_primary, status, days_to_event, source.get("source_type", "Official"), red_flag, title)
        priority_score += int(source.get("priority_weight", 0))
        record_key = self.canonical_key(title, source["name"], action_date)
        executive_flag = int(priority_score >= 80 or red_flag == 1 or deleted_signal == 1)

        item = ParsedItem(
            dataset_title=title, source=source["name"], source_group=source.get("group", "Other"),
            source_type=source.get("source_type", "Official"), event_type=event_type, action_date=action_date,
            status=status, url=url or source.get("url", ""), summary=exec_summary,
            theme_primary=theme_primary, theme_secondary=theme_secondary, priority_score=int(priority_score),
            confidence=round(min(confidence, 0.99), 3), red_flag=int(red_flag), deleted_signal=int(deleted_signal),
            embargo=int(embargo), tags=", ".join(tags), raw_date=str(date_value or ""),
            last_checked=utcnow_naive().strftime("%Y-%m-%d %H:%M:%S"), source_page=source_page or url or source.get("url", ""),
            fallback_hit=int(fallback_hit), source_quality=float(source_quality), media_relevance=int(media_relevance),
            executive_flag=int(executive_flag), record_key=record_key,
        )
        return item.to_record()

    def parser_ons_release_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        """Injects keywords directly into the ONS Search to guarantee demographic data is found."""
        url_parts = list(urlparse(page_url))
        query = parse_qs(url_parts[4])
        
        # ONS strictly requires keywords to filter out non-demographic data
        if "keywords" not in query and source.get("keywords_any"):
            query["keywords"] = " ".join(source["keywords_any"][:5])
            
        url_parts[4] = urlencode(query, doseq=True)
        search_url = urlunparse(url_parts)

        resp = self.fetch(search_url, source)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []

        # Find any logical container for ONS results
        cards = soup.find_all(["li", "div", "article"])
        seen_links = set()

        for card in cards:
            text = card.get_text(" ", strip=True)
            if "Release date:" not in text: continue

            link = card.find("a", href=True)
            if not link: continue
                
            url = urljoin(page_url, link["href"])
            if url in seen_links: continue
                
            title = self.normalize_whitespace(link.get_text())
            m = re.search(r"Release date:\s*([^|]+)\|\s*([A-Za-z]+)", text)
            if not m: continue
                
            date_text, label = m.groups()
            date_text = date_text.strip()
            label = label.strip()

            summary = text.replace(title, "").replace(f"Release date: {date_text} | {label}", "").strip()
            seen_links.add(url)
            
            rec = self.record_from_fields(source, title, summary, date_text, url, extra_text=text, source_page=page_url, fallback_hit=fallback_hit)
            if rec:
                rec["status"] = {"Published": "Published", "Confirmed": "Upcoming", "Cancelled": "Cancelled"}.get(label, rec["status"])
                results.append(rec)

        return results

    def parser_census_upcoming(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        resp = self.fetch(page_url, source)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        lines = [self.normalize_whitespace(x) for x in soup.get_text("\n").splitlines() if self.normalize_whitespace(x)]
        results = []
        current_date = None
        current_channel = ""
        
        for line in lines:
            if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", line):
                current_date = line
                current_channel = ""
                continue
            if line in {"Microdata Access & API", "data.census.gov & API", "API", "News Releases"}:
                current_channel = line
                continue

            line = line.lstrip("*• ").strip()
            if current_date and len(line) > 4:
                rec = self.record_from_fields(
                    source, line, current_channel, current_date, page_url,
                    extra_text=f"{current_channel} {line}", source_page=page_url, fallback_hit=fallback_hit
                )
                if rec: results.append(rec)
        return results

    def parser_generic_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        """Actively hunts for structural titles (h2, a) to avoid UI bloating."""
        resp = self.fetch(page_url, source)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        results = []

        for row in soup.find_all(["tr", "li", "article", "section", "div"]):
            text = self.normalize_whitespace(row.get_text(" ", strip=True))
            if len(text) < 20: continue

            date_text = None
            for pattern in DATE_PATTERNS:
                m = re.search(pattern, text)
                if m:
                    date_text = m.group(0)
                    break

            if not date_text: continue

            title = ""
            head_node = row.find(["h2", "h3", "h4", "strong"])
            link_node = row.find("a", href=True)
            
            if head_node and len(head_node.get_text(strip=True)) > 5:
                 title = self.normalize_whitespace(head_node.get_text(strip=True))
            elif link_node and len(link_node.get_text(strip=True)) > 5:
                 title = self.normalize_whitespace(link_node.get_text(strip=True))

            if not title:
                 title = text.replace(date_text, "").strip(" |-:")[:150]

            url = urljoin(page_url, link_node["href"]) if link_node else page_url
            summary = text.replace(title, "").replace(date_text, "").strip(" -:|")

            rec = self.record_from_fields(source, title, summary, date_text, url, extra_text=text, source_page=page_url, fallback_hit=fallback_hit)
            if rec: results.append(rec)
        return results

    def parser_xml_release(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        resp = self.fetch(page_url, source)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "xml")
        results = []

        for node in soup.find_all(["release", "item", "entry"]):
            title = node.find(["title", "headline"]).get_text(" ", strip=True) if node.find(["title", "headline"]) else ""
            summary = node.find(["description", "summary"]).get_text(" ", strip=True) if node.find(["description", "summary"]) else ""
            date_val = node.find(["release_date", "pubDate", "updated", "published", "date"]).get_text(" ", strip=True) if node.find(["release_date", "pubDate", "updated", "published", "date"]) else ""
            url = node.find(["link", "id"]).get_text(" ", strip=True) if node.find(["link", "id"]) else page_url

            rec = self.record_from_fields(source, title, summary, date_val, url, extra_text=f"{title} {summary}", source_page=page_url, fallback_hit=fallback_hit)
            if rec: results.append(rec)
        return results

    def parser_rss(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        resp = self.fetch(page_url, source)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        items = root.findall(".//item") + root.findall(".//{http://www.w3.org/2005/Atom}entry")
        results = []

        for entry in items[:int(source.get("max_items", 50))]:
            def ft(names):
                for n in names:
                    x = entry.find(n)
                    if x is not None and (x.text or "").strip(): return x.text.strip()
                return ""

            title = self.normalize_whitespace(ft(["title", "{http://www.w3.org/2005/Atom}title"]))
            summary = self.normalize_whitespace(ft(["description", "summary", "{http://www.w3.org/2005/Atom}summary"]))
            date_val = ft(["pubDate", "published", "updated", "{http://www.w3.org/2005/Atom}updated"])

            # FIX: If Eurostat gives us an ugly DB code, swap it for the human-readable description
            if "Dataset: updated data" in title or "Dataset: new data" in title:
                if len(summary) > 10:
                    code_match = title.split("-")[0].strip()
                    title = f"{summary} ({code_match})"
                    summary = "Data updated or added in Eurostat database."

            link = entry.find("link")
            url = link.text.strip() if link is not None and link.text else (link.attrib.get("href") if link is not None else page_url)

            rec = self.record_from_fields(source, title, summary, date_val, url, extra_text=f"{title} {summary}", source_page=page_url, fallback_hit=fallback_hit)
            if rec: results.append(rec)
        return results

    def parser_ics_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        resp = self.fetch(page_url, source)
        resp.raise_for_status()
        results = []

        for block in resp.text.split("BEGIN:VEVENT")[1:]:
            lines = [x.strip() for x in block.split("END:VEVENT")[0].splitlines() if x.strip()]
            unf = []
            for l in lines:
                if unf and l.startswith(" "): unf[-1] += l.strip()
                else: unf.append(l)

            def rf(prefixes):
                for u in unf:
                    for p in prefixes:
                        if u.startswith(p): return u.split(":", 1)[-1].strip()
                return ""

            title = rf(["SUMMARY"])
            rec = self.record_from_fields(source, title, rf(["DESCRIPTION"]), rf(["DTSTART", "DTSTART;VALUE=DATE"]), rf(["URL"]) or page_url, extra_text=block, source_page=page_url, fallback_hit=fallback_hit)
            if rec: results.append(rec)
        return results

    def parser_html_signal_scan(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        return self.parser_generic_calendar(source, page_url, fallback_hit)

    def parser_cbs_calendar(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        return self.parser_generic_calendar(source, page_url, fallback_hit)

    def parser_gdelt_jsonfeed(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        q = quote_plus(source.get("gdelt_query", ""))
        gdelt_url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={q}&mode=artlist&maxrecords={int(source.get('max_items', 75))}&format=jsonfeed&sort=datedesc&timespan={source.get('gdelt_timespan', '7d')}"
        resp = self.fetch(gdelt_url, source)
        resp.raise_for_status()
        items = resp.json().get("items", []) if isinstance(resp.json(), dict) else []
        results = []
        target_keywords = [k.lower() for k in source.get("keywords_any", [])]

        for item in items:
            title = self.normalize_whitespace(item.get("title", ""))
            summary = self.normalize_whitespace(item.get("summary", item.get("content_text", "")))
            date_val = item.get("date_published") or item.get("date_modified")
            
            # GDELT Anti-Noise Logic
            t_l, s_l = title.lower(), summary.lower()
            if not any(k in t_l or s_l.count(k) >= 2 for k in target_keywords): continue

            rec = self.record_from_fields(source, title, summary, date_val, item.get("url", gdelt_url), extra_text=f"{title} {summary}", source_page=gdelt_url, fallback_hit=fallback_hit)
            if rec: results.append(rec)
        return results

    def parse_page(self, source: dict, page_url: str, fallback_hit: int) -> list[dict]:
        name = source.get("parser", "generic_calendar")
        aliases = {"ons_json_api": "ons_release_calendar", "html_deep_scan": "generic_calendar", "html_table_scan": "generic_calendar", "eurostat_xml": "xml_release", "rss_feed": "rss", "ics": "ics_calendar", "gdelt": "gdelt_jsonfeed"}
        parsers = {
            "ons_release_calendar": self.parser_ons_release_calendar, "census_upcoming": self.parser_census_upcoming,
            "cbs_calendar": self.parser_cbs_calendar, "generic_calendar": self.parser_generic_calendar,
            "xml_release": self.parser_xml_release, "rss": self.parser_rss, "ics_calendar": self.parser_ics_calendar,
            "html_signal_scan": self.parser_html_signal_scan, "gdelt_jsonfeed": self.parser_gdelt_jsonfeed,
        }
        return parsers.get(aliases.get(name, name), self.parser_generic_calendar)(source, page_url, fallback_hit)

    def parse_source(self, source: dict) -> tuple[dict, list[dict], dict]:
        pages = self.source_page_list(source)
        all_items = []
        with ThreadPoolExecutor(max_workers=min(self.page_workers, max(1, len(pages)))) as ex:
            futures = {ex.submit(self.parse_page, source, pu, fh): (pu, fh) for pu, fh in pages}
            for future in as_completed(futures):
                pu, fh = futures[future]
                try:
                    items = future.result()
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
                missing_keys = set(previous_snapshot.keys()) - set(current_snapshot.keys())
                for key in missing_keys:
                    row = prev_lookup[prev_lookup["record_key"] == key]
                    if not row.empty:
                        r = row.iloc[0].to_dict()
                        r["status"] = "Deleted"
                        r["event_type"] = "Deletion"
                        r["priority_score"] = STATUS_PRIORITY["Deleted"]
                        r["red_flag"] = 1
                        r["deleted_signal"] = 1
                        all_items.append(r)
        return source, all_items, current_snapshot | previous_snapshot

    def postprocess(self, rows: list[dict]) -> pd.DataFrame:
        base_cols = [
            "dataset_title", "source", "source_group", "source_type", "event_type", "action_date",
            "status", "url", "summary", "theme_primary", "theme_secondary", "priority_score",
            "confidence", "red_flag", "deleted_signal", "embargo", "tags", "raw_date",
            "last_checked", "source_page", "fallback_hit", "source_quality", "media_relevance",
            "executive_flag", "record_key"
        ]
        if not rows: return pd.DataFrame(columns=base_cols + ["days_to_event", "display_date", "sort_rank"])

        df = pd.DataFrame(rows)
        defaults = {
            "dataset_title": "", "source": "", "source_group": "Other", "source_type": "Official",
            "event_type": "Announcement", "status": "Announcement", "url": "", "summary": "",
            "theme_primary": "General", "theme_secondary": "", "priority_score": 0, "confidence": 0.0,
            "red_flag": 0, "deleted_signal": 0, "embargo": 0, "tags": "", "raw_date": "", "last_checked": "",
            "source_page": "", "fallback_hit": 0, "source_quality": 0.5, "media_relevance": 0,
            "executive_flag": 0, "record_key": ""
        }
        for col, default in defaults.items():
            if col not in df.columns: df[col] = default

        df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
        for col in ["priority_score", "red_flag", "deleted_signal", "embargo", "fallback_hit", "media_relevance", "executive_flag"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["confidence", "source_quality"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

        df["dataset_title_norm"] = df["dataset_title"].fillna("").str.lower().str.replace(r"[^a-z0-9]+", " ", regex=True).str.strip()
        missing_key = df["record_key"].fillna("") == ""
        if missing_key.any():
            df.loc[missing_key, "record_key"] = df.loc[missing_key].apply(lambda r: self.canonical_key(r.get("dataset_title", ""), r.get("source", ""), r.get("action_date")), axis=1)

        df = df.sort_values(["deleted_signal", "red_flag", "priority_score", "confidence", "source_quality", "fallback_hit", "action_date"], ascending=[False, False, False, False, False, True, True])
        df = df.drop_duplicates(subset=["record_key"], keep="first")
        date_key = df["action_date"].dt.strftime("%Y-%m-%d").fillna("nodate")
        df = df.groupby([df["dataset_title_norm"], date_key], group_keys=False).apply(lambda g: g.sort_values(["deleted_signal", "red_flag", "priority_score", "confidence", "source_quality", "fallback_hit"], ascending=[False, False, False, False, False, True]).head(1)).reset_index(drop=True)

        df["days_to_event"] = (df["action_date"].dt.normalize() - pd.Timestamp(self.today)).dt.days
        df["display_date"] = df["action_date"].dt.strftime("%d %b %Y")
        df.loc[df["action_date"].isna(), "display_date"] = "Date TBC"
        df["executive_flag"] = ((df["priority_score"] >= 80) | (df["red_flag"] == 1) | (df["deleted_signal"] == 1)).astype(int)

        missing_media = df["media_relevance"] <= 0
        if missing_media.any():
            df.loc[missing_media, "media_relevance"] = df.loc[missing_media].apply(lambda r: self.compute_media_relevance(r["theme_primary"], r["status"], r["days_to_event"], r["source_type"], r["red_flag"], r["dataset_title"]), axis=1)

        df = df[(df["action_date"].isna()) | (df["days_to_event"].abs() <= self.max_abs_days)]
        df["sort_rank"] = (df["deleted_signal"] * 1000 + df["red_flag"] * 500 + df["priority_score"] + df["media_relevance"] + (df["source_quality"] * 20).round().astype(int) - df["fallback_hit"] * 2 - df["days_to_event"].fillna(9999).clip(lower=-365, upper=365))

        keep_cols = base_cols + ["days_to_event", "display_date", "sort_rank"]
        return df[keep_cols].sort_values(["sort_rank", "action_date", "source"], ascending=[False, True, True]).reset_index(drop=True)

    def build_metrics(self, df: pd.DataFrame) -> dict:
        if df.empty: return {"records": 0, "upcoming": 0, "red_flags": 0, "deletions": 0, "next_14_days": 0, "fallback_hits": 0, "generated_at": utcnow_naive().isoformat()}
        return {"records": int(len(df)), "upcoming": int(((df["status"] == "Upcoming") & (df["days_to_event"] >= 0)).sum()), "red_flags": int(df["red_flag"].sum()), "deletions": int(df["deleted_signal"].sum()), "next_14_days": int(((df["days_to_event"] >= 0) & (df["days_to_event"] <= 14)).sum()), "fallback_hits": int(df["fallback_hit"].sum()), "generated_at": utcnow_naive().isoformat()}

    def run(self) -> pd.DataFrame:
        sources = self.config.get("sources", [])
        snapshots_out = {}
        all_rows = []
        logs = []
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
    print(f"Saved {len(frame)} records to {OUTPUT_FILE}")
