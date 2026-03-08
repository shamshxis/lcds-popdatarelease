import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
STATUS_CSV = DATA_DIR / "source_status.csv"
CANDIDATES_CSV = DATA_DIR / "candidate_sources.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/2.2; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()

DEFAULT_SETTINGS = {
    "history_days": 365,
    "lookback_days": 180,
    "lookahead_days": 180,
    "request_timeout_seconds": 20,
    "user_agent": DEFAULT_HEADERS["User-Agent"],
    "discovery_max_links_per_source": 30,
    "discovery_keywords": [
        "population", "migration", "demographic", "fertility", "mortality",
        "census", "labour", "household", "births", "deaths", "asylum",
        "projections", "estimates", "pyramid", "age", "release", "update"
    ],
    "trusted_domains": [
        "ons.gov.uk", "ec.europa.eu", "census.gov", "dhsprogram.com",
        "population.un.org", "populationpyramid.net", "scb.se", "ssb.no",
        "dst.dk", "stat.fi"
    ],
}

GENERIC_TERMS = [
    "population", "migration", "fertility", "mortality", "birth", "death",
    "census", "labour", "employment", "household", "demography", "asylum",
    "refugee", "projection", "estimate", "pyramid", "aging", "ageing",
    "life expectancy", "survey", "release", "update", "statistics", "dataset"
]

SUMMARY_HINTS = {
    "population": "Population counts, estimates, projections, or age structure.",
    "migration": "Migration, asylum, or mobility related statistics and releases.",
    "fertility": "Births, fertility rates, or family formation statistics.",
    "mortality": "Deaths, survival, life expectancy, or mortality trends.",
    "census": "Census-related release, update, or dissemination notice.",
    "labour": "Employment, labour market, or workforce-related statistics.",
    "household": "Households, living conditions, or family structure data.",
    "pyramid": "Population pyramid or age structure dataset or update.",
    "survey": "Survey dataset availability, access, or update notice.",
}

def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

def load_watchlist() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    with open("watchlist.yml", "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    settings = DEFAULT_SETTINGS.copy()
    settings.update(raw.get("settings", {}))
    return settings, raw.get("sources", [])

def get_headers(settings: dict[str, Any]) -> dict[str, str]:
    return {"User-Agent": settings.get("user_agent", DEFAULT_HEADERS["User-Agent"])}

def fetch_html(url: str, settings: dict[str, Any]) -> str:
    response = requests.get(
        url,
        headers=get_headers(settings),
        timeout=int(settings.get("request_timeout_seconds", 20)),
    )
    response.raise_for_status()
    return response.text

def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()

def infer_summary(title: str, themes: list[str], snippet: str) -> str:
    text = f"{title} {snippet} {' '.join(themes)}".lower()
    for key, sentence in SUMMARY_HINTS.items():
        if key in text:
            return sentence
    return "Dataset release, update, access notice, or planned publication relevant to population-related research."

def detect_status(text: str) -> str:
    t = text.lower()
    if any(x in t for x in ["removed", "withdrawn", "archived", "archive", "discontinued", "no longer available", "access suspended"]):
        return "warning"
    if any(x in t for x in ["upcoming", "planned", "release", "next update", "scheduled", "to be published", "forthcoming"]):
        return "upcoming"
    if any(x in t for x in ["updated", "published", "released", "new data", "available now", "last updated"]):
        return "updated"
    return "monitor"

def extract_date(text: str):
    if not text or not isinstance(text, str):
        return None
    patterns = [
        r"\b\d{1,2}\s+[A-Z][a-z]+\s+\d{4}\b",
        r"\b[A-Z][a-z]+\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                dt = date_parser.parse(match.group(0), fuzzy=True, dayfirst=True)
                return dt.date().isoformat()
            except Exception:
                continue
    return None

def get_windows(settings: dict[str, Any]):
    past_window = TODAY - timedelta(days=int(settings.get("lookback_days", 180)))
    future_window = TODAY + timedelta(days=int(settings.get("lookahead_days", 180)))
    return past_window, future_window

def keep_row_by_window(action_date: str | None, announcement_date: str | None, settings: dict[str, Any]) -> bool:
    past_window, future_window = get_windows(settings)
    dates = []
    for value in [action_date, announcement_date]:
        if value:
            try:
                dates.append(date_parser.parse(value).date())
            except Exception:
                pass
    if not dates:
        return True
    return any(past_window <= d <= future_window for d in dates)

def source_row_template(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_id": source.get("id", ""),
        "source": source.get("name", ""),
        "country": source.get("country", ""),
        "region": source.get("region", ""),
        "source_type": source.get("source_type", ""),
        "parser": source.get("parser", ""),
        "themes": ", ".join(source.get("themes", [])),
        "priority": source.get("priority", 5),
        "dataset_title": "",
        "summary": "",
        "status": "monitor",
        "announcement_date": TODAY.isoformat(),
        "action_date": "",
        "url": source.get("url", ""),
        "notes": "",
        "last_seen": NOW.isoformat(),
    }

def relevant_terms(source: dict[str, Any], settings: dict[str, Any]) -> list[str]:
    terms = []
    if source.get("keywords"):
        terms.extend([x.strip().lower() for x in str(source["keywords"]).split(",") if x.strip()])
    terms.extend([str(x).lower() for x in source.get("themes", [])])
    terms.extend([str(x).lower() for x in settings.get("discovery_keywords", [])])
    terms.extend(GENERIC_TERMS)
    return sorted(set([t for t in terms if t]))

def filter_relevant_text(text: str, source: dict[str, Any], settings: dict[str, Any]) -> bool:
    lower = text.lower()
    return any(term in lower for term in relevant_terms(source, settings))

def add_row(rows: list[dict[str, Any]], source: dict[str, Any], settings: dict[str, Any], title: str, context: str):
    row = source_row_template(source)
    row["dataset_title"] = clean_text(title)[:220]
    row["summary"] = infer_summary(title, source.get("themes", []), context)
    row["status"] = detect_status(context)
    row["action_date"] = extract_date(context) or ""
    row["notes"] = clean_text(context)[:500]
    if keep_row_by_window(row["action_date"], row["announcement_date"], settings):
        rows.append(row)

def parse_ons_release_calendar(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    params = {"highlight": "true", "release-type": "type-upcoming", "sort": "date-newest"}
    if source.get("keywords"):
        params["keywords"] = source["keywords"]
    url = source["url"]
    if "?" not in url:
        url = f"{url}?{urlencode(params)}"

    html = fetch_html(url, settings)
    soup = BeautifulSoup(html, "lxml")
    rows = []

    cards = soup.find_all(["li", "article", "div"])
    for card in cards:
        text = clean_text(card.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        if filter_relevant_text(text, source, settings):
            add_row(rows, source, settings, text[:220], text)

    if not rows:
        add_row(rows, source, settings, source["name"], "No matching ONS entries found on the current page.")
    return dedupe_rows(rows)

def parse_census_upcoming_releases(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"], settings)
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for tag in soup.find_all(["li", "p", "tr", "td", "a", "h2", "h3", "div"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        if filter_relevant_text(text, source, settings):
            add_row(rows, source, settings, text[:220], text)

    if not rows:
        add_row(rows, source, settings, source["name"], "No matching Census entries found on the current page.")
    return dedupe_rows(rows)

def parse_eurostat_release_calendar(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"], settings)
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for tag in soup.find_all(["li", "a", "p", "h2", "h3", "h4", "td", "tr", "div"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        if filter_relevant_text(text, source, settings):
            add_row(rows, source, settings, text[:220], text)

    if not rows:
        add_row(rows, source, settings, source["name"], "No matching Eurostat entries found on the current page.")
    return dedupe_rows(rows)

def parse_dhs_available_datasets(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"], settings)
    soup = BeautifulSoup(html, "lxml")
    rows = []

    page_text = clean_text(soup.get_text(" ", strip=True))
    add_row(rows, source, settings, source["name"], page_text[:700])

    for tag in soup.find_all(["a", "li", "p", "td", "tr", "div"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        if filter_relevant_text(text, source, settings):
            add_row(rows, source, settings, text[:220], text)

    return dedupe_rows(rows)

def parse_simple_page(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"], settings)
    soup = BeautifulSoup(html, "lxml")
    rows = []

    page_text = clean_text(soup.get_text(" ", strip=True))
    if page_text:
        add_row(rows, source, settings, source["name"], page_text[:700])

    for tag in soup.find_all(["a", "li", "p", "h1", "h2", "h3", "h4", "td", "tr", "div", "span"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        if filter_relevant_text(text, source, settings):
            add_row(rows, source, settings, text[:220], text)

    return dedupe_rows(rows)

def dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    df = pd.DataFrame(rows)
    if "priority" in df.columns:
        df["priority"] = pd.to_numeric(df["priority"], errors="coerce").fillna(0).astype(int)
    df = df.drop_duplicates(subset=["source_id", "dataset_title", "action_date", "url"])
    return df.to_dict(orient="records")

PARSERS = {
    "ons_release_calendar": parse_ons_release_calendar,
    "eurostat_release_calendar": parse_eurostat_release_calendar,
    "census_upcoming_releases": parse_census_upcoming_releases,
    "dhs_available_datasets": parse_dhs_available_datasets,
    "simple_page": parse_simple_page,
}

def load_existing(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path)
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            return df[columns]
        except Exception:
            pass
    return pd.DataFrame(columns=columns)

def discover_candidate_links(source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    trusted_domains = settings.get("trusted_domains", [])
    max_links = int(settings.get("discovery_max_links_per_source", 30))

    try:
        html = fetch_html(source["url"], settings)
        soup = BeautifulSoup(html, "lxml")
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            text = clean_text(a.get_text(" ", strip=True))
            if not href or len(text) < 3:
                continue

            if href.startswith("/"):
                parsed = urlparse(source["url"])
                href = f"{parsed.scheme}://{parsed.netloc}{href}"

            domain = urlparse(href).netloc.replace("www.", "")
            if not any(domain.endswith(td) for td in trusted_domains):
                continue

            combined = f"{text} {href}".lower()
            if not any(k.lower() in combined for k in settings.get("discovery_keywords", [])):
                continue

            key = (text, href)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "candidate_name": text[:180] or source["name"],
                "country": source.get("country", ""),
                "region": source.get("region", ""),
                "theme": ", ".join(source.get("themes", [])),
                "candidate_url": href,
                "reason": f"Discovered from {source['name']}",
                "status": "review",
                "last_seen": NOW.isoformat(),
            })

            if len(rows) >= max_links:
                break

    except Exception:
        pass

    return rows

def compute_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["source_id", "dataset_title", "url"]
    compare_cols = ["status", "action_date", "summary", "notes"]

    if old_df.empty:
        rows = []
        for _, row in new_df.iterrows():
            rows.append({
                "change_type": "new",
                "source_id": row["source_id"],
                "source": row["source"],
                "dataset_title": row["dataset_title"],
                "url": row["url"],
                "old_value": "",
                "new_value": row["status"],
                "changed_at": NOW.isoformat(),
            })
        return pd.DataFrame(rows)

    old_map = old_df.set_index(key_cols).to_dict(orient="index")
    new_map = new_df.set_index(key_cols).to_dict(orient="index")
    changes = []

    for key, new_vals in new_map.items():
        if key not in old_map:
            changes.append({
                "change_type": "new",
                "source_id": new_vals.get("source_id", ""),
                "source": new_vals.get("source", ""),
                "dataset_title": key[1],
                "url": key[2],
                "old_value": "",
                "new_value": new_vals.get("status", ""),
                "changed_at": NOW.isoformat(),
            })
            continue

        old_vals = old_map[key]
        for col in compare_cols:
            old_v = str(old_vals.get(col, ""))
            new_v = str(new_vals.get(col, ""))
            if old_v != new_v:
                changes.append({
                    "change_type": f"changed_{col}",
                    "source_id": new_vals.get("source_id", ""),
                    "source": new_vals.get("source", ""),
                    "dataset_title": key[1],
                    "url": key[2],
                    "old_value": old_v,
                    "new_value": new_v,
                    "changed_at": NOW.isoformat(),
                })

    for key, old_vals in old_map.items():
        if key not in new_map:
            changes.append({
                "change_type": "missing_from_latest_run",
                "source_id": old_vals.get("source_id", ""),
                "source": old_vals.get("source", ""),
                "dataset_title": key[1],
                "url": key[2],
                "old_value": old_vals.get("status", ""),
                "new_value": "",
                "changed_at": NOW.isoformat(),
            })

    return pd.DataFrame(changes)

def main() -> None:
    ensure_dirs()
    settings, sources = load_watchlist()

    tracker_columns = [
        "source_id", "source", "country", "region", "source_type", "parser",
        "themes", "priority", "dataset_title", "summary", "status",
        "announcement_date", "action_date", "url", "notes", "last_seen"
    ]

    status_rows = []
    all_rows = []
    candidate_rows = []

    for source in sources:
        parser_name = source.get("parser", "simple_page")
        parser_func = PARSERS.get(parser_name, parse_simple_page)
        started = datetime.now(timezone.utc)

        try:
            rows = parser_func(source, settings)
            all_rows.extend(rows)
            candidate_rows.extend(discover_candidate_links(source, settings))

            status_rows.append({
                "source_id": source.get("id", ""),
                "source": source["name"],
                "url": source["url"],
                "parser": parser_name,
                "ok": True,
                "row_count": len(rows),
                "error": "",
                "run_at": started.isoformat(),
            })
        except Exception as e:
            status_rows.append({
                "source_id": source.get("id", ""),
                "source": source["name"],
                "url": source["url"],
                "parser": parser_name,
                "ok": False,
                "row_count": 0,
                "error": str(e)[:500],
                "run_at": started.isoformat(),
            })

    new_df = pd.DataFrame(all_rows, columns=tracker_columns)
    if new_df.empty:
        new_df = pd.DataFrame(columns=tracker_columns)
    else:
        new_df["priority"] = pd.to_numeric(new_df["priority"], errors="coerce").fillna(0).astype(int)
        new_df = new_df.fillna("").drop_duplicates(subset=["source_id", "dataset_title", "action_date", "url"])

    old_df = load_existing(CURRENT_CSV, tracker_columns)
    changes_df = compute_changes(old_df, new_df)
    status_df = pd.DataFrame(status_rows)
    candidates_df = pd.DataFrame(candidate_rows)

    if candidates_df.empty:
        candidates_df = pd.DataFrame(columns=[
            "candidate_name", "country", "region", "theme",
            "candidate_url", "reason", "status", "last_seen"
        ])
    else:
        candidates_df = candidates_df.drop_duplicates(subset=["candidate_name", "candidate_url"])

    new_df.to_csv(CURRENT_CSV, index=False)
    changes_df.to_csv(CHANGES_CSV, index=False)
    status_df.to_csv(STATUS_CSV, index=False)
    candidates_df.to_csv(CANDIDATES_CSV, index=False)

    META_JSON.write_text(
        json.dumps(
            {
                "run_at_utc": NOW.isoformat(),
                "source_count": len(sources),
                "record_count": int(len(new_df)),
                "change_count": int(len(changes_df)),
                "ok_sources": int(status_df["ok"].sum()) if not status_df.empty else 0,
                "failed_sources": int((~status_df["ok"]).sum()) if not status_df.empty else 0,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(f"Run complete. Records: {len(new_df)} | Changes: {len(changes_df)} | Sources: {len(sources)}")

if __name__ == "__main__":
    main()
