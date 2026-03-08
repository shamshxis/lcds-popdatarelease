import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GlobalPopWatch/1.0; +https://github.com/)"
}

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
PAST_WINDOW = TODAY - timedelta(days=180)
FUTURE_WINDOW = TODAY + timedelta(days=180)

THEME_KEYWORDS = [
    "population",
    "migration",
    "fertility",
    "mortality",
    "birth",
    "death",
    "census",
    "labour",
    "employment",
    "household",
    "demography",
    "asylum",
    "refugee",
    "projection",
    "estimate",
    "pyramid",
    "aging",
    "ageing",
    "life expectancy",
]

SUMMARY_HINTS = {
    "population": "Population counts, estimates, projections, or structure.",
    "migration": "Migration, asylum, or mobility related statistics and releases.",
    "fertility": "Births, fertility rates, or family formation statistics.",
    "mortality": "Deaths, survival, life expectancy, or mortality trends.",
    "census": "Census-related release, update, or dissemination notice.",
    "labour": "Employment, labour market, or workforce-related statistics.",
    "household": "Households, living conditions, or family structure data.",
    "pyramid": "Population pyramid or age structure dataset or update.",
}

DEFAULT_WATCHLIST = {
    "sources": [
        {
            "name": "ONS Migration Releases",
            "country": "UK",
            "region": "Europe",
            "theme": "migration",
            "type": "ons_release_calendar",
            "url": "https://www.ons.gov.uk/releasecalendar?highlight=true&keywords=migration&release-type=type-upcoming&sort=date-newest",
            "active": True,
            "priority": "high",
        },
        {
            "name": "ONS Population Releases",
            "country": "UK",
            "region": "Europe",
            "theme": "population",
            "type": "ons_release_calendar",
            "url": "https://www.ons.gov.uk/releasecalendar?highlight=true&keywords=population&release-type=type-upcoming&sort=date-newest",
            "active": True,
            "priority": "high",
        },
        {
            "name": "US Census Upcoming Releases",
            "country": "USA",
            "region": "North America",
            "theme": "population",
            "type": "census_upcoming",
            "url": "https://www.census.gov/data/what-is-data-census-gov/upcoming-releases.html",
            "active": True,
            "priority": "high",
        },
        {
            "name": "Eurostat Release Calendar",
            "country": "EU",
            "region": "Europe",
            "theme": "population",
            "type": "generic_release_page",
            "url": "https://ec.europa.eu/eurostat/news/release-calendar",
            "active": True,
            "priority": "high",
        },
        {
            "name": "DHS Data",
            "country": "Global",
            "region": "Global",
            "theme": "fertility",
            "type": "generic_release_page",
            "url": "https://dhsprogram.com/data/",
            "active": True,
            "priority": "high",
        },
        {
            "name": "UN World Population Prospects",
            "country": "Global",
            "region": "Global",
            "theme": "population,pyramid",
            "type": "generic_release_page",
            "url": "https://population.un.org/wpp/",
            "active": True,
            "priority": "high",
        },
        {
            "name": "Statistics Denmark Planned Releases",
            "country": "Denmark",
            "region": "Scandinavia",
            "theme": "population",
            "type": "generic_release_page",
            "url": "https://www.dst.dk/en/Statistik/planlagte",
            "active": True,
            "priority": "medium",
        },
    ]
}


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def ensure_watchlist() -> list[dict[str, Any]]:
    watchlist_path = Path("watchlist.yml")
    if not watchlist_path.exists():
        watchlist_path.write_text(yaml.safe_dump(DEFAULT_WATCHLIST, sort_keys=False), encoding="utf-8")
    with watchlist_path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return [s for s in data.get("sources", []) if s.get("active", True)]


def fetch_html(url: str) -> str:
    response = requests.get(url, headers=HEADERS, timeout=40)
    response.raise_for_status()
    return response.text


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def infer_summary(title: str, theme: str, snippet: str) -> str:
    text = f"{title} {snippet}".lower()
    for key, sentence in SUMMARY_HINTS.items():
        if key in text or key in (theme or "").lower():
            return sentence
    return "Dataset release, update, access notice, or planned publication relevant to population-related research."


def detect_status(text: str) -> str:
    t = text.lower()
    if any(x in t for x in ["removed", "withdrawn", "archived", "archive", "discontinued", "no longer available"]):
        return "warning"
    if any(x in t for x in ["upcoming", "planned", "release", "next update", "scheduled", "to be published"]):
        return "upcoming"
    if any(x in t for x in ["updated", "published", "released", "new data"]):
        return "updated"
    return "monitor"


def extract_date(text: str):
    if not text or not isinstance(text, str):
        return None

    candidates = [
        r"\b\d{1,2}\s+[A-Z][a-z]+\s+\d{4}\b",
        r"\b[A-Z][a-z]+\s+\d{4}\b",
        r"\b\d{4}-\d{2}-\d{2}\b",
        r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    ]
    for pattern in candidates:
        match = re.search(pattern, text)
        if match:
            raw = match.group(0)
            try:
                dt = date_parser.parse(raw, fuzzy=True, dayfirst=True)
                return dt.date().isoformat()
            except Exception:
                continue
    return None


def keep_row_by_window(action_date: str | None, announcement_date: str | None) -> bool:
    dates = []
    for value in [action_date, announcement_date]:
        if value:
            try:
                dates.append(date_parser.parse(value).date())
            except Exception:
                pass
    if not dates:
        return True
    return any(PAST_WINDOW <= d <= FUTURE_WINDOW for d in dates)


def row_template(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": source.get("name", ""),
        "country": source.get("country", ""),
        "region": source.get("region", ""),
        "theme": source.get("theme", ""),
        "priority": source.get("priority", "medium"),
        "dataset_title": "",
        "summary": "",
        "status": "monitor",
        "announcement_date": TODAY.isoformat(),
        "action_date": "",
        "url": source.get("url", ""),
        "notes": "",
        "last_seen": NOW.isoformat(),
    }


def parse_ons_release_calendar(source: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "lxml")
    rows = []

    texts = []
    for tag in soup.find_all(["h2", "h3", "a", "time", "p", "li"]):
        txt = clean_text(tag.get_text(" ", strip=True))
        if txt:
            texts.append(txt)

    joined = "\n".join(texts)
    lines = [x for x in joined.split("\n") if x.strip()]

    for i, line in enumerate(lines):
        lower = line.lower()
        if any(k in lower for k in THEME_KEYWORDS) and len(line) > 10:
            date_val = extract_date(line)
            context = " ".join(lines[i:i + 3])
            title = line[:220]
            row = row_template(source)
            row["dataset_title"] = title
            row["summary"] = infer_summary(title, source.get("theme", ""), context)
            row["status"] = detect_status(context)
            row["action_date"] = date_val or ""
            row["notes"] = context[:300]
            if keep_row_by_window(row["action_date"], row["announcement_date"]):
                rows.append(row)

    deduped = pd.DataFrame(rows).drop_duplicates(subset=["source", "dataset_title", "action_date", "url"])
    return deduped.to_dict(orient="records")


def parse_census_upcoming(source: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for li in soup.find_all(["li", "p", "tr"]):
        text = clean_text(li.get_text(" ", strip=True))
        if len(text) < 20:
            continue
        lower = text.lower()
        if any(k in lower for k in THEME_KEYWORDS):
            row = row_template(source)
            row["dataset_title"] = text[:220]
            row["summary"] = infer_summary(text, source.get("theme", ""), text)
            row["status"] = detect_status(text)
            row["action_date"] = extract_date(text) or ""
            row["notes"] = text[:300]
            if keep_row_by_window(row["action_date"], row["announcement_date"]):
                rows.append(row)

    deduped = pd.DataFrame(rows).drop_duplicates(subset=["source", "dataset_title", "action_date", "url"])
    return deduped.to_dict(orient="records")


def parse_generic_release_page(source: dict[str, Any]) -> list[dict[str, Any]]:
    html = fetch_html(source["url"])
    soup = BeautifulSoup(html, "lxml")
    rows = []

    for tag in soup.find_all(["a", "li", "p", "h2", "h3", "h4"]):
        text = clean_text(tag.get_text(" ", strip=True))
        if len(text) < 15:
            continue

        lower = text.lower()
        if any(k in lower for k in THEME_KEYWORDS) or any(x in lower for x in ["release", "update", "dataset", "population", "migration"]):
            row = row_template(source)
            row["dataset_title"] = text[:220]
            row["summary"] = infer_summary(text, source.get("theme", ""), text)
            row["status"] = detect_status(text)
            row["action_date"] = extract_date(text) or ""
            row["notes"] = text[:300]
            if keep_row_by_window(row["action_date"], row["announcement_date"]):
                rows.append(row)

    if not rows:
        page_text = clean_text(soup.get_text(" ", strip=True))[:800]
        row = row_template(source)
        row["dataset_title"] = source.get("name", source["url"])
        row["summary"] = infer_summary(row["dataset_title"], source.get("theme", ""), page_text)
        row["status"] = detect_status(page_text)
        row["notes"] = page_text
        rows.append(row)

    deduped = pd.DataFrame(rows).drop_duplicates(subset=["source", "dataset_title", "action_date", "url"])
    return deduped.to_dict(orient="records")


PARSERS = {
    "ons_release_calendar": parse_ons_release_calendar,
    "census_upcoming": parse_census_upcoming,
    "generic_release_page": parse_generic_release_page,
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


def generate_candidates(sources: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for source in sources:
        rows.append(
            {
                "candidate_name": source["name"],
                "country": source.get("country", ""),
                "region": source.get("region", ""),
                "theme": source.get("theme", ""),
                "candidate_url": source.get("url", ""),
                "reason": "Seed source from watchlist",
                "status": "approved",
                "last_seen": NOW.isoformat(),
            }
        )
    return pd.DataFrame(rows)


def compute_changes(old_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["source", "dataset_title", "url"]
    compare_cols = ["status", "action_date", "summary", "notes"]

    if old_df.empty:
        rows = []
        for _, row in new_df.iterrows():
            rows.append(
                {
                    "change_type": "new",
                    "source": row["source"],
                    "dataset_title": row["dataset_title"],
                    "url": row["url"],
                    "old_value": "",
                    "new_value": row["status"],
                    "changed_at": NOW.isoformat(),
                }
            )
        return pd.DataFrame(rows)

    old_map = old_df.set_index(key_cols).to_dict(orient="index")
    new_map = new_df.set_index(key_cols).to_dict(orient="index")

    changes = []

    for key, new_vals in new_map.items():
        if key not in old_map:
            changes.append(
                {
                    "change_type": "new",
                    "source": key[0],
                    "dataset_title": key[1],
                    "url": key[2],
                    "old_value": "",
                    "new_value": new_vals.get("status", ""),
                    "changed_at": NOW.isoformat(),
                }
            )
            continue

        old_vals = old_map[key]
        for col in compare_cols:
            old_v = str(old_vals.get(col, ""))
            new_v = str(new_vals.get(col, ""))
            if old_v != new_v:
                changes.append(
                    {
                        "change_type": f"changed_{col}",
                        "source": key[0],
                        "dataset_title": key[1],
                        "url": key[2],
                        "old_value": old_v,
                        "new_value": new_v,
                        "changed_at": NOW.isoformat(),
                    }
                )

    for key, old_vals in old_map.items():
        if key not in new_map:
            changes.append(
                {
                    "change_type": "missing_from_latest_run",
                    "source": key[0],
                    "dataset_title": key[1],
                    "url": key[2],
                    "old_value": old_vals.get("status", ""),
                    "new_value": "",
                    "changed_at": NOW.isoformat(),
                }
            )

    return pd.DataFrame(changes)


def main() -> None:
    ensure_dirs()
    sources = ensure_watchlist()

    tracker_columns = [
        "source",
        "country",
        "region",
        "theme",
        "priority",
        "dataset_title",
        "summary",
        "status",
        "announcement_date",
        "action_date",
        "url",
        "notes",
        "last_seen",
    ]
    status_rows = []
    all_rows = []

    for source in sources:
        parser_name = source.get("type", "generic_release_page")
        parser_func = PARSERS.get(parser_name, parse_generic_release_page)
        started = datetime.now(timezone.utc)

        try:
            rows = parser_func(source)
            all_rows.extend(rows)
            status_rows.append(
                {
                    "source": source["name"],
                    "url": source["url"],
                    "parser": parser_name,
                    "ok": True,
                    "row_count": len(rows),
                    "error": "",
                    "run_at": started.isoformat(),
                }
            )
        except Exception as e:
            status_rows.append(
                {
                    "source": source["name"],
                    "url": source["url"],
                    "parser": parser_name,
                    "ok": False,
                    "row_count": 0,
                    "error": str(e)[:500],
                    "run_at": started.isoformat(),
                }
            )

    new_df = pd.DataFrame(all_rows, columns=tracker_columns)
    if new_df.empty:
        new_df = pd.DataFrame(columns=tracker_columns)
    else:
        new_df = new_df.fillna("").drop_duplicates(subset=["source", "dataset_title", "action_date", "url"])

    old_df = load_existing(CURRENT_CSV, tracker_columns)
    changes_df = compute_changes(old_df, new_df)
    status_df = pd.DataFrame(status_rows)
    candidates_df = generate_candidates(sources)

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
