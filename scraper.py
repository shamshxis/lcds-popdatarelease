from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
CURRENT_CSV = DATA_DIR / 'dataset_tracker.csv'
HISTORY_CSV = DATA_DIR / 'dataset_tracker_history.csv'
CHANGES_CSV = DATA_DIR / 'dataset_changes.csv'
DISCOVERY_CSV = DATA_DIR / 'candidate_sources.csv'
SOURCE_STATUS_CSV = DATA_DIR / 'source_status.csv'
RUN_META_JSON = DATA_DIR / 'last_run_meta.json'

DATE_PATTERNS = [
    r"\b\d{1,2}\s+[A-Z][a-z]+\s+\d{4}(?:\s+\d{1,2}:\d{2}(?:am|pm)?)?\b",
    r"\b[A-Z][a-z]+\s+\d{1,2},\s*\d{4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    r"\b\d{1,2}-\d{2}-\d{4}\b",
    r"\b[A-Z][a-z]+\s+\d{4}\b",
]

KEYWORDS = {
    'population', 'migration', 'demograph', 'fertility', 'mortality', 'census',
    'labour', 'household', 'birth', 'death', 'asylum', 'projection', 'estimate',
    'pyramid', 'survey', 'health', 'family', 'income', 'age', 'employment'
}


@dataclass
class Config:
    settings: dict[str, Any]
    sources: list[dict[str, Any]]


def load_config() -> Config:
    with open(BASE_DIR / 'watchlist.yml', 'r', encoding='utf-8') as f:
        raw = yaml.safe_load(f)
    return Config(settings=raw.get('settings', {}), sources=raw.get('sources', []))


def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def make_session(user_agent: str) -> requests.Session:
    s = requests.Session()
    retries = Retry(total=2, connect=2, read=2, backoff_factor=0.8, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    s.headers.update({'User-Agent': user_agent, 'Accept-Language': 'en-GB,en;q=0.9'})
    return s


def clean_text(text: str | None) -> str:
    return re.sub(r'\s+', ' ', text or '').strip()


def fetch_html(session: requests.Session, url: str, timeout: int) -> tuple[str, BeautifulSoup]:
    r = session.get(url, timeout=timeout)
    r.raise_for_status()
    html = r.text
    return html, BeautifulSoup(html, 'html.parser')


def parse_date_from_text(text: str) -> datetime | None:
    text = clean_text(text)
    if not text:
        return None
    for pattern in DATE_PATTERNS:
        m = re.search(pattern, text)
        if not m:
            continue
        try:
            return date_parser.parse(m.group(0), fuzzy=True, dayfirst=True)
        except Exception:
            pass
    try:
        return date_parser.parse(text, fuzzy=True, dayfirst=True, default=datetime(1900, 1, 1))
    except Exception:
        return None


def within_window(action_date: datetime | None, settings: dict[str, Any]) -> bool:
    if action_date is None:
        return True
    today = now_utc().replace(tzinfo=None)
    lookback_days = int(settings.get('lookback_days', 180))
    lookahead_days = int(settings.get('lookahead_days', 180))
    dt = action_date.replace(tzinfo=None)
    return today - timedelta(days=lookback_days) <= dt <= today + timedelta(days=lookahead_days)


def build_record(source: dict[str, Any], title: str, url: str, summary: str, action_date: datetime | None,
                 status: str, action_type: str, confidence: float, raw_context: str) -> dict[str, Any]:
    item_key = f"{source.get('id')}|{clean_text(title)}|{clean_text(url)}|{action_date.date().isoformat() if action_date else ''}"
    item_id = str(abs(hash(item_key)))[:16]
    return {
        'item_id': item_id,
        'source_id': source.get('id'),
        'source_name': source.get('name'),
        'source_type': source.get('source_type'),
        'parser': source.get('parser'),
        'region': source.get('region'),
        'country': source.get('country'),
        'priority': source.get('priority', 0),
        'dataset_title': clean_text(title)[:220],
        'dataset_url': url,
        'summary': clean_text(summary)[:350],
        'tags': ', '.join(source.get('themes', [])),
        'status': status,
        'action_type': action_type,
        'announcement_date': now_utc().date().isoformat(),
        'action_date': action_date.date().isoformat() if action_date else '',
        'days_until_action': (action_date.date() - now_utc().date()).days if action_date else None,
        'confidence': round(confidence, 2),
        'raw_context': clean_text(raw_context)[:700],
        'captured_at': now_utc().isoformat(),
    }


def dedupe(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    out = []
    for r in records:
        key = (r['item_id'], r['status'], r['action_date'])
        if key not in seen:
            out.append(r)
            seen.add(key)
    return out


def generic_link_scan(soup: BeautifulSoup, source: dict[str, Any]) -> list[dict[str, Any]]:
    records = []
    for a in soup.find_all('a', href=True):
        title = clean_text(a.get_text(' ', strip=True))
        if len(title) < 8:
            continue
        context = clean_text(a.parent.get_text(' ', strip=True))
        blob = f"{title} {context}".lower()
        if not any(k in blob for k in source.get('themes', [])):
            continue
        dt = parse_date_from_text(context)
        href = urljoin(source['url'], a['href'])
        records.append(build_record(
            source, title, href,
            summary=context[:260] or title,
            action_date=dt,
            status='upcoming' if dt and dt.date() >= now_utc().date() else 'page_signal',
            action_type='update', confidence=0.55, raw_context=context
        ))
    return dedupe(records)


def parser_ons_release_calendar(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    url = source['url']
    if source.get('keywords'):
        url = f"{url}?highlight=true&keywords={source['keywords']}&limit=25&page=1&release-type=type-upcoming&sort=date-newest"
    _, soup = fetch_html(session, url, settings['request_timeout_seconds'])
    text = clean_text(soup.get_text('\n', strip=True))
    records = []
    for chunk in re.split(r'\s{2,}|\n+', text):
        low = chunk.lower()
        if 'release date' not in low:
            continue
        if not any(k in low for k in source.get('themes', [])):
            continue
        dt = parse_date_from_text(chunk)
        title = re.split(r'Release date:|\|', chunk, maxsplit=1)[0].strip(' .|')
        if len(title) < 10:
            continue
        records.append(build_record(source, title, url, chunk, dt, 'upcoming', 'release', 0.9, chunk))
    if not records:
        records.extend(generic_link_scan(soup, source))
    return dedupe([r for r in records if within_window(parse_date_from_text(r['action_date']) if r['action_date'] else None, settings)])


def parser_eurostat_release_calendar(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    _, soup = fetch_html(session, source['url'], settings['request_timeout_seconds'])
    text = soup.get_text('\n', strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    records = []
    for i, line in enumerate(lines):
        low = line.lower()
        if not any(k in low for k in source.get('themes', [])):
            continue
        if i + 1 < len(lines) and re.search(r'\b\d{1,2}\s+[A-Z][a-z]+\s+\d{4}\b', lines[i + 1]):
            dt = parse_date_from_text(lines[i + 1])
            summary = ' '.join(lines[i:i + 3])
            records.append(build_record(source, line, source['url'], summary, dt, 'upcoming', 'release', 0.82, summary))
    if not records:
        records.extend(generic_link_scan(soup, source))
    return dedupe(records)


def parser_census_upcoming_releases(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    _, soup = fetch_html(session, source['url'], settings['request_timeout_seconds'])
    text = soup.get_text('\n', strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    records = []
    current_date = None
    for i, line in enumerate(lines):
        if re.fullmatch(r'\d{1,2}/\d{1,2}/\d{4}', line):
            current_date = parse_date_from_text(line)
            continue
        low = line.lower()
        if current_date and any(k in low for k in source.get('themes', [])):
            summary = ' '.join(lines[max(0, i - 1): min(len(lines), i + 2)])
            records.append(build_record(source, line, source['url'], summary, current_date, 'upcoming', 'release', 0.87, summary))
    if not records:
        records.extend(generic_link_scan(soup, source))
    return dedupe(records)


def parser_simple_page(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    _, soup = fetch_html(session, source['url'], settings['request_timeout_seconds'])
    records = generic_link_scan(soup, source)
    if records:
        return records[:60]
    title = clean_text(soup.title.get_text(' ', strip=True)) if soup.title else source['name']
    body = clean_text(soup.get_text(' ', strip=True))
    dt = parse_date_from_text(body)
    return [build_record(source, title, source['url'], body[:260], dt, 'page_signal', 'update', 0.4, body[:600])]


def parser_dhs_available_datasets(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    _, soup = fetch_html(session, source['url'], settings['request_timeout_seconds'])
    text = soup.get_text('\n', strip=True)
    lines = [clean_text(x) for x in text.splitlines() if clean_text(x)]
    records = []
    for line in lines:
        low = line.lower()
        if len(line) < 8:
            continue
        if not re.search(r'\b(19|20)\d{2}\b', line):
            continue
        if not any(k in low for k in ['dhs', 'survey', 'mis', 'ais', 'malaria']):
            continue
        records.append(build_record(source, line[:160], source['url'], 'DHS catalogue entry detected on the available datasets page.', parse_date_from_text(line), 'dataset_catalogue_entry', 'dataset_update', 0.6, line))
    return dedupe(records[:200])


def discover_candidates(session: requests.Session, source: dict[str, Any], settings: dict[str, Any]) -> list[dict[str, Any]]:
    trusted = {d.lower() for d in settings.get('trusted_domains', [])}
    _, soup = fetch_html(session, source['url'], settings['request_timeout_seconds'])
    rows = []
    seen = set()
    for a in soup.find_all('a', href=True):
        href = urljoin(source['url'], a['href'])
        domain = href.split('/')[2].replace('www.', '').lower() if '://' in href else ''
        if domain not in trusted or href in seen:
            continue
        seen.add(href)
        anchor = clean_text(a.get_text(' ', strip=True))
        context = clean_text(a.parent.get_text(' ', strip=True))
        blob = f"{anchor} {context} {href}".lower()
        hits = [k for k in settings.get('discovery_keywords', []) if k in blob]
        if not hits:
            continue
        rows.append({
            'seed_source_id': source.get('id'),
            'seed_source_name': source.get('name'),
            'candidate_title': anchor[:180] or source.get('name'),
            'candidate_url': href,
            'candidate_domain': domain,
            'themes': ', '.join(sorted(set(hits))),
            'relevance_score': min(1.0, 0.2 + 0.12 * len(set(hits))),
            'captured_at': now_utc().isoformat(),
            'status': 'review',
        })
        if len(rows) >= int(settings.get('discovery_max_links_per_source', 30)):
            break
    return rows


PARSERS = {
    'ons_release_calendar': parser_ons_release_calendar,
    'eurostat_release_calendar': parser_eurostat_release_calendar,
    'census_upcoming_releases': parser_census_upcoming_releases,
    'dhs_available_datasets': parser_dhs_available_datasets,
    'simple_page': parser_simple_page,
}


def load_existing(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def classify_changes(new_df: pd.DataFrame, old_df: pd.DataFrame) -> pd.DataFrame:
    if new_df.empty:
        return pd.DataFrame(columns=list(new_df.columns) + ['change_type', 'previous_action_date', 'previous_status'])
    if old_df.empty:
        out = new_df.copy()
        out['change_type'] = 'new'
        out['previous_action_date'] = ''
        out['previous_status'] = ''
        return out
    old_subset = old_df[['item_id', 'action_date', 'status']].rename(columns={'action_date': 'previous_action_date', 'status': 'previous_status'})
    merged = new_df.merge(old_subset, on='item_id', how='left')
    def decide(row: pd.Series) -> str:
        if pd.isna(row.get('previous_status')) and pd.isna(row.get('previous_action_date')):
            return 'new'
        if str(row.get('action_date', '')) != str(row.get('previous_action_date', '')):
            return 'date_changed'
        if str(row.get('status', '')) != str(row.get('previous_status', '')):
            return 'status_changed'
        return 'unchanged'
    merged['change_type'] = merged.apply(decide, axis=1)
    return merged


def write_csv(df: pd.DataFrame, path: Path, columns: list[str]) -> None:
    if df.empty:
        pd.DataFrame(columns=columns).to_csv(path, index=False)
    else:
        df.to_csv(path, index=False)


def run() -> None:
    ensure_dirs()
    config = load_config()
    session = make_session(config.settings.get('user_agent', 'GlobalPopWatch/2.0'))
    all_records: list[dict[str, Any]] = []
    source_status_rows: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []

    for source in config.sources:
        parser_name = source.get('parser', 'simple_page')
        parser_fn = PARSERS.get(parser_name, parser_simple_page)
        start = now_utc()
        try:
            rows = parser_fn(session, source, config.settings)
            all_records.extend(rows)
            source_status_rows.append({
                'source_id': source.get('id'), 'source_name': source.get('name'), 'url': source.get('url'),
                'parser': parser_name, 'ok': True, 'records': len(rows), 'error': '', 'ran_at': now_utc().isoformat(),
                'elapsed_seconds': round((now_utc() - start).total_seconds(), 2)
            })
        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            source_status_rows.append({
                'source_id': source.get('id'), 'source_name': source.get('name'), 'url': source.get('url'),
                'parser': parser_name, 'ok': False, 'records': 0, 'error': msg[:400], 'ran_at': now_utc().isoformat(),
                'elapsed_seconds': round((now_utc() - start).total_seconds(), 2)
            })
            all_records.append(build_record(source, source.get('name', 'Unknown source'), source.get('url', ''), f'Parser error for this source in this run: {msg}', None, 'parser_error', 'error', 0.05, msg))
        try:
            candidate_rows.extend(discover_candidates(session, source, config.settings))
        except Exception:
            pass

    current_df = pd.DataFrame(all_records)
    if not current_df.empty:
        current_df = current_df.drop_duplicates(subset=['item_id', 'status', 'action_date']).copy()
        current_df = current_df.sort_values(['priority', 'country', 'action_date'], ascending=[False, True, True])

    old_df = load_existing(CURRENT_CSV)
    changes_df = classify_changes(current_df, old_df)
    history_df = load_existing(HISTORY_CSV)
    if not current_df.empty:
        snap = current_df.copy()
        snap['snapshot_date'] = now_utc().date().isoformat()
        history_df = pd.concat([history_df, snap], ignore_index=True)
        history_df['snapshot_date'] = pd.to_datetime(history_df['snapshot_date'], errors='coerce')
        cutoff = pd.Timestamp(now_utc().date() - timedelta(days=int(config.settings.get('history_days', 365))))
        history_df = history_df[history_df['snapshot_date'] >= cutoff]

    discovery_df = pd.DataFrame(candidate_rows).drop_duplicates(subset=['candidate_url']) if candidate_rows else pd.DataFrame()
    status_df = pd.DataFrame(source_status_rows)

    base_cols = ['item_id','source_id','source_name','source_type','parser','region','country','priority','dataset_title','dataset_url','summary','tags','status','action_type','announcement_date','action_date','days_until_action','confidence','raw_context','captured_at']
    change_cols = base_cols + ['previous_action_date','previous_status','change_type']
    discovery_cols = ['seed_source_id','seed_source_name','candidate_title','candidate_url','candidate_domain','themes','relevance_score','captured_at','status']
    status_cols = ['source_id','source_name','url','parser','ok','records','error','ran_at','elapsed_seconds']

    write_csv(current_df, CURRENT_CSV, base_cols)
    write_csv(changes_df, CHANGES_CSV, change_cols)
    write_csv(history_df, HISTORY_CSV, base_cols + ['snapshot_date'])
    write_csv(discovery_df, DISCOVERY_CSV, discovery_cols)
    write_csv(status_df, SOURCE_STATUS_CSV, status_cols)

    with open(RUN_META_JSON, 'w', encoding='utf-8') as f:
        json.dump({
            'last_run_utc': now_utc().isoformat(),
            'records_current': int(len(current_df)),
            'changes_current': int(len(changes_df)),
            'candidate_sources': int(len(discovery_df)),
            'sources_checked': int(len(status_df)),
            'sources_ok': int(status_df['ok'].sum()) if not status_df.empty else 0,
        }, f, indent=2)

    print(f"Saved {len(current_df)} tracker rows, {len(changes_df)} change rows, {len(discovery_df)} candidate rows, {len(status_df)} source status rows.")


if __name__ == '__main__':
    run()
