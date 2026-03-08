from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
HISTORY_CSV = DATA_DIR / "dataset_tracker_history.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
DISCOVERY_CSV = DATA_DIR / "candidate_sources.csv"
RUN_META_JSON = DATA_DIR / "last_run_meta.json"

st.set_page_config(
    page_title="Global Pop Data Watch",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .main {
        background: linear-gradient(180deg, #f8fafc 0%, #eef4ff 100%);
    }
    .stat-card {
        background: white;
        border-radius: 18px;
        padding: 14px 18px;
        box-shadow: 0 8px 24px rgba(20, 40, 90, 0.08);
        border: 1px solid rgba(80, 110, 180, 0.08);
    }
    .small-note {
        color: #51627a;
        font-size: 0.92rem;
    }
    .title-block {
        background: linear-gradient(135deg, #123c7c 0%, #295faa 55%, #4d86cf 100%);
        color: white;
        padding: 22px 24px;
        border-radius: 22px;
        box-shadow: 0 12px 32px rgba(18, 60, 124, 0.22);
        margin-bottom: 18px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def load_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


def load_meta() -> dict:
    if RUN_META_JSON.exists():
        return json.loads(RUN_META_JSON.read_text(encoding="utf-8"))
    return {}


def prep_main_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ["action_date", "announcement_date", "captured_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    df["days_until_action"] = pd.to_numeric(df.get("days_until_action"), errors="coerce")
    df["priority"] = pd.to_numeric(df.get("priority"), errors="coerce")
    df["confidence"] = pd.to_numeric(df.get("confidence"), errors="coerce")
    df["is_upcoming"] = df["days_until_action"].fillna(999999).between(0, 180)
    return df


def nice_status(status: str) -> str:
    mapping = {
        "upcoming": "Upcoming",
        "scheduled": "Scheduled",
        "recent_update": "Recent update",
        "release_notice": "Release notice",
        "future_release": "Future release",
        "future_release_signal": "Future release signal",
        "global_revision": "Global revision",
        "dataset_catalogue_entry": "Catalogue entry",
        "access_warning": "Access warning",
        "registration_required": "Registration required",
        "dataset_removed": "Dataset removed",
        "parser_error": "Parser error",
        "pyramid_update_signal": "Pyramid update signal",
    }
    return mapping.get(status, str(status).replace("_", " ").title())


def date_label(x) -> str:
    if pd.isna(x):
        return ""
    return pd.Timestamp(x).strftime("%d %b %Y")


main_df = prep_main_df(load_csv(CURRENT_CSV))
changes_df = load_csv(CHANGES_CSV)
history_df = load_csv(HISTORY_CSV)
discovery_df = load_csv(DISCOVERY_CSV)
meta = load_meta()

st.markdown(
    """
    <div class="title-block">
        <h1 style="margin:0; font-size:2rem;">Global Pop Data Watch</h1>
        <p style="margin:8px 0 0 0; font-size:1rem; opacity:0.95;">
            Daily monitor for upcoming releases, update signals, access changes, and population data events across the UK, US, Europe, Scandinavia, and global sources.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.markdown(f"<div class='stat-card'><div class='small-note'>Current tracked items</div><h2>{len(main_df)}</h2></div>", unsafe_allow_html=True)
with c2:
    upcoming = int(main_df[main_df.get("days_until_action").fillna(999999).between(0, 30)].shape[0]) if not main_df.empty else 0
    st.markdown(f"<div class='stat-card'><div class='small-note'>Next 30 days</div><h2>{upcoming}</h2></div>", unsafe_allow_html=True)
with c3:
    changed = int(changes_df[changes_df.get("change_type", "").ne("unchanged")].shape[0]) if not changes_df.empty else 0
    st.markdown(f"<div class='stat-card'><div class='small-note'>Changed this run</div><h2>{changed}</h2></div>", unsafe_allow_html=True)
with c4:
    candidates = len(discovery_df)
    st.markdown(f"<div class='stat-card'><div class='small-note'>Candidate new sources</div><h2>{candidates}</h2></div>", unsafe_allow_html=True)

if meta.get("last_run_utc"):
    st.caption(f"Last refresh: {meta['last_run_utc']}")

st.sidebar.header("Filters")

if not main_df.empty:
    regions = sorted(main_df["region"].dropna().unique().tolist())
    countries = sorted(main_df["country"].dropna().unique().tolist())
    statuses = sorted(main_df["status"].dropna().unique().tolist())
    source_names = sorted(main_df["source_name"].dropna().unique().tolist())
else:
    regions, countries, statuses, source_names = [], [], [], []

selected_regions = st.sidebar.multiselect("Region", regions, default=regions)
selected_countries = st.sidebar.multiselect("Country", countries, default=countries)
selected_sources = st.sidebar.multiselect("Source", source_names, default=source_names)
selected_statuses = st.sidebar.multiselect("Status", statuses, default=statuses)
keyword = st.sidebar.text_input("Keyword")
days_max = st.sidebar.slider("Upcoming window in days", min_value=7, max_value=180, value=90, step=1)
priority_floor = st.sidebar.slider("Minimum priority", min_value=1, max_value=10, value=1, step=1)
show_only_changed = st.sidebar.checkbox("Show only changed items", value=False)
show_only_upcoming = st.sidebar.checkbox("Show only upcoming items", value=True)

filtered = main_df.copy()
if not filtered.empty:
    if selected_regions:
        filtered = filtered[filtered["region"].isin(selected_regions)]
    if selected_countries:
        filtered = filtered[filtered["country"].isin(selected_countries)]
    if selected_sources:
        filtered = filtered[filtered["source_name"].isin(selected_sources)]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    filtered = filtered[filtered["priority"] >= priority_floor]
    if keyword:
        blob = (
            filtered["dataset_title"].fillna("") + " " +
            filtered["summary"].fillna("") + " " +
            filtered["tags"].fillna("") + " " +
            filtered["country"].fillna("")
        )
        filtered = filtered[blob.str.contains(keyword, case=False, na=False)]
    if show_only_upcoming:
        filtered = filtered[filtered["days_until_action"].fillna(999999).between(0, days_max)]

    if show_only_changed and not changes_df.empty:
        changed_ids = changes_df.loc[changes_df["change_type"] != "unchanged", "item_id"].astype(str).unique().tolist()
        filtered = filtered[filtered["item_id"].astype(str).isin(changed_ids)]

st.subheader("Priority queue")
if filtered.empty:
    st.info("No items match the current filters yet. Run the scraper or widen the filters.")
else:
    view = filtered.copy()
    view["status_label"] = view["status"].map(nice_status)
    view["action_date_label"] = view["action_date"].apply(date_label)
    view["announcement_date_label"] = view["announcement_date"].apply(date_label)
    view["dataset"] = view.apply(lambda r: f"[{r['dataset_title']}]({r['dataset_url']})" if pd.notna(r.get("dataset_url")) and str(r.get("dataset_url")).startswith("http") else r["dataset_title"], axis=1)
    display_cols = [
        "dataset", "country", "region", "source_name", "status_label", "action_type",
        "action_date_label", "announcement_date_label", "days_until_action", "tags", "summary"
    ]
    st.dataframe(
        view[display_cols].rename(columns={
            "dataset": "Dataset",
            "country": "Country",
            "region": "Region",
            "source_name": "Source",
            "status_label": "Status",
            "action_type": "Type",
            "action_date_label": "Action date",
            "announcement_date_label": "Announcement date",
            "days_until_action": "Days left",
            "tags": "Themes",
            "summary": "Plain-language summary",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Dataset": st.column_config.LinkColumn("Dataset", display_text=r"https?://.*/(.+)"),
            "Plain-language summary": st.column_config.TextColumn(width="large"),
        },
    )

st.subheader("What changed in this run")
if changes_df.empty:
    st.write("No change log available yet.")
else:
    cdf = changes_df.copy()
    for col in ["action_date", "previous_action_date"]:
        if col in cdf.columns:
            cdf[col] = pd.to_datetime(cdf[col], errors="coerce")
            cdf[col] = cdf[col].apply(date_label)
    cdf = cdf[cdf["change_type"] != "unchanged"]
    if cdf.empty:
        st.write("No changes detected in the latest run.")
    else:
        st.dataframe(
            cdf[["dataset_title", "source_name", "country", "change_type", "previous_action_date", "action_date", "status", "summary"]].rename(columns={
                "dataset_title": "Dataset",
                "source_name": "Source",
                "country": "Country",
                "change_type": "Change",
                "previous_action_date": "Previous date",
                "action_date": "Current date",
                "status": "Status",
                "summary": "Summary",
            }),
            use_container_width=True,
            hide_index=True,
        )

st.subheader("Source discovery queue")
if discovery_df.empty:
    st.write("No candidate sources have been queued yet.")
else:
    ddf = discovery_df.copy()
    domains = sorted(ddf["candidate_domain"].dropna().unique().tolist())
    selected_domains = st.multiselect("Candidate domains", domains, default=domains, key="candidate_domains")
    if selected_domains:
        ddf = ddf[ddf["candidate_domain"].isin(selected_domains)]
    st.dataframe(
        ddf[["candidate_title", "candidate_domain", "themes", "relevance_score", "candidate_url", "seed_source_name"]].rename(columns={
            "candidate_title": "Candidate page",
            "candidate_domain": "Domain",
            "themes": "Detected themes",
            "relevance_score": "Score",
            "candidate_url": "URL",
            "seed_source_name": "Found from",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "URL": st.column_config.LinkColumn("URL"),
        },
    )

st.subheader("Export")
exp1, exp2, exp3 = st.columns(3)
with exp1:
    st.download_button("Download filtered tracker CSV", filtered.to_csv(index=False).encode("utf-8"), file_name="global_pop_watch_filtered.csv", mime="text/csv")
with exp2:
    st.download_button("Download change log CSV", changes_df.to_csv(index=False).encode("utf-8"), file_name="global_pop_watch_changes.csv", mime="text/csv")
with exp3:
    st.download_button("Download candidate sources CSV", discovery_df.to_csv(index=False).encode("utf-8"), file_name="global_pop_watch_candidates.csv", mime="text/csv")

st.subheader("Notes on interpretation")
st.markdown(
    """
    This dashboard blends official release dates, update signals, access warnings, and topic-page clues.
    The short summaries are there to make the tracker readable at a glance, so you do not end up with source jargon only.
    Candidate sources are not added automatically, they are queued for review so the tracker can expand safely over time.
    """
)
