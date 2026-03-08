from pathlib import Path
import json

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Global Population Data Watch",
    page_icon="🌍",
    layout="wide",
)

DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
STATUS_CSV = DATA_DIR / "source_status.csv"
CANDIDATES_CSV = DATA_DIR / "candidate_sources.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

def load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if path.exists():
        try:
            df = pd.read_csv(path)
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            return df
        except Exception:
            pass
    return pd.DataFrame(columns=columns)

tracker_cols = [
    "source_id", "source", "country", "region", "source_type", "parser",
    "themes", "priority", "dataset_title", "summary", "status",
    "announcement_date", "action_date", "url", "notes", "last_seen"
]
change_cols = [
    "change_type", "source_id", "source", "dataset_title",
    "url", "old_value", "new_value", "changed_at"
]
status_cols = ["source_id", "source", "url", "parser", "ok", "row_count", "error", "run_at"]
candidate_cols = [
    "candidate_name", "country", "region", "theme",
    "candidate_url", "reason", "status", "last_seen"
]

df = load_csv(CURRENT_CSV, tracker_cols)
changes = load_csv(CHANGES_CSV, change_cols)
source_status = load_csv(STATUS_CSV, status_cols)
candidates = load_csv(CANDIDATES_CSV, candidate_cols)

meta = {}
if META_JSON.exists():
    try:
        meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except Exception:
        meta = {}

st.title("🌍 Global Population Data Watch")
st.caption("Daily monitor for population, migration, census, labour, fertility, mortality, household, and population pyramid datasets.")

top1, top2, top3, top4 = st.columns(4)
top1.metric("Tracked rows", len(df))
top2.metric("Changes logged", len(changes))
top3.metric("Sources OK", int(source_status["ok"].sum()) if not source_status.empty and "ok" in source_status.columns else 0)
top4.metric("Failed sources", int((~source_status["ok"]).sum()) if not source_status.empty and "ok" in source_status.columns else 0)

if meta:
    st.info(
        f"Last run: {meta.get('run_at_utc', 'n/a')} | Sources: {meta.get('source_count', 0)} | "
        f"Records: {meta.get('record_count', 0)} | Changes: {meta.get('change_count', 0)} | "
        f"Window: -{meta.get('lookback_days', 180)} / +{meta.get('lookahead_days', 180)} days"
    )

with st.sidebar:
    st.header("Filters")

    countries = sorted(df["country"].dropna().astype(str).unique().tolist()) if not df.empty else []
    selected_countries = st.multiselect("Country", countries, default=countries)

    regions = sorted(df["region"].dropna().astype(str).unique().tolist()) if not df.empty else []
    selected_regions = st.multiselect("Region", regions, default=regions)

    source_types = sorted(df["source_type"].dropna().astype(str).unique().tolist()) if not df.empty else []
    selected_source_types = st.multiselect("Source type", source_types, default=source_types)

    statuses = sorted(df["status"].dropna().astype(str).unique().tolist()) if not df.empty else []
    selected_statuses = st.multiselect("Status", statuses, default=statuses)

    priorities = sorted(df["priority"].dropna().tolist()) if not df.empty else []
    selected_priorities = st.multiselect("Priority", priorities, default=priorities)

    keyword = st.text_input("Keyword search")
    warnings_only = st.checkbox("Warnings only", value=False)
    dated_only = st.checkbox("Rows with action date only", value=False)

filtered = df.copy()

if not filtered.empty:
    if selected_countries:
        filtered = filtered[filtered["country"].isin(selected_countries)]
    if selected_regions:
        filtered = filtered[filtered["region"].isin(selected_regions)]
    if selected_source_types:
        filtered = filtered[filtered["source_type"].isin(selected_source_types)]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    if selected_priorities:
        filtered = filtered[filtered["priority"].isin(selected_priorities)]

    if keyword.strip():
        mask = (
            filtered["dataset_title"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["summary"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["notes"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["themes"].fillna("").str.contains(keyword, case=False, na=False)
        )
        filtered = filtered[mask]

    if warnings_only:
        filtered = filtered[filtered["status"] == "warning"]

    if dated_only:
        filtered = filtered[filtered["action_date"].fillna("").astype(str).str.strip() != ""]

tab1, tab2, tab3, tab4 = st.tabs(["Tracker", "Changes", "Source status", "Candidate sources"])

with tab1:
    st.subheader("Tracked releases and updates")
    if filtered.empty:
        st.warning("No rows available. Check the Source status tab first to see whether the scraper ran and which sources failed.")
    else:
        display = filtered[
            [
                "source", "country", "region", "source_type", "priority", "status",
                "dataset_title", "summary", "announcement_date", "action_date", "url"
            ]
        ].copy()

        display = display.sort_values(by=["priority", "action_date", "source"], ascending=[False, True, True])
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.download_button(
            label="Download filtered tracker CSV",
            data=filtered.to_csv(index=False).encode("utf-8"),
            file_name="dataset_tracker_filtered.csv",
            mime="text/csv",
        )

with tab2:
    st.subheader("Detected changes")
    if changes.empty:
        st.info("No changes logged yet.")
    else:
        st.dataframe(changes.sort_values(by="changed_at", ascending=False), use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Source status")
    if source_status.empty:
        st.warning("No source status file found yet.")
    else:
        st.dataframe(source_status.sort_values(by=["ok", "source"], ascending=[False, True]), use_container_width=True, hide_index=True)

with tab4:
    st.subheader("Candidate sources for review")
    if candidates.empty:
        st.info("No candidate sources found.")
    else:
        st.dataframe(candidates, use_container_width=True, hide_index=True)

st.markdown("### Notes")
st.write(
    "This dashboard is designed to support a rolling global population data watch with readable summaries, "
    "source-health visibility, and expansion through reviewed candidate sources."
)
