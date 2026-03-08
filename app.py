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
change_cols = ["change_type", "source", "dataset_title", "url", "old_value", "new_value", "changed_at"]
status_cols = ["source", "url", "parser", "ok", "row_count", "error", "run_at"]
candidate_cols = ["candidate_name", "country", "region", "theme", "candidate_url", "reason", "status", "last_seen"]

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
st.caption("Daily monitor for demographic, migration, labour, census, and population-related dataset releases and updates.")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Tracked rows", len(df))
c2.metric("Changes logged", len(changes))
c3.metric("Sources OK", int(source_status["ok"].sum()) if not source_status.empty and "ok" in source_status.columns else 0)
c4.metric("Failed sources", int((~source_status["ok"]).sum()) if not source_status.empty and "ok" in source_status.columns else 0)

if meta:
    st.info(
        f"Last run: {meta.get('run_at_utc', 'n/a')} | Sources: {meta.get('source_count', 0)} | "
        f"Records: {meta.get('record_count', 0)} | Changes: {meta.get('change_count', 0)}"
    )

with st.sidebar:
    st.header("Filters")

    countries = sorted([x for x in df.get("country", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()]) if not df.empty else []
    selected_countries = st.multiselect("Country", countries, default=countries)

    regions = sorted([x for x in df.get("region", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()]) if not df.empty else []
    selected_regions = st.multiselect("Region", regions, default=regions)

    statuses = sorted([x for x in df.get("status", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()]) if not df.empty else []
    selected_statuses = st.multiselect("Status", statuses, default=statuses)

    priorities = sorted([x for x in df.get("priority", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()]) if not df.empty else []
    selected_priorities = st.multiselect("Priority", priorities, default=priorities)

    keyword = st.text_input("Keyword search")
    upcoming_only = st.checkbox("Show upcoming only", value=False)
    warnings_only = st.checkbox("Show warnings only", value=False)

filtered = df.copy()

if not filtered.empty:
    if selected_countries:
        filtered = filtered[filtered["country"].isin(selected_countries)]
    if selected_regions:
        filtered = filtered[filtered["region"].isin(selected_regions)]
    if selected_statuses:
        filtered = filtered[filtered["status"].isin(selected_statuses)]
    if selected_priorities:
        filtered = filtered[filtered["priority"].isin(selected_priorities)]

    if keyword.strip():
        mask = (
            filtered["dataset_title"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["summary"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["notes"].fillna("").str.contains(keyword, case=False, na=False)
            | filtered["theme"].fillna("").str.contains(keyword, case=False, na=False)
        )
        filtered = filtered[mask]

    if upcoming_only:
        filtered = filtered[filtered["status"].isin(["upcoming", "updated"])]

    if warnings_only:
        filtered = filtered[filtered["status"] == "warning"]

tab1, tab2, tab3, tab4 = st.tabs(["Tracker", "Changes", "Source status", "Candidate sources"])

with tab1:
    st.subheader("Tracked releases and updates")
    if filtered.empty:
        st.warning("No rows available. Check the Source status tab first to see whether the scraper ran and which sources failed.")
    else:
        display = filtered[
            [
                "source",
                "country",
                "region",
                "priority",
                "status",
                "dataset_title",
                "summary",
                "announcement_date",
                "action_date",
                "url",
            ]
        ].copy()

        display = display.sort_values(by=["priority", "action_date", "source"], ascending=[True, True, True])
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.download_button(
            label="Download tracker CSV",
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
    st.subheader("Candidate sources")
    if candidates.empty:
        st.info("No candidate sources found.")
    else:
        st.dataframe(candidates, use_container_width=True, hide_index=True)

st.markdown("### Notes")
st.write(
    "This dashboard keeps a rolling watch on official demographic and population-related sources. "
    "It is designed to prioritise human-readable summaries, visible source health, and easy filtering."
)
