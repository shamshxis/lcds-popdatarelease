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
            # FIX: Read as string (dtype=str) to prevent PyArrow/Streamlit crashes 
            # due to mixed types (e.g. ints mixed with strings)
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
            for col in columns:
                if col not in df.columns:
                    df[col] = ""
            return df
        except Exception as e:
            st.error(f"Could not read {path.name}: {e}")
    return pd.DataFrame(columns=columns)


def normalise_bool(series: pd.Series) -> pd.Series:
    if series.empty:
        return series
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map({"true": True, "false": False, "1": True, "0": False})
        .fillna(False)
    )


def prepare_tracker(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()

    # Ensure all columns exist
    for col in ["source_id", "source", "country", "region", "source_type", "parser",
                "themes", "dataset_title", "summary", "status", "announcement_date",
                "action_date", "url", "notes", "last_seen"]:
        if col not in df.columns:
            df[col] = ""

    # FIX: Explicit type conversion for numeric/date columns
    # We force errors="coerce" to turn bad data into NaT/NaN safely
    df["priority"] = pd.to_numeric(df.get("priority", 0), errors="coerce").fillna(0).astype(int)
    df["announcement_date_dt"] = pd.to_datetime(df["announcement_date"], errors="coerce")
    df["action_date_dt"] = pd.to_datetime(df["action_date"], errors="coerce")

    # Ensure text columns are actually strings to please PyArrow
    text_cols = ["source", "country", "region", "source_type", "parser", 
                 "themes", "dataset_title", "summary", "status", "url", "notes"]
    for col in text_cols:
        df[col] = df[col].astype(str).replace("nan", "")

    return df


def prepare_status(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for col in ["source_id", "source", "url", "parser", "ok", "row_count", "error", "run_at"]:
        if col not in df.columns:
            df[col] = ""

    df["ok"] = normalise_bool(df["ok"])
    df["row_count"] = pd.to_numeric(df["row_count"], errors="coerce").fillna(0).astype(int)
    return df


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

# Load Data
df = prepare_tracker(load_csv(CURRENT_CSV, tracker_cols))
changes = load_csv(CHANGES_CSV, change_cols)
source_status = prepare_status(load_csv(STATUS_CSV, status_cols))
candidates = load_csv(CANDIDATES_CSV, candidate_cols)

meta = {}
if META_JSON.exists():
    try:
        meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except Exception:
        meta = {}

st.title("🌍 Global Population Data Watch")
st.caption("Daily monitor for population, migration, census, labour, fertility, mortality, household, and population pyramid datasets.")

ok_sources = int(source_status["ok"].sum()) if not source_status.empty else 0
failed_sources = int((~source_status["ok"]).sum()) if not source_status.empty else 0

m1, m2, m3, m4 = st.columns(4)
m1.metric("Tracked rows", len(df))
m2.metric("Changes logged", len(changes))
m3.metric("Sources OK", ok_sources)
m4.metric("Failed sources", failed_sources)

if meta:
    st.info(
        f"Last run: {meta.get('run_at_utc', 'n/a')} | Sources: {meta.get('source_count', 0)} | "
        f"Records: {meta.get('record_count', 0)} | Changes: {meta.get('change_count', 0)}"
    )

with st.sidebar:
    st.header("Filters")

    # Safely get unique values
    countries = sorted(list(set(df["country"].astype(str).dropna()) - {"", "nan"})) if not df.empty else []
    selected_countries = st.multiselect("Country", countries)

    regions = sorted(list(set(df["region"].astype(str).dropna()) - {"", "nan"})) if not df.empty else []
    selected_regions = st.multiselect("Region", regions)

    source_types = sorted(list(set(df["source_type"].astype(str).dropna()) - {"", "nan"})) if not df.empty else []
    selected_source_types = st.multiselect("Source type", source_types)

    statuses = sorted(list(set(df["status"].astype(str).dropna()) - {"", "nan"})) if not df.empty else []
    selected_statuses = st.multiselect("Status", statuses)

    priorities = sorted(df["priority"].unique().tolist()) if not df.empty else []
    selected_priorities = st.multiselect("Priority", priorities)

    keyword = st.text_input("Keyword search")
    dated_only = st.checkbox("Rows with action date only", value=False)
    warnings_only = st.checkbox("Warnings only", value=False)

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
        # Case-insensitive string matching
        mask = (
            filtered["dataset_title"].astype(str).str.contains(keyword, case=False, na=False) |
            filtered["summary"].astype(str).str.contains(keyword, case=False, na=False) |
            filtered["notes"].astype(str).str.contains(keyword, case=False, na=False) |
            filtered["themes"].astype(str).str.contains(keyword, case=False, na=False)
        )
        filtered = filtered[mask]

    if dated_only:
        filtered = filtered[filtered["action_date_dt"].notna()]

    if warnings_only:
        filtered = filtered[filtered["status"] == "warning"]

tab1, tab2, tab3, tab4 = st.tabs(["Tracker", "Changes", "Source status", "Candidate sources"])

with tab1:
    st.subheader("Tracked releases and updates")
    if filtered.empty:
        st.warning("No rows available. Check the Source status tab first.")
    else:
        # Prepare display DF
        display = filtered[
            [
                "source", "country", "region", "source_type", "priority", "status",
                "dataset_title", "summary", "announcement_date", "action_date", "url"
            ]
        ].copy()

        # Safe Sort
        display = display.sort_values(
            by=["priority", "action_date_dt", "source"],
            ascending=[False, True, True],
            na_position="last"
        )

        st.dataframe(
            display.drop(columns=["action_date_dt"], errors="ignore"), 
            use_container_width=True, 
            hide_index=True
        )
        
        st.download_button(
            label="Download filtered tracker CSV",
            data=filtered.drop(columns=["announcement_date_dt", "action_date_dt"], errors="ignore").to_csv(index=False).encode("utf-8"),
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
        st.dataframe(source_status.sort_values(by=["ok", "row_count", "source"], ascending=[False, False, True]), use_container_width=True, hide_index=True)

with tab4:
    st.subheader("Candidate sources for review")
    if candidates.empty:
        st.info("No candidate sources found.")
    else:
        st.dataframe(candidates, use_container_width=True, hide_index=True)

st.markdown("### Notes")
st.write("This dashboard is designed to show source health clearly and keep filters stable even when the scraped CSV has mixed types.")
