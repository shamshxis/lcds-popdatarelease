import json
import os
import subprocess

import pandas as pd
import streamlit as st

st.set_page_config(page_title="LCDS Executive Data Watch", page_icon="📡", layout="wide")

DATA_FILE = "data/dataset_tracker.csv"
RUNLOG_FILE = "data/run_log.json"


def run_scan():
    proc = subprocess.run(["python", "scraper.py"], capture_output=True, text=True)
    if proc.returncode != 0:
        st.error("Scan failed")
        st.code(proc.stderr or proc.stdout)
        return False
    st.success("Scan complete")
    if proc.stdout:
        with st.expander("Scanner log"):
            st.code(proc.stdout)
    return True


def load_data() -> pd.DataFrame:
    if not os.path.exists(DATA_FILE):
        return pd.DataFrame()
    df = pd.read_csv(DATA_FILE)
    if "action_date" in df.columns:
        df["action_date"] = pd.to_datetime(df["action_date"], errors="coerce")
    if "last_checked" in df.columns:
        df["last_checked"] = pd.to_datetime(df["last_checked"], errors="coerce")
    return df


def load_metrics() -> dict:
    if not os.path.exists(RUNLOG_FILE):
        return {}
    try:
        with open(RUNLOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("metrics", {})
    except Exception:
        return {}


def style_status(v: str) -> str:
    if v == "Deleted":
        return "background-color: #7f1d1d; color: white; font-weight: 700"
    if v == "Cancelled":
        return "background-color: #991b1b; color: white; font-weight: 700"
    if v == "Rescheduled":
        return "background-color: #92400e; color: white; font-weight: 700"
    if v == "Restricted":
        return "background-color: #7c2d12; color: white; font-weight: 700"
    if v == "Upcoming":
        return "background-color: #1d4ed8; color: white"
    if v == "Published":
        return "background-color: #065f46; color: white"
    if v == "Monitor":
        return "background-color: #374151; color: white"
    return ""


def style_date(row):
    styles = []
    deleted = int(row.get("deleted_signal", 0)) == 1
    red = int(row.get("red_flag", 0)) == 1
    for c in row.index:
        if c == "display_date" and (deleted or red):
            styles.append("color: #b91c1c; font-weight: 700")
        else:
            styles.append("")
    return styles


st.title("📡 LCDS Executive Data Watch")
st.caption("Executive monitoring for releases, revisions, withdrawals, restrictions, and signals across demographic and population data sources")

top_left, top_right = st.columns([1, 4])
with top_left:
    if st.button("Run scan", use_container_width=True):
        ok = run_scan()
        if ok:
            st.rerun()
with top_right:
    metrics = load_metrics()
    generated = metrics.get("generated_at")
    st.write(f"Last engine update: {generated if generated else 'Not available'}")

if not os.path.exists(DATA_FILE):
    st.info("No dataset file found yet. Run a scan to initialize the dashboard.")
    st.stop()

df = load_data()
if df.empty:
    st.warning("Dataset file exists but contains no records.")
    st.stop()

if "days_to_event" not in df.columns:
    now = pd.Timestamp.utcnow().normalize().tz_localize(None)
    df["days_to_event"] = (df["action_date"].dt.normalize() - now).dt.days

if "display_date" not in df.columns:
    df["display_date"] = df["action_date"].dt.strftime("%d %b %Y")
    df.loc[df["action_date"].isna(), "display_date"] = "Date TBC"

if "executive_flag" not in df.columns:
    priority = pd.to_numeric(df["priority_score"], errors="coerce").fillna(0)
    red_flag = pd.to_numeric(df["red_flag"], errors="coerce").fillna(0)
    deleted_signal = pd.to_numeric(df["deleted_signal"], errors="coerce").fillna(0)
    df["executive_flag"] = ((priority >= 80) | (red_flag == 1) | (deleted_signal == 1)).astype(int)

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Total records", int(metrics.get("records", len(df))))
m2.metric("Upcoming", int(metrics.get("upcoming", ((df["status"] == "Upcoming") & (df["days_to_event"] >= 0)).sum())))
m3.metric("Next 14 days", int(metrics.get("next_14_days", ((df["days_to_event"] >= 0) & (df["days_to_event"] <= 14)).sum())))
m4.metric("Red flags", int(metrics.get("red_flags", pd.to_numeric(df.get("red_flag", 0), errors="coerce").fillna(0).sum())))
m5.metric("Deletion signals", int(metrics.get("deletions", pd.to_numeric(df.get("deleted_signal", 0), errors="coerce").fillna(0).sum())))
m6.metric("Fallback hits", int(metrics.get("fallback_hits", pd.to_numeric(df.get("fallback_hit", 0), errors="coerce").fillna(0).sum())))

with st.sidebar:
    st.header("Filters")

    groups = sorted(df["source_group"].dropna().unique().tolist()) if "source_group" in df.columns else []
    sources = sorted(df["source"].dropna().unique().tolist()) if "source" in df.columns else []
    themes = sorted(df["theme_primary"].dropna().unique().tolist()) if "theme_primary" in df.columns else []
    statuses = sorted(df["status"].dropna().unique().tolist()) if "status" in df.columns else []

    selected_groups = st.multiselect("Source group", groups, default=groups)
    selected_sources = st.multiselect("Source", sources, default=sources)
    selected_themes = st.multiselect("Theme", themes, default=themes)
    selected_statuses = st.multiselect("Status", statuses, default=statuses)
    executive_only = st.checkbox("Executive issues only", value=True)
    primary_only = st.checkbox("Primary sources only", value=False)
    text_filter = st.text_input("Search title or summary")

view = df.copy()
if selected_groups and "source_group" in view.columns:
    view = view[view["source_group"].isin(selected_groups)]
if selected_sources and "source" in view.columns:
    view = view[view["source"].isin(selected_sources)]
if selected_themes and "theme_primary" in view.columns:
    view = view[view["theme_primary"].isin(selected_themes)]
if selected_statuses and "status" in view.columns:
    view = view[view["status"].isin(selected_statuses)]
if executive_only and "executive_flag" in view.columns:
    view = view[view["executive_flag"] == 1]
if primary_only and "fallback_hit" in view.columns:
    view = view[view["fallback_hit"] == 0]
if text_filter:
    q = text_filter.lower()
    title_series = view["dataset_title"].fillna("").str.lower() if "dataset_title" in view.columns else pd.Series([], dtype=str)
    summary_series = view["summary"].fillna("").str.lower() if "summary" in view.columns else pd.Series([], dtype=str)
    view = view[title_series.str.contains(q) | summary_series.str.contains(q)]

briefing = view.sort_values(
    ["deleted_signal", "red_flag", "media_relevance", "priority_score", "fallback_hit", "action_date"],
    ascending=[False, False, False, False, True, True]
).head(15)

st.subheader("Executive briefing")
for _, row in briefing.iterrows():
    icon = "🟥" if int(row.get("deleted_signal", 0)) == 1 else "🟧" if int(row.get("red_flag", 0)) == 1 else "🟦"
    with st.container(border=True):
        st.markdown(f"{icon} **{row.get('dataset_title', '')}**")
        cols = st.columns([1.2, 1.1, 1.2, 1, 1, 1])
        cols[0].write(f"**Source**  \n{row.get('source', '')}")
        cols[1].write(f"**Date**  \n{row.get('display_date', 'Date TBC')}")
        cols[2].write(f"**Theme**  \n{row.get('theme_primary', 'General')}")
        cols[3].write(f"**Status**  \n{row.get('status', '')}")
        cols[4].write(f"**Priority**  \n{int(row.get('priority_score', 0))}")
        cols[5].write(f"**Fallback**  \n{'Yes' if int(row.get('fallback_hit', 0)) == 1 else 'No'}")
        if row.get("summary"):
            st.write(row.get("summary"))
        if row.get("url"):
            st.link_button("Open source", row.get("url"), use_container_width=False)

st.subheader("Release and signal table")
show_cols = [
    c for c in [
        "status", "display_date", "days_to_event", "source_group", "source", "theme_primary",
        "dataset_title", "summary", "priority_score", "media_relevance", "fallback_hit",
        "red_flag", "deleted_signal", "embargo", "url", "source_page", "last_checked"
    ] if c in view.columns
]

styled = (
    view[show_cols]
    .sort_values(["deleted_signal", "red_flag", "media_relevance", "priority_score", "fallback_hit", "action_date"], ascending=[False, False, False, False, True, True])
    .style
    .map(style_status, subset=[c for c in ["status"] if c in show_cols])
    .apply(style_date, axis=1)
)

st.dataframe(
    styled,
    column_config={
        "url": st.column_config.LinkColumn("Link"),
        "source_page": st.column_config.LinkColumn("Source page"),
        "days_to_event": st.column_config.NumberColumn("Days", format="%d"),
        "priority_score": st.column_config.NumberColumn("Priority", format="%d"),
        "media_relevance": st.column_config.NumberColumn("Media relevance", format="%d"),
        "fallback_hit": st.column_config.CheckboxColumn("Fallback"),
        "red_flag": st.column_config.CheckboxColumn("Red flag"),
        "deleted_signal": st.column_config.CheckboxColumn("Deleted"),
        "embargo": st.column_config.CheckboxColumn("Embargo"),
    },
    hide_index=True,
    use_container_width=True,
)

left, right = st.columns(2)
with left:
    st.subheader("By theme")
    if "theme_primary" in view.columns and not view.empty:
        theme_counts = view["theme_primary"].value_counts().reset_index()
        theme_counts.columns = ["Theme", "Count"]
        st.bar_chart(theme_counts.set_index("Theme"))
with right:
    st.subheader("By source")
    if "source" in view.columns and not view.empty:
        source_counts = view["source"].value_counts().head(15).reset_index()
        source_counts.columns = ["Source", "Count"]
        st.bar_chart(source_counts.set_index("Source"))

with st.expander("Source diagnostics"):
    if "source" in df.columns:
        diag = (
            df.groupby("source", dropna=False)
            .agg(
                records=("dataset_title", "count"),
                red_flags=("red_flag", "sum"),
                deleted=("deleted_signal", "sum"),
                fallback_hits=("fallback_hit", "sum"),
                avg_priority=("priority_score", "mean"),
            )
            .reset_index()
        )
        st.dataframe(diag, hide_index=True, use_container_width=True)
