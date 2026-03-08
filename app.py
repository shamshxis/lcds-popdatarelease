from pathlib import Path
import json
import re
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Population Data Commentary Planner",
    page_icon="🎙️",
    layout="wide",
)

DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
STATUS_CSV = DATA_DIR / "source_status.csv"
CANDIDATES_CSV = DATA_DIR / "candidate_sources.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

# --- HELPERS ---

def clean_title(text: str) -> str:
    """
    Intelligently cleans scraped titles to make them human-readable.
    Removes common scraper noise, dates, and technical jargon.
    """
    if not isinstance(text, str):
        return ""
    
    # 1. Remove common "junk" phrases often found in these scrapers
    noise_patterns = [
        r"data\.census\.gov & API",
        r"Microdata Access & API",
        r"Current Population Survey Basic Monthly",
        r"Top of Section",
        r"release release",
        r"updated updated",
        r"\d{1,2}/\d{1,2}/\d{4}",  # Raw dates like 5/7/2026
        r"\d{4}-\d{2}-\d{2}",      # ISO dates
        r"(January|February|March|April|May|June|July|August|September|October|November|December) \d{4}", # "May 2026"
    ]
    
    clean = text
    for pattern in noise_patterns:
        clean = re.sub(pattern, "", clean, flags=re.IGNORECASE)
    
    # 2. Fix double spaces and trimming
    clean = re.sub(r"\s+", " ", clean).strip()
    
    # 3. Remove leading colons or hyphens (leftover from stripping)
    clean = re.sub(r"^[:\-\s]+", "", clean)
    
    # 4. Fallback: If we stripped everything, return original
    if len(clean) < 5:
        return text
        
    return clean

def get_relative_time(date_obj):
    """Returns a human string like 'In 3 days' or '2 weeks ago'."""
    if pd.isna(date_obj):
        return "Date unknown"
    
    diff = date_obj.date() - datetime.now().date()
    days = diff.days
    
    if days == 0: return "Today"
    if days == 1: return "Tomorrow"
    if days == -1: return "Yesterday"
    
    if days > 0:
        if days < 7: return f"In {days} days"
        if days < 30: return f"In {int(days/7)} weeks"
        return f"In {int(days/30)} months"
    else:
        days = abs(days)
        if days < 7: return f"{days} days ago"
        if days < 30: return f"{int(days/7)} weeks ago"
        return f"{int(days/30)} months ago"

def load_data():
    """Loads and preprocesses data for the dashboard."""
    if not CURRENT_CSV.exists():
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(CURRENT_CSV, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

    # --- Type Conversion ---
    # We force errors="coerce" to handle bad data safely
    df["priority"] = pd.to_numeric(df.get("priority", 0), errors="coerce").fillna(0).astype(int)
    df["action_date_dt"] = pd.to_datetime(df.get("action_date", ""), errors="coerce")
    
    # --- Intelligence Layer ---
    # 1. Clean the Titles
    df["clean_title"] = df["dataset_title"].apply(clean_title)
    
    # 2. Human Status Mapping
    status_map = {
        "monitor": "📅 Expected",
        "upcoming": "📅 Scheduled",
        "updated": "✅ Published",
        "warning": "⚠️ Removed/Risk",
        "new": "✨ New Discovery"
    }
    df["human_status"] = df["status"].str.lower().map(status_map).fillna("Unknown")
    
    # 3. Relative Timing
    df["when"] = df["action_date_dt"].apply(get_relative_time)
    
    # 4. Commentary Hint (The "Intelligence" Sentence)
    def make_commentary(row):
        date_str = row["action_date"] if row["action_date"] else "an unknown date"
        if "warning" in str(row["status"]).lower():
            return f"🚨 ALERT: The {row['source']} release '{row['clean_title']}' may have been removed or withdrawn."
        elif "updated" in str(row["status"]).lower():
            return f"✅ READY: {row['source']} has released '{row['clean_title']}'."
        else:
            return f"ℹ️ PLANNING: {row['source']} is expected to release '{row['clean_title']}' on {date_str}."

    df["commentary_hint"] = df.apply(make_commentary, axis=1)

    return df

# --- MAIN APP UI ---

df = load_data()
meta = {}
if META_JSON.exists():
    try:
        meta = json.loads(META_JSON.read_text(encoding="utf-8"))
    except: pass

st.title("🎙️ Commentary Planner: Population Data")
st.markdown("""
**Goal:** Prepare commentaries based on upcoming data releases, deletions, and updates.
*This view interprets raw scraper data into a human-readable schedule.*
""")

# --- TOP METRICS ---
if not df.empty:
    today = pd.Timestamp.now()
    this_week = df[
        (df["action_date_dt"] >= today) & 
        (df["action_date_dt"] <= today + timedelta(days=7))
    ]
    warnings = df[df["status"] == "warning"]
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Releases This Week", len(this_week))
    c2.metric("⚠️ At Risk / Deleted", len(warnings))
    c3.metric("Total Tracked", len(df))
    c4.metric("Last Scrape", meta.get("run_at_utc", "Unknown")[:10])

# --- TABS ---
tab_schedule, tab_risk, tab_feed, tab_raw = st.tabs([
    "📅 Release Schedule", 
    "⚠️ At Risk / Withdrawn", 
    "📰 News Feed (Changes)", 
    "🔍 Raw Data"
])

# --- TAB 1: SCHEDULE (The "Human" View) ---
with tab_schedule:
    if df.empty:
        st.info("No data available yet.")
    else:
        # Filter controls
        c_filter1, c_filter2 = st.columns(2)
        with c_filter1:
            countries = sorted(list(set(df["country"].astype(str)) - {"", "nan"}))
            sel_country = st.multiselect("Filter by Country", countries)
        with c_filter2:
            sources = sorted(list(set(df["source"].astype(str)) - {"", "nan"}))
            sel_source = st.multiselect("Filter by Source", sources)

        # Apply Filters
        view_df = df.copy()
        if sel_country: view_df = view_df[view_df["country"].isin(sel_country)]
        if sel_source: view_df = view_df[view_df["source"].isin(sel_source)]

        # Sort: Soonest items first
        view_df = view_df.sort_values(by=["action_date_dt"], ascending=True, na_position="last")
        
        # Grouping Logic
        now = pd.Timestamp.now()
        
        # 1. Past / Recently Released
        past = view_df[view_df["action_date_dt"] < now]
        if not past.empty:
            with st.expander(f"⏮️ Past / Released ({len(past)} items)", expanded=False):
                st.dataframe(
                    past[["human_status", "when", "clean_title", "source", "url"]],
                    column_config={
                        "url": st.column_config.LinkColumn("Link"),
                        "clean_title": "Dataset / Release Name",
                        "human_status": "Status",
                        "when": "Released"
                    },
                    hide_index=True,
                    use_container_width=True
                )

        # 2. Upcoming (The Main Event)
        future = view_df[view_df["action_date_dt"] >= now]
        
        if future.empty:
            st.success("Nothing scheduled for the immediate future!")
        else:
            st.subheader("Upcoming Releases")
            for _, row in future.iterrows():
                # Card-like layout
                with st.container():
                    c1, c2 = st.columns([1, 4])
                    with c1:
                        st.caption(row["human_status"])
                        st.markdown(f"**{row['when']}**")
                        st.caption(row["action_date"])
                    with c2:
                        st.markdown(f"##### {row['clean_title']}")
                        st.markdown(f"**Source:** {row['source']} ({row['country']})")
                        st.info(row["commentary_hint"])
                        st.markdown(f"[🔗 Go to Source]({row['url']})")
                    st.divider()

# --- TAB 2: RISKS (Deletions/Warnings) ---
with tab_risk:
    st.header("⚠️ Data at Risk")
    st.write("These datasets may have been **removed, withdrawn, or marked as discontinued**.")
    
    risks = df[df["status"] == "warning"]
    if risks.empty:
        st.success("No data currently marked as 'at risk' or 'withdrawn'.")
    else:
        for _, row in risks.iterrows():
            st.error(f"""
            **{row['clean_title']}** ({row['source']})
            \nDetected Status: **{row['status'].upper()}**
            \n[🔗 Verify Link]({row['url']})
            """)

# --- TAB 3: CHANGES (Diff Log) ---
with tab_feed:
    st.header("📝 Change Log")
    if CHANGES_CSV.exists():
        try:
            changes = pd.read_csv(CHANGES_CSV)
            if changes.empty:
                st.info("No recent changes detected.")
            else:
                # Make changes readable
                changes = changes.sort_values(by="changed_at", ascending=False)
                for _, row in changes.iterrows():
                    icon = "🆕" if row["change_type"] == "new" else "🔄"
                    st.markdown(f"""
                    **{icon} {row['change_type'].replace('_', ' ').title()}** | {row['source']}
                    \n> *{row['dataset_title']}*
                    \nChanged: {row['changed_at'][:10]}
                    """)
                    st.divider()
        except:
            st.error("Could not read changes file.")
    else:
        st.info("No change history file found.")

# --- TAB 4: RAW DATA (Backup) ---
with tab_raw:
    st.subheader("Raw Scraper Output")
    st.write("Full unfiltered dataset for verification.")
    st.dataframe(df)

# --- SIDEBAR INFO ---
with st.sidebar:
    st.header("About")
    st.info("""
    **Intelligence Logic:**
    - **Clean Titles:** Removes scraper noise (e.g. "API", dates in titles).
    - **Status Mapping:** Converts "monitor" to "📅 Expected".
    - **Context:** Generates commentary sentences automatically.
    """)
    st.warning("Note: Intelligence is automated. Always verify the source link before publishing commentary.")
