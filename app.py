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
META_JSON = DATA_DIR / "last_run_meta.json"

# --- HELPERS ---

def clean_title(text: str) -> str:
    """
    Intelligently cleans scraped titles to make them human-readable.
    """
    if not isinstance(text, str):
        return ""
    
    # Remove common scraper noise
    noise_patterns = [
        r"data\.census\.gov & API",
        r"Microdata Access & API",
        r"Current Population Survey Basic Monthly",
        r"Top of Section",
        r"release release",
        r"updated updated",
        r"\d{1,2}/\d{1,2}/\d{4}",
        r"\d{4}-\d{2}-\d{2}",
    ]
    
    clean = text
    for pattern in noise_patterns:
        clean = re.sub(pattern, "", clean, flags=re.IGNORECASE)
    
    clean = re.sub(r"\s+", " ", clean).strip()
    clean = re.sub(r"^[:\-\s]+", "", clean)
    
    if len(clean) < 5:
        return text
        
    return clean

def get_relative_time(date_obj):
    """Returns a human string like 'In 3 days'."""
    if pd.isna(date_obj):
        return "Date unknown"
    
    diff = date_obj.date() - datetime.now().date()
    days = diff.days
    
    if days == 0: return "Today"
    if days == 1: return "Tomorrow"
    if days == -1: return "Yesterday"
    
    if days > 0:
        if days < 7: return f"In {days} days"
        return f"In {int(days/7)} weeks" if days < 30 else f"In {int(days/30)} months"
    else:
        days = abs(days)
        return f"{days} days ago"

def load_data():
    """Loads and preprocesses data."""
    if not CURRENT_CSV.exists():
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(CURRENT_CSV, dtype=str, keep_default_na=False)
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

    # Convert types
    df["action_date_dt"] = pd.to_datetime(df.get("action_date", ""), errors="coerce")
    
    # --- Intelligence Layer ---
    df["clean_title"] = df["dataset_title"].apply(clean_title)
    
    # FORMAT: 08 March 2026
    df["nice_date"] = df["action_date_dt"].dt.strftime("%d %B %Y").fillna("Unknown Date")
    
    status_map = {
        "monitor": "📅 Expected",
        "upcoming": "📅 Scheduled",
        "updated": "✅ Published",
        "warning": "⚠️ Removed/Risk",
        "new": "✨ New Discovery"
    }
    df["human_status"] = df["status"].str.lower().map(status_map).fillna("Unknown")
    df["when"] = df["action_date_dt"].apply(get_relative_time)

    # Commentary generation
    def make_commentary(row):
        d = row["nice_date"]
        if "warning" in str(row["status"]).lower():
            return f"🚨 ALERT: The {row['source']} release '{row['clean_title']}' has been withdrawn."
        elif "updated" in str(row["status"]).lower():
            return f"✅ READY: {row['source']} released '{row['clean_title']}' on {d}."
        else:
            return f"ℹ️ PLANNING: {row['source']} is scheduled to release '{row['clean_title']}' on {d}."

    df["commentary_hint"] = df.apply(make_commentary, axis=1)

    return df

# --- MAIN APP ---

df = load_data()

st.title("🎙️ Commentary Planner")
st.markdown("**Upcoming Release Schedule** | *Sorted chronologically (Soonest → Future)*")

# --- FILTERS ---
with st.container():
    c1, c2, c3 = st.columns([1, 1, 2])
    
    # Source Filter (e.g. ONS)
    available_sources = sorted(list(set(df["source"].astype(str)) - {"", "nan"}))
    with c1:
        sel_source = st.multiselect("Filter by Source (e.g., ONS)", available_sources)
        
    # Country Filter
    available_countries = sorted(list(set(df["country"].astype(str)) - {"", "nan"}))
    with c2:
        sel_country = st.multiselect("Filter by Country", available_countries)
        
    # Search
    with c3:
        search_q = st.text_input("Search keywords (e.g., 'migration')", "")

# Apply Filters
view_df = df.copy()
if sel_source:
    view_df = view_df[view_df["source"].isin(sel_source)]
if sel_country:
    view_df = view_df[view_df["country"].isin(sel_country)]
if search_q:
    view_df = view_df[
        view_df["clean_title"].str.contains(search_q, case=False) | 
        view_df["source"].str.contains(search_q, case=False)
    ]

# --- SORTING LOGIC ---
# Split into Future and Past
now = pd.Timestamp.now()
future_mask = view_df["action_date_dt"] >= now
past_mask = view_df["action_date_dt"] < now

# Sort Future: ASCENDING (Tomorrow first, then next week, etc.)
future_df = view_df[future_mask].sort_values(by="action_date_dt", ascending=True)

# Sort Past: DESCENDING (Yesterday first, then last week, etc.)
past_df = view_df[past_mask].sort_values(by="action_date_dt", ascending=False)

# --- DISPLAY: UPCOMING ---
if future_df.empty:
    st.info("No upcoming releases found matching your filters.")
else:
    for _, row in future_df.iterrows():
        # Clean Card Layout
        with st.container():
            col_date, col_content = st.columns([1, 4])
            
            with col_date:
                st.subheader(row["nice_date"].split(" ")[0]) # Day Number big
                st.markdown(f"**{row['nice_date'].split(' ')[1]}** {row['nice_date'].split(' ')[2]}") # Month Year
                st.caption(row["when"]) # "In 3 days"
            
            with col_content:
                st.markdown(f"#### {row['clean_title']}")
                st.markdown(f"**{row['source']}** • {row['country']}")
                st.info(row["commentary_hint"])
                st.markdown(f"[🔗 Open Source Link]({row['url']})")
            
            st.divider()

# --- DISPLAY: PAST ---
if not past_df.empty:
    with st.expander(f"📚 Past Releases ({len(past_df)} items)", expanded=False):
        st.dataframe(
            past_df[["nice_date", "clean_title", "source", "human_status"]],
            use_container_width=True,
            hide_index=True
        )
