from pathlib import Path
import json
import re
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Population Data Agenda",
    page_icon="🗓️",
    layout="wide",
)

DATA_DIR = Path("data")
CURRENT_CSV = DATA_DIR / "dataset_tracker.csv"
CHANGES_CSV = DATA_DIR / "dataset_changes.csv"
META_JSON = DATA_DIR / "last_run_meta.json"

# --- INTELLIGENCE LAYER ---

def get_category_icon(title: str, themes: str) -> str:
    """Assigns an emoji based on the content."""
    text = (title + " " + themes).lower()
    if any(x in text for x in ["money", "finance", "pension", "economy", "gdp"]): return "💰"
    if any(x in text for x in ["health", "mortality", "death", "cancer", "hospital"]): return "🏥"
    if any(x in text for x in ["migra", "asylum", "refugee", "visa"]): return "✈️"
    if any(x in text for x in ["birth", "fertil", "baby"]): return "👶"
    if any(x in text for x in ["work", "labour", "employ", "job"]): return "💼"
    if "census" in text: return "📊"
    return "📄"

def smart_clean_title(text: str) -> str:
    """
    Aggressively strips dates and noise to find the 'Real' title.
    Input: "May 2026 Public Sector: School System Finances 5/28/"
    Output: "Public Sector: School System Finances"
    """
    if not isinstance(text, str): return ""
    
    clean = text
    
    # 1. Remove long date phrases (e.g. "January 2026", "May 2026")
    clean = re.sub(r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b', '', clean, flags=re.IGNORECASE)
    
    # 2. Remove short date patterns (e.g. "5/28/", "2026-05-01")
    clean = re.sub(r'\b\d{1,2}/\d{1,2}/(\d{2,4})?', '', clean)
    clean = re.sub(r'\b\d{4}-\d{2}-\d{2}\b', '', clean)
    
    # 3. Remove common junk phrases
    junk = [
        "data.census.gov", "& API", "Microdata Access", "Top of Section", 
        "release release", "updated updated", "upcoming release",
        "Public Sector:", "Current Population Survey" # Optional: shortening common prefixes
    ]
    for j in junk:
        clean = re.sub(re.escape(j), "", clean, flags=re.IGNORECASE)

    # 4. Clean up punctuation mess (e.g. " : Finance - ")
    clean = re.sub(r'\s+[:\-]\s+', ' ', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    
    # 5. Fallback: If we deleted everything, revert to original (truncated)
    if len(clean) < 3:
        return text[:50] + "..."
        
    return clean.strip()

def load_data():
    if not CURRENT_CSV.exists(): return pd.DataFrame()
    try:
        df = pd.read_csv(CURRENT_CSV, dtype=str, keep_default_na=False)
    except: return pd.DataFrame()

    # Dates
    df["action_date_dt"] = pd.to_datetime(df.get("action_date", ""), errors="coerce")
    df["month_year"] = df["action_date_dt"].dt.strftime("%B %Y")  # e.g., "May 2026"
    df["day_str"] = df["action_date_dt"].dt.strftime("%d (%A)")   # e.g., "28 (Thursday)"
    
    # Text Cleaning
    df["clean_title"] = df["dataset_title"].apply(smart_clean_title)
    df["icon"] = df.apply(lambda x: get_category_icon(x["clean_title"], x.get("themes", "")), axis=1)
    
    return df

# --- UI LAYOUT ---

df = load_data()

st.title("🗓️ Population Data Agenda")
st.markdown("A clean, bulleted schedule of upcoming data releases.")

if df.empty:
    st.warning("No data found. Please run the scraper first.")
    st.stop()

# --- FILTERS (Sidebar) ---
with st.sidebar:
    st.header("Filters")
    # Source Filter
    all_sources = sorted(list(set(df["source"])))
    sel_source = st.multiselect("Source", all_sources, default=None)
    
    # Country Filter
    all_countries = sorted(list(set(df["country"])))
    sel_country = st.multiselect("Country", all_countries, default=None)

    # Search
    search = st.text_input("Search (e.g., 'Pension')")

# Apply Filters
view = df.copy()
if sel_source: view = view[view["source"].isin(sel_source)]
if sel_country: view = view[view["country"].isin(sel_country)]
if search: view = view[view["clean_title"].str.contains(search, case=False) | view["source"].str.contains(search, case=False)]

# --- AGENDA VIEW ---

# 1. Split Future / Past
today = pd.Timestamp.now()
future_mask = view["action_date_dt"] >= today
past_mask = view["action_date_dt"] < today

upcoming = view[future_mask].sort_values("action_date_dt")
past = view[past_mask].sort_values("action_date_dt", ascending=False)

# 2. Render Upcoming
if upcoming.empty:
    st.info("✅ No upcoming releases scheduled.")
else:
    # Group by Month
    months = upcoming["month_year"].unique()
    
    for month in months:
        # Create a visual container for the Month
        st.markdown(f"### 📅 {month}")
        month_data = upcoming[upcoming["month_year"] == month]
        
        # Group by Day within that month
        days = month_data["action_date_dt"].unique()
        
        for day in days:
            day_items = month_data[month_data["action_date_dt"] == day]
            day_label = pd.to_datetime(day).strftime("**%d %a**") # "28 Thu"
            
            # Use columns to create a "Timeline" look
            c1, c2 = st.columns([1, 8])
            
            with c1:
                st.markdown(day_label)
            
            with c2:
                for _, row in day_items.iterrows():
                    # The "Bulleted" content
                    with st.expander(f"{row['icon']} {row['clean_title']}", expanded=True):
                        st.markdown(f"""
                        * **Source:** {row['source']} ({row['country']})
                        * **Context:** {row['summary']}
                        * [🔗 Link to Data]({row['url']})
                        """)
        
        st.divider() # Line between months

# 3. Render Past (Collapsed)
if not past.empty:
    with st.expander(f"📚 Past Releases ({len(past)} items)"):
        st.dataframe(past[["action_date", "source", "clean_title", "url"]], hide_index=True)
