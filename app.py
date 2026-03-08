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
    if any(x in text for x in ["money", "finance", "pension", "economy", "gdp", "economic"]): return "💰"
    if any(x in text for x in ["health", "mortality", "death", "cancer", "hospital"]): return "🏥"
    if any(x in text for x in ["migra", "asylum", "refugee", "visa"]): return "✈️"
    if any(x in text for x in ["birth", "fertil", "baby"]): return "👶"
    if any(x in text for x in ["work", "labour", "employ", "job"]): return "💼"
    if "census" in text: return "📊"
    return "📄"

def smart_clean_title(text: str) -> str:
    """
    Aggressively strips noise. 
    It splits text at 'junk' markers and keeps only the left part (the real title).
    """
    if not isinstance(text, str): return ""
    
    clean = text
    
    # FILTER: Ignore known cookie noise titles completely
    if "Cookies on" in clean or "Essential cookies" in clean:
        return ""

    # 1. Hard-coded "Splitters": If we see these, cut everything after them.
    splitters = [
        "API", "data.census.gov", "Microdata Access", "Basic Monthly", 
        "Top of Section", "Current Population Survey", "https:", "http:",
        "View all", "Hide all"
    ]
    
    for s in splitters:
        if s in clean:
            clean = clean.split(s)[0]  # Keep only the left side

    # 2. Remove Dates appearing at the START or END
    date_patterns = [
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/(\d{2,4})?', 
        r'\b\d{4}-\d{2}-\d{2}\b'
    ]
    for p in date_patterns:
        clean = re.sub(p, "", clean, flags=re.IGNORECASE)

    # 3. Specific Replacements for known messy sources
    clean = clean.replace("Public Sector:", "").replace("Release:", "")
    
    # 4. Final Cleanup
    clean = re.sub(r'\s+[:\-]\s+', ' ', clean)  # Remove hanging colons " : "
    clean = re.sub(r'\s+', ' ', clean).strip()  # Remove double spaces
    
    # 5. Fix empty result (if we over-cleaned)
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
    df["month_year"] = df["action_date_dt"].dt.strftime("%B %Y")
    
    # Text Cleaning
    df["clean_title"] = df["dataset_title"].apply(smart_clean_title)
    # Exclude empty titles (cookie noise)
    df = df[df["clean_title"] != ""]
    
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
    all_sources = sorted(list(set(df["source"])))
    sel_source = st.multiselect("Source", all_sources, default=None)
    
    all_countries = sorted(list(set(df["country"])))
    sel_country = st.multiselect("Country", all_countries, default=None)

    search = st.text_input("Search (e.g., 'Pension')")

# Apply Filters
view = df.copy()
if sel_source: view = view[view["source"].isin(sel_source)]
if sel_country: view = view[view["country"].isin(sel_country)]
if search: view = view[view["clean_title"].str.contains(search, case=False) | view["source"].str.contains(search, case=False)]

# --- AGENDA VIEW ---

today = pd.Timestamp.now()
future_mask = view["action_date_dt"] >= today
upcoming = view[future_mask].sort_values("action_date_dt")

if upcoming.empty:
    st.info("✅ No upcoming releases scheduled.")
else:
    # Group by Month
    months = upcoming["month_year"].unique()
    
    for month in months:
        st.markdown(f"### 📅 {month}")
        month_data = upcoming[upcoming["month_year"] == month]
        
        # Group by Day
        days = month_data["action_date_dt"].unique()
        
        for day in days:
            day_items = month_data[month_data["action_date_dt"] == day]
            day_label = pd.to_datetime(day).strftime("**%d %a**") # "28 Thu"
            
            c1, c2 = st.columns([1, 10])
            with c1:
                st.markdown(day_label)
            with c2:
                for _, row in day_items.iterrows():
                    st.markdown(f"**{row['icon']} {row['clean_title']}**")
                    st.caption(f"{row['source']} • [Source Link]({row['url']})")
                    
        st.divider()

# Past Releases
past = view[~future_mask].sort_values("action_date_dt", ascending=False)
if not past.empty:
    with st.expander(f"📚 Past Releases ({len(past)} items)"):
        st.dataframe(past[["action_date", "source", "clean_title", "url"]], hide_index=True)
