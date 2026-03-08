import pandas as pd
import streamlit as st
from pathlib import Path
import re
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Population Data Agenda", page_icon="🗓️", layout="centered")

DATA_FILE = Path("data/dataset_tracker.csv")

# --- INTELLIGENCE: TEXT CLEANER ---
def smart_clean_title(text: str) -> str:
    """
    Cleans up the specific 'junk' from Census/ONS data to find the real title.
    """
    if not isinstance(text, str): return ""
    
    clean = text
    
    # 1. Remove specific junk phrases identified in your logs
    junk_phrases = [
        "Top of Section", "Microdata Access & API", "data.census.gov & API",
        "Microdata Access", "API", "Download", "Link", "Details"
    ]
    for junk in junk_phrases:
        clean = re.sub(re.escape(junk), "", clean, flags=re.IGNORECASE)

    # 2. Remove embedded dates (e.g. "June 2026", "6/11/2026")
    date_patterns = [
        r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b',
        r'\b\d{1,2}/\d{1,2}/\d{2,4}\b',
        r'\b\d{4}-\d{2}-\d{2}\b'
    ]
    for pat in date_patterns:
        clean = re.sub(pat, "", clean, flags=re.IGNORECASE)
        
    # 3. Clean up whitespace and stray punctuation
    clean = re.sub(r'\s+', ' ', clean).strip()
    clean = clean.strip(" -:.")
    
    return clean

# --- DATA LOADER ---
def load_data():
    if not DATA_FILE.exists():
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
        
        # Convert Date
        df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
        df = df.dropna(subset=["dt"]) # Drop invalid dates
        
        # Create Month Grouping Key (e.g. "2026-06") for sorting
        df["month_sort"] = df["dt"].dt.to_period("M")
        df["month_label"] = df["dt"].dt.strftime("%B %Y") # "June 2026"
        
        # Clean Titles
        df["clean_title"] = df["dataset_title"].apply(smart_clean_title)
        
        # FILTER: Drop rows where title became empty or is just "Release"
        df = df[df["clean_title"].str.len() > 3]
        
        # DEDUPLICATE: Keep only 1 entry per Title per Month
        # This solves the "5 rows for the same survey" problem
        df = df.sort_values(by=["dt", "dataset_title"], ascending=True) # Prefer earlier dates?
        df = df.drop_duplicates(subset=["month_sort", "clean_title"])
        
        return df
    except Exception as e:
        st.error(f"Error processing data: {e}")
        return pd.DataFrame()

# --- MAIN UI ---
st.title("🗓️ Population Data Agenda")
st.markdown("##### Compact Monthly Release Schedule")

df = load_data()

if df.empty:
    st.info("No valid release data found. (Try running the scraper first)")
    st.stop()

# --- FILTERS ---
with st.sidebar:
    st.header("Filters")
    sources = ["All"] + sorted(df["source"].unique().tolist())
    sel_source = st.selectbox("Source", sources)
    
    countries = ["All"] + sorted(df["country"].unique().tolist())
    sel_country = st.selectbox("Country", countries)

# Apply Filters
view = df.copy()
if sel_source != "All": view = view[view["source"] == sel_source]
if sel_country != "All": view = view[view["country"] == sel_country]

# --- RENDER MONTHLY LIST ---

# 1. Sort by Month (Upcoming first)
today_period = pd.Timestamp.now().to_period("M")
view = view.sort_values("month_sort")
unique_months = view["month_sort"].unique()

for m_sort in unique_months:
    # Skip past months if you want (optional)
    if m_sort < today_period:
        continue
        
    month_data = view[view["month_sort"] == m_sort]
    month_name = month_data["month_label"].iloc[0]
    
    # Visual Header for Month
    st.markdown(f"### {month_name}")
    
    # List items
    for _, row in month_data.iterrows():
        # Determine Status Color
        status_icon = "🔹"
        if "Remove" in row['status']: status_icon = "❌"
        if "Release" in row['status']: status_icon = "✅"
        
        # Render Compact Row
        # Format: [Icon] Title (Source) -> Link
        with st.container():
            c1, c2 = st.columns([0.05, 0.95])
            with c1:
                st.write(status_icon)
            with c2:
                st.markdown(
                    f"**[{row['clean_title']}]({row['url']})** \n"
                    f"<span style='color:grey; font-size:0.9em'>{row['dt'].strftime('%d %b')} • {row['source']}</span>",
                    unsafe_allow_html=True
                )
    
    st.divider()

# --- HISTORY EXPANDER ---
past_data = view[view["month_sort"] < today_period]
if not past_data.empty:
    with st.expander("📂 Past Releases"):
        st.dataframe(past_data[["action_date", "clean_title", "source"]], hide_index=True)
