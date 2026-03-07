import streamlit as st
import pandas as pd
import os
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Data Tracker", page_icon="🧬")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-confirmed {background-color: #2ca02c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-estimated {background-color: #ff7f0e; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-news {background-color: #1f77b4; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-unknown {background-color: #777; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .days-tag {font-weight: bold; font-size: 0.9em; color: #555;}
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Demographic Data Tracker")
st.caption("Consolidated release schedule for ONS, Eurostat, INSEE, Statice, and FinData.")

# --- LOAD DATA ---
DATA_FILE = "data/releases.json"

if not os.path.exists(DATA_FILE):
    st.warning("⚠️ No data found. Please run the scraper.")
    # Add a button to run scraper from UI if file is missing
    if st.button("Run Scraper Now"):
        os.system("python scraper.py")
        st.rerun()
    st.stop()

try:
    df = pd.read_json(DATA_FILE)
except ValueError:
    st.error("⚠️ Data file is corrupt or empty. Please run the scraper again.")
    st.stop()

if df.empty:
    st.warning("⚠️ Data file exists but contains 0 records.")
    st.stop()

# --- CRITICAL FIX: ENSURE COLUMNS EXIST ---
# If old data is loaded, these columns might be missing. We add them with defaults.
required_cols = {
    'status': '⚠️ UNKNOWN',
    'country': 'Unknown',
    'source': 'Unknown',
    'title': 'Untitled',
    'url': '#',
    'start': datetime.now().strftime("%Y-%m-%d")
}

for col, default_val in required_cols.items():
    if col not in df.columns:
        df[col] = default_val

# --- CALCULATE "DAYS TO GO" ---
today = pd.Timestamp.now().normalize()
df['start'] = pd.to_datetime(df['start'], errors='coerce')
df['days_diff'] = (df['start'] - today).dt.days

def format_timing(days):
    if pd.isna(days): return "Unknown"
    if days < 0: return f"Released {abs(int(days))} days ago"
    if days == 0: return "🔥 TODAY"
    return f"In {int(days)} days"

df['timing'] = df['days_diff'].apply(format_timing)

# --- SIDEBAR REFRESH ---
with st.sidebar:
    st.header("Controls")
    if st.button("🔄 Force Refresh Data"):
        with st.spinner("Running Scraper..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()

# --- FILTERS ---
col1, col2 = st.columns(2)
sel_country = col1.multiselect("Filter Country/Region", df['country'].unique(), default=df['country'].unique())
sel_status = col2.multiselect("Filter Status", df['status'].unique(), default=df['status'].unique())

# Apply Filters
filtered = df[
    (df['country'].isin(sel_country)) & 
    (df['status'].isin(sel_status))
].sort_values(by='days_diff', ascending=True)

# --- DISPLAY TABLE ---
st.divider()
for idx, row in filtered.iterrows():
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1, 1.5, 4, 1])
        
        # 1. Timing
        start_str = row['start'].strftime('%Y-%m-%d') if pd.notnull(row['start']) else "Unknown"
        c1.markdown(f"**{start_str}**")
        c1.markdown(f"<span class='days-tag'>{row['timing']}</span>", unsafe_allow_html=True)
        
        # 2. Source
        c2.write(f"**{row['country']}**")
        c2.caption(row['source'])
        
        # 3. Title & Status
        # Handle case where status might be missing or different format
        status_slug = str(row['status']).split(' ')[1].lower() if len(str(row['status']).split(' ')) > 1 else 'unknown'
        c3.markdown(f"<span class='status-{status_slug}'>{row['status']}</span> **{row['title']}**", unsafe_allow_html=True)
        
        # 4. Link
        c4.link_button("🔗 Open", row['url'], use_container_width=True)
