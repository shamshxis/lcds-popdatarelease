import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Release Tracker", page_icon="🧬")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-confirmed {background-color: #2ca02c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-news {background-color: #1f77b4; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-unknown {background-color: #777; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .days-tag {font-weight: bold; font-size: 0.9em; color: #333;}
    .block-container {padding-top: 1rem;}
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Precision Tracker")
st.caption("Tracking Future Demographic Releases (ONS, Eurostat, INSEE, Statice)")

# --- DATA LOADING & SANITIZATION ---
DATA_FILE = "data/releases.json"

# 1. Auto-Run Scraper if missing
if not os.path.exists(DATA_FILE):
    st.warning("⚠️ Initializing Data... Please wait.")
    os.system("python scraper.py")
    time.sleep(1) # Wait for file write
    st.rerun()

# 2. Load Data safely
try:
    df = pd.read_json(DATA_FILE)
except ValueError:
    st.error("Data file is corrupt. Re-running scraper...")
    os.system("python scraper.py")
    st.rerun()

if df.empty:
    st.info("No upcoming releases found in the scan.")
    if st.button("Run Scraper Again"):
        os.system("python scraper.py")
        st.rerun()
    st.stop()

# 3. CRITICAL FIX: Ensure 'status' column exists
if 'status' not in df.columns:
    df['status'] = '⚠️ UNKNOWN'  # Default value if missing

# 4. CRITICAL FIX: Ensure 'country' column exists
if 'country' not in df.columns:
    df['country'] = 'Global'

# --- PROCESSING ---
# Calculate Timing
today = pd.Timestamp.now().normalize()
df['start'] = pd.to_datetime(df['start'])
df['days_diff'] = (df['start'] - today).dt.days

def format_timing(row):
    days = row['days_diff']
    if days < 0: return f"Released {abs(int(days))} days ago"
    if days == 0: return "🔥 TODAY"
    return f"In {int(days)} days"

df['timing'] = df.apply(format_timing, axis=1)

# Sort (Soonest First)
df = df.sort_values(by='days_diff', ascending=True)

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Check for New Releases"):
        with st.spinner("Scanning Agencies..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    
    # Filters
    countries = df['country'].unique().tolist()
    sel_country = st.multiselect("Filter Country", countries, default=countries)
    
    # Filter Dataframe
    if sel_country:
        filtered = df[df['country'].isin(sel_country)]
    else:
        filtered = df

# --- MAIN TABLE ---
# Metrics
c1, c2, c3 = st.columns(3)
upcoming_count = len(filtered[filtered['days_diff'] >= 0])
c1.metric("Upcoming Releases", upcoming_count)

next_date = "-"
if upcoming_count > 0:
    next_date = filtered[filtered['days_diff'] >= 0].iloc[0]['start'].strftime("%Y-%m-%d")
c2.metric("Next Key Date", next_date)

st.divider()

# Table Render
for idx, row in filtered.iterrows():
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1, 1, 4, 1])
        
        # Date & Timing
        c1.write(f"**{row['start'].strftime('%Y-%m-%d')}**")
        c1.markdown(f"<span class='days-tag'>{row['timing']}</span>", unsafe_allow_html=True)
        
        # Source
        c2.write(f"**{row['country']}**")
        c2.caption(row.get('source', 'Unknown'))
        
        # Title & Status Logic
        status_val = row.get('status', 'UNKNOWN')
        status_slug = "status-confirmed" if "CONFIRMED" in str(status_val) else "status-news"
        
        c3.markdown(f"<span class='{status_slug}'>{status_val}</span> **{row['title']}**", unsafe_allow_html=True)
        
        # Link
        c4.link_button("🔗 Open", row['url'], use_container_width=True)
