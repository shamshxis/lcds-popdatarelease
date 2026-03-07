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
    .days-tag {font-weight: bold; font-size: 0.9em; color: #555;}
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Demographic Data Tracker")
st.caption("Consolidated release schedule for ONS, Eurostat, INSEE, Statice, and FinData.")

# --- LOAD DATA ---
DATA_FILE = "data/releases.json"
if not os.path.exists(DATA_FILE):
    st.warning("⚠️ No data found. Please run the scraper.")
    st.stop()

df = pd.read_json(DATA_FILE)

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

# --- FILTERS ---
col1, col2 = st.columns(2)
sel_country = col1.multiselect("Filter Country/Region", df['country'].unique(), default=df['country'].unique())
sel_status = col2.multiselect("Filter Status", df['status'].unique(), default=df['status'].unique())

filtered = df[
    (df['country'].isin(sel_country)) & 
    (df['status'].isin(sel_status))
].sort_values(by='days_diff', ascending=True) # Sort by nearest date

# --- DISPLAY TABLE ---
# We use a custom iteration to make it look like a high-end dashboard
for idx, row in filtered.iterrows():
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1, 1.5, 4, 1])
        
        # 1. Timing
        c1.markdown(f"**{row['start'].strftime('%Y-%m-%d')}**")
        c1.markdown(f"<span class='days-tag'>{row['timing']}</span>", unsafe_allow_html=True)
        
        # 2. Source
        c2.write(f"**{row['country']}**")
        c2.caption(row['source'])
        
        # 3. Title & Status
        color_class = f"status-{row['status'].split(' ')[1].lower()}"
        c3.markdown(f"<span class='{color_class}'>{row['status']}</span> **{row['title']}**", unsafe_allow_html=True)
        
        # 4. Link
        c4.link_button("🔗 Open", row['url'], use_container_width=True)
