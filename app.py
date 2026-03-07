import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Precision Tracker", page_icon="🧬")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-confirmed {background-color: #2ca02c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-released {background-color: #777; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .days-tag {font-weight: bold; font-size: 0.9em; color: #333;}
    .log-box {font-family: monospace; font-size: 0.8em; background: #f0f0f0; padding: 10px; border-radius: 5px; height: 200px; overflow-y: scroll;}
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Precision Tracker")
st.caption("Slow & Steady Engine (Human-Paced)")

DATA_FILE = "data/releases.json"
LOG_FILE = "data/scraper.log"

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("Controls")
    
    if st.button("🔄 Start Slow Scan", type="primary"):
        with st.status("Scanning... (This takes time)", expanded=True) as status:
            st.write("Initializing Session...")
            os.system("python scraper.py")
            status.update(label="Scan Complete", state="complete", expanded=False)
            time.sleep(1)
            st.rerun()

    # DEBUG LOG VIEWER
    with st.expander("🛠️ Debug Logs"):
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE, "r") as f:
                st.code(f.read(), language="text")
        else:
            st.write("No logs yet.")

# --- DATA LOADING ---
if not os.path.exists(DATA_FILE):
    st.info("👋 Welcome. Please click 'Start Slow Scan' in the sidebar.")
    st.stop()

try:
    df = pd.read_json(DATA_FILE)
except:
    st.error("Data file corrupt. Please Rescan.")
    st.stop()

if df.empty:
    st.warning("Scan finished but found 0 items. Check 'Debug Logs' to see why.")
    st.stop()

# --- PROCESSING ---
# Ensure columns exist
for col in ['title', 'start', 'country', 'source', 'url', 'status']:
    if col not in df.columns: df[col] = ''

df['start'] = pd.to_datetime(df['start'])
today = pd.Timestamp.now().normalize()
df['days_diff'] = (df['start'] - today).dt.days

def format_timing(row):
    days = row['days_diff']
    if days < 0: return f"Released {abs(int(days))} days ago"
    if days == 0: return "🔥 TODAY"
    return f"In {int(days)} days"

df['timing'] = df.apply(format_timing, axis=1)
df = df.sort_values(by='days_diff', ascending=True)

# --- DISPLAY ---
st.divider()

# METRICS
upcoming = df[df['days_diff'] >= 0]
next_date = upcoming.iloc[0]['start'].strftime("%Y-%m-%d") if not upcoming.empty else "-"
c1, c2 = st.columns(2)
c1.metric("Upcoming Datasets", len(upcoming))
c2.metric("Next Release", next_date)

st.divider()

# TABLE
for idx, row in df.iterrows():
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1, 1, 4, 1])
        
        c1.write(f"**{row['start'].strftime('%Y-%m-%d')}**")
        c1.markdown(f"<span class='days-tag'>{row['timing']}</span>", unsafe_allow_html=True)
        
        c2.write(f"**{row['country']}**")
        c2.caption(row['source'])
        
        status_color = "status-confirmed" if row['days_diff'] >= 0 else "status-released"
        c3.markdown(f"<span class='{status_color}'>{row['status']}</span> **{row['title']}**", unsafe_allow_html=True)
        
        c4.link_button("🔗 Open", row['url'], use_container_width=True)
