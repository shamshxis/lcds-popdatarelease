import streamlit as st
import pandas as pd
import os
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Precision Tracker", page_icon="🧬")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-confirmed {background-color: #2ca02c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-news {background-color: #1f77b4; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-released {background-color: #777; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    
    /* Yellow Tinge for New Items */
    .new-item-box {
        border-left: 5px solid #ffd700;
        background-color: rgba(255, 215, 0, 0.05);
        padding: 10px; margin-bottom: 10px; border-radius: 4px;
    }
    .old-item-box {
        border-left: 5px solid #444;
        padding: 10px; margin-bottom: 10px;
    }
    .meta-text {font-size: 0.85em; color: #aaa; font-style: italic;}
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Precision Tracker")
st.caption("Intelligence Feed: ONS, Eurostat, Statice, GDELT (Waterfall Architecture)")

DATA_FILE = "data/releases.json"

if not os.path.exists(DATA_FILE):
    st.warning("Running Initial Scan...")
    os.system("python scraper.py")
    st.rerun()

try:
    df = pd.read_json(DATA_FILE)
except:
    os.system("python scraper.py")
    st.rerun()

if df.empty:
    st.info("No active signals found.")
    if st.button("Run Scraper"):
        os.system("python scraper.py")
        st.rerun()
    st.stop()

# --- PROCESSING ---
df['start'] = pd.to_datetime(df['start'])
today = pd.Timestamp.now().normalize()
df['days_diff'] = (df['start'] - today).dt.days
df = df.sort_values(by='days_diff', ascending=True)

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Force Refresh (Waterfall)"):
        with st.spinner("Checking API -> Feed -> HTML..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
            
    st.divider()
    countries = df['country'].unique().tolist()
    sel_country = st.multiselect("Filter Source", countries, default=countries)
    filtered = df[df['country'].isin(sel_country)]

# --- SUMMARY ---
upcoming = filtered[filtered['days_diff'] >= 0]
if not upcoming.empty:
    next_item = upcoming.iloc[0]
    st.info(f"📅 **Next Key Date:** {next_item['start'].strftime('%Y-%m-%d')} — {next_item['title']}")

st.divider()

# --- MAIN FEED ---
for idx, row in filtered.iterrows():
    # Visual Logic
    box_class = "new-item-box" if row.get('is_new', False) else "old-item-box"
    
    st.markdown(f'<div class="{box_class}">', unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns([1, 1.2, 4, 1])
    
    # 1. Date
    c1.write(f"**{row['start'].strftime('%Y-%m-%d')}**")
    if row['days_diff'] == 0: c1.caption("🔥 TODAY")
    elif row['days_diff'] > 0: c1.caption(f"In {row['days_diff']} days")
    else: c1.caption(f"{abs(row['days_diff'])} days ago")

    # 2. Source
    c2.write(f"**{row['country']}**")
    c2.caption(row['source'])
    
    # 3. Content
    status_color = "status-confirmed"
    if "NEWS" in row['status']: status_color = "status-news"
    elif "RELEASED" in row['status']: status_color = "status-released"
    
    c3.markdown(f"<span class='{status_color}'>{row['status']}</span> **{row['title']}**", unsafe_allow_html=True)
    c3.markdown(f"<span class='meta-text'>📝 {row.get('commentary', '')}</span>", unsafe_allow_html=True)
    
    # 4. Link
    c4.link_button("🔗 Open", row['url'], use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)
