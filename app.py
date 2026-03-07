import streamlit as st
import pandas as pd
import os
import time
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Precision Tracker", page_icon="🧬")

# --- CUSTOM CSS (DARK THEME + YELLOW TINGE) ---
st.markdown("""
<style>
    /* Status Pills */
    .status-confirmed {background-color: #2ca02c; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-expected {background-color: #d4ac0d; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    .status-released {background-color: #777; color: white; padding: 2px 8px; border-radius: 4px; font-size: 0.8em;}
    
    /* Yellow Tinge for New Items */
    div[data-testid="stVerticalBlock"] > div.element-container {
    }
    .new-release-box {
        border-left: 5px solid #ffd700 !important;
        background-color: rgba(255, 215, 0, 0.05);
        padding: 10px;
        border-radius: 5px;
        margin-bottom: 10px;
    }
    .regular-release-box {
        border-left: 5px solid #444;
        padding: 10px;
        margin-bottom: 10px;
    }
    
    .commentary-text {
        font-style: italic;
        color: #aaa;
        font-size: 0.9em;
    }
</style>
""", unsafe_allow_html=True)

st.title("🧬 LCDS Precision Tracker")
st.caption("Intelligence Feed: ONS API, Eurostat, Statice, GDELT")

# --- DATA LOADING ---
DATA_FILE = "data/releases.json"

if not os.path.exists(DATA_FILE):
    st.warning("⚠️ Initializing Intelligence Engine...")
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

# Sort: Upcoming first
df = df.sort_values(by='days_diff', ascending=True)

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Force Refresh (API)", type="primary"):
        with st.spinner("Querying Global APIs..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
            
    st.divider()
    
    # Filters
    countries = df['country'].unique().tolist()
    sel_country = st.multiselect("Filter Source", countries, default=countries)
    filtered = df[df['country'].isin(sel_country)]

# --- SUMMARY / FORECAST SECTION ---
upcoming = filtered[filtered['days_diff'] >= 0]
if not upcoming.empty:
    next_item = upcoming.iloc[0]
    st.info(f"📅 **Next Key Date:** {next_item['start'].strftime('%Y-%m-%d')} — {next_item['title']} ({next_item['country']})")

st.divider()

# --- MAIN FEED RENDER ---
for idx, row in filtered.iterrows():
    # Determine Style (Yellow Tinge if 'is_new' is true)
    # Since JSON booleans might be lost, we check logic or column
    is_new = row.get('is_new', False)
    box_class = "new-release-box" if is_new else "regular-release-box"
    
    # HTML Container to apply custom CSS class
    st.markdown(f'<div class="{box_class}">', unsafe_allow_html=True)
    
    c1, c2, c3, c4 = st.columns([1, 1.2, 4, 1])
    
    # 1. Date
    c1.write(f"**{row['start'].strftime('%Y-%m-%d')}**")
    if row['days_diff'] == 0:
        c1.caption("🔥 TODAY")
    elif row['days_diff'] > 0:
        c1.caption(f"In {row['days_diff']} days")
    else:
        c1.caption(f"{abs(row['days_diff'])} days ago")

    # 2. Source
    c2.write(f"**{row['country']}**")
    c2.caption(row['source'])
    
    # 3. Content + Commentary
    status_color = "status-confirmed"
    if "EXPECTED" in row['status']: status_color = "status-expected"
    elif "RELEASED" in row['status']: status_color = "status-released"
    
    c3.markdown(f"<span class='{status_color}'>{row['status']}</span> **{row['title']}**", unsafe_allow_html=True)
    c3.markdown(f"<span class='commentary-text'>📝 {row.get('commentary', 'No details')}</span>", unsafe_allow_html=True)
    
    # 4. Link
    c4.link_button("🔗 Open", row['url'], use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)
