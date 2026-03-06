import streamlit as st
from streamlit_calendar import calendar
import json
import os
import time
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
st.set_page_config(layout="wide", page_title="Global Population Data Calendar", page_icon="🌍")

DATA_DIR = "data"
JSON_FILE = os.path.join(DATA_DIR, "releases.json")
CSV_FILE = os.path.join(DATA_DIR, "releases.csv")

# --- CSS Styling ---
st.markdown("""
<style>
    .main-header {font-size: 2.5rem; font-weight: 700; color: #2c3e50; margin-bottom: 0px;}
    .sub-header {font-size: 1.2rem; color: #7f8c8d; margin-bottom: 20px;}
    .metric-card {background-color: #f8f9fa; border: 1px solid #e9ecef; padding: 15px; border-radius: 10px; text-align: center;}
    /* Compact calendar styling for multi-month view */
    .fc-col-header-cell-cushion { font-size: 0.8rem; }
    .fc-daygrid-day-number { font-size: 0.8rem; }
</style>
""", unsafe_allow_html=True)

# --- Helpers ---
@st.cache_data(ttl=3600)
def load_data():
    """Safely load data with retries for atomic write collisions"""
    retries = 3
    for i in range(retries):
        try:
            if not os.path.exists(JSON_FILE):
                return []
            with open(JSON_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            time.sleep(0.1)
    return []

def get_color(source):
    colors = {
        "ONS": "#00664F",        # UK Green
        "Eurostat": "#004494",   # EU Blue
        "US Census": "#B31B1B",  # US Red
        "UN Data": "#009EDB",    # UN Blue
        "StatCan": "#FF0000",    # Canada Red
    }
    return colors.get(source, "#7f8c8d")

# --- Main App ---

# 1. Header & Data Loading
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown('<div class="main-header">🌍 Global PopData Calendar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Tracking Official Statistical Releases (ONS, Eurostat, UN, Census, etc.)</div>', unsafe_allow_html=True)

data = load_data()

# 2. Sidebar Filters & Download
st.sidebar.header("🔍 Controls")

# CSV Download Button
if os.path.exists(CSV_FILE):
    with open(CSV_FILE, "rb") as f:
        st.sidebar.download_button(
            label="📥 Download Offline Data (CSV)",
            data=f,
            file_name=f"pop_data_releases_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

# Filters
all_countries = sorted(list(set(d['country'] for d in data))) if data else []
sel_country = st.sidebar.multiselect("Region/Country", all_countries, default=all_countries)

all_topics = sorted(list(set(d['topic'] for d in data))) if data else []
sel_topic = st.sidebar.multiselect("Dataset Topic", all_topics, default=all_topics)

# Filter Logic
filtered = [
    d for d in data 
    if (not sel_country or d['country'] in sel_country) 
    and (not sel_topic or d['topic'] in sel_topic)
]

if not data:
    st.warning("⚠️ No data found. Please run scraper.py first.")
    st.stop()

# 3. Stats Row
last_scraped = data[0].get('scraped_at', 'Unknown') if data else 'Unknown'

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Releases", len(filtered))
c2.metric("Countries", len(set(d['country'] for d in filtered)))
c3.metric("Next Release", min([d['start'] for d in filtered if d['start'] >= datetime.now().strftime("%Y-%m-%d")] + ["N/A"]))
c4.metric("Last Updated", last_scraped)

st.divider()

# 4. Calendar (6-Month View) & Details
cal_col, list_col = st.columns([3, 1])  # Widen calendar column

with cal_col:
    cal_events = []
    for event in filtered:
        cal_events.append({
            "title": event['title'],
            "start": event['start'],
            "backgroundColor": get_color(event['source']),
            "borderColor": get_color(event['source']),
            "extendedProps": event
        })

    # Calendar Options with Custom 6-Month View
    cal_options = {
        "headerToolbar": {
            "left": "today prev,next",
            "center": "title",
            "right": "multiMonth6Month,dayGridMonth,listMonth" # Added custom view button
        },
        "initialView": "multiMonth6Month", # Default to 6-month view
        "views": {
            "multiMonth6Month": {
                "type": "multiMonthYear",
                "duration": {"months": 6},
                "multiMonthMaxColumns": 3 # 3 cols x 2 rows = 6 months
            }
        },
        "height": 800,
    }
    state = calendar(events=cal_events, options=cal_options, key="main_cal")

with list_col:
    st.subheader("📋 Dataset Details")
    
    selected_event = None
    if state.get("eventClick"):
        selected_event = state["eventClick"]["event"]["extendedProps"]
        st.info("👇 Selected from Calendar")
    
    if selected_event:
        with st.container(border=True):
            bg = get_color(selected_event['source'])
            st.markdown(f"""
            <div style="background-color:{bg}; color:white; padding:5px 10px; border-radius:5px; display:inline-block; margin-bottom:10px;">
                {selected_event['source']}
            </div>
            """, unsafe_allow_html=True)
            st.markdown(f"### {selected_event['title']}")
            st.write(f"**Date:** {selected_event['start']}")
            st.write(f"**Region:** {selected_event['country']}")
            st.caption(selected_event['summary'])
            st.markdown(f"[🔗 **Access Data**]({selected_event['url']})")
    else:
        st.markdown("*Click on an event to view full details.*")
        
        # Show "Next Up" quick list if nothing selected
        st.markdown("---")
        st.markdown("**🔜 Upcoming Highlights**")
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        upcoming = sorted([e for e in filtered if e['start'] >= today_str], key=lambda x: x['start'])[:5]
        
        for up in upcoming:
            st.markdown(f"**{up['start']}** | {up['country']}")
            st.caption(up['title'])

# Manual Refresh
if st.sidebar.button("🔄 Force Refresh"):
    os.system("python scraper.py")
    st.rerun()
