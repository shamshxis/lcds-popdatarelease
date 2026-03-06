import streamlit as st
from streamlit_calendar import calendar
import json
import os
import time
import pandas as pd
from datetime import datetime

# --- Page Config ---
st.set_page_config(layout="wide", page_title="Global PopData Tracker", page_icon="🌍")

DATA_FILE = os.path.join("data", "releases.json")

# --- CSS for Compact UI ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem; padding-bottom: 2rem;}
    h1 {font-size: 1.8rem;}
    .stMetric {background-color: #f0f2f6; padding: 10px; border-radius: 5px;}
    div[data-testid="stExpander"] details summary p {font-weight: bold; font-size: 1.1rem;}
</style>
""", unsafe_allow_html=True)

# --- Load Data ---
@st.cache_data(ttl=600)
def load_data():
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

raw_data = load_data()

# --- Sidebar Controls ---
st.sidebar.title("🌍 PopData Tracker")

# 1. Global Search (Powerful Filter)
search_query = st.sidebar.text_input("🔍 Search Datasets", placeholder="e.g. Migration, CPI, Births...")

# 2. Filters
all_countries = sorted(list(set(d['country'] for d in raw_data))) if raw_data else []
sel_countries = st.sidebar.multiselect("Region", all_countries, default=all_countries)

# Filter Logic
filtered_data = []
if raw_data:
    for d in raw_data:
        # Check Country
        if sel_countries and d['country'] not in sel_countries: continue
        
        # Check Search Query (Case Insensitive)
        if search_query:
            query = search_query.lower()
            if query not in d['title'].lower() and query not in d['summary'].lower() and query not in d['topic'].lower():
                continue
        
        filtered_data.append(d)

# --- Layout ---

# Top Metrics
if filtered_data:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Upcoming Releases", len(filtered_data))
    
    # Next Release Logic
    today_str = datetime.now().strftime("%Y-%m-%d")
    future = sorted([x for x in filtered_data if x['start'] >= today_str], key=lambda x: x['start'])
    next_up = future[0] if future else None
    
    c2.metric("Next Key Date", next_up['start'] if next_up else "-")
    c3.metric("Next Dataset", next_up['topic'] if next_up else "-")
    c4.metric("Source", next_up['source'] if next_up else "-")

st.divider()

# Main View: Calendar + Details
col_cal, col_list = st.columns([3, 1])

with col_cal:
    # Calendar Events
    events = []
    color_map = {"ONS": "#00664F", "Eurostat": "#004494", "US Census": "#B31B1B", "StatCan": "#FF0000"}
    
    for d in filtered_data:
        events.append({
            "title": f"{d['country']}: {d['title']}",
            "start": d['start'],
            "color": color_map.get(d['source'], "#555"),
            "extendedProps": d
        })
    
    # Compact Calendar Options
    cal_ops = {
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,listMonth"},
        "initialView": "listMonth",  # Default to List View for readability
        "height": 500,
        "contentHeight": "auto"
    }
    
    state = calendar(events=events, options=cal_ops, key="cal")

with col_list:
    st.subheader("📌 Selected Details")
    if state.get("eventClick"):
        e = state["eventClick"]["event"]["extendedProps"]
        st.info(f"**{e['title']}**")
        st.write(f"📅 **Date:** {e['start']}")
        st.write(f"🏛 **Source:** {e['source']}")
        st.write(f"🏷 **Topic:** {e['topic']}")
        st.caption(e['summary'])
        st.link_button("🔗 Open Source", e['url'])
    elif next_up:
        st.success(f"🚀 **Next Up: {next_up['title']}**")
        st.write(f"📅 {next_up['start']}")
        st.caption(next_up['summary'])
        st.link_button("🔗 Open Source", next_up['url'])
    else:
        st.write("Select an event to see details.")

# --- Data Grid (Power User View) ---
st.subheader("📊 Data Grid")
if filtered_data:
    df = pd.DataFrame(filtered_data)[['start', 'country', 'source', 'title', 'topic', 'url']]
    st.dataframe(
        df, 
        use_container_width=True, 
        hide_index=True,
        column_config={"url": st.column_config.LinkColumn("Link")}
    )

# Refresh
if st.sidebar.button("🔄 Refresh Data"):
    os.system("python scraper.py")
    st.rerun()
