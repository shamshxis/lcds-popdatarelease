import streamlit as st
from streamlit_calendar import calendar
import json
import os
import pandas as pd
from datetime import datetime

# --- Configuration ---
st.set_page_config(layout="wide", page_title="Global Population Data Calendar", page_icon="🌍")

DATA_FILE = os.path.join("data", "releases.json")

# --- CSS Styling ---
st.markdown("""
<style>
    .main-header {font-size: 2.5rem; font-weight: 700; color: #2c3e50; margin-bottom: 0px;}
    .sub-header {font-size: 1.2rem; color: #7f8c8d; margin-bottom: 20px;}
    .metric-card {background-color: #f8f9fa; border: 1px solid #e9ecef; padding: 15px; border-radius: 10px; text-align: center;}
    .source-tag {padding: 2px 8px; border-radius: 4px; font-size: 0.8rem; font-weight: bold; color: white;}
</style>
""", unsafe_allow_html=True)

# --- Helpers ---
@st.cache_data(ttl=3600)
def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    with open(DATA_FILE, 'r') as f:
        data = json.load(f)
    return data

def get_color(source):
    colors = {
        "ONS": "#00664F",        # UK Green
        "Eurostat": "#004494",   # EU Blue
        "US Census": "#B31B1B",  # US Red
        "UN Data": "#009EDB",    # UN Blue
        "StatCan": "#FF0000",    # Canada Red
    }
    return colors.get(source, "#7f8c8d") # Default Grey

# --- Main App ---

# 1. Header
col1, col2 = st.columns([3, 1])
with col1:
    st.markdown('<div class="main-header">🌍 Global PopData Calendar</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Tracking Official Statistical Releases (ONS, Eurostat, UN, Census, etc.)</div>', unsafe_allow_html=True)

data = load_data()
if not data:
    st.warning("No data found. Please run scraper.py first.")
    st.stop()

# 2. Sidebar Filters
st.sidebar.header("🔍 Filter View")

# Country Filter
all_countries = sorted(list(set(d['country'] for d in data)))
sel_country = st.sidebar.multiselect("Region/Country", all_countries, default=all_countries)

# Topic Filter
all_topics = sorted(list(set(d['topic'] for d in data)))
sel_topic = st.sidebar.multiselect("Dataset Topic", all_topics, default=all_topics)

# Filter Logic
filtered = [
    d for d in data 
    if d['country'] in sel_country 
    and d['topic'] in sel_topic
]

# 3. Stats Row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Upcoming Releases", len(filtered))
c2.metric("Countries Tracked", len(set(d['country'] for d in filtered)))
c3.metric("Data Sources", len(set(d['source'] for d in filtered)))
next_rel = sorted([d for d in filtered if d['start'] >= datetime.now().strftime("%Y-%m-%d")], key=lambda x: x['start'])
c4.metric("Next Key Release", next_rel[0]['start'] if next_rel else "N/A")

st.divider()

# 4. Layout: Calendar (Left) + Details (Right)
cal_col, list_col = st.columns([2, 1])

with cal_col:
    # Prepare Calendar Events
    cal_events = []
    for event in filtered:
        cal_events.append({
            "title": event['title'],
            "start": event['start'],
            "backgroundColor": get_color(event['source']),
            "borderColor": get_color(event['source']),
            "extendedProps": event
        })

    cal_options = {
        "headerToolbar": {
            "left": "today prev,next",
            "center": "title",
            "right": "dayGridMonth,listMonth"
        },
        "initialView": "dayGridMonth",
        "height": 650
    }

    state = calendar(events=cal_events, options=cal_options, key="main_cal")

with list_col:
    st.subheader("📋 Release Details")
    
    selected_event = None
    
    # Handle Click Event
    if state.get("eventClick"):
        selected_event = state["eventClick"]["event"]["extendedProps"]
        st.info("👇 Selected from Calendar")
    elif next_rel:
        selected_event = next_rel[0]
        st.success("🚀 Next Upcoming Release")

    if selected_event:
        with st.container(border=True):
            # Header with Source Badge
            bg = get_color(selected_event['source'])
            st.markdown(f"""
            <div style="background-color:{bg}; color:white; padding:5px 10px; border-radius:5px; display:inline-block; margin-bottom:10px;">
                {selected_event['source']}
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown(f"### {selected_event['title']}")
            st.write(f"**🗓 Date:** {selected_event['start']}")
            st.write(f"**📍 Region:** {selected_event['country']}")
            st.write(f"**🏷 Topic:** {selected_event['topic']}")
            
            st.markdown("#### Summary")
            st.caption(selected_event['summary'])
            
            st.markdown(f"""
            <a href="{selected_event['url']}" target="_blank" style="text-decoration:none;">
                <button style="width:100%; background-color:#2c3e50; color:white; padding:10px; border:none; border-radius:5px; cursor:pointer;">
                    🔗 Access Official Dataset
                </button>
            </a>
            """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📅 Next 5 Releases")
    for item in next_rel[:5]:
        with st.expander(f"{item['start']} | {item['source']}"):
            st.write(f"**{item['title']}**")
            st.caption(item['summary'])
            st.markdown(f"[Link]({item['url']})")

# Auto-refresh helper
if st.sidebar.button("🔄 Trigger Manual Refresh"):
    os.system("python scraper.py")
    st.rerun()
