import streamlit as st
from streamlit_calendar import calendar
import json
import os
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Release Tracker", page_icon="🧬")
DATA_FILE = os.path.join("data", "releases.json")

# Styling
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    .topic-tag {padding: 3px 8px; border-radius: 4px; color: white; font-weight: bold; font-size: 0.8rem;}
</style>
""", unsafe_allow_html=True)

# Load Data
@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

data = load_data()

# Sidebar
with st.sidebar:
    st.title("🧬 LCDS Tracker")
    if st.button("🔄 Force Refresh (Multi-Agent)"):
        with st.spinner("Deploying Agents..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
    st.divider()
    
    # Filters
    all_topics = sorted(list(set(d['topic'] for d in data)))
    sel_topics = st.multiselect("Theme", all_topics, default=all_topics)
    all_regions = sorted(list(set(d['country'] for d in data)))
    sel_regions = st.multiselect("Region", all_regions, default=all_regions)

# Main
filtered = [d for d in data if d['topic'] in sel_topics and d['country'] in sel_regions]

if not filtered:
    st.warning("No data found. Run the refresher.")
    st.stop()

# Metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Relevant Releases", len(filtered))
c2.metric("Mortality", len([x for x in filtered if x['topic'] == 'Mortality']))
c3.metric("Migration", len([x for x in filtered if x['topic'] == 'Migration']))

today = datetime.now().strftime("%Y-%m-%d")
future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
next_up = future[0] if future else None
c4.metric("Next Key Date", next_up['start'] if next_up else "-")

st.divider()

# Calendar
col1, col2 = st.columns([3, 1])
with col1:
    events = []
    colors = {
        "Mortality": "#d62728", "Fertility": "#e377c2", "Migration": "#ff7f0e",
        "Population": "#2ca02c", "Health": "#1f77b4", "Inequality": "#9467bd"
    }
    for d in filtered:
        events.append({
            "title": f"[{d['topic']}] {d['title']}", 
            "start": d['start'], 
            "color": colors.get(d['topic'], "#777"), 
            "extendedProps": d
        })
    calendar(events, options={"initialView": "dayGridMonth", "height": 700})

with col2:
    st.subheader("Details")
    if next_up:
        st.success("🚀 **Next Highlight**")
        with st.container(border=True):
            bg = colors.get(next_up['topic'], "#777")
            st.markdown(f"<span class='topic-tag' style='background:{bg}'>{next_up['topic']}</span>", unsafe_allow_html=True)
            st.markdown(f"### {next_up['title']}")
            st.write(f"**Date:** {next_up['start']}")
            st.write(f"**Source:** {next_up['source']}")
            st.link_button("🔗 Access Data", next_up['url'])
