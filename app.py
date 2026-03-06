import streamlit as st
from streamlit_calendar import calendar
import json
import os
from datetime import datetime

st.set_page_config(layout="wide", page_title="LCDS Release Tracker", page_icon="🧬")
DATA_FILE = os.path.join("data", "releases.json")
HEALTH_FILE = os.path.join("data", "sources_health.json")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    .stMetric {background: #f9f9f9; border-radius: 8px; padding: 10px; border: 1px solid #eee;}
    .topic-tag {padding: 2px 8px; border-radius: 4px; color: white; font-weight: bold; font-size: 0.8rem;}
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=60)
def load_data():
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

def load_health():
    if not os.path.exists(HEALTH_FILE): return {}
    with open(HEALTH_FILE, 'r') as f: return json.load(f)

data = load_data()
health = load_health()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🧬 LCDS Tracker")
    
    # Health Status
    with st.expander("API Connection Status", expanded=True):
        if not health: st.info("Run scraper first.")
        for src, info in health.items():
            icon = "✅" if info['status'] == 'ok' else "⚠️"
            st.write(f"{icon} **{src}**")
            
    if st.button("🔄 Refresh Data"):
        os.system("python scraper.py")
        st.cache_data.clear()
        st.rerun()

    st.divider()

    # FILTERS
    # Filter out "Economy (Ignored)" by default
    all_topics = sorted(list(set(d['topic'] for d in data)))
    valid_topics = [t for t in all_topics if "Ignored" not in t]
    
    sel_topics = st.multiselect("LCDS Theme", valid_topics, default=valid_topics)
    
    all_countries = sorted(list(set(d['country'] for d in data)))
    sel_countries = st.multiselect("Region", all_countries, default=all_countries)

# --- MAIN ---
filtered = [d for d in data if d['topic'] in sel_topics and d['country'] in sel_countries]

if not filtered:
    st.warning("No datasets found. Try adjusting filters.")
    st.stop()

# Metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Relevant Releases", len(filtered))
c2.metric("Mortality/Health", len([x for x in filtered if x['topic'] in ['Mortality', 'Health']]))
c3.metric("Migration", len([x for x in filtered if x['topic'] == 'Migration']))

# Next Release
today = datetime.now().strftime("%Y-%m-%d")
future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
next_up = future[0] if future else None
c4.metric("Next Key Date", next_up['start'] if next_up else "-")

st.divider()

col_cal, col_list = st.columns([3, 1])

with col_cal:
    events = []
    # LCDS Theme Colors
    colors = {
        "Mortality": "#d62728", # Red
        "Fertility": "#e377c2", # Pink
        "Migration": "#ff7f0e", # Orange
        "Population": "#2ca02c",# Green
        "Health": "#1f77b4",    # Blue
        "Inequality": "#9467bd",# Purple
        "Environment": "#bcbd22" # Olive
    }
    
    for d in filtered:
        events.append({
            "title": f"[{d['topic']}] {d['title']}",
            "start": d['start'],
            "color": colors.get(d['topic'], "#777"),
            "extendedProps": d
        })
        
    cal = calendar(events, options={"initialView": "dayGridMonth", "height": 700})

with col_list:
    st.subheader("Details")
    sel = None
    if cal.get("eventClick"):
        sel = cal["eventClick"]["event"]["extendedProps"]
    elif next_up:
        sel = next_up
        st.success("🚀 **Next Highlight**")

    if sel:
        with st.container(border=True):
            bg = colors.get(sel['topic'], "#777")
            st.markdown(f"<span class='topic-tag' style='background:{bg}'>{sel['topic']}</span>", unsafe_allow_html=True)
            st.markdown(f"### {sel['title']}")
            st.write(f"**Date:** {sel['start']}")
            st.write(f"**Source:** {sel['source']} ({sel['country']})")
            st.link_button("🔗 Access Data", sel['url'])
