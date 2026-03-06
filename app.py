import streamlit as st
from streamlit_calendar import calendar
import json
import os
from datetime import datetime

# --- CONFIG ---
st.set_page_config(layout="wide", page_title="LCDS PopData Tracker", page_icon="🧬")
DATA_FILE = os.path.join("data", "releases.json")
HEALTH_FILE = os.path.join("data", "sources_health.json")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    .topic-tag {
        display: inline-block; padding: 2px 8px; border-radius: 4px; 
        color: white; font-weight: bold; font-size: 0.8rem; margin-right: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- LOADERS ---
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
    
    # 1. Scraper Health
    with st.expander("System Status", expanded=True):
        if not health: st.info("Run scraper first.")
        for src, info in health.items():
            icon = "✅" if info['status'] == 'ok' else "⚠️"
            st.write(f"{icon} **{src}**")
            if info['status'] != 'ok': st.caption(info.get('error'))

    # 2. Force Refresh
    if st.button("🔄 Update Data"):
        os.system("python scraper.py")
        st.cache_data.clear()
        st.rerun()

    st.divider()

    # 3. FILTERS
    # Default: Hide "Economy (Low Priority)"
    all_topics = sorted(list(set(d['topic'] for d in data)))
    default_topics = [t for t in all_topics if "Economy" not in t]
    
    sel_topics = st.multiselect("Research Area", all_topics, default=default_topics)
    
    all_countries = sorted(list(set(d['country'] for d in data)))
    sel_countries = st.multiselect("Region", all_countries, default=all_countries)

# --- MAIN ---
filtered = [d for d in data if d['topic'] in sel_topics and d['country'] in sel_countries]

if not filtered:
    st.warning("No datasets match your filters.")
    st.stop()

# Metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("Relevant Datasets", len(filtered))
c2.metric("Mortality/Health", len([x for x in filtered if x['topic'] in ['Mortality', 'Health']]))
c3.metric("Migration", len([x for x in filtered if x['topic'] == 'Migration']))

# Next Release
today = datetime.now().strftime("%Y-%m-%d")
future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
next_up = future[0] if future else None
c4.metric("Next Key Date", next_up['start'] if next_up else "-")

st.divider()

# Layout
col_cal, col_list = st.columns([3, 1])

with col_cal:
    events = []
    # LCDS-Themed Colors
    colors = {
        "Mortality": "#d62728", # Red
        "Fertility": "#e377c2", # Pink
        "Migration": "#ff7f0e", # Orange
        "Population": "#2ca02c",# Green
        "Health": "#1f77b4",    # Blue
        "Inequality": "#9467bd",# Purple
        "Environment": "#bcbd22",# Olive
        "Economy (Low Priority)": "#7f7f7f" # Grey
    }
    
    for d in filtered:
        events.append({
            "title": f"[{d['topic']}] {d['title']}",
            "start": d['start'],
            "color": colors.get(d['topic'], "#555"),
            "extendedProps": d
        })
        
    cal = calendar(events, options={"initialView": "dayGridMonth", "height": 700})

with col_list:
    st.subheader("📌 Selected Details")
    
    sel = None
    if cal.get("eventClick"):
        sel = cal["eventClick"]["event"]["extendedProps"]
    elif next_up:
        sel = next_up
        st.success("🚀 **Next Highlight**")

    if sel:
        with st.container(border=True):
            bg = colors.get(sel['topic'], "#555")
            st.markdown(f"<span class='topic-tag' style='background:{bg}'>{sel['topic']}</span>", unsafe_allow_html=True)
            st.markdown(f"### {sel['title']}")
            st.write(f"**Date:** {sel['start']}")
            st.write(f"**Source:** {sel['source']} ({sel['country']})")
            st.link_button("🔗 Access Data", sel['url'])
