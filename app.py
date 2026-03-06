import streamlit as st
from streamlit_calendar import calendar
import json
import os
import pandas as pd
from datetime import datetime

# --- CONFIG ---
st.set_page_config(layout="wide", page_title="Global PopData Tracker", page_icon="🧬")
DATA_FILE = os.path.join("data", "releases.json")
HEALTH_FILE = os.path.join("data", "sources_health.json")

# --- STYLES ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    /* Clean metrics */
    div[data-testid="stMetricValue"] {font-size: 1.4rem;}
</style>
""", unsafe_allow_html=True)

# --- LOADERS ---
@st.cache_data(ttl=300) 
def load_data():
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

def load_health():
    if not os.path.exists(HEALTH_FILE): return {}
    with open(HEALTH_FILE, 'r') as f: return json.load(f)

raw_data = load_data()
health_data = load_health()

# --- SIDEBAR: SYSTEM & FILTERS ---
with st.sidebar:
    st.title("⚙️ PopData System")
    
    # 1. System Health Status
    with st.expander("🔌 Source Status", expanded=True):
        if not health_data:
            st.info("Run scraper to see health.")
        for source, info in health_data.items():
            status = info.get('status', 'error')
            icon = "✅" if status == 'ok' else "⚠️" if status == 'warning' else "❌"
            st.write(f"{icon} **{source}**")
            if status != 'ok':
                st.caption(f"Error: {info.get('error', 'Unknown')}")

    # 2. Force Refresh Button
    if st.button("🔄 Force Refresh Now", type="primary", use_container_width=True):
        with st.status("Running Global Scrapers...", expanded=True) as status:
            os.system("python scraper.py")
            st.cache_data.clear()
            status.update(label="✅ Update Complete!", state="complete", expanded=False)
        st.rerun()
    
    st.divider()
    
    # 3. Smart Filters
    # Calculate Default Topics (exclude Economy unless empty)
    all_topics = sorted(list(set(d['topic'] for d in raw_data)))
    default_topics = [t for t in all_topics if t != "Economy"]
    if not default_topics: default_topics = all_topics
    
    sel_topics = st.multiselect("Topic", all_topics, default=default_topics)
    sel_countries = st.multiselect("Country", sorted(list(set(d['country'] for d in raw_data))), default=sorted(list(set(d['country'] for d in raw_data))))

# --- MAIN LOGIC ---
filtered = [d for d in raw_data if d['topic'] in sel_topics and d['country'] in sel_countries]

# --- DASHBOARD ---
col1, col2 = st.columns([3, 1])

with col1:
    st.title("🧬 Global Demography Calendar")
    
    if not filtered:
        st.warning("No data found. Try refreshing or changing filters.")
        st.stop()

    # Metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Datasets", len(filtered))
    m2.metric("Mortality/Births", len([x for x in filtered if x['topic'] in ['Mortality', 'Births']]))
    m3.metric("Migration", len([x for x in filtered if x['topic'] == 'Migration']))
    
    # Next Priority Release
    today = datetime.now().strftime("%Y-%m-%d")
    future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
    next_rel = future[0] if future else None
    
    m4.metric("Next Key Date", next_rel['start'] if next_rel else "-")

    st.divider()

    # Calendar View
    events = []
    # Visual Coding
    colors = {
        "Mortality": "#d62728", # Red
        "Births": "#e377c2",    # Pink
        "Migration": "#ff7f0e", # Orange
        "Population": "#2ca02c",# Green
        "Health": "#1f77b4",    # Blue
        "Economy": "#7f7f7f"    # Grey
    }
    
    for item in filtered:
        events.append({
            "title": f"[{item['topic']}] {item['title']}",
            "start": item['start'],
            "color": colors.get(item['topic'], "#555"),
            "extendedProps": item
        })
    
    cal_ops = {
        "headerToolbar": {"left": "today prev,next", "center": "title", "right": "dayGridMonth,listMonth"},
        "initialView": "dayGridMonth",
        "height": 700
    }
    state = calendar(events=events, options=cal_ops, key="cal")

with col2:
    st.subheader("📌 Details")
    
    selected = None
    if state.get("eventClick"):
        selected = state["eventClick"]["event"]["extendedProps"]
    elif next_rel:
        selected = next_rel
        st.success("🚀 **Next Up**")

    if selected:
        with st.container(border=True):
            bg = colors.get(selected['topic'], "#555")
            st.markdown(f"<span style='background:{bg}; padding:4px 8px; border-radius:4px; color:white; font-weight:bold'>{selected['topic']}</span>", unsafe_allow_html=True)
            
            st.markdown(f"### {selected['title']}")
            st.write(f"**🗓 Date:** {selected['start']}")
            st.write(f"**🏛 Agency:** {selected['source']} ({selected['country']})")
            st.caption(selected.get('summary', ''))
            
            st.link_button("🔗 Open Source", selected['url'])
