import streamlit as st
from streamlit_calendar import calendar
import json
import os
import pandas as pd
from datetime import datetime

# --- Page Config ---
st.set_page_config(layout="wide", page_title="PopData Tracker", page_icon="🧬")

DATA_FILE = os.path.join("data", "releases.json")

# --- CSS: Clean & Compact ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem;} 
    div[data-testid="stMetricValue"] {font-size: 1.2rem;}
</style>
""", unsafe_allow_html=True)

# --- Load Data & Force Refresh Logic ---
@st.cache_data(ttl=3600)
def get_data_from_file(timestamp):
    # Timestamp arg forces cache invalidation when changed
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

# Sidebar: Admin & Filters
with st.sidebar:
    st.title("⚙️ Controls")
    
    # 1. FORCE REFRESH BUTTON
    if st.button("🔄 Force Refresh Data", type="primary", use_container_width=True):
        with st.status("Running Scrapers...", expanded=True) as status:
            os.system("python scraper.py")
            st.cache_data.clear() # Clear Streamlit cache
            status.update(label="✅ Data Updated!", state="complete", expanded=False)
        st.rerun()

    st.divider()
    
    # 2. DATA FILTERS
    st.subheader("🔍 Filters")
    
    # Load raw data first
    raw_data = get_data_from_file(os.path.getmtime(DATA_FILE) if os.path.exists(DATA_FILE) else 0)
    
    # Source Filter
    all_sources = sorted(list(set(d['source'] for d in raw_data)))
    sel_sources = st.multiselect("Agency", all_sources, default=all_sources)
    
    # Topic Filter (Crucial for hiding Economy)
    all_topics = sorted(list(set(d['topic'] for d in raw_data)))
    # Default: Select everything EXCEPT 'Economy' if possible, or all if mixed
    default_topics = [t for t in all_topics if t != "Economy"]
    if not default_topics: default_topics = all_topics
    
    sel_topics = st.multiselect("Topic", all_topics, default=default_topics)
    
    # Country Filter
    all_countries = sorted(list(set(d['country'] for d in raw_data)))
    sel_countries = st.multiselect("Country", all_countries, default=all_countries)

# --- FILTERING LOGIC ---
filtered = []
for d in raw_data:
    if d['source'] in sel_sources and d['topic'] in sel_topics and d['country'] in sel_countries:
        filtered.append(d)

# --- MAIN DASHBOARD ---
st.title("🧬 Global Population Data Calendar")

if not filtered:
    st.warning("No data matches your filters. Try selecting more Topics or Agencies.")
    st.stop()

# Metrics Row
c1, c2, c3, c4 = st.columns(4)
c1.metric("Datasets", len(filtered))
c2.metric("Mortality/Health", len([x for x in filtered if x['topic'] in ['Mortality', 'Health']]))
c3.metric("Migration", len([x for x in filtered if x['topic'] == 'Migration']))

# Find Next Release
today = datetime.now().strftime("%Y-%m-%d")
future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
if future:
    c4.metric("Next Release", f"{future[0]['start']} ({future[0]['source']})")

st.divider()

# Calendar & List Layout
col_cal, col_list = st.columns([2, 1])

with col_cal:
    events = []
    # Color Coding by Topic (Visual Clues)
    topic_colors = {
        "Mortality": "#d62728", # Red
        "Births": "#e377c2",    # Pink
        "Migration": "#ff7f0e", # Orange
        "Population": "#2ca02c",# Green
        "Economy": "#7f7f7f",   # Grey (Boring)
        "Health": "#1f77b4"     # Blue
    }
    
    for item in filtered:
        events.append({
            "title": f"[{item['topic']}] {item['title']}",
            "start": item['start'],
            "color": topic_colors.get(item['topic'], "#555"),
            "extendedProps": item
        })
        
    calendar_ops = {
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,listMonth"},
        "initialView": "dayGridMonth",
        "height": 650
    }
    state = calendar(events=events, options=calendar_ops, key="cal")

with col_list:
    st.subheader("📌 Release Details")
    
    selected = None
    if state.get("eventClick"):
        selected = state["eventClick"]["event"]["extendedProps"]
    elif future:
        selected = future[0]
        st.caption("🚀 Next Upcoming Release")
        
    if selected:
        with st.container(border=True):
            # Header Badge
            color = topic_colors.get(selected['topic'], "#555")
            st.markdown(f"<span style='background:{color}; padding:4px 8px; border-radius:4px; color:white; font-weight:bold'>{selected['topic']}</span>", unsafe_allow_html=True)
            
            st.markdown(f"### {selected['title']}")
            st.write(f"**🗓 Date:** {selected['start']}")
            st.write(f"**🏛 Agency:** {selected['source']} ({selected['country']})")
            st.info(selected.get('summary', 'No summary available.'))
            
            if selected['url']:
                st.link_button("🔗 Open Official Page", selected['url'])
    else:
        st.write("Select an event to see details.")

# --- Data Grid for Power Users ---
st.divider()
with st.expander("📊 View as Spreadsheet", expanded=False):
    df = pd.DataFrame(filtered)[['start', 'country', 'topic', 'title', 'source', 'url']]
    st.dataframe(df, use_container_width=True, hide_index=True, column_config={"url": st.column_config.LinkColumn()})
