import streamlit as st
from streamlit_calendar import calendar
import json
import os
import pandas as pd
from datetime import datetime

st.set_page_config(layout="wide", page_title="Global Data Calendar", page_icon="🌍")
DATA_FILE = os.path.join("data", "releases.json")

# --- Styles ---
st.markdown("""
<style>
    .stMetric {background-color: #f8f9fa; border: 1px solid #ddd; padding: 10px; border-radius: 8px;}
    .block-container {padding-top: 1rem;}
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=600)
def load_data():
    if not os.path.exists(DATA_FILE): return []
    with open(DATA_FILE, 'r') as f: return json.load(f)

data = load_data()

# --- TOP BAR: SEARCH & FILTERS ---
col_title, col_search = st.columns([2, 3])
with col_title:
    st.title("🌍 Global Data Tracker")
    st.caption("Tracking ONS, Eurostat, BLS, NBS China, StatJapan, IBGE Brazil")

with col_search:
    # THE POWER FEATURE: Free text search
    search_term = st.text_input("🔍 Global Search", placeholder="Try 'Mortality Spain', 'China GDP', 'US Inflation'...")

# --- ADVANCED FILTERS (Sidebar) ---
st.sidebar.header("Filter Options")
all_regions = sorted(list(set(d.get('region', 'Other') for d in data))) if data else []
sel_region = st.sidebar.multiselect("Continent/Region", all_regions)

all_countries = sorted(list(set(d['country'] for d in data))) if data else []
# Filter countries based on region selection
if sel_region:
    available_countries = sorted(list(set(d['country'] for d in data if d.get('region') in sel_region)))
else:
    available_countries = all_countries
sel_country = st.sidebar.multiselect("Country", available_countries)

all_topics = sorted(list(set(d['topic'] for d in data))) if data else []
sel_topic = st.sidebar.multiselect("Topic", all_topics)

# --- FILTERING LOGIC ---
filtered = []
if data:
    terms = search_term.lower().split() # Split "Spain Mortality" -> ["spain", "mortality"]
    
    for d in data:
        # 1. Sidebar Filters
        if sel_region and d.get('region') not in sel_region: continue
        if sel_country and d['country'] not in sel_country: continue
        if sel_topic and d['topic'] not in sel_topic: continue
        
        # 2. Search Bar (AND Logic: Must match ALL terms)
        if terms:
            text_corpus = f"{d['title']} {d['country']} {d['topic']} {d['summary']}".lower()
            if not all(term in text_corpus for term in terms):
                continue
        
        filtered.append(d)

# --- DISPLAY ---
if not filtered:
    st.warning("No datasets match your filters.")
else:
    # Metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Datasets Found", len(filtered))
    c2.metric("Countries", len(set(d['country'] for d in filtered)))
    
    # Next Release
    today = datetime.now().strftime("%Y-%m-%d")
    future = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
    next_up = future[0] if future else None
    
    c3.metric("Next Release", next_up['start'] if next_up else "-")
    c4.metric("Key Dataset", next_up['title'][:20]+"..." if next_up else "-")
    
    st.divider()

    # Calendar & List
    cal_col, list_col = st.columns([2, 1])
    
    with cal_col:
        events = []
        # Region Color Mapping
        colors = {"Europe": "#004494", "Americas": "#B31B1B", "Asia": "#EB6608"}
        
        for item in filtered:
            events.append({
                "title": f"{item['country']}: {item['title']}",
                "start": item['start'],
                "color": colors.get(item.get('region'), "#555"),
                "extendedProps": item
            })
            
        calendar_ops = {
            "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,listMonth"},
            "initialView": "dayGridMonth",
            "height": 600
        }
        state = calendar(events=events, options=calendar_ops, key="cal")

    with list_col:
        st.subheader("Results List")
        for item in filtered[:10]: # Show top 10
            with st.expander(f"{item['start']} | {item['country']} {item['topic']}"):
                st.markdown(f"**{item['title']}**")
                st.caption(item['summary'])
                st.link_button("Go to Source", item['url'])
