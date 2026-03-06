import streamlit as st
from streamlit_calendar import calendar
import json
import os
import pandas as pd
from datetime import datetime
import time

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="LCDS Release Tracker", page_icon="🧬")

# --- FILES ---
DATA_FILE = os.path.join("data", "releases.json")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .block-container {padding-top: 1rem;}
    .stMetric {background-color: #f9f9f9; padding: 10px; border-radius: 8px; border: 1px solid #ddd;}
    div[data-testid="stExpander"] details summary p {font-weight: bold; font-size: 1.1rem;}
    .topic-tag {
        display: inline-block; padding: 4px 10px; border-radius: 12px; 
        color: white; font-weight: bold; font-size: 0.8rem; margin-right: 5px;
    }
</style>
""", unsafe_allow_html=True)

# --- LOAD DATA ---
@st.cache_data(ttl=60) # Cache for 60 seconds
def load_data():
    if not os.path.exists(DATA_FILE):
        return []
    try:
        with open(DATA_FILE, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        return []

data = load_data()

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.title("🧬 LCDS Tracker")
    
    # 1. REFRESH BUTTON
    if st.button("🔄 Force Refresh Data", type="primary"):
        with st.status("Running Scraper Engine...", expanded=True) as status:
            st.write("🕷️ Deploying Agents...")
            os.system("python scraper.py")
            time.sleep(1)
            st.write("💾 Reloading Data...")
            st.cache_data.clear()
            status.update(label="✅ Data Updated!", state="complete", expanded=False)
        st.rerun()

    st.divider()

    # 2. FILTERS
    if not data:
        st.warning("⚠️ No data found. Please click 'Force Refresh'.")
        st.stop()

    # Topic Filter
    all_topics = sorted(list(set(d['topic'] for d in data)))
    sel_topics = st.multiselect("Research Theme", all_topics, default=all_topics)
    
    # Country Filter
    all_countries = sorted(list(set(d['country'] for d in data)))
    sel_countries = st.multiselect("Region / Country", all_countries, default=all_countries)

    st.info(f"Loaded {len(data)} datasets.")

# --- MAIN DASHBOARD ---
# Filter Data
filtered = [d for d in data if d['topic'] in sel_topics and d['country'] in sel_countries]

if not filtered:
    st.warning("No datasets match your filters.")
    st.stop()

# 1. METRICS ROW
col1, col2, col3, col4 = st.columns(4)
col1.metric("Relevant Releases", len(filtered))
col2.metric("Mortality Data", len([x for x in filtered if x['topic'] == 'Mortality']))
col3.metric("Migration Data", len([x for x in filtered if x['topic'] == 'Migration']))

# Calculate Next Release
today = datetime.now().strftime("%Y-%m-%d")
future_releases = sorted([x for x in filtered if x['start'] >= today], key=lambda x: x['start'])
next_up = future_releases[0] if future_releases else None
col4.metric("Next Key Date", next_up['start'] if next_up else "None")

st.divider()

# 2. CALENDAR & DETAILS LAYOUT
col_cal, col_details = st.columns([3, 1.2])

with col_cal:
    # Prepare Calendar Events
    events = []
    # Color Scheme for LCDS Themes
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
            "title": f"[{d['country']}] {d['title']}",
            "start": d['start'],
            "color": colors.get(d['topic'], "#777"),
            "extendedProps": d
        })
    
    # Render Calendar
    cal_state = calendar(
        events=events,
        options={
            "headerToolbar": {"left": "today prev,next", "center": "title", "right": "dayGridMonth,listMonth"},
            "initialView": "dayGridMonth",
            "height": 700
        },
        key="cal"
    )

with col_details:
    st.subheader("📌 Release Details")
    
    selected = None
    
    # Logic: Show clicked event OR show 'Next Up'
    if cal_state.get("eventClick"):
        selected = cal_state["eventClick"]["event"]["extendedProps"]
    elif next_up:
        selected = next_up
        st.success("🚀 **Coming Up Next**")

    if selected:
        with st.container(border=True):
            # Topic Badge
            bg_color = colors.get(selected['topic'], "#777")
            st.markdown(f"<span class='topic-tag' style='background:{bg_color}'>{selected['topic']}</span>", unsafe_allow_html=True)
            
            st.markdown(f"### {selected['title']}")
            st.write(f"**🗓 Date:** {selected['start']}")
            st.write(f"**🏛 Source:** {selected['source']} ({selected['country']})")
            
            st.link_button("🔗 Open Data Source", selected['url'], use_container_width=True)
    else:
        st.info("Select an event on the calendar to see details.")

# --- 3. DATA GRID (Bottom) ---
with st.expander("📊 View All Data as Table", expanded=False):
    df = pd.DataFrame(filtered)
    # Reorder columns for readability
    if not df.empty:
        df = df[['start', 'country', 'topic', 'title', 'source', 'url']]
        st.dataframe(
            df, 
            use_container_width=True, 
            hide_index=True,
            column_config={"url": st.column_config.LinkColumn("Link")}
        )
