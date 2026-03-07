import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide", page_title="LCDS Data Watchtower", page_icon="📡")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-tag {
        padding: 4px 8px; border-radius: 4px; font-weight: bold; color: white;
    }
    .status-release {background-color: #ff4b4b;} /* Red for New */
    .status-update {background-color: #ffa421;}   /* Orange for Update */
    a {text-decoration: none; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

st.title("📡 LCDS Data Watchtower")
st.caption("Monitoring Releases, Updates, and Cuts across Global Health & Demography")

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Check for Updates Now"):
        with st.spinner("Scanning global feeds..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
    st.divider()
    st.write("**Monitored Sources:**")
    st.markdown("- USAID/DHS\n- INSEE (France)\n- FinData\n- Statice (Iceland)\n- Eurostat\n- ONS")

# --- MAIN TABLE ---
FILE = "data/change_log.csv"

if not os.path.exists(FILE):
    st.info("System initializing... No history yet. Click 'Check for Updates'.")
    st.stop()

# Load Data
try:
    df = pd.read_csv(FILE)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=False)
except Exception as e:
    st.error(f"Error reading history: {e}")
    st.stop()

# Filters
col1, col2 = st.columns(2)
source_filter = col1.multiselect("Filter Source", df['source'].unique())
type_filter = col2.multiselect("Filter Type", df['type'].unique())

if source_filter:
    df = df[df['source'].isin(source_filter)]
if type_filter:
    df = df[df['type'].isin(type_filter)]

# --- RENDER TABLE ---
# We use a custom HTML loop for better control over the "Link" and "Status" pill
for index, row in df.iterrows():
    with st.container(border=True):
        c1, c2, c3 = st.columns([1, 4, 1])
        
        # Status Pill
        status_color = "status-release" if "RELEASE" in row['status'] else "status-update"
        c1.markdown(f"<span class='status-tag {status_color}'>{row['status']}</span>", unsafe_allow_html=True)
        c1.caption(row['date'].strftime('%Y-%m-%d'))
        
        # Description & Source
        c2.markdown(f"**{row['source']}**: {row['description']}")
        
        # Link Button
        c3.link_button("🔗 Open Source", row['link'], use_container_width=True)
