import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(layout="wide", page_title="LCDS Watchtower", page_icon="📡")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .status-tag {padding: 4px 8px; border-radius: 4px; font-weight: bold; color: white;}
    .status-release {background-color: #ff4b4b;}
    .status-update {background-color: #ffa421;}
    .block-container {padding-top: 1rem;}
</style>
""", unsafe_allow_html=True)

st.title("📡 LCDS Data Watchtower")
st.caption("Tracking Releases, Updates, and Cuts in Global Demography")

# --- FILE SETUP (Auto-Healing) ---
DATA_DIR = "data"
FILE = os.path.join(DATA_DIR, "change_log.csv")
os.makedirs(DATA_DIR, exist_ok=True)

# Auto-create history if missing so app doesn't crash
if not os.path.exists(FILE):
    pd.DataFrame(columns=['status', 'source', 'date', 'type', 'description', 'link']).to_csv(FILE, index=False)

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Check for Updates Now", type="primary"):
        with st.status("📡 Scanning Agencies...", expanded=True) as status:
            st.write("Connecting to Global Feeds...")
            os.system("python scraper.py")
            status.update(label="Sync Complete!", state="complete", expanded=False)
            time.sleep(1)
            st.rerun()
            
    st.divider()
    st.markdown("### 🌍 Sources Active")
    st.markdown("""
    - **USAID/DHS** (Global Health)
    - **INSEE** (France)
    - **FinData** (Finland)
    - **Statice** (Iceland)
    - **Eurostat** (EU)
    - **ONS** (UK)
    """)

# --- LOAD DATA ---
try:
    df = pd.read_csv(FILE)
except:
    st.error("Could not read history file.")
    st.stop()

if df.empty:
    st.info("👋 **System Initialized.** No events logged yet.")
    st.warning("👉 Click 'Check for Updates Now' to populate the first batch of data.")
    st.stop()

# --- FILTERS ---
# Sort by date descending (Newest first)
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values(by='date', ascending=False)

col1, col2 = st.columns(2)
sources = df['source'].unique().tolist()
sel_source = col1.multiselect("Filter Source", sources, default=sources)

if sel_source:
    df = df[df['source'].isin(sel_source)]

# --- RENDER TICKER ---
st.divider()

for index, row in df.iterrows():
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.2, 5, 1])
        
        # 1. Status Pill
        status = row.get('status', 'UPDATE')
        color = "status-release" if "RELEASE" in status else "status-update"
        c1.markdown(f"<span class='status-tag {color}'>{status}</span>", unsafe_allow_html=True)
        c1.caption(row['date'].strftime('%Y-%m-%d'))
        
        # 2. Description
        c2.markdown(f"**{row['source']}** ({row['type']})")
        c2.write(row['description'])
        
        # 3. Link
        c3.link_button("🔗 Open Source", row['link'], use_container_width=True)
