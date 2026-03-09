import pandas as pd
import streamlit as st
import os
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="LCDS Data Brief", page_icon="📋", layout="wide")

st.markdown("""
<style>
    h1 { margin-bottom: 0px; font-family: 'Helvetica', sans-serif; }
    div[data-testid="stStatusWidget"] { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

DATA_FILE = "data/dataset_tracker.csv"

# --- HELPER: LOAD DATA ---
def load_data():
    if not os.path.exists(DATA_FILE): return pd.DataFrame()
    
    df = pd.read_csv(DATA_FILE)
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"])
    
    # Format for Display
    df["Date"] = df["dt"].dt.strftime("%d %b %Y")
    df["Month"] = df["dt"].dt.strftime("%B %Y")
    df["YearMonth"] = df["dt"].dt.strftime("%Y-%m") # For sorting
    
    # Sort: Future first (Ascending date) or Newest first? 
    # Management usually wants "What's Next?" -> Ascending
    return df.sort_values(by=["dt"], ascending=True)

# --- UI HEADER ---
st.title("📋 LCDS Management Data Brief")
st.markdown("Monitoring **Migration, Fertility, Mortality, and Population** releases (±1 Year).")

# --- SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("🎛️ Controls")
    if st.button("🔄 Refresh Data", type="primary"):
        with st.spinner("Scanning Watchlist & Media..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
    
    st.divider()
    st.info("Tracking ONS, Eurostat, DHS, Census, and Nordic Registers.")

# --- MAIN LOGIC ---
df = load_data()

if df.empty:
    st.info("⚠️ System Initializing. Please click 'Refresh Data'.")
    st.stop()

# --- FILTERS ---
col1, col2, col3 = st.columns([1, 1, 2])
sel_source = col1.selectbox("Source", ["All"] + sorted(df["source"].unique().tolist()))
sel_status = col2.selectbox("Status", ["All"] + sorted(df["status"].unique().tolist()))
search = col3.text_input("Search Datasets", "")

view = df.copy()
if sel_source != "All": view = view[view["source"] == sel_source]
if sel_status != "All": view = view[view["status"] == sel_status]
if search: view = view[view["dataset_title"].str.contains(search, case=False)]

# --- METRICS ---
c1, c2, c3 = st.columns(3)
upcoming = view[view['dt'] >= datetime.now()]
today_release = view[view['dt'].dt.date == datetime.now().date()]

c1.metric("Upcoming Releases", len(upcoming))
c2.metric("Released Today", len(today_release))
c3.metric("Total Assets", len(view))

st.divider()

# --- TABLE RENDER (By Month) ---
# Sort months chronologically
unique_months = view.sort_values("YearMonth")["Month"].unique()

for month in unique_months:
    st.subheader(month)
    m_data = view[view["Month"] == month]
    
    st.dataframe(
        m_data[["status", "Date", "source", "dataset_title", "url"]],
        column_config={
            "status": st.column_config.TextColumn("Status", width="small"),
            "Date": st.column_config.TextColumn("Date", width="small"),
            "source": st.column_config.TextColumn("Owner", width="small"),
            "dataset_title": st.column_config.TextColumn("Dataset Brief", width="large"),
            "url": st.column_config.LinkColumn("Link", display_text="Open Source"),
        },
        hide_index=True,
        use_container_width=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
