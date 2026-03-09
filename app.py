import pandas as pd
import streamlit as st
import os
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="LCDS Data Brief", page_icon="📋", layout="wide")

st.markdown("""
<style>
    h1 { margin-bottom: 0px; font-family: 'Helvetica', sans-serif; }
    .status-scheduled { color: #d39e00; font-weight: bold; }
    .status-published { color: #198754; font-weight: bold; }
    .status-announce { color: #0d6efd; font-weight: bold; }
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
    
    return df.sort_values(by=["YearMonth", "dt"], ascending=[False, False])

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
    st.info("Data Sources: ONS, Eurostat, US Census, Human Mortality Database, Scandinavian Registers.")

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
past = view[view['dt'] < datetime.now()]

c1.metric("Total Tracked Assets", len(view))
c2.metric("Upcoming Releases", len(upcoming))
c3.metric("Recent Releases", len(past))

st.divider()

# --- TABLE RENDER (By Month) ---
# Get unique months from the *view*
unique_months = view.sort_values("YearMonth", ascending=False)["Month"].unique()

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
