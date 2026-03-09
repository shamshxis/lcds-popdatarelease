import pandas as pd
import streamlit as st
import os
from datetime import datetime

st.set_page_config(page_title="LCDS Data Watchdog", page_icon="🛡️", layout="wide")

st.markdown("""
<style>
    .status-scheduled { color: #d39e00; font-weight: bold; }
    .status-published { color: #198754; font-weight: bold; }
    .status-announce { color: #0d6efd; font-weight: bold; }
    .cache-tag { font-size: 0.8em; color: #888; font-style: italic; }
</style>
""", unsafe_allow_html=True)

DATA_FILE = "data/dataset_tracker.csv"

# --- SIDEBAR ---
with st.sidebar:
    st.title("🛡️ Data Watchdog")
    if st.button("🔄 Run Smart Scan", type="primary"):
        with st.spinner("Scanning & merging history..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()

# --- LOAD DATA ---
if not os.path.exists(DATA_FILE):
    st.info("⚠️ Initializing database...")
    os.system("python scraper.py")
    st.rerun()

try:
    df = pd.read_csv(DATA_FILE)
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"])
    df = df.sort_values(by="dt", ascending=True)
except:
    st.error("Database error. Rescanning...")
    os.system("python scraper.py")
    st.rerun()

# --- MAIN UI ---
st.subheader("📅 Population Data Release Schedule")

# Metrics
upcoming = len(df[df['dt'] >= datetime.now()])
st.metric("Upcoming Releases", upcoming, f"Total Assets: {len(df)}")

# Table
st.dataframe(
    df[["status", "action_date", "source", "dataset_title", "url", "last_checked"]],
    column_config={
        "status": st.column_config.TextColumn("Status", width="small"),
        "action_date": st.column_config.DateColumn("Date", format="DD MMM YYYY"),
        "source": st.column_config.TextColumn("Owner"),
        "dataset_title": st.column_config.TextColumn("Dataset", width="large"),
        "url": st.column_config.LinkColumn("Link"),
        "last_checked": st.column_config.TextColumn("Verified", width="small")
    },
    hide_index=True,
    use_container_width=True
)
