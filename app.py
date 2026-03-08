import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(page_title="Data Release Tracker", page_icon="📉", layout="wide")

DATA_FILE = Path("data/dataset_tracker.csv")

# --- LOAD DATA ---
def load_data():
    if not DATA_FILE.exists():
        return pd.DataFrame()
    
    df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    
    # Sort: Upcoming dates first
    df["date_sort"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.sort_values(by="date_sort", ascending=True)
    
    return df

# --- MAIN ---
df = load_data()

st.title("📉 Population Data Releases")
st.markdown(f"**Current Date:** {datetime.now().strftime('%d %B %Y')}")

if df.empty:
    st.info("No data found. Run the scraper.")
    st.stop()

# --- FILTERS ---
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    # Filter by Source
    sources = ["All"] + sorted(list(set(df["source"])))
    sel_source = st.selectbox("Filter by Source", sources)

with col2:
    # Filter for History
    show_history = st.checkbox("Show Past Releases", value=False)

# Apply Filters
view = df.copy()
today = pd.Timestamp.now()

# Time Filter
if not show_history:
    view = view[view["date_sort"] >= today]

if sel_source != "All":
    view = view[view["source"] == sel_source]

# --- DISPLAY AS TABLE ---
st.markdown(f"**Showing {len(view)} releases**")

for index, row in view.iterrows():
    
    # Color code
    status_color = "blue"
    if "Remove" in row['status']: status_color = "red"
    if "Release" in row['status']: status_color = "green"
    
    c1, c2, c3, c4 = st.columns([2, 2, 6, 1])
    
    with c1:
        st.write(f"**{row['action_date']}**")
    
    with c2:
        st.markdown(f":{status_color}[{row['status']}]")
        
    with c3:
        clean_summary = row['summary'].replace(row['dataset_title'], "").strip()
        st.markdown(f"**{row['dataset_title']}**")
        if clean_summary:
            st.caption(clean_summary[:200] + "..." if len(clean_summary) > 200 else clean_summary)
            
    with c4:
        st.markdown(f"[Link]({row['url']})")
    
    st.divider()
