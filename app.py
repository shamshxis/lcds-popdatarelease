import pandas as pd
import streamlit as st
from pathlib import Path

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
st.markdown("One-line summary of upcoming releases, deletions, and updates.")

if df.empty:
    st.info("No data found. Run the scraper.")
    st.stop()

# --- FILTERS ---
col1, col2 = st.columns([1, 3])
with col1:
    # Filter by Status (Type)
    types = ["All"] + sorted(list(set(df["status"])))
    sel_type = st.selectbox("Filter by Type", types)
    
    # Filter by Source
    sources = ["All"] + sorted(list(set(df["source"])))
    sel_source = st.selectbox("Filter by Source", sources)

# Apply Filters
view = df.copy()
if sel_type != "All":
    view = view[view["status"] == sel_type]
if sel_source != "All":
    view = view[view["source"] == sel_source]

# --- DISPLAY AS TABLE ---
# We manually construct the table to ensure specific column order and formatting

st.markdown(f"**Showing {len(view)} releases**")

# Loop through rows to create a clean list view
for index, row in view.iterrows():
    
    # Color code the status
    status_color = "blue"
    if "Remove" in row['status']: status_color = "red"
    if "Release" in row['status']: status_color = "green"
    
    # Layout: Date | Status | Title/Summary | Link
    c1, c2, c3, c4 = st.columns([2, 2, 6, 1])
    
    with c1:
        st.write(f"**{row['action_date']}**")
    
    with c2:
        st.markdown(f":{status_color}[{row['status']}]")
        
    with c3:
        # The One Liner
        clean_summary = row['summary'].replace(row['dataset_title'], "").strip()
        st.markdown(f"**{row['dataset_title']}**")
        if clean_summary:
            st.caption(clean_summary)
            
    with c4:
        st.markdown(f"[Link]({row['url']})")
    
    st.divider()
