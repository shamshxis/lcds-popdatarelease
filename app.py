import pandas as pd
import streamlit as st
from pathlib import Path

# --- CONFIG ---
st.set_page_config(page_title="Data Management Brief", page_icon="📋", layout="wide")
DATA_FILE = Path("data/dataset_tracker.csv")

# --- MAPPINGS ---
CONTROLLER_MAP = {
    "ONS Population Releases": "ONS",
    "ONS Migration Releases": "ONS",
    "US Census Upcoming Releases": "US Census",
    "Eurostat Release Calendar": "Eurostat",
    "DHS Available Datasets": "DHS",
    "Statistics Sweden Population Statistics": "SCB (Sweden)",
    "Statistics Norway Population": "SSB (Norway)",
    "Statistics Finland Population": "StatFi",
    "Statistics Denmark Scheduled Releases": "DST (Denmark)"
}

def load_data():
    if not DATA_FILE.exists(): return pd.DataFrame()
    
    df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    
    # Formatting
    df["Controller"] = df["source"].map(CONTROLLER_MAP).fillna(df["source"])
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"])
    df["Date"] = df["dt"].dt.strftime("%d %b %y")
    df["Month"] = df["dt"].dt.strftime("%B %Y")
    
    # Clean Action
    df["Action"] = df["status"].apply(lambda x: "⚠️ Delete" if "Remov" in x else "🚀 Release")
    
    return df.sort_values(by=["dt", "Controller"])

# --- UI ---
st.title("📋 Management Data Brief")
st.markdown("##### Upcoming Data Releases & Deletions")

df = load_data()
if df.empty:
    st.warning("No data found. Run the scraper.")
    st.stop()

# --- FILTERS ---
c1, c2 = st.columns(2)
with c1:
    sel_cont = st.selectbox("Controller", ["All"] + sorted(df["Controller"].unique().tolist()))
with c2:
    search = st.text_input("Search", "")

view = df.copy()
if sel_cont != "All": view = view[view["Controller"] == sel_cont]
if search: view = view[view["dataset_title"].str.contains(search, case=False)]

# --- RENDER TABLE ---
months = view["Month"].unique()

for month in months:
    st.markdown(f"**{month}**")
    m_data = view[view["Month"] == month]
    
    # Clean Table
    st.dataframe(
        m_data[["Controller", "Action", "Date", "dataset_title", "url"]],
        column_config={
            "url": st.column_config.LinkColumn("Link", display_text="Open"),
            "dataset_title": st.column_config.TextColumn("Dataset / Brief", width="large"),
            "Action": st.column_config.TextColumn("Action", width="small"),
            "Controller": st.column_config.TextColumn("Controller", width="small"),
            "Date": st.column_config.TextColumn("Date", width="small"),
        },
        hide_index=True,
        use_container_width=True
    )
