import pandas as pd
import streamlit as st
from pathlib import Path

# --- CONFIG ---
st.set_page_config(page_title="Data Management View", page_icon="📋", layout="wide")

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

# --- DATA PROCESSING ---
def load_data():
    if not DATA_FILE.exists(): return pd.DataFrame()
    
    df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    
    # 1. Create "Controller" column
    df["Controller"] = df["source"].map(CONTROLLER_MAP).fillna(df["source"])
    
    # 2. Format Date "08 Mar 26"
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"])
    df["Date"] = df["dt"].dt.strftime("%d %b %y")
    df["Month"] = df["dt"].dt.strftime("%B %Y")
    
    # 3. Define "Action"
    df["Action"] = df["status"].apply(lambda x: "⚠️ Delete" if "Remov" in x else "🚀 Release")
    
    # 4. Clean "Brief"
    # If summary is same as title, just use title.
    df["Brief"] = df.apply(lambda row: row["dataset_title"] if len(row["summary"]) < 5 else row["dataset_title"], axis=1)
    
    # Sort
    df = df.sort_values(by=["dt", "Controller"])
    
    return df

# --- UI ---
st.title("📋 Management Data Brief")
st.markdown("Top-level schedule of data releases and deletions.")

df = load_data()

if df.empty:
    st.warning("No data. Run scraper.")
    st.stop()

# --- FILTERS ---
col1, col2 = st.columns(2)
with col1:
    controllers = ["All"] + sorted(df["Controller"].unique().tolist())
    sel_cont = st.selectbox("Filter by Controller", controllers)
with col2:
    search = st.text_input("Search Briefs", "")

view = df.copy()
if sel_cont != "All": view = view[view["Controller"] == sel_cont]
if search: view = view[view["Brief"].str.contains(search, case=False)]

# --- RENDER MONTHLY TABLES ---
months = view["Month"].unique()

for month in months:
    st.subheader(month)
    
    month_data = view[view["Month"] == month]
    
    # Display as a clean Streamlit Table
    # We construct a simplified dataframe for display
    display_table = month_data[["Controller", "Action", "Date", "Brief", "url"]].copy()
    
    # Make URL clickable in standard dataframe using LinkColumn (Streamlit 1.23+)
    st.dataframe(
        display_table,
        column_config={
            "url": st.column_config.LinkColumn("Link", display_text="Open"),
            "Brief": st.column_config.TextColumn("Summary of Brief", width="large"),
            "Action": st.column_config.TextColumn("Action", width="small"),
            "Controller": st.column_config.TextColumn("Controller", width="small"),
            "Date": st.column_config.TextColumn("Date", width="small"),
        },
        hide_index=True,
        use_container_width=True
    )
