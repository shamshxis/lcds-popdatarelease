import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="Population Data Brief", 
    page_icon="📋", 
    layout="wide"
)

# Custom CSS for clean headers
st.markdown("""
<style>
    h1 { margin-bottom: 0px; }
    div[data-testid="stStatusWidget"] { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

DATA_FILE = Path("data/dataset_tracker.csv")

CONTROLLER_MAP = {
    "ONS Population Releases": "ONS",
    "ONS Migration Releases": "ONS",
    "US Census Upcoming Releases": "US Census",
    "Eurostat Release Calendar": "Eurostat",
    "DHS Available Datasets": "DHS",
    "Statistics Sweden Population Statistics": "SCB (SE)",
    "Statistics Norway Population": "SSB (NO)",
    "Statistics Finland Population": "StatFi",
    "Statistics Denmark Scheduled Releases": "DST (DK)"
}

def load_data():
    if not DATA_FILE.exists(): return pd.DataFrame()
    
    df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    if df.empty: return df

    # Formatting
    df["Controller"] = df["source"].map(CONTROLLER_MAP).fillna(df["source"])
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"])
    
    df["Date"] = df["dt"].dt.strftime("%d %b %Y")
    df["Month"] = df["dt"].dt.strftime("%B %Y")
    
    # Ensure Status is clean for the badge
    # Maps "✅ Published" -> "Published" for the column config
    df["Status_Clean"] = df["status"].str.replace("✅ ", "").str.replace("📅 ", "").str.replace("⚠️ ", "")
    
    return df.sort_values(by=["dt", "Controller"])

# --- UI ---

st.title("📋 Management Data Brief")
st.markdown("Schedule of population data releases.")

df = load_data()

if df.empty:
    st.info("No data available.")
    st.stop()

# --- FILTERS ---
with st.sidebar:
    st.header("Filters")
    sel_cont = st.selectbox("Controller", ["All"] + sorted(df["Controller"].unique().tolist()))
    sel_month = st.selectbox("Month", ["All"] + list(df["Month"].unique()))
    search = st.text_input("Search", "")

view = df.copy()
if sel_cont != "All": view = view[view["Controller"] == sel_cont]
if sel_month != "All": view = view[view["Month"] == sel_month]
if search: view = view[view["dataset_title"].str.contains(search, case=False)]

# --- TABLE ---
# Group by Month if "All" months are selected
months = view["Month"].unique() if sel_month == "All" else [sel_month]

for month in months:
    if month == "All": continue
    
    st.subheader(month)
    m_data = view[view["Month"] == month]
    
    # Display columns
    display = m_data[["Status_Clean", "Date", "Controller", "dataset_title", "url"]]
    
    st.dataframe(
        display,
        column_config={
            "Status_Clean": st.column_config.Column(
                "Status",
                width="small",
                help="Current status of the dataset",
            ),
            "Date": st.column_config.TextColumn("Release Date", width="small"),
            "Controller": st.column_config.TextColumn("Owner", width="small"),
            "dataset_title": st.column_config.TextColumn("Dataset Brief", width="large"),
            "url": st.column_config.LinkColumn("Link", display_text="Open Source"),
        },
        hide_index=True,
        use_container_width=True
    )
    st.markdown("<br>", unsafe_allow_html=True)
