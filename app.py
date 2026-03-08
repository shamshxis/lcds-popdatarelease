import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="Semantic Data Brief", 
    page_icon="🧠", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a professional "Management Report" look
st.markdown("""
<style>
    .reportview-container { background: #f0f2f6 }
    .main { background: #ffffff; padding: 2rem; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
    h1 { color: #2c3e50; font-family: 'Helvetica Neue', sans-serif; }
    h3 { color: #34495e; border-bottom: 2px solid #ecf0f1; padding-bottom: 10px; margin-top: 20px; }
    .stDataFrame { border: none !important; }
</style>
""", unsafe_allow_html=True)

DATA_FILE = Path("data/dataset_tracker.csv")

# --- MAPPINGS ---
# Maps long scraper names to short, punchy codes for the table
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
    if not DATA_FILE.exists():
        return pd.DataFrame()
    
    # Load data
    df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    
    if df.empty:
        return df

    # 1. Controller: Map raw source names to short codes
    df["Controller"] = df["source"].map(CONTROLLER_MAP).fillna(df["source"])
    
    # 2. Date Handling: Parse ISO dates from scraper
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"]) # Drop rows with invalid dates
    
    # Create Display Columns
    df["Date"] = df["dt"].dt.strftime("%d %b %y") # "08 Mar 26"
    df["Month"] = df["dt"].dt.strftime("%B %Y")   # "March 2026"
    
    # 3. Action Tag: Simple "Release" vs "Delete" logic
    # The Semantic scraper handles status, but we format it here for the UI
    df["Action"] = df["status"].apply(lambda x: "⚠️ Delete" if "Remov" in str(x) else "🚀 Release")
    
    # 4. Summary Cleanup: Ensure we don't have empty summaries
    # Use title as fallback if summary is missing
    df["Brief"] = df.apply(lambda row: row["dataset_title"] if len(str(row.get("summary", ""))) < 5 else row["summary"], axis=1)
    
    # 5. Sorting: Chronological by default
    df = df.sort_values(by=["dt", "Controller"])
    
    return df

# --- MAIN UI ---

st.title("🧠 Semantic Data Brief")
st.markdown("""
**AI-Curated Schedule of Population Data.** *Filtered for relevance using Semantic Vector Analysis (all-MiniLM-L6-v2).*
""")

df = load_data()

if df.empty:
    st.info("ℹ️ No relevant data found. The AI filter might be too strict, or the scraper hasn't run yet.")
    st.stop()

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filter Brief")
    
    # Source Filter
    all_controllers = ["All"] + sorted(df["Controller"].unique().tolist())
    sel_cont = st.selectbox("By Controller", all_controllers)
    
    # Month Filter
    # Sort months chronologically
    all_months = ["All"] + sorted(df["Month"].unique().tolist(), key=lambda m: datetime.strptime(m, "%B %Y"))
    sel_month = st.selectbox("By Month", all_months)
    
    # Search
    search_q = st.text_input("Keyword Search", placeholder="e.g. Migration")
    
    st.markdown("---")
    st.caption(f"**Total Records:** {len(df)}")
    st.caption(f"**Last Scrape:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- FILTER LOGIC ---
view = df.copy()

if sel_cont != "All":
    view = view[view["Controller"] == sel_cont]

if sel_month != "All":
    view = view[view["Month"] == sel_month]

if search_q:
    # Search in Title, Brief, or Controller
    mask = (
        view["dataset_title"].str.contains(search_q, case=False) |
        view["Brief"].str.contains(search_q, case=False) |
        view["Controller"].str.contains(search_q, case=False)
    )
    view = view[mask]

# --- RENDER MANAGEMENT TABLES ---

# Group by Month for the main view
months = view["Month"].unique()

for month in months:
    st.subheader(f"📅 {month}")
    
    month_data = view[view["Month"] == month]
    
    # Construct the exact table for management
    display_table = month_data[["Controller", "Action", "Date", "Brief", "url"]].copy()
    
    # Rename for display
    display_table.columns = ["Controller", "Action", "Date", "Summary / Brief", "Link"]
    
    st.dataframe(
        display_table,
        column_config={
            "Link": st.column_config.LinkColumn("Source", display_text="Open"),
            "Summary / Brief": st.column_config.TextColumn("Dataset Brief", width="large"),
            "Action": st.column_config.TextColumn("Status", width="small"),
            "Controller": st.column_config.TextColumn("Owner", width="small"),
            "Date": st.column_config.TextColumn("Date", width="small"),
        },
        hide_index=True,
        use_container_width=True
    )
    
    st.markdown("<br>", unsafe_allow_html=True) # Spacer

# --- DOWNLOAD ---
csv = view.to_csv(index=False).encode('utf-8')
st.download_button(
    "📥 Download Brief as CSV",
    csv,
    "semantic_data_brief.csv",
    "text/csv",
    key='download-csv'
)
