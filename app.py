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
    try:
        df = pd.read_csv(DATA_FILE, dtype=str).fillna("")
    except Exception:
        return pd.DataFrame()
    
    if df.empty:
        return df

    # 1. Controller Mapping
    df["Controller"] = df["source"].map(CONTROLLER_MAP).fillna(df["source"])
    
    # 2. Date Handling
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df = df.dropna(subset=["dt"]) 
    
    # Display Columns
    df["Date"] = df["dt"].dt.strftime("%d %b %y") 
    df["Month"] = df["dt"].dt.strftime("%B %Y")   
    
    # 3. Action Tag
    df["Action"] = df["status"].apply(lambda x: "⚠️ Delete" if "Remov" in str(x) else "🚀 Release")
    
    # 4. Summary Cleanup
    df["Brief"] = df.apply(lambda row: row["dataset_title"] if len(str(row.get("summary", ""))) < 5 else row["summary"], axis=1)
    
    return df.sort_values(by=["dt", "Controller"])

# --- MAIN UI ---

st.title("🧠 Semantic Data Brief")
st.markdown("""
**AI-Curated Schedule of Population Data.** *Filtered for relevance using Semantic Vector Analysis (all-MiniLM-L6-v2).*
""")

df = load_data()

if df.empty:
    st.info("ℹ️ No relevant data found. The scraper is running or the AI filter is too strict.")
    st.stop()

# --- SIDEBAR ---
with st.sidebar:
    st.header("🔍 Filter Brief")
    
    # Source Filter
    all_controllers = ["All"] + sorted(df["Controller"].unique().tolist())
    sel_cont = st.selectbox("By Controller", all_controllers)
    
    # Month Filter
    months_list = sorted(df["Month"].unique().tolist(), key=lambda m: datetime.strptime(m, "%B %Y"))
    sel_month = st.selectbox("By Month", ["All"] + months_list)
    
    # Search
    search_q = st.text_input("Keyword Search", placeholder="e.g. Migration")
    
    st.markdown("---")
    st.caption(f"**Total Records:** {len(df)}")
    st.caption(f"**Last Update:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")

# --- FILTER LOGIC ---
view = df.copy()

if sel_cont != "All":
    view = view[view["Controller"] == sel_cont]

if sel_month != "All":
    view = view[view["Month"] == sel_month]

if search_q:
    mask = (
        view["dataset_title"].str.contains(search_q, case=False) |
        view["Brief"].str.contains(search_q, case=False) |
        view["Controller"].str.contains(search_q, case=False)
    )
    view = view[mask]

# --- RENDER TABLES ---
if view.empty:
    st.warning("No records match your filters.")
else:
    # Group by Month
    display_months = view["Month"].unique()
    
    for month in display_months:
        st.subheader(f"📅 {month}")
        month_data = view[view["Month"] == month]
        
        # Prepare Table
        table_data = month_data[["Controller", "Action", "Date", "Brief", "url"]].copy()
        table_data.columns = ["Controller", "Action", "Date", "Summary / Brief", "Link"]
        
        st.dataframe(
            table_data,
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
        st.markdown("<br>", unsafe_allow_html=True)

# --- DOWNLOAD ---
csv = view.to_csv(index=False).encode('utf-8')
st.download_button(
    "📥 Download Brief as CSV",
    csv,
    "semantic_data_brief.csv",
    "text/csv",
    key='download-csv'
)
