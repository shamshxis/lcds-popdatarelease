import json
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="LCDS Executive Watch", 
    page_icon="🛡️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for a professional report look
st.markdown("""
<style>
    .reportview-container { background: #f4f6f9; }
    .main { padding: 1rem 2rem; }
    h1, h2, h3 { color: #1e293b; font-family: 'Helvetica Neue', sans-serif; }
    h3 { border-bottom: 2px solid #e2e8f0; padding-bottom: 5px; margin-top: 30px; }
    div[data-testid="stMetricValue"] { color: #0f172a; }
    .stDataFrame { border: none !important; }
</style>
""", unsafe_allow_html=True)

DATA_DIR = Path("data")
DATA_FILE = DATA_DIR / "dataset_tracker.csv"
RUNLOG_FILE = DATA_DIR / "run_log.json"

# --- DATA PROCESSING ---

@st.cache_data(ttl=60)
def load_metrics():
    if not RUNLOG_FILE.exists():
        return {}
    try:
        with open(RUNLOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("metrics", {})
    except Exception:
        return {}

@st.cache_data(ttl=60)
def load_data():
    if not DATA_FILE.exists():
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(DATA_FILE).fillna("")
    except Exception:
        return pd.DataFrame()
    
    if df.empty: return df

    # Parse Action Date to create Month Groupings
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    
    # Format Month. If no date (e.g. TBC/Monitor), group under "Unscheduled / TBC"
    df["Month"] = df["dt"].apply(lambda x: x.strftime("%B %Y") if pd.notnull(x) else "Unscheduled / TBC")
    
    # Visual Status mapping
    status_icons = {
        "Deleted": "🛑 Deleted",
        "Cancelled": "❌ Cancelled",
        "Rescheduled": "🔄 Rescheduled",
        "Restricted": "🔒 Restricted",
        "Upcoming": "📅 Upcoming",
        "Published": "✅ Published",
        "Announcement": "📢 Announcement",
        "Monitor": "👁️ Monitor"
    }
    df["Status_Badge"] = df["status"].map(status_icons).fillna(df["status"])
    
    # Fallback to Title if Summary is empty
    df["Brief"] = df.apply(lambda row: row["summary"] if len(str(row.get("summary", ""))) > 5 else row["dataset_title"], axis=1)

    # Clean display date
    df["Date"] = df["display_date"]

    return df

# --- UI ---

st.title("🛡️ LCDS Executive Watch")
st.markdown("Global Demographics & Population Data Intelligence")

# --- EXECUTIVE METRICS ---
metrics = load_metrics()
if metrics:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Datasets Tracked", metrics.get("records", 0))
    m2.metric("Upcoming (Next 14 Days)", metrics.get("next_14_days", 0))
    m3.metric("Red Flags / Restrictions", metrics.get("red_flags", 0))
    m4.metric("Deletions Detected", metrics.get("deletions", 0))
    st.markdown("---")

df = load_data()

if df.empty:
    st.info("No data available yet. Ensure the new LCDS Scraper has run successfully.")
    st.stop()

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filter Intelligence")
    
    # Quick Views
    exec_only = st.checkbox("🚩 Show Executive Red Flags Only", value=False)
    
    all_groups = ["All"] + sorted(df["source_group"].unique().tolist())
    sel_group = st.selectbox("Region / Group", all_controllers=all_groups)
    
    all_sources = ["All"] + sorted(df["source"].unique().tolist())
    sel_source = st.selectbox("Data Controller", all_sources)
    
    all_themes = ["All"] + sorted(df["theme_primary"].unique().tolist())
    sel_theme = st.selectbox("Primary Theme", all_themes)
    
    search_q = st.text_input("Keyword Search", placeholder="e.g. Migration")

# --- FILTER LOGIC ---
view = df.copy()

if exec_only: view = view[view["executive_flag"] == 1]
if sel_group != "All": view = view[view["source_group"] == sel_group]
if sel_source != "All": view = view[view["source"] == sel_source]
if sel_theme != "All": view = view[view["theme_primary"] == sel_theme]

if search_q:
    mask = (
        view["dataset_title"].str.contains(search_q, case=False) |
        view["Brief"].str.contains(search_q, case=False) |
        view["tags"].str.contains(search_q, case=False)
    )
    view = view[mask]

# --- RENDER MONTHLY TABLES ---
if view.empty:
    st.warning("No records match the current intelligence filters.")
else:
    # Sort months (handle Unscheduled separately)
    def sort_month(m_str):
        if m_str == "Unscheduled / TBC": return datetime.max
        try: return datetime.strptime(m_str, "%B %Y")
        except: return datetime.max
        
    display_months = sorted(view["Month"].unique(), key=sort_month)
    
    for month in display_months:
        st.subheader(f"🗓️ {month}")
        month_data = view[view["Month"] == month]
        
        # Prepare table view
        table_data = month_data[["source", "Status_Badge", "Date", "theme_primary", "dataset_title", "url"]].copy()
        
        st.dataframe(
            table_data,
            column_config={
                "source": st.column_config.TextColumn("Controller", width="medium"),
                "Status_Badge": st.column_config.TextColumn("Status", width="small"),
                "Date": st.column_config.TextColumn("Date", width="small"),
                "theme_primary": st.column_config.TextColumn("Theme", width="small"),
                "dataset_title": st.column_config.TextColumn("Dataset Brief", width="large"),
                "url": st.column_config.LinkColumn("Source", display_text="Open 🔗"),
            },
            hide_index=True
        )

# --- DOWNLOAD ---
st.download_button(
    "📥 Download Full Intelligence Report (CSV)",
    view.to_csv(index=False).encode('utf-8'),
    f"lcds_executive_watch_{datetime.now().strftime('%Y%m%d')}.csv",
    "text/csv"
)
