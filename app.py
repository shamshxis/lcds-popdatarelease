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

# Custom CSS for a streamlined, dynamic look
st.markdown("""
<style>
    .reportview-container { background: #f4f6f9; }
    .main { padding: 1rem 2rem; }
    h1, h2, h3 { color: #1e293b; font-family: 'Helvetica Neue', sans-serif; }
    div[data-testid="stMetricValue"] { color: #0f172a; font-weight: 700; }
    .stDataFrame { border: none !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: transparent; border-radius: 4px 4px 0 0; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
    .stTabs [aria-selected="true"] { background-color: #f1f5f9; border-bottom: 2px solid #3b82f6; }
</style>
""", unsafe_allow_html=True)

DATA_DIR = Path("data")
DATA_FILE = DATA_DIR / "dataset_tracker.csv"
RUNLOG_FILE = DATA_DIR / "run_log.json"

# --- DATA PROCESSING ---

@st.cache_data(ttl=60)
def load_metrics():
    if not RUNLOG_FILE.exists(): return {}
    try:
        with open(RUNLOG_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("metrics", {})
    except: return {}

@st.cache_data(ttl=60)
def load_data():
    if not DATA_FILE.exists(): return pd.DataFrame()
    try: df = pd.read_csv(DATA_FILE).fillna("")
    except: return pd.DataFrame()
    
    if df.empty: return df

    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    df["Month"] = df["dt"].apply(lambda x: x.strftime("%B %Y") if pd.notnull(x) else "Unscheduled / TBC")
    
    status_icons = {
        "Deleted": "🛑 Deleted", "Cancelled": "❌ Cancelled", "Rescheduled": "🔄 Rescheduled",
        "Restricted": "🔒 Restricted", "Upcoming": "📅 Upcoming", "Published": "✅ Published",
        "Announcement": "📢 Announcement", "Monitor": "👁️ Monitor"
    }
    df["Status_Badge"] = df["status"].map(status_icons).fillna(df["status"])
    
    # Visual badge for ORCID match
    df["dataset_title"] = df.apply(lambda r: f"🧬 {r['dataset_title']}" if r.get("academic_match", 0) == 1 else r['dataset_title'], axis=1)
    df["Brief"] = df.apply(lambda row: row["summary"] if len(str(row.get("summary", ""))) > 5 else row["dataset_title"], axis=1)
    df["Date"] = df["display_date"]

    return df

# --- UI ---

st.title("🛡️ LCDS Executive Watch")
st.markdown("Global Demographics, Population Data & Biobank Intelligence")

# --- EXECUTIVE METRICS ---
metrics = load_metrics()
if metrics:
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Tracked", metrics.get("records", 0))
    m2.metric("Academic Matches", metrics.get("academic_matches", 0))
    m3.metric("Upcoming (14 Days)", metrics.get("next_14_days", 0))
    m4.metric("Red Flags", metrics.get("red_flags", 0))
    m5.metric("Deletions", metrics.get("deletions", 0))
    st.markdown("---")

df = load_data()

if df.empty:
    st.info("No data available yet. Please wait for the scraper to finish its first run.")
    st.stop()

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Filter Intelligence")
    
    all_groups = ["All"] + sorted(df["source_group"].unique().tolist())
    sel_group = st.selectbox("Region / Group", options=all_groups)
    
    all_sources = ["All"] + sorted(df["source"].unique().tolist())
    sel_source = st.selectbox("Data Controller", options=all_sources)
    
    all_themes = ["All"] + sorted(df["theme_primary"].unique().tolist())
    sel_theme = st.selectbox("Primary Theme", options=all_themes)
    
    search_q = st.text_input("Keyword Search", placeholder="e.g. Migration, Biobank")

# --- FILTER LOGIC ---
view = df.copy()

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

# --- RENDER TABS ---
def render_table(data_view):
    display_months = sorted(data_view["Month"].unique(), key=lambda m: datetime.max if m == "Unscheduled / TBC" else datetime.strptime(m, "%B %Y"))
    for month in display_months:
        st.subheader(f"🗓️ {month}")
        month_data = data_view[data_view["Month"] == month]
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

if view.empty:
    st.warning("No records match the current intelligence filters.")
else:
    tab1, tab2, tab3 = st.tabs(["📊 Executive Dashboard", "🔬 Academic Priorities", "🗃️ Full Intelligence DB"])
    
    with tab1:
        st.caption("Urgent updates, red flags, and releases scheduled in the next 14 days.")
        # Handle mixed types for executive_flag and days_to_event
        view["executive_flag"] = pd.to_numeric(view["executive_flag"], errors="coerce").fillna(0)
        view["days_to_event"] = pd.to_numeric(view["days_to_event"], errors="coerce").fillna(999)
        exec_view = view[(view["executive_flag"] == 1) | ((view["days_to_event"] >= 0) & (view["days_to_event"] <= 14))]
        if exec_view.empty: st.info("No immediate executive alerts.")
        else: render_table(exec_view)
            
    with tab2:
        st.caption("Datasets automatically matched to your team via ORCID Publication History (marked with 🧬).")
        view["academic_match"] = pd.to_numeric(view.get("academic_match", pd.Series([0])), errors="coerce").fillna(0)
        acad_view = view[view["academic_match"] == 1]
        if acad_view.empty: st.info("No specific matches found against the ORCID academic profile.")
        else: render_table(acad_view)

    with tab3:
        st.caption("Comprehensive view of all holistic data sources across all dates.")
        render_table(view)

# --- DOWNLOAD ---
st.markdown("---")
st.download_button(
    "📥 Download Full Intelligence Report (CSV)",
    view.to_csv(index=False).encode('utf-8'),
    f"lcds_executive_watch_{datetime.now().strftime('%Y%m%d')}.csv",
    "text/csv"
)
