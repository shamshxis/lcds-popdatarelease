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

# Custom CSS stripped down to respect Streamlit's native Light/Dark Mode
st.markdown("""
<style>
    .main { padding: 1rem 2rem; }
    h1, h2, h3, h4, h5, h6 { font-family: 'Inter', 'Helvetica Neue', sans-serif; }
    .stDataFrame { border: none !important; }
    /* Enhance metric text slightly for readability in both modes */
    div[data-testid="stMetricValue"] { font-weight: 700; }
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
    
    # Process Title: Add Academic Match Badge
    df["Title"] = df["dataset_title"].apply(lambda x: str(x).strip())
    df["Title"] = df.apply(lambda r: f"🧬 {r['Title']}" if r.get("academic_match", 0) == 1 else r['Title'], axis=1)
    
    # Process Abstract: Clean up redundancies and provide context
    def refine_abstract(row):
        title = str(row['dataset_title']).strip()
        summary = str(row.get('summary', '')).strip()
        
        # If the summary is just repeating the title, strip it out for cleaner reading
        if summary.lower().startswith(title.lower()):
            summary = summary[len(title):].strip(" -:|")
            
        if not summary or len(summary) < 5:
            return "No additional abstract provided."
        return summary

    df["Abstract"] = df.apply(refine_abstract, axis=1)
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
        view["Title"].str.contains(search_q, case=False) |
        view["Abstract"].str.contains(search_q, case=False) |
        view["tags"].str.contains(search_q, case=False)
    )
    view = view[mask]

# --- RENDER TABS ---
def render_table(data_view):
    display_months = sorted(data_view["Month"].unique(), key=lambda m: datetime.max if m == "Unscheduled / TBC" else datetime.strptime(m, "%B %Y"))
    
    for month in display_months:
        st.subheader(f"🗓️ {month}")
        month_data = data_view[data_view["Month"] == month]
        
        # Explicitly order the refined columns
        table_data = month_data[["source", "Status_Badge", "Date", "theme_primary", "Title", "Abstract", "url"]].copy()
        
        st.dataframe(
            table_data,
            column_config={
                "source": st.column_config.TextColumn("Source", width="small"),
                "Status_Badge": st.column_config.TextColumn("Status", width="small"),
                "Date": st.column_config.TextColumn("Date", width="small"),
                "theme_primary": st.column_config.TextColumn("Theme", width="small"),
                "Title": st.column_config.TextColumn("Dataset Name", width="medium"),
                "Abstract": st.column_config.TextColumn("Context & Abstract", width="large"),
                "url": st.column_config.LinkColumn("Link", display_text="Open 🔗"),
            },
            hide_index=True
        )

if view.empty:
    st.warning("No records match the current intelligence filters.")
else:
    tab1, tab2, tab3 = st.tabs(["📊 Executive Dashboard", "🔬 Academic Priorities", "🗃️ Full Intelligence DB"])
    
    with tab1:
        st.caption("Urgent updates, red flags, and releases scheduled in the next 14 days.")
        view["executive_flag"] = pd.to_numeric(view["executive_flag"], errors="coerce").fillna(0)
        view["days_to_event"] = pd.to_numeric(view["days_to_event"], errors="coerce").fillna(999)
        
        exec_view = view[(view["executive_flag"] == 1) | ((view["days_to_event"] >= 0) & (view["days_to_event"] <= 14))]
        if exec_view.empty: 
            st.info("No immediate executive alerts.")
        else: 
            render_table(exec_view)
            
    with tab2:
        st.caption("Datasets automatically matched to your team via ORCID Publication History (marked with 🧬).")
        view["academic_match"] = pd.to_numeric(view.get("academic_match", pd.Series([0])), errors="coerce").fillna(0)
        
        acad_view = view[view["academic_match"] == 1]
        if acad_view.empty: 
            st.info("No specific matches found against the ORCID academic profile.")
        else: 
            render_table(acad_view)

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
