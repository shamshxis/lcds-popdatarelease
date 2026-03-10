import json
import pandas as pd
import streamlit as st
import plotly.express as px
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="LCDS Executive Watch", 
    page_icon="🛡️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { padding: 1rem 2rem; }
    h1, h2, h3, h4, h5, h6 { font-family: 'Inter', 'Helvetica Neue', sans-serif; }
    .stDataFrame { border: none !important; }
    div[data-testid="stMetricValue"] { font-weight: 700; color: #0f172a; }
    .stTabs [data-baseweb="tab-list"] { gap: 2rem; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: transparent; border-radius: 4px 4px 0 0; gap: 1px; padding-top: 10px; padding-bottom: 10px; }
    .stTabs [aria-selected="true"] { background-color: #f8fafc; border-bottom: 2px solid #3b82f6; }
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
    
    df["Title"] = df["dataset_title"].apply(lambda x: str(x).strip())
    df["Title"] = df.apply(lambda r: f"🧬 {r['Title']}" if r.get("academic_match", 0) == 1 else r['Title'], axis=1)
    
    def refine_abstract(row):
        t, s = str(row['dataset_title']).strip(), str(row.get('summary', '')).strip()
        if s.lower().startswith(t.lower()): s = s[len(t):].strip(" -:|")
        return s if s and len(s) >= 5 else "No additional abstract provided."

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

# --- VISUALIZATIONS ---
if not view.empty:
    with st.expander("📊 View Intelligence Plots", expanded=True):
        c1, c2, c3 = st.columns([2, 1, 1])
        
        with c1:
            # Scatter Timeline of upcoming releases
            time_df = view.dropna(subset=["dt"]).copy()
            # Filter to show only dates within the next 90 days for clarity
            time_df = time_df[(time_df['dt'] >= pd.Timestamp.now()) & (time_df['dt'] <= pd.Timestamp.now() + pd.Timedelta(days=90))]
            if not time_df.empty:
                fig_timeline = px.scatter(
                    time_df, x="dt", y="source_group", color="theme_primary", 
                    hover_data=["Title", "Date"],
                    title="Release Horizon (Next 90 Days)",
                    labels={"dt": "Release Date", "source_group": "Region"}
                )
                fig_timeline.update_traces(marker=dict(size=12, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
                fig_timeline.update_layout(margin=dict(l=20, r=20, t=40, b=20), showlegend=False)
                st.plotly_chart(fig_timeline, use_container_width=True)
            else:
                st.info("No upcoming dates in the next 90 days for current filter.")

        with c2:
            # Thematic Distribution Donut
            theme_counts = view["theme_primary"].value_counts().reset_index()
            theme_counts.columns = ["Theme", "Count"]
            fig_donut = px.pie(
                theme_counts, values='Count', names='Theme', hole=0.6,
                title="Thematic Focus"
            )
            fig_donut.update_traces(textposition='inside', textinfo='percent')
            fig_donut.update_layout(margin=dict(l=20, r=20, t=40, b=20), showlegend=False)
            st.plotly_chart(fig_donut, use_container_width=True)

        with c3:
            # Top Sources Bar Chart
            source_counts = view["source"].value_counts().head(5).reset_index()
            source_counts.columns = ["Source", "Count"]
            fig_bar = px.bar(
                source_counts, x="Count", y="Source", orientation='h',
                title="Top Publishing Sources"
            )
            fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_bar, use_container_width=True)

# --- RENDER TABS ---
def render_table(data_view):
    display_months = sorted(data_view["Month"].unique(), key=lambda m: datetime.max if m == "Unscheduled / TBC" else datetime.strptime(m, "%B %Y"))
    for month in display_months:
        st.subheader(f"🗓️ {month}")
        month_data = data_view[data_view["Month"] == month]
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
