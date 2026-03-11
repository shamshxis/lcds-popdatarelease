import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
st.set_page_config(
    page_title="LCDS Intelligence Feed", 
    page_icon="🛡️", 
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_DIR = Path("data")
DATA_FILE = DATA_DIR / "dataset_tracker.csv"

# --- DATA LOADING & CLEANING ---
@st.cache_data(ttl=60)
def load_data():
    if not DATA_FILE.exists(): 
        return pd.DataFrame()
    
    try: 
        df = pd.read_csv(DATA_FILE).fillna("")
    except Exception: 
        return pd.DataFrame()
    
    if df.empty: 
        return df

    # Parse dates for sorting and grouping
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    
    def get_month_sort(d):
        if pd.isnull(d): return "9999-12", "Date TBC"
        return d.strftime("%Y-%m"), d.strftime("%B %Y")
        
    month_info = df["dt"].apply(get_month_sort)
    df["month_sort"] = [x[0] for x in month_info]
    df["Month"] = [x[1] for x in month_info]

    # Clean up abstracts
    def refine_abstract(row):
        t, s = str(row['dataset_title']).strip(), str(row.get('summary', '')).strip()
        if s.lower().startswith(t.lower()): 
            s = s[len(t):].strip(" -:|")
        return s if s and len(s) >= 5 else "No abstract provided."
    df["Abstract"] = df.apply(refine_abstract, axis=1)
    
    # Status Icons
    status_icons = {
        "Deleted": "🛑 Deleted", "Cancelled": "❌ Cancelled", "Rescheduled": "🔄 Rescheduled",
        "Restricted": "🔒 Restricted", "Upcoming": "📅 Upcoming", "Published": "✅ Published",
        "Announcement": "📢 Announcement", "Monitor": "👁️ Monitor"
    }
    df["Status"] = df["status"].map(status_icons).fillna(df["status"])

    # Ensure numeric columns
    for col in ["academic_match", "executive_flag", "priority_score", "days_to_event", "red_flag"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Title Flags
    df["Title"] = df["dataset_title"].apply(lambda x: str(x).strip())
    df["Title"] = df.apply(lambda r: f"⚠️ {r['Title']}" if r.get("red_flag", 0) == 1 else r['Title'], axis=1)
    df["Title"] = df.apply(lambda r: f"🧬 {r['Title']}" if r.get("academic_match", 0) == 1 else r['Title'], axis=1)

    return df

df = load_data()

if df.empty:
    st.info("📡 Awaiting data... Please let the scraper run its first cycle.")
    st.stop()

# --- SIDEBAR FILTERS ---
with st.sidebar:
    st.header("🔍 Intelligence Filters")
    
    search_q = st.text_input("Keyword Search", placeholder="e.g. Migration, Biobank...")
    
    all_groups = ["All"] + sorted([x for x in df["source_group"].unique() if x])
    sel_group = st.selectbox("Region / Group", options=all_groups)
    
    if sel_group != "All":
        filtered_sources = df[df["source_group"] == sel_group]["source"].unique()
    else:
        filtered_sources = df["source"].unique()
        
    all_sources = ["All"] + sorted([x for x in filtered_sources if x])
    sel_source = st.selectbox("Publisher", options=all_sources)
    
    all_themes = ["All"] + sorted([x for x in df["theme_primary"].unique() if x])
    sel_theme = st.selectbox("Primary Theme", options=all_themes)
    
    st.divider()
    st.caption("Data provided by Leverhulme Centre for Demographic Science")

# --- FILTER LOGIC ---
view = df.copy()

if sel_group != "All": view = view[view["source_group"] == sel_group]
if sel_source != "All": view = view[view["source"] == sel_source]
if sel_theme != "All": view = view[view["theme_primary"] == sel_theme]
if search_q:
    mask = (
        view["dataset_title"].str.contains(search_q, case=False, na=False) | 
        view["Abstract"].str.contains(search_q, case=False, na=False) | 
        view["tags"].str.contains(search_q, case=False, na=False)
    )
    view = view[mask]

# --- MAIN HEADER ---
st.title("🛡️ LCDS Executive Watch")
st.markdown("Automated intelligence feed for population data, demographic surveys, and biobank releases.")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Tracked", len(view))
col2.metric("Upcoming (Next 30 Days)", len(view[(view["days_to_event"] >= 0) & (view["days_to_event"] <= 30)]))
col3.metric("Academic Matches", len(view[view["academic_match"] == 1]))
col4.metric("High Priority Alerts", len(view[view["executive_flag"] == 1]))

st.divider()

if view.empty:
    st.warning("No records match the current filters.")
    st.stop()

tab1, tab2 = st.tabs(["📅 Release Calendar (Compact View)", "📊 Analytics & Deep Dive"])

# --- TAB 1: COMPACT CALENDAR VIEW ---
with tab1:
    st.caption("Grouped by month. Upcoming releases are shown first. Click a table header to sort, or hover over text to read full details.")
    
    months = sorted(view["month_sort"].unique())
    current_ym = datetime.now().strftime("%Y-%m")
    
    # Sort months logically: Upcoming/Current -> Future -> Past -> TBC
    future_months = [m for m in months if m >= current_ym and m != "9999-12"]
    past_months = sorted([m for m in months if m < current_ym], reverse=True)
    tbc_months = [m for m in months if m == "9999-12"]
    
    ordered_months = future_months + past_months + tbc_months

    # Determine which months should be expanded by default (Current and Next month)
    months_to_expand = future_months[:2] if future_months else []

    for ym in ordered_months:
        month_df = view[view["month_sort"] == ym].sort_values(["dt", "sort_rank"], ascending=[True, False])
        if month_df.empty: continue
        
        month_label = month_df["Month"].iloc[0]
        is_expanded = (ym in months_to_expand) or (ym == "9999-12" and not future_months)
        
        with st.expander(f"🗓️ {month_label} ({len(month_df)} releases)", expanded=is_expanded):
            
            display_cols = ["Title", "source", "theme_primary", "display_date", "Status", "url"]
            table_view = month_df[display_cols].copy()
            
            st.dataframe(
                table_view,
                column_config={
                    "Title": st.column_config.TextColumn("Dataset Headline", width="large"),
                    "source": st.column_config.TextColumn("Publisher", width="medium"),
                    "theme_primary": st.column_config.TextColumn("Theme", width="small"),
                    "display_date": st.column_config.TextColumn("Date", width="small"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "url": st.column_config.LinkColumn("Link", display_text="Open 🔗", width="small"),
                },
                hide_index=True,
                use_container_width=True
            )

# --- TAB 2: ANALYTICS & EXPORT ---
with tab2:
    st.subheader("Intelligence Analytics")
    
    c1, c2 = st.columns(2)
    with c1:
        theme_counts = view["theme_primary"].value_counts().reset_index()
        theme_counts.columns = ["Theme", "Count"]
        fig_donut = px.pie(theme_counts, values='Count', names='Theme', hole=0.5, title="Thematic Focus", color_discrete_sequence=px.colors.qualitative.Prism)
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        fig_donut.update_layout(showlegend=False)
        st.plotly_chart(fig_donut, use_container_width=True)

    with c2:
        source_counts = view["source"].value_counts().head(8).reset_index()
        source_counts.columns = ["Publisher", "Count"]
        fig_bar = px.bar(source_counts, x="Count", y="Publisher", orientation='h', title="Top Publishers", color_discrete_sequence=["#1f77b4"])
        fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()
    st.subheader("Export Filtered Data")
    st.caption("Download the raw data behind your current filter view.")
    
    export_df = view[["dataset_title", "source", "source_group", "action_date", "status", "theme_primary", "summary", "url"]].copy()
    
    st.download_button(
        label="📥 Download Current View (CSV)",
        data=export_df.to_csv(index=False).encode('utf-8'),
        file_name=f"lcds_intelligence_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        type="primary"
    )
