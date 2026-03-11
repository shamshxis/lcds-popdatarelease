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

tab1, tab2, tab3 = st.tabs(["📅 Release Calendar", "📊 Analytics & Deep Dive", "🗃️ Raw Data & Export"])

# --- TAB 1: COMPACT CALENDAR VIEW ---
with tab1:
    st.caption("Releases are logically grouped by timeframe. Click a table header to sort, or hover over text to read full details.")
    
    current_ym = datetime.now().strftime("%Y-%m")
    
    # Split into Upcoming/Current, Past, and TBC
    future_mask = (view["month_sort"] >= current_ym) & (view["month_sort"] != "9999-12")
    past_mask = view["month_sort"] < current_ym
    tbc_mask = view["month_sort"] == "9999-12"
    
    future_months = sorted(view[future_mask]["month_sort"].unique())
    past_months = sorted(view[past_mask]["month_sort"].unique(), reverse=True)
    has_tbc = tbc_mask.any()

    display_cols = ["Title", "source", "theme_primary", "display_date", "Status", "url"]
    col_config = {
        "Title": st.column_config.TextColumn("Dataset Headline", width="large"),
        "source": st.column_config.TextColumn("Publisher", width="medium"),
        "theme_primary": st.column_config.TextColumn("Theme", width="small"),
        "display_date": st.column_config.TextColumn("Date", width="small"),
        "Status": st.column_config.TextColumn("Status", width="small"),
        "url": st.column_config.LinkColumn("Link", display_text="Open 🔗", width="small"),
    }

    if future_months:
        st.subheader("🟢 Upcoming & Current Releases")
        for i, ym in enumerate(future_months):
            month_df = view[view["month_sort"] == ym].sort_values(["dt", "sort_rank"], ascending=[True, False])
            month_label = month_df["Month"].iloc[0]
            # Keep current month and next month open by default to save scrolling
            is_expanded = (i < 2)
            
            with st.expander(f"🗓️ {month_label} ({len(month_df)} releases)", expanded=is_expanded):
                st.dataframe(month_df[display_cols], column_config=col_config, hide_index=True, use_container_width=True)

    if past_months:
        st.subheader("🕰️ Past Releases")
        for ym in past_months:
            month_df = view[view["month_sort"] == ym].sort_values(["dt", "sort_rank"], ascending=[False, False])
            month_label = month_df["Month"].iloc[0]
            
            with st.expander(f"🗓️ {month_label} ({len(month_df)} releases)", expanded=False):
                st.dataframe(month_df[display_cols], column_config=col_config, hide_index=True, use_container_width=True)

    if has_tbc:
        st.subheader("⏳ Unscheduled / To Be Confirmed")
        tbc_df = view[tbc_mask].sort_values("sort_rank", ascending=False)
        # Only expand this automatically if there are no future dates at all
        with st.expander(f"🗓️ Date TBC ({len(tbc_df)} releases)", expanded=(not future_months)):
            st.dataframe(tbc_df[display_cols], column_config=col_config, hide_index=True, use_container_width=True)


# --- TAB 2: ANALYTICS & DEEP DIVE ---
with tab2:
    st.subheader("Intelligence Analytics")
    
    # ROW 1: Themes and Publishers
    c1, c2 = st.columns(2)
    with c1:
        theme_counts = view["theme_primary"].value_counts().reset_index()
        theme_counts.columns = ["Theme", "Count"]
        fig_donut = px.pie(theme_counts, values='Count', names='Theme', hole=0.5, title="Thematic Focus", color_discrete_sequence=px.colors.qualitative.Prism)
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        fig_donut.update_layout(showlegend=False, margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_donut, use_container_width=True)

    with c2:
        source_counts = view["source"].value_counts().head(10).reset_index()
        source_counts.columns = ["Publisher", "Count"]
        fig_bar = px.bar(source_counts, x="Count", y="Publisher", orientation='h', title="Top Publishers", color_discrete_sequence=["#1f77b4"])
        fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_bar, use_container_width=True)

    st.divider()
    
    # ROW 2: Timeline & Status
    c3, c4 = st.columns([2, 1])
    with c3:
        # Timeline Histogram
        valid_dates = view[view["month_sort"] != "9999-12"].copy()
        if not valid_dates.empty:
            valid_dates["Month_Fmt"] = valid_dates["dt"].dt.to_period("M").astype(str)
            timeline_counts = valid_dates.groupby(["Month_Fmt", "status"]).size().reset_index(name="Count")
            
            fig_hist = px.bar(
                timeline_counts, x="Month_Fmt", y="Count", color="status",
                title="Release Velocity (Over Time)",
                labels={"Month_Fmt": "Month", "Count": "Number of Releases", "status": "Status"},
                color_discrete_map={
                    "Published": "#2ca02c", "Upcoming": "#ff7f0e", 
                    "Announcement": "#1f77b4", "Cancelled": "#d62728", 
                    "Deleted": "#9467bd", "Rescheduled": "#8c564b"
                }
            )
            fig_hist.update_layout(xaxis_tickangle=-45, barmode='stack', margin=dict(t=40, b=0, l=0, r=0))
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("No dated records available to display timeline.")
            
    with c4:
        # Status Breakdown
        status_counts = view["status"].value_counts().reset_index()
        status_counts.columns = ["Status", "Count"]
        fig_status = px.pie(
            status_counts, values="Count", names="Status", 
            title="Status Breakdown",
            color="Status",
            color_discrete_map={
                "Published": "#2ca02c", "Upcoming": "#ff7f0e", 
                "Announcement": "#1f77b4", "Cancelled": "#d62728", 
                "Deleted": "#9467bd", "Rescheduled": "#8c564b",
                "Monitor": "#7f7f7f", "Restricted": "#e377c2"
            }
        )
        fig_status.update_layout(margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_status, use_container_width=True)

    st.divider()
    
    # ROW 3: Deep Dive Treemap
    st.subheader("Ecosystem Deep Dive")
    st.caption("Click into regions, then publishers, to explore their specific thematic focuses.")
    fig_tree = px.treemap(
        view, 
        path=[px.Constant("Global Overview"), 'source_group', 'source', 'theme_primary'],
        color='theme_primary',
        color_discrete_sequence=px.colors.qualitative.Prism
    )
    fig_tree.update_traces(root_color="lightgrey")
    fig_tree.update_layout(margin=dict(t=20, l=10, r=10, b=10))
    st.plotly_chart(fig_tree, use_container_width=True)

# --- TAB 3: RAW DATA EXPORT ---
with tab3:
    st.subheader("Raw Data & Exports")
    st.caption("View the full, unfiltered dataset behind your current selection and export it.")
    
    export_df = view[["dataset_title", "source", "source_group", "action_date", "status", "theme_primary", "summary", "url"]].copy()
    
    st.download_button(
        label="📥 Download Current View (CSV)",
        data=export_df.to_csv(index=False).encode('utf-8'),
        file_name=f"lcds_intelligence_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        type="primary"
    )
    
    st.dataframe(export_df, use_container_width=True, hide_index=True)
