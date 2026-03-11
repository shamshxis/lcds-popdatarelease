import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
from datetime import datetime

# --- CONFIG ---
# We rely completely on native Streamlit rendering - no hacky HTML CSS!
st.set_page_config(
    page_title="LCDS Population Data Release Tracker", 
    page_icon="📰", 
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

    # Parse dates for plotting
    df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
    
    # Clean up abstracts so they don't just repeat the title
    def refine_abstract(row):
        t, s = str(row['dataset_title']).strip(), str(row.get('summary', '')).strip()
        if s.lower().startswith(t.lower()): 
            s = s[len(t):].strip(" -:|")
        return s if s and len(s) >= 5 else "No additional abstract provided."

    df["Abstract"] = df.apply(refine_abstract, axis=1)
    
    # Ensure numeric columns are actually numeric
    for col in ["academic_match", "executive_flag", "priority_score", "days_to_event"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            
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
    
    # Dynamically update source dropdown based on group selection
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

if sel_group != "All": 
    view = view[view["source_group"] == sel_group]
if sel_source != "All": 
    view = view[view["source"] == sel_source]
if sel_theme != "All": 
    view = view[view["theme_primary"] == sel_theme]
if search_q:
    mask = (
        view["dataset_title"].str.contains(search_q, case=False, na=False) | 
        view["Abstract"].str.contains(search_q, case=False, na=False) | 
        view["tags"].str.contains(search_q, case=False, na=False)
    )
    view = view[mask]

# --- MAIN HEADER ---
st.title("📰 LCDS Demography Watch")
st.markdown("Automated global intelligence feed for population data, surveys, and biobank releases.")

# Top-level KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Datasets Tracked", len(view))
col2.metric("Upcoming (Next 30 Days)", len(view[(view["days_to_event"] >= 0) & (view["days_to_event"] <= 30)]))
col3.metric("Academic Profile Matches", len(view[view["academic_match"] == 1]))
col4.metric("High Priority Flags", len(view[view["executive_flag"] == 1]))

st.divider()

if view.empty:
    st.warning("No records match the current filters.")
    st.stop()

# --- TABS LAYOUT ---
tab1, tab2, tab3 = st.tabs(["🗞️ News Feed", "📊 Analytics & Charts", "🗃️ Data & Export"])

# --- TAB 1: NEWS FEED (VERTICAL TIMELINE) ---
with tab1:
    st.subheader("Latest Intelligence Feed")
    st.caption(f"Showing top 100 most relevant results from current filters. Scroll to view.")
    
    # Sort logically: high priority, then newest/closest date
    feed_view = view.sort_values(by=["sort_rank", "action_date"], ascending=[False, True]).head(100)
    
    for _, row in feed_view.iterrows():
        # Native Streamlit container creates a clean card look
        with st.container(border=True):
            c_main, c_meta = st.columns([3, 1])
            
            with c_main:
                # Title as a clickable markdown link
                st.markdown(f"#### [{row['dataset_title']}]({row['url']})")
                st.write(row['Abstract'])
                
                # Tags as clean markdown inline text
                tags = [f"*{t.strip()}*" for t in str(row['tags']).split(",") if t.strip()]
                if tags:
                    st.markdown(" ".join(tags))
            
            with c_meta:
                st.caption(f"**Publisher:** {row['source']}")
                st.caption(f"**Theme:** {row['theme_primary']}")
                
                # Status formatting
                status_emoji = "🟢" if row['status'] in ["Published", "Announcement"] else "🟠" if row['status'] == "Upcoming" else "🔴"
                st.caption(f"**Status:** {status_emoji} {row['status']}")
                
                date_str = row['display_date']
                st.caption(f"**Date:** {date_str}")
                
                if row['academic_match'] == 1:
                    st.success("🧬 Academic Match")
                if row['executive_flag'] == 1:
                    st.error("⚠️ High Priority")

# --- TAB 2: ANALYTICS (PLOTLY) ---
with tab2:
    st.subheader("Intelligence Analytics")
    
    chart_cols = st.columns(2)
    
    with chart_cols[0]:
        # Donut Chart for Themes
        theme_counts = view["theme_primary"].value_counts().reset_index()
        theme_counts.columns = ["Theme", "Count"]
        fig_donut = px.pie(
            theme_counts, values='Count', names='Theme', hole=0.5, 
            title="Thematic Focus of Datasets",
            color_discrete_sequence=px.colors.qualitative.Prism
        )
        fig_donut.update_traces(textposition='inside', textinfo='percent+label')
        fig_donut.update_layout(showlegend=False)
        st.plotly_chart(fig_donut, use_container_width=True)

    with chart_cols[1]:
        # Bar Chart for Top Publishers
        source_counts = view["source"].value_counts().head(8).reset_index()
        source_counts.columns = ["Publisher", "Count"]
        fig_bar = px.bar(
            source_counts, x="Count", y="Publisher", orientation='h', 
            title="Top Active Publishers",
            color_discrete_sequence=["#1f77b4"]
        )
        fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # Timeline Scatter Plot for upcoming events
    time_df = view.dropna(subset=["dt"]).copy()
    time_df = time_df[(time_df['dt'] >= pd.Timestamp.now() - pd.Timedelta(days=30)) & (time_df['dt'] <= pd.Timestamp.now() + pd.Timedelta(days=120))]
    
    if not time_df.empty:
        fig_timeline = px.scatter(
            time_df, x="dt", y="source", color="theme_primary", 
            hover_data=["dataset_title", "display_date"], 
            title="Release Horizon (Past 30 to Next 120 Days)",
            labels={"dt": "Release Date", "source": "Publisher", "theme_primary": "Theme"},
            color_discrete_sequence=px.colors.qualitative.Prism
        )
        fig_timeline.update_traces(marker=dict(size=12, opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
        st.plotly_chart(fig_timeline, use_container_width=True)
    else:
        st.info("No dated releases found in the immediate 120-day horizon for the current filters.")

# --- TAB 3: DATA EXPLORER & EXPORT ---
with tab3:
    st.subheader("Raw Data Explorer")
    st.caption("View and download the tabular data backing the news feed and charts.")
    
    # Clean up the view for the table
    export_cols = [
        "dataset_title", "source", "source_group", "display_date", 
        "status", "theme_primary", "url"
    ]
    
    table_view = view[export_cols].copy()
    table_view.rename(columns={
        "dataset_title": "Headline", "source": "Publisher", "source_group": "Region",
        "display_date": "Date", "status": "Status", "theme_primary": "Theme", "url": "Link"
    }, inplace=True)
    
    # Display native interactive dataframe
    st.dataframe(
        table_view,
        column_config={"Link": st.column_config.LinkColumn("Link")},
        use_container_width=True,
        hide_index=True
    )
    
    # Generate CSV dynamically based on whatever the user filtered
    csv = view.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download Current View as CSV",
        data=csv,
        file_name=f"lcds_intelligence_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
        type="primary"
    )
