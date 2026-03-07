import streamlit as st
import pandas as pd
import os
import time

st.set_page_config(layout="wide", page_title="LCDS Global Pulse", page_icon="📡")

# --- CSS (News Room Style) ---
st.markdown("""
<style>
    /* Card Styling */
    .news-card {
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 15px;
        border-left: 5px solid #444;
        background-color: #262730;
    }
    .news-card.today {
        border-left: 5px solid #FFD700; /* Gold for Today */
        background-color: rgba(255, 215, 0, 0.05);
    }
    
    /* Typography */
    .news-date { font-size: 0.85em; color: #aaa; font-family: monospace; }
    .news-source { font-weight: bold; font-size: 0.9em; text-transform: uppercase; }
    .news-title { font-size: 1.1em; font-weight: 600; margin: 5px 0; color: #fff; }
    .news-summary { font-size: 0.95em; color: #ddd; }
    
    /* Badges */
    .badge { padding: 2px 8px; border-radius: 4px; font-size: 0.75em; color: white; margin-right: 5px; }
</style>
""", unsafe_allow_html=True)

st.title("📡 LCDS Global Pulse")
st.caption("Real-Time Data Intelligence: ONS, Eurostat, DHS, INSEE, Statice")

DATA_FILE = "data/releases.json"

# --- SIDEBAR ---
with st.sidebar:
    if st.button("🔄 Refresh Live Feeds", type="primary"):
        with st.spinner("Connecting to Government RSS Feeds..."):
            os.system("python scraper.py")
            st.cache_data.clear()
            st.rerun()
            
    st.divider()
    st.markdown("**Active Sources:**")
    st.markdown("""
    - 🇬🇧 ONS (Release Calendar)
    - 🇪🇺 Eurostat (Data Updates)
    - 🇫🇷 INSEE (Press)
    - 🇮🇸 Statice (News)
    - 🇫🇮 FinData (Health)
    - 🌍 USAID/DHS (Program News)
    """)

# --- LOAD DATA ---
if not os.path.exists(DATA_FILE):
    st.warning("⚠️ Feed is empty. initializing...")
    os.system("python scraper.py")
    st.rerun()

try:
    df = pd.read_json(DATA_FILE)
    if df.empty: raise ValueError
except:
    st.error("Feed error. Re-running...")
    os.system("python scraper.py")
    st.rerun()

# --- METRICS ROW ---
today_count = df[df['is_new'] == True].shape[0] if 'is_new' in df.columns else 0 # legacy check
today_count = df[df['is_today'] == True].shape[0]

c1, c2, c3 = st.columns(3)
c1.metric("Total Updates (30d)", len(df))
c2.metric("Released Today", today_count)
c3.metric("Latest Source", df.iloc[0]['source'] if not df.empty else "-")

st.divider()

# --- NEWS FEED RENDER ---
for idx, row in df.iterrows():
    # Card Class (Highlight if Today)
    card_class = "news-card today" if row['is_today'] else "news-card"
    
    st.markdown(f"""
    <div class="{card_class}">
        <div style="display:flex; justify-content:space-between;">
            <span class="news-source" style="color:{row['color']}">{row['source']} • {row['country']}</span>
            <span class="news-date">{row['date']}</span>
        </div>
        <div class="news-title">{row['title']}</div>
        <div class="news-summary">{row['summary']}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Action Button (Streamlit button must be outside HTML block)
    col1, col2 = st.columns([0.1, 0.9])
    col1.link_button("🔗 Read", row['url'])
    st.write("") # Spacer
