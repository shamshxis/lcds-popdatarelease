import pandas as pd
import streamlit as st
import os
from datetime import datetime

st.set_page_config(page_title="LCDS Data Watchdog", page_icon="🛡️", layout="wide")
st.title("🛡️ LCDS Data Watchdog")

DATA_FILE = "data/dataset_tracker.csv"

if st.button("🔄 Run Smart Scan"):
    os.system("python scraper.py")
    st.cache_data.clear()
    st.rerun()

if os.path.exists(DATA_FILE):
    try:
        df = pd.read_csv(DATA_FILE)
        df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
        df = df.dropna(subset=["dt"]).sort_values(by="dt")
        
        st.subheader("Upcoming Releases")
        upcoming = df[df['dt'] >= datetime.now()]
        st.dataframe(
            upcoming[["status", "action_date", "source", "dataset_title", "url", "last_checked"]],
            column_config={"url": st.column_config.LinkColumn("Link")},
            hide_index=True, use_container_width=True
        )

        with st.expander("Recent History"):
            past = df[df['dt'] < datetime.now()]
            st.dataframe(past, use_container_width=True)

    except: st.error("Database error.")
else:
    st.info("Initializing...")
    os.system("python scraper.py")
    st.rerun()
