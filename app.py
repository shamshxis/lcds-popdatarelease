import pandas as pd
import streamlit as st
import os
from datetime import datetime

st.set_page_config(page_title="LCDS Global Brief", page_icon="📋", layout="wide")
st.title("📋 LCDS Management Brief")

DATA_FILE = "data/dataset_tracker.csv"

if st.button("🔄 Refresh Data"):
    os.system("python scraper.py")
    st.cache_data.clear()
    st.rerun()

if os.path.exists(DATA_FILE):
    try:
        df = pd.read_csv(DATA_FILE)
        df["dt"] = pd.to_datetime(df["action_date"], errors="coerce")
        df = df.dropna(subset=["dt"]).sort_values(by="dt")
        
        # Group by Month
        df["Month"] = df["dt"].dt.strftime("%B %Y")
        
        for month in df["Month"].unique():
            st.subheader(month)
            m_data = df[df["Month"] == month]
            st.dataframe(
                m_data[["status", "action_date", "source", "dataset_title", "url"]],
                column_config={"url": st.column_config.LinkColumn("Link")},
                hide_index=True,
                use_container_width=True
            )
    except:
        st.error("Data file corrupt. Click Refresh.")
else:
    st.info("System Initializing...")
    os.system("python scraper.py")
    st.rerun()
