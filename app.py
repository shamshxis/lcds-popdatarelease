import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")

st.title("Global Population Data Watch")

df = pd.read_csv("data/dataset_tracker.csv")

st.sidebar.header("Filters")

country = st.sidebar.multiselect(
    "Country",
    df["country"].unique(),
    default=df["country"].unique()
)

source = st.sidebar.multiselect(
    "Source",
    df["source"].unique(),
    default=df["source"].unique()
)

keyword = st.sidebar.text_input("Keyword search")

filtered = df[
    (df.country.isin(country)) &
    (df.source.isin(source))
]

if keyword:
    filtered = filtered[
        filtered.summary.str.contains(keyword, case=False)
    ]

st.metric("Tracked releases", len(filtered))

st.dataframe(
    filtered.sort_values("action_date"),
    use_container_width=True
)

st.download_button(
    "Download CSV",
    filtered.to_csv(index=False),
    file_name="population_tracker.csv"
)
