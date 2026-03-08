from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / 'data'
CURRENT_CSV = DATA_DIR / 'dataset_tracker.csv'
CHANGES_CSV = DATA_DIR / 'dataset_changes.csv'
DISCOVERY_CSV = DATA_DIR / 'candidate_sources.csv'
STATUS_CSV = DATA_DIR / 'source_status.csv'
META_JSON = DATA_DIR / 'last_run_meta.json'

st.set_page_config(page_title='Global Pop Data Watch', page_icon='🌍', layout='wide')

st.markdown("""
<style>
.block-container {padding-top: 1.2rem; padding-bottom: 1.2rem;}
.hero {background: linear-gradient(135deg,#163e7a,#3b6db1); color: white; padding: 20px 24px; border-radius: 18px; margin-bottom: 18px;}
.small {color:#5d6f87; font-size: 0.92rem;}
</style>
""", unsafe_allow_html=True)


def load_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


current = load_csv(CURRENT_CSV)
changes = load_csv(CHANGES_CSV)
discovery = load_csv(DISCOVERY_CSV)
status = load_csv(STATUS_CSV)
meta = {}
if META_JSON.exists():
    try:
        meta = json.loads(META_JSON.read_text(encoding='utf-8'))
    except Exception:
        meta = {}

st.markdown('<div class="hero"><h2 style="margin:0;">Global Population Data Watch</h2><div style="margin-top:6px;">Daily monitor for releases, access changes, removal signals, and population-data updates across UK, US, Europe, Nordics, DHS, and global sources.</div></div>', unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
c1.metric('Tracker rows', int(meta.get('records_current', len(current))))
c2.metric('Sources checked', int(meta.get('sources_checked', len(status))))
c3.metric('Sources ok', int(meta.get('sources_ok', int(status['ok'].sum()) if not status.empty and 'ok' in status.columns else 0)))
c4.metric('Candidate pages', int(meta.get('candidate_sources', len(discovery))))

if current.empty:
    st.warning('No tracker rows are currently loaded. Check the Source status tab first. If all sources failed, open the GitHub Actions log and inspect source_status.csv.')

with st.sidebar:
    st.header('Filters')
    country_opts = sorted(current['country'].dropna().astype(str).unique().tolist()) if not current.empty and 'country' in current.columns else []
    status_opts = sorted(current['status'].dropna().astype(str).unique().tolist()) if not current.empty and 'status' in current.columns else []
    source_opts = sorted(current['source_name'].dropna().astype(str).unique().tolist()) if not current.empty and 'source_name' in current.columns else []
    selected_countries = st.multiselect('Country', country_opts, default=country_opts)
    selected_status = st.multiselect('Status', status_opts, default=status_opts)
    selected_sources = st.multiselect('Source', source_opts, default=source_opts)
    keyword = st.text_input('Keyword')
    show_errors_only = st.checkbox('Show parser errors only', value=False)


def prep_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in ['action_date', 'announcement_date']:
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors='coerce')
    return out

current = prep_dates(current)
changes = prep_dates(changes)

filtered = current.copy()
if not filtered.empty:
    if selected_countries:
        filtered = filtered[filtered['country'].astype(str).isin(selected_countries)]
    if selected_status:
        filtered = filtered[filtered['status'].astype(str).isin(selected_status)]
    if selected_sources:
        filtered = filtered[filtered['source_name'].astype(str).isin(selected_sources)]
    if keyword:
        mask = filtered['dataset_title'].astype(str).str.contains(keyword, case=False, na=False) | filtered['summary'].astype(str).str.contains(keyword, case=False, na=False)
        filtered = filtered[mask]
    if show_errors_only:
        filtered = filtered[filtered['status'].astype(str).eq('parser_error')]


tab1, tab2, tab3, tab4 = st.tabs(['Tracker', 'Changes', 'Source status', 'Discovery'])

with tab1:
    if filtered.empty:
        st.info('No rows match the current filters.')
    else:
        show = filtered.copy()
        show['dataset_link'] = show.apply(lambda r: r['dataset_url'] if str(r.get('dataset_url', '')).startswith('http') else '', axis=1)
        show = show.rename(columns={
            'dataset_title': 'Dataset', 'country': 'Country', 'region': 'Region', 'source_name': 'Source',
            'status': 'Status', 'action_type': 'Type', 'action_date': 'Action date', 'announcement_date': 'Announcement date',
            'days_until_action': 'Days left', 'summary': 'Plain-language summary', 'tags': 'Themes'
        })
        st.dataframe(show[['Dataset','Country','Region','Source','Status','Type','Action date','Announcement date','Days left','Themes','Plain-language summary']], use_container_width=True, hide_index=True)
        st.download_button('Download tracker CSV', filtered.to_csv(index=False), file_name='dataset_tracker.csv')

with tab2:
    if changes.empty:
        st.info('No change log yet.')
    else:
        st.dataframe(changes[['dataset_title','source_name','country','status','action_date','change_type','previous_action_date','previous_status']].rename(columns={'dataset_title':'Dataset','source_name':'Source','country':'Country','status':'Status','action_date':'Action date','change_type':'Change'}), use_container_width=True, hide_index=True)

with tab3:
    if status.empty:
        st.info('No source status file found.')
    else:
        ok_count = int(status['ok'].sum()) if 'ok' in status.columns else 0
        st.caption(f'{ok_count} of {len(status)} sources completed without parser exceptions.')
        st.dataframe(status.rename(columns={'source_name':'Source','parser':'Parser','records':'Rows','error':'Error','elapsed_seconds':'Seconds'}), use_container_width=True, hide_index=True)
        bad = status[~status['ok'].astype(bool)] if 'ok' in status.columns else pd.DataFrame()
        if not bad.empty:
            st.error('Some sources failed. The Error column usually shows the exact page or parser problem.')

with tab4:
    if discovery.empty:
        st.info('No candidate pages discovered yet.')
    else:
        st.dataframe(discovery.rename(columns={'candidate_title':'Candidate title','candidate_url':'Candidate URL','candidate_domain':'Domain','relevance_score':'Score','seed_source_name':'Seed source'}), use_container_width=True, hide_index=True)
