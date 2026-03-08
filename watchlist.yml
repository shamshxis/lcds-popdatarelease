# Global Pop Data Watch

A GitHub + Streamlit monitor for official population-related dataset releases, update signals, access warnings, and discovery of new relevant sources.

## What it tracks

This project is designed around LCDS-relevant themes:

- population estimates and projections
- migration and asylum
- fertility and mortality
- census and household statistics
- labour and demographic releases
- population pyramid update signals
- access changes such as DHS registration or dataset availability changes

## Current source coverage

The starter watchlist includes official pages from:

- ONS
- Eurostat
- U.S. Census Bureau
- DHS Program
- UN World Population Prospects
- PopulationPyramid.net
- Statistics Sweden
- Statistics Norway
- Statistics Denmark
- Statistics Finland

## Core outputs

The scraper writes four files into `data/`:

- `dataset_tracker.csv` for the current watchlist view
- `dataset_tracker_history.csv` for rolling historical snapshots
- `dataset_changes.csv` for change detection between runs
- `candidate_sources.csv` for reviewable new source candidates

## How it works

1. `watchlist.yml` defines trusted sources, themes, and parser types.
2. `scraper.py` pulls each page, applies a source-specific parser, and normalises the results.
3. Results are retained in a rolling window and compared to the previous run.
4. A discovery pass scans approved domains for promising new pages and adds them to a review queue.
5. `app.py` presents the tracker through Streamlit with active filtering and export tools.
6. GitHub Actions refreshes the tracker daily.

## Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scraper.py
streamlit run app.py
```

## GitHub setup

1. Create a new repository.
2. Copy all project files into the repo.
3. Enable GitHub Actions.
4. Push the repo.
5. Run the workflow manually once using `workflow_dispatch`.
6. Deploy `app.py` on Streamlit Community Cloud or another Streamlit host.

## Important notes

- Some official sites change their HTML often, so parsers should be reviewed over time.
- Candidate sources are discovered automatically but not trusted automatically.
- The tracker stores plain-language summaries to keep the dashboard readable.
- The default window keeps signals from roughly six months back and six months ahead while preserving a longer rolling history file.

## Good next upgrades

- add email or Slack alerts for date changes
- add manual approval for candidate sources from inside the Streamlit app
- add more official sources such as World Bank, OECD, national statistical offices, and HMD/HFD where appropriate
- add topic scoring for stronger LCDS prioritisation
