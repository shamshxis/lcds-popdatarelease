name: Update Global Pop Data Watch

on:
  schedule:
    - cron: "20 5 * * *"
  workflow_dispatch:

permissions:
  contents: write

jobs:
  update:
    runs-on: ubuntu-latest

    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run tracker
        run: |
          python scraper.py

      - name: Commit updated data files
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
          git add data/*.csv data/*.json || true
          git diff --cached --quiet && exit 0
          git commit -m "Daily tracker refresh"
          git push
