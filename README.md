
# E‑Commerce Analytics Automation (Case Study)

Implements scraping (Selenium), processing (pandas), sentiment (NLTK VADER), Excel, PDF, and a Dash app.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run (Sample Data)

```bash
python main.py --sample
```

## Run (Live Scrape)
Update selectors in `scraper.py` to match the target site's DOM and ensure compliance with robots.txt and terms.

```bash
python main.py --scrape
```

## Outputs
- reports/ecommerce_analysis.xlsx
- reports/ecommerce_report.pdf
- reports/wordcloud.png
- reports/dashboard.html
- data/raw_products.json

## Tests
```bash
pytest -q
```
