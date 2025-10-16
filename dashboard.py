
from __future__ import annotations
from pathlib import Path
import pandas as pd
from config import SETTINGS
from utils import setup_logger

logger = setup_logger(SETTINGS.logs_dir, "dashboard")

def save_static_dashboard(product_df: pd.DataFrame, reviews_scored: pd.DataFrame, outfile: Path | None=None):
    '''
    Creates a minimal static HTML dashboard stub (no external CDN), so it's viewable offline.
    The runnable Dash/Plotly app is provided in app_dash.py for live use.
    '''
    outfile = outfile or SETTINGS.dashboard_html_path
    outfile.parent.mkdir(parents=True, exist_ok=True)
    html = f'''
    <!doctype html>
    <html><head><meta charset="utf-8"><title>E‑Commerce Dashboard</title>
    <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    .kpi {{ display:inline-block; margin-right:2rem; padding:1rem; border:1px solid #ddd; border-radius:12px; }}
    </style>
    </head><body>
    <h1>E‑Commerce Dashboard (Static Preview)</h1>
    <div class="kpi"><b>Products</b><div>{len(product_df)}</div></div>
    <div class="kpi"><b>Avg Price</b><div>${product_df['price'].dropna().mean():.2f}</div></div>
    <div class="kpi"><b>Avg Rating</b><div>{product_df['rating'].dropna().mean():.2f} / 5</div></div>
    <div class="kpi"><b>Total Reviews</b><div>{len(reviews_scored)}</div></div>
    <hr>
    <p>This is a static preview. Run <code>python app_dash.py</code> to launch the interactive Plotly Dash app.</p>
    </body></html>
    '''
    outfile.write_text(html, encoding="utf-8")
    logger.info(f"Wrote static dashboard: {outfile}")
    return outfile
