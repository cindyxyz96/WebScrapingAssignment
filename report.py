# report.py
from pathlib import Path
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import pandas as pd
from config import SETTINGS
from utils import setup_logger

logger = setup_logger(SETTINGS.logs_dir, "report")

def _col(df: pd.DataFrame, name: str) -> pd.Series:
    return df[name] if name in df.columns else pd.Series(dtype="float64")

def make_pdf(product_df: pd.DataFrame, reviews_scored: pd.DataFrame, outfile: Path | None = None):
    outfile = outfile or SETTINGS.pdf_output_path
    outfile.parent.mkdir(parents=True, exist_ok=True)

    prices = pd.to_numeric(_col(product_df, "price"), errors="coerce").dropna()
    ratings = pd.to_numeric(_col(product_df, "rating"), errors="coerce").dropna()

    with PdfPages(outfile) as pdf:
        # Page 1 – Summary
        fig = plt.figure(figsize=(8.27, 11.69))
        fig.text(0.1, 0.9, "E-Commerce Analytics: Executive Summary", fontsize=18, weight="bold")
        fig.text(0.1, 0.85, f"Products: {len(product_df)}")
        fig.text(0.1, 0.81, f"Avg Price: ${prices.mean():.2f}" if not prices.empty else "Avg Price: N/A")
        fig.text(0.1, 0.77, f"Avg Rating: {ratings.mean():.2f} / 5" if not ratings.empty else "Avg Rating: N/A")
        fig.text(0.1, 0.73, f"Total Reviews: {len(reviews_scored) if reviews_scored is not None else 0}")
        pdf.savefig(fig); plt.close(fig)

        # Page 2 – Visuals
        fig = plt.figure(figsize=(8.27, 11.69))
        # Price histogram
        ax = fig.add_axes([0.1, 0.55, 0.8, 0.35])
        if not prices.empty:
            prices.plot(kind="hist", bins=20, ax=ax)
            ax.set_title("Price Distribution")
            ax.set_xlabel("USD"); ax.set_ylabel("Count")
        else:
            ax.text(0.5, 0.5, "No price data", ha="center", va="center")
            ax.set_axis_off()

        # Rating vs Price
        ax2 = fig.add_axes([0.1, 0.1, 0.8, 0.35])
        sample = pd.DataFrame({"price": prices, "rating": ratings}).dropna()
        if not sample.empty:
            ax2.scatter(sample["price"], sample["rating"])
            ax2.set_title("Rating vs Price"); ax2.set_xlabel("Price"); ax2.set_ylabel("Rating")
        else:
            ax2.text(0.5, 0.5, "No (price, rating) pairs", ha="center", va="center")
            ax2.set_axis_off()

        pdf.savefig(fig); plt.close(fig)

    logger.info(f"Wrote PDF report: {outfile}")
    return outfile
