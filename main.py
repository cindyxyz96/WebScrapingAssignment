
import argparse, json
from pathlib import Path
from config import SETTINGS
from utils import setup_logger
from scraper import run_scrape
from analysis import to_dataframe, analyze_reviews, wordcloud_from_reviews
from processing import create_excel
from report import make_pdf
from dashboard import save_static_dashboard

logger = setup_logger(SETTINGS.logs_dir, "main")

SAMPLE_JSON = [
    {"name":"Apple MacBook Air 13","price":1099.99,"rating":4.7,"reviews":1280,"url":"https://example.com/1",
     "specs":{"CPU":"M2","RAM":"8GB","Storage":"256GB SSD"},
     "reviews":[{"text":"Battery life is amazing!","score":5},{"text":"Keyboard feels great.","score":4.5}]},
    {"name":"Dell XPS 13","price":1299.00,"rating":4.5,"reviews":980,"url":"https://example.com/2",
     "specs":{"CPU":"Intel i7","RAM":"16GB","Storage":"512GB SSD"},
     "reviews":[{"text":"Display is outstanding.","score":5},{"text":"Gets a bit warm.","score":3.5}]},
    {"name":"HP Envy 15","price":999.00,"rating":4.3,"reviews":640,"url":"https://example.com/3",
     "specs":{"CPU":"AMD Ryzen 7","RAM":"16GB","Storage":"1TB SSD"},
     "reviews":[{"text":"Great performance for price.","score":4.5},{"text":"Fans are audible under load.","score":3.0}]}
]

def main():
    parser = argparse.ArgumentParser(description="E‑Commerce Analytics Automation")
    parser.add_argument("--scrape", action="store_true", help="Run Selenium scraper")
    parser.add_argument("--sample", action="store_true", help="Use sample data (default if --scrape not given)")
    args = parser.parse_args()
    if args.scrape:
        raw = run_scrape()
    else:
        raw = SAMPLE_JSON
        Path(SETTINGS.raw_json_path).write_text(json.dumps(raw, indent=2), encoding="utf-8")
        logger.info(f"Wrote sample raw JSON to {SETTINGS.raw_json_path}")
    product_df, reviews_df = to_dataframe(raw)
    reviews_scored = analyze_reviews(reviews_df) if not reviews_df.empty else reviews_df
    create_excel(product_df, product_df.copy(), reviews_scored, SETTINGS.excel_output_path)
    wordcloud_from_reviews(reviews_scored, SETTINGS.reports_dir / "wordcloud.png")
    make_pdf(product_df, reviews_scored, SETTINGS.pdf_output_path)
    save_static_dashboard(product_df, reviews_scored, SETTINGS.dashboard_html_path)
    logger.info("Artifacts written to reports/.")

if __name__ == "__main__":
    main()
