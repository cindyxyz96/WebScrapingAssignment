
from pathlib import Path
import pandas as pd
from config import SETTINGS
from utils import setup_logger

logger = setup_logger(SETTINGS.logs_dir, "processing")

def create_excel(product_df: pd.DataFrame, specs_df: pd.DataFrame, reviews_scored: pd.DataFrame, outfile: Path | None = None):
    outfile = outfile or SETTINGS.excel_output_path
    outfile.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as writer:
        product_df.to_excel(writer, sheet_name="Product Summary", index=False)
        specs_df.to_excel(writer, sheet_name="Specifications", index=False)
        reviews_scored.to_excel(writer, sheet_name="Review Analysis", index=False)
        wb = writer.book; ws = writer.sheets["Product Summary"]
        if "price" in product_df.columns:
            col = product_df.columns.get_loc("price")
            ws.conditional_format(1, col, len(product_df)+1, col, {"type":"3_color_scale"})
    logger.info(f"Wrote Excel workbook: {outfile}")
    return outfile
