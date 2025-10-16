
import pandas as pd
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from wordcloud import WordCloud
import matplotlib.pyplot as plt
from pathlib import Path
from config import SETTINGS
from utils import setup_logger

logger = setup_logger(SETTINGS.logs_dir, "analysis")

def ensure_nltk():
    try:
        nltk.data.find("sentiment/vader_lexicon.zip")
    except LookupError:
        nltk.download("vader_lexicon")

def to_dataframe(products: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    cols = ["name", "price", "rating", "reviews_count", "url", "brand"]
    rows, reviews_rows = [], []

    for p in products or []:
        rows.append({
            "name": p.get("name"),
            "price": p.get("price"),
            "rating": p.get("rating"),
            "reviews_count": p.get("reviews") if isinstance(p.get("reviews"), int) else (p.get("reviews_count") or 0),
            "url": p.get("url"),
            "brand": (p.get("name") or "").split()[0] if p.get("name") else None
        })
        for r in (p.get("reviews") or []):
            reviews_rows.append({"product": p.get("name"), "text": r.get("text"), "score": r.get("score")})

    product_df = pd.DataFrame(rows, columns=cols)
    reviews_df = pd.DataFrame(reviews_rows, columns=["product", "text", "score"]) if reviews_rows else pd.DataFrame(columns=["product","text","score"])
    return product_df, reviews_df


def analyze_reviews(df_reviews: pd.DataFrame) -> pd.DataFrame:
    ensure_nltk()
    sia = SentimentIntensityAnalyzer()
    df = df_reviews.copy()
    df["text"] = df["text"].fillna("")
    df["sentiment"] = df["text"].apply(lambda t: sia.polarity_scores(t)["compound"])
    return df

def wordcloud_from_reviews(df_reviews: pd.DataFrame, outfile: Path):
    if df_reviews is None or df_reviews.empty or "text" not in df_reviews.columns:
        logger.warning("No reviews available for wordcloud; creating placeholder image.")
        import matplotlib.pyplot as plt
        outfile.parent.mkdir(parents=True, exist_ok=True)
        fig = plt.figure(figsize=(8, 3))
        fig.text(0.1, 0.5, "No reviews available", fontsize=16)
        plt.axis("off"); plt.tight_layout(); fig.savefig(outfile, dpi=120); plt.close(fig)
        return

    text = " ".join(df_reviews["text"].dropna().astype(str).tolist())
    if not text.strip():
        logger.warning("Review text is empty; skipping wordcloud.")
        return

    from wordcloud import WordCloud
    wc = WordCloud(width=1200, height=600, background_color="white").generate(text)
    import matplotlib.pyplot as plt
    plt.figure(figsize=(12,6)); plt.imshow(wc); plt.axis("off"); plt.tight_layout()
    outfile.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(outfile, dpi=150); plt.close()
    logger.info(f"Wrote wordcloud: {outfile}")

