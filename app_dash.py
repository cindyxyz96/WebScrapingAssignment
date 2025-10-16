
import json
import pandas as pd
from pathlib import Path
from dash import Dash, html, dcc, Input, Output
import plotly.express as px
from config import SETTINGS
from analysis import to_dataframe, analyze_reviews

def load_data():
    raw = json.loads(Path(SETTINGS.raw_json_path).read_text(encoding="utf-8"))
    product_df, reviews_df = to_dataframe(raw)
    reviews_scored = analyze_reviews(reviews_df) if not reviews_df.empty else reviews_df
    return product_df, reviews_scored

app = Dash(__name__)
product_df, reviews_scored = load_data()

app.layout = html.Div([
    html.H1("E‑Commerce Analytics Dashboard"),
    dcc.Graph(id="price_vs_rating"),
    dcc.Graph(id="sentiment_hist"),
])

@app.callback(
    Output("price_vs_rating","figure"),
    Output("sentiment_hist","figure"),
    Input("price_vs_rating","id")
)
def update(_):
    fig1 = px.scatter(product_df, x="price", y="rating", hover_name="name", title="Price vs Rating")
    fig2 = px.histogram(reviews_scored, x="sentiment", nbins=40, title="Review Sentiment")
    return fig1, fig2

if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8050)
