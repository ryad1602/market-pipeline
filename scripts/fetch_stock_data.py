import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
from db import get_engine

TICKERS = [
    "AAPL", "MSFT", "GOOGL", "NVDA", "META", "AMZN",
    "JPM", "GS", "BAC", "V",
    "JNJ", "PFE", "UNH",
    "XOM", "CVX",
    "TSLA", "PG", "KO", "MCD", "DIS",
]

def fetch_stock_data(tickers: list[str], period: str = "1d") -> pd.DataFrame:
    """
    Récupère les données de marché pour plusieurs tickers en un seul appel groupé.
    """
    raw = yf.download(tickers, period=period, group_by="ticker", auto_adjust=False)

    all_data = []
    for ticker in tickers:
        df_ticker = raw[ticker].copy()
        df_ticker["ticker"] = ticker
        df_ticker["fetched_at"] = datetime.now(timezone.utc)
        all_data.append(df_ticker)

    combined = pd.concat(all_data)
    return combined

def save_to_db(df: pd.DataFrame, table_name: str = "raw_stock_prices"):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="append", index=True)
    print(f"{len(df)} lignes écrites dans la table '{table_name}'")

if __name__ == "__main__":
    df = fetch_stock_data(TICKERS)
    save_to_db(df)
