import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import sqlalchemy
from db import get_engine
from kafka_producer import get_producer, publish_stock_prices

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

def ensure_table_exists(engine):
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS raw_stock_prices (
                price_date DATE NOT NULL,
                ticker TEXT NOT NULL,
                open_price FLOAT,
                high_price FLOAT,
                low_price FLOAT,
                close_price FLOAT,
                volume BIGINT,
                fetched_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (ticker, price_date)
            )
        """))
        conn.commit()

def save_to_db(df: pd.DataFrame, table_name: str = "raw_stock_prices"):
    engine = get_engine()
    ensure_table_exists(engine)

    df = df.reset_index().rename(columns={
        "Date": "price_date",
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Volume": "volume",
    })

    records = df[["price_date", "ticker", "open_price", "high_price",
                  "low_price", "close_price", "volume", "fetched_at"]].to_dict(orient="records")

    with engine.connect() as conn:
        for record in records:
            conn.execute(
                sqlalchemy.text("""
                    INSERT INTO raw_stock_prices
                        (price_date, ticker, open_price, high_price, low_price, close_price, volume, fetched_at)
                    VALUES
                        (:price_date, :ticker, :open_price, :high_price, :low_price, :close_price, :volume, :fetched_at)
                    ON CONFLICT (ticker, price_date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume,
                        fetched_at = EXCLUDED.fetched_at
                """),
                record
            )
        conn.commit()

    print(f"{len(records)} lignes upsertées dans '{table_name}'")

if __name__ == "__main__":
    df = fetch_stock_data(TICKERS)
    save_to_db(df)
    
    producer = get_producer()
    publish_stock_prices(df, producer)
