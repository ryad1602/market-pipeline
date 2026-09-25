import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import sqlalchemy
from db import get_engine
from kafka_producer import get_producer, publish_stock_prices
from validation import validate_records

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
    raw = yf.download(
        tickers,
        period=period,
        group_by="ticker",
        auto_adjust=False,
        progress=False,
    )

    all_data = []
    for ticker in tickers:
        if raw.empty:
            continue

        # yfinance renvoie des colonnes multi-index pour plusieurs tickers,
        # mais des colonnes simples lorsqu'un seul ticker est demandé.
        if isinstance(raw.columns, pd.MultiIndex):
            if ticker not in raw.columns.get_level_values(0):
                continue
            df_ticker = raw[ticker].copy()
        elif len(tickers) == 1:
            df_ticker = raw.copy()
        else:
            continue

        if df_ticker.empty:
            continue
        df_ticker["ticker"] = ticker
        df_ticker["fetched_at"] = datetime.now(timezone.utc)
        all_data.append(df_ticker)

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data)

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

def ensure_quarantine_table_exists(engine):
    with engine.connect() as conn:
        conn.execute(sqlalchemy.text("""
            CREATE TABLE IF NOT EXISTS quarantine_stock_prices (
                id SERIAL PRIMARY KEY,
                raw_data JSONB,
                validation_error TEXT,
                quarantined_at TIMESTAMPTZ
            )
        """))
        conn.commit()

def save_to_db(df: pd.DataFrame, table_name: str = "raw_stock_prices"):
    if df.empty:
        print(f"Aucune donnée à enregistrer dans '{table_name}'")
        return

    engine = get_engine()
    ensure_table_exists(engine)
    ensure_quarantine_table_exists(engine)

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

    valid_records, invalid_records = validate_records(records)

    with engine.connect() as conn:
        for record in valid_records:
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

        for record in invalid_records:
            conn.execute(
                sqlalchemy.text("""
                    INSERT INTO quarantine_stock_prices
                        (raw_data, validation_error, quarantined_at)
                    VALUES
                        (:raw_data, :validation_error, :quarantined_at)
                """),
                {
                    "raw_data": json.dumps(record, default=str),
                    "validation_error": record.get("validation_error", "inconnu"),
                    "quarantined_at": datetime.now(timezone.utc),
                }
            )
        conn.commit()

    print(f"{len(valid_records)} lignes upsertées, {len(invalid_records)} en quarantaine dans '{table_name}'")

if __name__ == "__main__":
    df = fetch_stock_data(TICKERS)
    save_to_db(df)
    
    producer = get_producer()
    publish_stock_prices(df, producer)
