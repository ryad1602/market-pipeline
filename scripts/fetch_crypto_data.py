import time
from datetime import datetime, timezone

import pandas as pd
import requests
from db import get_engine
from monitoring import track_pipeline_run

COIN_TO_BINANCE = {
    "bitcoin": "BTCUSDT",
    "ethereum": "ETHUSDT",
    "solana": "SOLUSDT",
    "ripple": "XRPUSDT",
    "cardano": "ADAUSDT",
    "dogecoin": "DOGEUSDT",
    "polkadot": "DOTUSDT",
    "chainlink": "LINKUSDT",
    "litecoin": "LTCUSDT",
    "avalanche-2": "AVAXUSDT",
}

COINS = list(COIN_TO_BINANCE.keys())

@track_pipeline_run("crypto_pipeline")
def fetch_crypto_data(coins: list[str]) -> int:
    """
    Récupère le prix actuel d'une liste de cryptomonnaies via l'API REST Binance.
    Retourne le nombre de lignes traitées.
    """
    symbols = [COIN_TO_BINANCE[c] for c in coins if c in COIN_TO_BINANCE]

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(
                "https://api.binance.com/api/v3/ticker/24hr",
        params={"symbols": str(symbols).replace("'", '"').replace(" ", "")},                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            break
        except requests.exceptions.RequestException as e:
            print(f"Tentative {attempt}/{max_retries} échouée : {e}")
            if attempt == max_retries:
                raise
            time.sleep(5)

    symbol_to_coin = {v: k for k, v in COIN_TO_BINANCE.items()}
    rows = []
    for item in data:
        coin = symbol_to_coin.get(item["symbol"])
        if not coin:
            continue
        rows.append({
            "coin": coin,
            "price_usd": float(item["lastPrice"]),
            "market_cap_usd": None,
            "volume_24h_usd": float(item["quoteVolume"]),
            "change_24h_pct": float(item["priceChangePercent"]),
            "fetched_at": datetime.now(timezone.utc),
        })

    if not rows:
        return 0

    df = pd.DataFrame(rows)
    save_to_db(df)
    return len(df)

def save_to_db(df: pd.DataFrame, table_name: str = "raw_crypto_prices"):
    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"{len(df)} lignes écrites dans la table '{table_name}'")

if __name__ == "__main__":
    fetch_crypto_data(COINS)
