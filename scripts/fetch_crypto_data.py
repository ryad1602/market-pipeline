import requests
import pandas as pd
import time
from datetime import datetime, timezone
from db import get_engine

COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano",
    "dogecoin", "polkadot", "chainlink", "litecoin", "avalanche-2",
]

def fetch_crypto_data(coins: list[str]) -> pd.DataFrame:
    """
    Récupère le prix actuel d'une liste de cryptomonnaies via l'API CoinGecko.
    Réessaie automatiquement en cas d'échec réseau ponctuel.
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(coins),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true",
    }

    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            break
        except requests.exceptions.RequestException as e:
            print(f"Tentative {attempt}/{max_retries} échouée : {e}")
            if attempt == max_retries:
                raise
            time.sleep(5)

    rows = []
    for coin, values in data.items():
        rows.append({
            "coin": coin,
            "price_usd": values.get("usd"),
            "market_cap_usd": values.get("usd_market_cap"),
            "volume_24h_usd": values.get("usd_24h_vol"),
            "change_24h_pct": values.get("usd_24h_change"),
            "fetched_at": datetime.now(timezone.utc),
        })

    return pd.DataFrame(rows)

def save_to_db(df: pd.DataFrame, table_name: str = "raw_crypto_prices"):
    engine = get_engine()
    df.to_sql(table_name, engine, if_exists="append", index=False)
    print(f"{len(df)} lignes écrites dans la table '{table_name}'")

if __name__ == "__main__":
    df = fetch_crypto_data(COINS)
    save_to_db(df)
