import os

import pandas as pd
from db import get_engine
from sqlalchemy import create_engine


def get_neon_engine():
    neon_url = os.getenv("NEON_DATABASE_URL")
    return create_engine(neon_url)

def sync_table(table_name: str):
    """
    Copie une table depuis la base locale vers la base Neon (démo),
    en remplaçant entièrement son contenu à chaque synchronisation.
    """
    local_engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name}", local_engine)

    neon_engine = get_neon_engine()
    with neon_engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    print(f"{len(df)} lignes synchronisées vers Neon : '{table_name}'")

if __name__ == "__main__":
    sync_table("latest_stock_prices")
    sync_table("latest_crypto_prices")
