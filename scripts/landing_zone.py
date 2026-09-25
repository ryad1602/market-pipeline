import os
import boto3
import pandas as pd
from datetime import datetime, timezone
from db import get_engine

def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )

def write_to_landing_zone(table_name: str, source: str):
    """
    Exporte une table PostgreSQL vers S3 au format Parquet,
    partitionné par source et par date (ex: source=crypto/date=2026-09-26/).
    """
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    timestamp = datetime.now(timezone.utc).strftime("%H%M%S")

    local_path = f"/tmp/{table_name}_{timestamp}.parquet"
    df.to_parquet(local_path, index=False)

    s3 = get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_key = f"landing/source={source}/date={today}/{table_name}_{timestamp}.parquet"

    s3.upload_file(local_path, bucket, s3_key)
    os.remove(local_path)

    print(f"{len(df)} lignes écrites vers s3://{bucket}/{s3_key}")

if __name__ == "__main__":
    write_to_landing_zone("raw_stock_prices", source="stocks")
    write_to_landing_zone("raw_crypto_trades", source="crypto")
