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

def export_table_to_s3(table_name: str):
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"{table_name}_{timestamp}.csv"
    local_path = f"/tmp/{file_name}"

    df.to_csv(local_path, index=False)

    s3 = get_s3_client()
    bucket = os.getenv("S3_BUCKET_NAME")
    s3_key = f"backups/{table_name}/{file_name}"

    s3.upload_file(local_path, bucket, s3_key)
    os.remove(local_path)

    print(f"{table_name} exporté vers s3://{bucket}/{s3_key} ({len(df)} lignes)")

if __name__ == "__main__":
    export_table_to_s3("raw_stock_prices")
    export_table_to_s3("raw_crypto_prices")
