from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/scripts")

from fetch_crypto_data import fetch_crypto_data, save_to_db, COINS

default_args = {
    "owner": "ryad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_pipeline():
    df = fetch_crypto_data(COINS)
    save_to_db(df)

with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["market-pipeline"],
) as dag:

    fetch_and_store = PythonOperator(
        task_id="fetch_and_store_crypto_data",
        python_callable=run_pipeline,
    )
