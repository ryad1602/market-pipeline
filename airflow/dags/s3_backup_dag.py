from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/scripts")

from export_to_s3 import export_table_to_s3

default_args = {
    "owner": "ryad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_export():
    export_table_to_s3("raw_stock_prices")
    export_table_to_s3("raw_crypto_prices")

with DAG(
    dag_id="s3_backup",
    default_args=default_args,
    schedule="0 23 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["market-pipeline"],
) as dag:

    export_task = PythonOperator(
        task_id="export_to_s3",
        python_callable=run_export,
    )
