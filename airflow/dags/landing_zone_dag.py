from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow/scripts")

from landing_zone import write_to_landing_zone

default_args = {
    "owner": "ryad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def run_landing_zone():
    write_to_landing_zone("raw_stock_prices", source="stocks")
    write_to_landing_zone("raw_crypto_trades", source="crypto")

with DAG(
    dag_id="landing_zone_export",
    default_args=default_args,
    schedule="0 22 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["market-pipeline"],
) as dag:

    export_task = PythonOperator(
        task_id="export_to_landing_zone",
        python_callable=run_landing_zone,
    )
