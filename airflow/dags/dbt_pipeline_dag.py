from datetime import datetime, timedelta
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

profile_config = ProfileConfig(
    profile_name="market_pipeline_dbt",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="market_db_conn",
        profile_args={"schema": "public"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt",
)

default_args = {
    "owner": "ryad",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

dbt_dag = DbtDag(
    project_config=ProjectConfig("/opt/airflow/dbt/market_pipeline_dbt"),
    profile_config=profile_config,
    execution_config=execution_config,
    default_args=default_args,
    dag_id="dbt_pipeline",
    schedule="30 */1 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["market-pipeline"],
)
