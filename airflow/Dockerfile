FROM apache/airflow:2.10.4

RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    python-dotenv \
    boto3 \
    pyarrow \
    astronomer-cosmos

# Environnement virtuel séparé, réservé à dbt uniquement —
# isolé de l'environnement principal d'Airflow, pour éviter tout conflit de dépendances
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-postgres
