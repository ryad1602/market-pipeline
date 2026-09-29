.PHONY: up down logs dbt-run dbt-test test

up:
	cd airflow && docker compose up -d
	cd kafka && docker compose up -d
	@echo "Pipeline démarré. Interface Airflow : http://localhost:8080"

down:
	cd airflow && docker compose down
	cd kafka && docker compose down

logs:
	cd airflow && docker compose logs -f airflow-worker

dbt-run:
	cd dbt/market_pipeline_dbt && dbt run

dbt-test:
	cd dbt/market_pipeline_dbt && dbt test

test:
	pytest tests/ -v
