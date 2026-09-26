# market-pipeline

Pipeline de données actions + crypto : ingestion, streaming, stockage, transformation et exposition, orchestré par Airflow.

## Architecture

- **Ingestion actions** : [scripts/fetch_stock_data.py](scripts/fetch_stock_data.py) (yfinance) → validation ([scripts/validation.py](scripts/validation.py)) → PostgreSQL, orchestré par le DAG `stock_pipeline`.
- **Ingestion crypto** : [scripts/fetch_crypto_data.py](scripts/fetch_crypto_data.py) (REST, DAG `crypto_pipeline`) et [scripts/crypto_ws_producer.py](scripts/crypto_ws_producer.py) → Kafka → [scripts/kafka_crypto_trades_consumer.py](scripts/kafka_crypto_trades_consumer.py) (streaming temps réel, process autonome hors Airflow).
- **Transformation** : dbt ([dbt/market_pipeline_dbt/](dbt/market_pipeline_dbt/)), orchestré par le DAG `dbt_pipeline` via astronomer-cosmos.
- **Export / archivage** : [scripts/landing_zone.py](scripts/landing_zone.py) (Parquet sur S3, DAG `landing_zone_export`) et [scripts/export_to_s3.py](scripts/export_to_s3.py) (backup, DAG `s3_backup`).
- **Visualisation** : [scripts/dashboard.py](scripts/dashboard.py) (Streamlit, process autonome).

## Setup

1. Copier les fichiers d'environnement et remplir les vraies valeurs :
   ```
   cp .env.example .env
   cp airflow/.env.example airflow/.env
   ```
2. Activer le hook Git anti-fuite de secrets (une fois par clone, voir [Sécurité](#sécurité)) :
   ```
   git config core.hooksPath .githooks
   ```
3. Environnement Python local (pour lancer les scripts manuellement / tests) :
   ```
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Lancer l'infrastructure

```
docker compose -f airflow/docker-compose.yaml up -d      # Airflow + market-db
docker compose -f kafka/docker-compose.yaml up -d         # Kafka
```

Airflow UI : http://localhost:8080 (identifiants par défaut `airflow` / `airflow`).

## Lancer les process manuels (hors Airflow)

Ces scripts ne sont pas déclenchés par un DAG ; ce sont des process longue durée à lancer soi-même, dans le venv local :

```
python scripts/crypto_ws_producer.py            # producteur Kafka (WebSocket crypto)
python scripts/kafka_crypto_trades_consumer.py   # consommateur Kafka -> PostgreSQL
python scripts/check_kafka_lag.py                # supervision du lag de consommation
streamlit run scripts/dashboard.py               # dashboard de visualisation
```

## dbt

```
cd dbt/market_pipeline_dbt
dbt run
dbt test
```

Toujours lancer les commandes dbt depuis `dbt/market_pipeline_dbt/` — les lancer depuis la racine du repo génère des fichiers `packages.yml` / `snapshots/` / `logs/` parasites à la racine (déjà arrivé, nettoyé).

## Tests

```
pytest
```

CI GitHub Actions : [.github/workflows/tests.yml](.github/workflows/tests.yml).

## Sécurité

- `.env` et `airflow/.env` contiennent de vraies clés (AWS, Gmail) et ne sont **jamais** committés (voir [.gitignore](.gitignore)). Utiliser les fichiers `.env.example` comme référence pour créer les siens.
- Un hook pre-commit ([.githooks/pre-commit](.githooks/pre-commit)) bloque tout commit qui contiendrait un fichier `.env` réel ou un motif de clé AWS/clé privée. Il n'est pas actif par défaut sur un nouveau clone : lancer `git config core.hooksPath .githooks` une fois après avoir cloné le repo.
- Si une clé a pu fuiter (partagée, affichée dans un terminal partagé, etc.), la faire tourner immédiatement :
  - **AWS** : IAM Console → Users → Security credentials → désactiver puis supprimer l'access key exposée, en créer une nouvelle, mettre à jour `.env` et `airflow/.env`.
  - **Gmail (mot de passe d'application)** : compte Google → Sécurité → Mots de passe des applications → révoquer l'ancien, en générer un nouveau.
- Le mot de passe PostgreSQL de `market-db` est piloté par `${DB_USER}` / `${DB_PASSWORD}` dans `airflow/docker-compose.yaml` (lu depuis `airflow/.env`), pas en dur.
