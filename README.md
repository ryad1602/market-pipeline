# Market Pipeline

Plateforme d'ingestion hybride streaming + batch de données de marché, modélisée en entrepôt avec dbt, testée, monitorée et déployée en CI/CD.

**[Démo en ligne](https://market-pipeline-ymwfbhto7opmmukfqwdj6h.streamlit.app/)** — dashboard connecté à une base PostgreSQL hébergée (Neon), synchronisée périodiquement depuis le pipeline principal.

## Architecture

WebSocket Binance --> Kafka --> Consumer Python --> PostgreSQL
API yfinance --> Airflow batch --> PostgreSQL

PostgreSQL --> dbt (staging vers marts) --> Dashboard Streamlit + S3 Parquet

Deux voies d'ingestion alimentent le même entrepôt :
- Streaming (crypto) : connexion WebSocket permanente a Binance, chaque transaction traitée dès son arrivée
- Batch (actions) : Airflow interroge l'API yfinance toutes les 15 minutes

## Stack technique

| Domaine | Outils |
|---|---|
| Orchestration | Apache Airflow (CeleryExecutor), Cosmos pour dbt |
| Streaming | Apache Kafka (mode KRaft) |
| Transformation | dbt (staging vers marts, snapshots SCD2, tests) |
| Stockage | PostgreSQL, AWS S3 (Parquet partitionné) |
| Visualisation | Streamlit |
| CI/CD | GitHub Actions, ruff, pre-commit, gitleaks |
| Conteneurisation | Docker, Docker Compose |

## Choix techniques

- Idempotence partout : upsert (ON CONFLICT ... DO UPDATE) sur les tables batch, ON CONFLICT ... DO NOTHING sur les trades crypto - relancer le pipeline ne crée jamais de doublon
- Validation des données : chaque ligne est validée avec Pydantic avant insertion ; les lignes invalides partent dans une table de quarantaine plutôt que de polluer les données brutes
- Modélisation dbt en couches : staging (nettoyage) vers marts (dernier état + faits incrémentaux avec window functions) ; dimension ticker_sectors historisée en SCD type 2 via un snapshot dbt
- Tests de qualité : not_null, unique, tests de plage de valeurs (dbt-expectations), test maison de détection de variations aberrantes (plus de 30% en une journée)
- Monitoring : chaque exécution de pipeline est tracée (durée, lignes traitées, statut) via un décorateur Python ; une page dédiée dans le dashboard affiche la fraîcheur des données par source
- Alertes : email automatique en cas d'échec d'un DAG

## Chiffres

- 20 actions suivies (secteurs tech, finance, santé, énergie, industrie, consommation)
- 10 cryptomonnaies suivies
- Collecte batch toutes les 15 minutes, streaming crypto en continu
- 40 lignes en base pour les prix actions, 2912 trades crypto capturés en streaming

## Limites connues

- Le pipeline local dépend de la disponibilité de la machine hôte (pas de serveur 24/7) - la démo en ligne reste accessible en continu grâce à la synchronisation vers Neon
- L'historique actuel couvre quelques semaines, pas encore un an complet
- Kubernetes a été exploré puis abandonné (Minikube) - jugé non pertinent pour un poste Data Engineer junior par rapport à la maîtrise de Kafka/Airflow/dbt

## Lancer le projet en local

git clone https://github.com/ryad1602/market-pipeline.git
cd market-pipeline
cp .env.example .env
make up

Interface Airflow : http://localhost:8080 (airflow/airflow)
Dashboard local : streamlit run scripts/dashboard.py

## Tests

make test
