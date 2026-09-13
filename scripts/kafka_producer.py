import json
from kafka import KafkaProducer


def get_producer():
    """
    Crée un producer Kafka, configuré pour se connecter
    au broker local (venv) ou au conteneur (Airflow) selon le contexte.
    """
    import os
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

def publish_stock_prices(df, producer, topic="stock-prices"):
    """
    Publie chaque ligne d'un DataFrame comme un message Kafka séparé.
    """
    records = df.reset_index().to_dict(orient="records")
    for record in records:
        producer.send(topic, value=record)
    producer.flush()
    print(f"{len(records)} messages publiés sur le topic '{topic}'")
