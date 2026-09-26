import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel, ValidationError
from db import get_engine
from sqlalchemy import text

class TradeMessage(BaseModel):
    """Définit la forme attendue d'un message de trade, version 1."""
    schema_version: int
    symbol: str
    price: float
    quantity: float
    trade_time: int
    received_at: str

def get_consumer(topic="crypto-trades"):
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # on commit nous-mêmes, après écriture réussie
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="crypto-trades-consumer-group",
    )

def get_dlq_producer():
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

def ensure_table_exists(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw_crypto_trades (
                symbol TEXT NOT NULL,
                price FLOAT NOT NULL,
                quantity FLOAT NOT NULL,
                trade_time BIGINT NOT NULL,
                received_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (symbol, trade_time)
            )
        """))
        conn.commit()

def save_trade(engine, data):
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO raw_crypto_trades (symbol, price, quantity, trade_time, received_at)
                VALUES (:symbol, :price, :quantity, :trade_time, :received_at)
                ON CONFLICT (symbol, trade_time) DO NOTHING
            """),
            data
        )
        conn.commit()

if __name__ == "__main__":
    engine = get_engine()
    ensure_table_exists(engine)

    consumer = get_consumer()
    dlq_producer = get_dlq_producer()

    print("En écoute sur 'crypto-trades'... (Ctrl+C pour arrêter)")

    count = 0
    for message in consumer:
        raw_data = message.value

        try:
            # 1. Validation du schéma
            validated = TradeMessage(**raw_data)

            # 2. Écriture en base
            save_trade(engine, validated.model_dump())

            # 3. Commit manuel — seulement si tout a réussi
            consumer.commit()

            count += 1
            if count % 50 == 0:
                print(f"{count} trades enregistrés...")

        except (ValidationError, Exception) as e:
            # Message illisible ou erreur d'écriture : direction la dead letter queue
            dlq_producer.send("crypto-trades-dlq", value={
                "original_message": raw_data,
                "error": str(e),
                "failed_at": datetime.now(timezone.utc).isoformat(),
            })
            dlq_producer.flush()
            consumer.commit()  # on commit quand même, pour ne pas rester bloqué sur ce message
            print(f"Message envoyé en DLQ : {e}")
