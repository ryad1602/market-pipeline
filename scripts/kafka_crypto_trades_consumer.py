import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from db import get_engine
from sqlalchemy import text

def get_consumer(topic="crypto-trades"):
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        group_id="crypto-trades-consumer-group",
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
    """
    INSERT ... ON CONFLICT DO NOTHING : si ce trade exact (même symbole,
    même trade_time) existe déjà, on l'ignore silencieusement.
    C'est l'idempotence du chantier 2 : relancer ne crée aucun doublon.
    """
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
    print("En écoute sur 'crypto-trades'... (Ctrl+C pour arrêter)")

    count = 0
    for message in consumer:
        data = message.value
        save_trade(engine, data)
        count += 1
        if count % 50 == 0:
            print(f"{count} trades enregistrés...")
