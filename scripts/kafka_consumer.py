import json
import os
from datetime import datetime, timezone
from kafka import KafkaConsumer
from db import get_engine
from sqlalchemy import text

def get_consumer(topic="stock-prices"):
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="stock-price-consumer-group",
    )

def ensure_table_exists(engine):
    """
    Crée la table streamed_stock_prices si elle n'existe pas encore.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS streamed_stock_prices (
                id SERIAL PRIMARY KEY,
                ticker TEXT,
                close_price FLOAT,
                received_at TIMESTAMPTZ
            )
        """))
        conn.commit()

def save_message_to_db(engine, data):
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO streamed_stock_prices (ticker, close_price, received_at)
                VALUES (:ticker, :close_price, :received_at)
            """),
            {
                "ticker": data.get("ticker"),
                "close_price": data.get("Close"),
                "received_at": datetime.now(timezone.utc),
            }
        )
        conn.commit()

if __name__ == "__main__":
    engine = get_engine()
    ensure_table_exists(engine)

    consumer = get_consumer()
    print("En écoute sur le topic 'stock-prices'... (Ctrl+C pour arrêter)")

    for message in consumer:
        data = message.value
        save_message_to_db(engine, data)
        print(f"Enregistré : {data['ticker']} — clôture: {data.get('Close')}")
