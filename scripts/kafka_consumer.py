import json
import os
from kafka import KafkaConsumer

def get_consumer(topic="stock-prices"):
    """
    Crée un consumer Kafka qui lit un topic en continu.
    """
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_server,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="stock-price-consumer-group",
    )

if __name__ == "__main__":
    consumer = get_consumer()
    print("En écoute sur le topic 'stock-prices'... (Ctrl+C pour arrêter)")

    for message in consumer:
        data = message.value
        print(f"Reçu : {data['ticker']} — clôture: {data.get('Close')}")
