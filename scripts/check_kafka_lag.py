from kafka import KafkaConsumer, TopicPartition
import os

def check_lag(topic="crypto-trades", group_id="crypto-trades-consumer-group"):
    bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_server, group_id=group_id)

    partitions = consumer.partitions_for_topic(topic)
    topic_partitions = [TopicPartition(topic, p) for p in partitions]

    end_offsets = consumer.end_offsets(topic_partitions)
    committed = {tp: consumer.committed(tp) or 0 for tp in topic_partitions}

    total_lag = 0
    for tp in topic_partitions:
        lag = end_offsets[tp] - committed[tp]
        total_lag += lag
        print(f"Partition {tp.partition} : lag = {lag}")

    print(f"Lag total : {total_lag}")
    consumer.close()

if __name__ == "__main__":
    check_lag()
