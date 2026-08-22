
from kafka import KafkaProducer

import json
import os
import time
import random
from dataclasses import asdict

from events import EventGenerator
from config import RATES


def wait_for_kafka(bootstrap_servers: str, max_retries: int = 30, delay: int = 5) -> KafkaProducer:
    """Wait until Kafka is ready, then return a producer."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {bootstrap_servers}")
            return producer
        except Exception as e:
            print(f"Kafka not ready ({e}), retrying ({attempt}/{max_retries})...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries")


def main():
    # Ask user to select load level

    level = os.getenv("LOAD_LEVEL", "low").strip().lower()
    if level not in RATES:
        print(f"Unknown LOAD_LEVEL '{level}', falling back to 'low'")
        level = "low"

    rate = RATES[level]
    print(f"Starting event simulator with load level: {level} ({rate} msg/sec per topic)")

    # Connect to Kafka (wait if not ready yet)
    producer = wait_for_kafka(bootstrap_servers="kafka:9092")

    generator = EventGenerator()

    # Map topic names to generator methods
    topics = {
        "sales": generator.generate_sale,
        "orders": generator.generate_orders,
        "logs": generator.generate_log,
    }

    try:
        while True:
            # Pick a random topic
            topic = random.choice(list(topics.keys()))
            events = topics[topic]()  # returns list of events

            for event in events:
                message = asdict(event)
                producer.send(topic, message)
                print(f"[{topic}] {json.dumps(message)}")

            # Sleep before next batch
            time.sleep(1.0 / rate)
    finally:
        producer.close()


if __name__ == "__main__":
    main()