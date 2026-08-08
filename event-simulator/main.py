import json
import time
import random
from dataclasses import asdict

from events import EventGenerator
from config import RATES


def main():
    # Ask user to select load level
    level = input("Select load level (low/medium/high): ").strip().lower()
    while level not in RATES:
        level = input("Invalid. Choose low, medium, or high: ").strip().lower()

    rate = RATES[level]
    print(f"Starting event simulator with load level: {level} ({rate} msg/sec per topic)")

    generator = EventGenerator()

    # Map topic names to generator methods
    topics = {
        "sales": generator.generate_sale,
        "orders": generator.generate_orders,
        "logs": generator.generate_log,
    }

    while True:
        # Pick a random topic
        topic = random.choice(list(topics.keys()))
        events = topics[topic]()  # returns list of events

        for event in events:
            # Convert dataclass to dict, then to JSON
            message = json.dumps(asdict(event))
            # For now: Write into file. Later: send to Kafka.
            with open("test.txt", "a") as f:
                f.write(f"[{topic}] {message}\n")

        # Sleep before next batch
        time.sleep(1.0 / rate)


if __name__ == "__main__":
    main()