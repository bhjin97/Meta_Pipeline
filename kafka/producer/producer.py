import json
import time
from pathlib import Path

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ["kafka:29092"]

INPUT_FILE = Path(
    "/app/origin_data_processing/data/event_source/all_events_sorted.jsonl"
)

SEND_INTERVAL_SECONDS = 0.05


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False
        ).encode("utf-8"),
    )


def read_jsonl(path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if line:
                yield json.loads(line)


def main():
    print("Kafka Producer Start")

    producer = create_producer()

    count = 0

    try:
        for row in read_jsonl(INPUT_FILE):
            topic = row["topic"]
            event = row["event"]

            key = (
                event.get("order_id")
                or event.get("review_id")
                or event.get("event_id")
            )

            producer.send(
                topic=topic,
                key=key,
                value=event,
            )

            count += 1

            if count % 1000 == 0:
                print(f"[PROGRESS] sent {count:,} events")
                producer.flush()

            time.sleep(SEND_INTERVAL_SECONDS)

        producer.flush()

    finally:
        producer.close()

    print(f"[DONE] total sent: {count:,}")


if __name__ == "__main__":
    main()

