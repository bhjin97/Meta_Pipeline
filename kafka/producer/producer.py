import json
import time
from pathlib import Path

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ["kafka:29092"]

EVENT_SOURCES = {
    "order-events": Path("/app/origin_data_processing/data/event_source/order_events.jsonl"),
    "delivery-events": Path("/app/origin_data_processing/data/event_source/delivery_events.jsonl"),
    "review-events": Path("/app/origin_data_processing/data/event_source/review_events.jsonl"),
}

SEND_INTERVAL_SECONDS = 0.1


def read_jsonl(file_path):
    if not file_path.exists():
        raise FileNotFoundError(f"이벤트 파일을 찾을 수 없습니다: {file_path}")

    with file_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def send_events(producer, topic, file_path):
    print(f"\n[START] topic={topic}")
    print(f"[FILE] {file_path}")

    count = 0

    for event in read_jsonl(file_path):
        key = event.get("order_id") or event.get("review_id") or event.get("event_id")

        producer.send(
            topic=topic,
            key=key,
            value=event,
        )

        count += 1

        if count % 1000 == 0:
            print(f"[{topic}] sent {count:,} events")

        time.sleep(SEND_INTERVAL_SECONDS)

    producer.flush()
    print(f"[DONE] topic={topic}, total={count:,}")


def main():
    print("Kafka Producer 시작")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")

    producer = create_producer()

    try:
        for topic, file_path in EVENT_SOURCES.items():
            send_events(producer, topic, file_path)
    finally:
        producer.flush()
        producer.close()

    print("\n모든 이벤트 전송 완료")


if __name__ == "__main__":
    main()