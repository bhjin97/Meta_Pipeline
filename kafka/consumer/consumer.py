import json
from pathlib import Path
from kafka import KafkaConsumer


BOOTSTRAP_SERVERS = ["kafka:29092"]

TOPICS = ["order-events", "delivery-events", "review-events"]

OUTPUT_BASE = Path("/app/data/sink")


def create_consumer():
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",  # 처음부터 읽기
        enable_auto_commit=True,
        group_id="file-sink-consumer",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def get_file_handles():
    OUTPUT_BASE.mkdir(parents=True, exist_ok=True)

    return {
        "order-events": open(OUTPUT_BASE / "order_events.jsonl", "a", encoding="utf-8"),
        "delivery-events": open(OUTPUT_BASE / "delivery_events.jsonl", "a", encoding="utf-8"),
        "review-events": open(OUTPUT_BASE / "review_events.jsonl", "a", encoding="utf-8"),
    }


def main():
    print("Kafka Consumer 시작")

    consumer = create_consumer()
    files = get_file_handles()

    try:
        for message in consumer:
            topic = message.topic
            data = message.value

            files[topic].write(json.dumps(data, ensure_ascii=False) + "\n")

    except KeyboardInterrupt:
        print("Consumer 종료")

    finally:
        for f in files.values():
            f.close()


if __name__ == "__main__":
    main()