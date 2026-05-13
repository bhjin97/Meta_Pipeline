import json
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ["kafka:29092"]

EVENT_SOURCES = {
    "order-events": Path("/app/origin_data_processing/data/event_source/order_events.jsonl"),
    "delivery-events": Path("/app/origin_data_processing/data/event_source/delivery_events.jsonl"),
    "review-events": Path("/app/origin_data_processing/data/event_source/review_events.jsonl"),
}

KST = ZoneInfo("Asia/Seoul")

NORMAL_SEND_INTERVAL_SECONDS = 0.05
BATCH_TIME_SEND_INTERVAL_SECONDS = 0.5

BATCH_SLOW_START_HOUR = 2
BATCH_SLOW_END_HOUR = 6


def get_send_interval_seconds():
    now_kst = datetime.now(KST)

    if BATCH_SLOW_START_HOUR <= now_kst.hour < BATCH_SLOW_END_HOUR:
        return BATCH_TIME_SEND_INTERVAL_SECONDS

    return NORMAL_SEND_INTERVAL_SECONDS


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
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )


def main():
    print("Kafka Producer2 시작", flush=True)
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}", flush=True)

    producer = create_producer()
    total_count = 0

    try:
        for topic, file_path in EVENT_SOURCES.items():
            print(f"[SEND START] {topic} from {file_path}", flush=True)

            topic_count = 0

            for event in read_jsonl(file_path):
                key = event.get("order_id") or event.get("review_id") or event.get("event_id")

                producer.send(
                    topic=topic,
                    key=key,
                    value=event,
                )

                total_count += 1
                topic_count += 1

                if topic_count % 1000 == 0:
                    interval = get_send_interval_seconds()
                    print(
                        f"[PROGRESS] topic={topic} sent={topic_count:,} total={total_count:,} interval={interval}s",
                        flush=True,
                    )
                    producer.flush()

                time.sleep(get_send_interval_seconds())

            print(f"[SEND DONE] {topic}: {topic_count:,} events", flush=True)

        producer.flush()

    finally:
        producer.close()

    print(f"[DONE] total sent: {total_count:,}", flush=True)


if __name__ == "__main__":
    main()