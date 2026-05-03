import json
import time
from pathlib import Path
from datetime import datetime

from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ["kafka:29092"]

EVENT_SOURCES = {
    "order-events": Path("/app/origin_data_processing/data/event_source/order_events.jsonl"),
    "delivery-events": Path("/app/origin_data_processing/data/event_source/delivery_events.jsonl"),
    "review-events": Path("/app/origin_data_processing/data/event_source/review_events.jsonl"),
}

SEND_INTERVAL_SECONDS = 0.05  # 조금 빠르게 조정


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


def parse_event_time(event):
    # event_time 문자열 → datetime 변환
    return datetime.fromisoformat(event["event_time"])


def load_all_events():
    all_events = []

    for topic, file_path in EVENT_SOURCES.items():
        print(f"[LOAD] {topic} from {file_path}")

        for event in read_jsonl(file_path):
            if "event_time" not in event:
                continue

            all_events.append((topic, event))

    print(f"[LOAD DONE] total events: {len(all_events):,}")
    return all_events


def main():
    print("Kafka Producer 시작")
    print(f"Bootstrap servers: {BOOTSTRAP_SERVERS}")

    producer = create_producer()

    # 1. 모든 이벤트 로드
    all_events = load_all_events()

    # 2. event_time 기준 정렬 
    print("[SORT] event_time 기준 정렬 중...")
    all_events.sort(key=lambda x: parse_event_time(x[1]))

    print("[SORT DONE]")

    # 3. 전송
    print("[START SENDING]")
    count = 0

    try:
        for topic, event in all_events:
            key = event.get("order_id") or event.get("review_id") or event.get("event_id")

            producer.send(
                topic=topic,
                key=key,
                value=event,
            )

            count += 1

            if count % 1000 == 0:
                print(f"[PROGRESS] sent {count:,} events")

            time.sleep(SEND_INTERVAL_SECONDS)

        producer.flush()

    finally:
        producer.close()

    print(f"[DONE] total sent: {count:,}")


if __name__ == "__main__":
    main()