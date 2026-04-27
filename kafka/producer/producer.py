import json
import time
from pathlib import Path
from kafka import KafkaProducer


BOOTSTRAP_SERVERS = ["kafka:29092"]
TOPIC_NAME = "order-events"

# docker-compose에서 kafka/ 폴더를 /app으로 마운트했다면
# /app 기준으로 ../data/events 접근이 안 될 수 있음.
# 그래서 compose volume 구조에 따라 경로를 맞춰야 함.
EVENT_FILE_PATH = Path("/app/data/events/order_events.jsonl")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: str(key).encode("utf-8") if key is not None else None,
    )


def read_jsonl(file_path: Path):
    if not file_path.exists():
        raise FileNotFoundError(f"이벤트 파일을 찾을 수 없습니다: {file_path}")

    with file_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line:
                continue

            yield json.loads(line)


def main():
    producer = create_producer()

    print(f"Kafka Producer 시작")
    print(f"Topic: {TOPIC_NAME}")
    print(f"Event file: {EVENT_FILE_PATH}")

    count = 0

    for event in read_jsonl(EVENT_FILE_PATH):
        key = event.get("order_id")

        producer.send(
            TOPIC_NAME,
            key=key,
            value=event,
        )

        count += 1
        print(f"[SEND] {count} | key={key} | event={event}")

        time.sleep(1)

    producer.flush()
    producer.close()

    print(f"전송 완료: 총 {count}개 이벤트")


if __name__ == "__main__":
    main()