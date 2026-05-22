import json
from pathlib import Path
from datetime import datetime

EVENT_SOURCE_PATH = Path("origin_data_processing/data/event_source")

INPUT_FILES = {
    "order-events": EVENT_SOURCE_PATH / "order_events.jsonl",
    "delivery-events": EVENT_SOURCE_PATH / "delivery_events.jsonl",
    "review-events": EVENT_SOURCE_PATH / "review_events.jsonl",
}

OUTPUT_FILE = EVENT_SOURCE_PATH / "all_events_sorted.jsonl"

EVENT_PRIORITY = {
    "ORDER_CREATED": 1,
    "ORDER_APPROVED": 2,
    "ORDER_CANCELED": 3,
    "DELIVERY_STARTED": 4,
    "DELIVERY_COMPLETED": 5,
    "REVIEW_CREATED": 6,
}


def parse_event_time(event):
    return datetime.fromisoformat(event["event_time"])


def read_jsonl(path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main():
    all_events = []

    for topic, file_path in INPUT_FILES.items():
        for event in read_jsonl(file_path):
            all_events.append({
                "topic": topic,
                "event": event,
            })

    all_events.sort(
        key=lambda row: (
            parse_event_time(row["event"]),
            EVENT_PRIORITY.get(row["event"].get("event_type"), 999),
            row["event"].get("event_id", ""),
        )
    )

    with OUTPUT_FILE.open("w", encoding="utf-8") as f:
        for row in all_events:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"saved: {OUTPUT_FILE}")
    print(f"total events: {len(all_events):,}")


if __name__ == "__main__":
    main()