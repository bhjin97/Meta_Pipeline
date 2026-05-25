import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Producer


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--input-file",
        required=True,
        help="Path to all_events_sorted.jsonl",
    )
    parser.add_argument(
        "--bootstrap-server",
        default="kafka:29092",
        help="Kafka bootstrap server",
    )
    parser.add_argument(
        "--events-per-sec",
        type=float,
        default=100.0,
        help="Target events per second",
    )
    parser.add_argument(
        "--start-offset",
        type=int,
        default=0,
        help="Line offset to start from",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Maximum number of events to send",
    )

    return parser.parse_args()


def delivery_report(err, msg):
    if err is not None:
        print(f"[DELIVERY FAILED] topic={msg.topic()} error={err}")


def main():
    args = parse_args()

    input_file = Path(args.input_file)

    if not input_file.exists():
        raise FileNotFoundError(f"input file not found: {input_file}")

    producer = Producer({
        "bootstrap.servers": args.bootstrap_server,
        "client.id": "load-test-producer",
        "acks": "all",
        "linger.ms": 10,
        "batch.num.messages": 10000,
    })

    sleep_interval = 1.0 / args.events_per_sec if args.events_per_sec > 0 else 0

    sent_count = 0
    skipped_count = 0
    start_time = time.time()
    last_log_time = start_time

    print("load test producer started")
    print(f"input_file: {input_file}")
    print(f"bootstrap_server: {args.bootstrap_server}")
    print(f"events_per_sec: {args.events_per_sec}")
    print(f"start_offset: {args.start_offset}")
    print(f"max_events: {args.max_events}")

    with input_file.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f):
            if line_no < args.start_offset:
                skipped_count += 1
                continue

            if args.max_events is not None and sent_count >= args.max_events:
                break

            line = line.strip()
            if not line:
                continue

            row = json.loads(line)
            topic = row["topic"]
            event = row["event"]

            value = json.dumps(event, ensure_ascii=False).encode("utf-8")
            key = event.get("order_id", event.get("event_id", "")).encode("utf-8")

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report,
            )

            producer.poll(0)

            sent_count += 1

            if sleep_interval > 0:
                time.sleep(sleep_interval)

            now = time.time()
            if now - last_log_time >= 10:
                elapsed = now - start_time
                current_eps = sent_count / elapsed if elapsed > 0 else 0

                print(
                    f"[PROGRESS] sent={sent_count:,}, "
                    f"skipped={skipped_count:,}, "
                    f"elapsed={elapsed:.1f}s, "
                    f"avg_eps={current_eps:.2f}"
                )

                last_log_time = now

    producer.flush()

    total_elapsed = time.time() - start_time
    avg_eps = sent_count / total_elapsed if total_elapsed > 0 else 0

    print("load test producer finished")
    print(f"sent_count: {sent_count:,}")
    print(f"skipped_count: {skipped_count:,}")
    print(f"elapsed_sec: {total_elapsed:.2f}")
    print(f"avg_eps: {avg_eps:.2f}")


if __name__ == "__main__":
    main()