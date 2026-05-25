import argparse
import json
from pathlib import Path
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


RAW_PATH = Path("data/raw")
OUTPUT_BASE_PATH = Path("origin_data_processing/data/load_test")

EVENT_PRIORITY = {
    "ORDER_CREATED": 1,
    "ORDER_APPROVED": 2,
    "DELIVERY_STARTED": 3,
    "DELIVERY_COMPLETED": 4,
    "REVIEW_CREATED": 5,
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, required=True)
    parser.add_argument("--test-run-id", type=str, required=True)
    parser.add_argument("--start-date", type=str, default="2018-01-01")
    parser.add_argument("--end-date", type=str, default="2018-12-31")
    parser.add_argument("--review-rate", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def random_datetimes(rng, start_date, end_date, size):
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)
    total_seconds = int((end - start).total_seconds())

    offsets = rng.integers(0, total_seconds, size=size)
    return [start + timedelta(seconds=int(offset)) for offset in offsets]


def to_str(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def main():
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    output_dir = OUTPUT_BASE_PATH / args.test_run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    orders_df = pd.read_csv(RAW_PATH / "olist_orders_dataset.csv")
    order_items_df = pd.read_csv(RAW_PATH / "olist_order_items_dataset.csv")

    customer_ids = orders_df["customer_id"].dropna().unique()

    sampled_customers = rng.choice(customer_ids, size=args.orders, replace=True)
    sampled_items = order_items_df.sample(
        n=args.orders,
        replace=True,
        random_state=args.seed,
    ).reset_index(drop=True)

    base_times = random_datetimes(
        rng=rng,
        start_date=args.start_date,
        end_date=args.end_date,
        size=args.orders,
    )

    all_events = []
    load_test_items = []

    for i in range(args.orders):
        order_no = i + 1
        order_id = f"test_order_{order_no:09d}"
        customer_id = sampled_customers[i]

        created_time = base_times[i]
        approved_time = created_time + timedelta(minutes=int(rng.integers(1, 120)))
        delivery_started_time = approved_time + timedelta(days=int(rng.integers(1, 4)))
        delivery_completed_time = delivery_started_time + timedelta(days=int(rng.integers(2, 11)))
        estimated_delivery_time = created_time + timedelta(days=int(rng.integers(5, 21)))
        shipping_limit_time = created_time + timedelta(days=int(rng.integers(1, 6)))

        item = sampled_items.iloc[i]

        load_test_items.append({
            "order_id": order_id,
            "order_item_id": 1,
            "product_id": item["product_id"],
            "seller_id": item["seller_id"],
            "shipping_limit_date": to_str(shipping_limit_time),
            "price": float(item["price"]),
            "freight_value": float(item["freight_value"]),
        })

        order_status = "delivered"

        events = [
            {
                "topic": "order-events",
                "event": {
                    "event_id": f"ORDER_CREATED_{order_id}",
                    "event_type": "ORDER_CREATED",
                    "event_time": to_str(created_time),
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_status": order_status,
                },
            },
            {
                "topic": "order-events",
                "event": {
                    "event_id": f"ORDER_APPROVED_{order_id}",
                    "event_type": "ORDER_APPROVED",
                    "event_time": to_str(approved_time),
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_status": order_status,
                },
            },
            {
                "topic": "delivery-events",
                "event": {
                    "event_id": f"DELIVERY_STARTED_{order_id}",
                    "event_type": "DELIVERY_STARTED",
                    "event_time": to_str(delivery_started_time),
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_status": order_status,
                    "order_estimated_delivery_date": to_str(estimated_delivery_time),
                },
            },
            {
                "topic": "delivery-events",
                "event": {
                    "event_id": f"DELIVERY_COMPLETED_{order_id}",
                    "event_type": "DELIVERY_COMPLETED",
                    "event_time": to_str(delivery_completed_time),
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "order_status": order_status,
                    "order_estimated_delivery_date": to_str(estimated_delivery_time),
                },
            },
        ]

        if rng.random() < args.review_rate:
            review_id = f"test_review_{order_no:09d}"
            review_time = delivery_completed_time + timedelta(days=int(rng.integers(1, 8)))

            events.append({
                "topic": "review-events",
                "event": {
                    "event_id": f"REVIEW_CREATED_{review_id}",
                    "event_type": "REVIEW_CREATED",
                    "event_time": to_str(review_time),
                    "review_id": review_id,
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "review_score": int(rng.integers(1, 6)),
                },
            })

        all_events.extend(events)

    all_events.sort(
        key=lambda row: (
            datetime.fromisoformat(row["event"]["event_time"]),
            EVENT_PRIORITY.get(row["event"]["event_type"], 999),
            row["event"]["event_id"],
        )
    )

    events_path = output_dir / "all_events_sorted.jsonl"
    with events_path.open("w", encoding="utf-8") as f:
        for row in all_events:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    load_test_items_df = pd.DataFrame(load_test_items)
    order_items_path = output_dir / "order_items.parquet"
    load_test_items_df.to_parquet(order_items_path, index=False)

    print("load test data generated")
    print(f"test_run_id: {args.test_run_id}")
    print(f"orders: {args.orders:,}")
    print(f"events: {len(all_events):,}")
    print(f"order_items: {len(load_test_items_df):,}")
    print(f"events_path: {events_path}")
    print(f"order_items_path: {order_items_path}")


if __name__ == "__main__":
    main()