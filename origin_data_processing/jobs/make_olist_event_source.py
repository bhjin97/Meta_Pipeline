import json
from pathlib import Path

import numpy as np
import pandas as pd


RAW_PATH = Path("origin_data_processing/data/raw")
OUTPUT_PATH = Path("origin_data_processing/data/event_source")

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


def format_datetime_columns(df, columns):
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def to_event_time(series):
    return series.dt.strftime("%Y-%m-%d %H:%M:%S")


def save_jsonl(df, path):
    with open(path, "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    orders = pd.read_csv(RAW_PATH / "olist_orders_dataset.csv")
    reviews = pd.read_csv(RAW_PATH / "olist_order_reviews_dataset.csv")

    orders = format_datetime_columns(
        orders,
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )

    reviews = format_datetime_columns(
        reviews,
        [
            "review_creation_date",
            "review_answer_timestamp",
        ],
    )

    # =========================
    # 1. ORDER EVENTS
    # =========================

    order_created = orders.dropna(subset=["order_purchase_timestamp"]).copy()
    order_created["event_type"] = "ORDER_CREATED"
    order_created["event_time_dt"] = order_created["order_purchase_timestamp"]
    order_created["event_time"] = to_event_time(order_created["event_time_dt"])
    order_created["event_id"] = order_created["event_type"] + "_" + order_created["order_id"]

    order_approved = orders.dropna(subset=["order_approved_at"]).copy()
    order_approved["event_type"] = "ORDER_APPROVED"
    order_approved["event_time_dt"] = order_approved["order_approved_at"]
    order_approved["event_time"] = to_event_time(order_approved["event_time_dt"])
    order_approved["event_id"] = order_approved["event_type"] + "_" + order_approved["order_id"]

    order_canceled = orders[
        (orders["order_status"] == "canceled")
        & (orders["order_purchase_timestamp"].notna())
    ].copy()

    order_canceled["event_type"] = "ORDER_CANCELED"

    rng = np.random.default_rng(seed=42)

    cancel_base_time = order_canceled["order_approved_at"].fillna(
        order_canceled["order_purchase_timestamp"]
    )

    random_delay_seconds = rng.integers(
        low=60,
        high=3600,
        size=len(order_canceled),
    )

    order_canceled["event_time_dt"] = (
        cancel_base_time
        + pd.to_timedelta(random_delay_seconds, unit="s")
    )
    order_canceled["event_time"] = to_event_time(order_canceled["event_time_dt"])
    order_canceled["event_id"] = order_canceled["event_type"] + "_" + order_canceled["order_id"]

    order_events = pd.concat(
        [order_created, order_approved, order_canceled],
        ignore_index=True,
    )

    priority_map = {
        "ORDER_CREATED": 1,
        "ORDER_APPROVED": 2,
        "ORDER_CANCELED": 3,
    }

    order_events["priority"] = order_events["event_type"].map(priority_map)

    order_events = (
        order_events[
            [
                "event_id",
                "event_type",
                "event_time",
                "event_time_dt",
                "priority",
                "order_id",
                "customer_id",
                "order_status",
            ]
        ]
        .sort_values(["event_time_dt", "priority"])
        .drop(columns=["event_time_dt", "priority"])
    )

    # =========================
    # 2. DELIVERY EVENTS
    # =========================

    valid_delivery_orders = orders[orders["order_status"] != "canceled"].copy()

    delivery_started = valid_delivery_orders.dropna(
        subset=["order_delivered_carrier_date"]
    ).copy()
    delivery_started["event_type"] = "DELIVERY_STARTED"
    delivery_started["event_time_dt"] = delivery_started["order_delivered_carrier_date"]
    delivery_started["event_time"] = to_event_time(delivery_started["event_time_dt"])
    delivery_started["event_id"] = (
        delivery_started["event_type"] + "_" + delivery_started["order_id"]
    )

    delivery_completed = valid_delivery_orders.dropna(
        subset=["order_delivered_customer_date"]
    ).copy()
    delivery_completed["event_type"] = "DELIVERY_COMPLETED"
    delivery_completed["event_time_dt"] = delivery_completed["order_delivered_customer_date"]
    delivery_completed["event_time"] = to_event_time(delivery_completed["event_time_dt"])
    delivery_completed["event_id"] = (
        delivery_completed["event_type"] + "_" + delivery_completed["order_id"]
    )

    delivery_events = pd.concat(
        [delivery_started, delivery_completed],
        ignore_index=True,
    )

    delivery_events["order_estimated_delivery_date"] = to_event_time(
        delivery_events["order_estimated_delivery_date"]
    )

    delivery_events = (
        delivery_events[
            [
                "event_id",
                "event_type",
                "event_time",
                "event_time_dt",
                "order_id",
                "customer_id",
                "order_status",
                "order_estimated_delivery_date",
            ]
        ]
        .sort_values("event_time_dt")
        .drop(columns=["event_time_dt"])
    )

    # =========================
    # 3. REVIEW EVENTS
    # =========================

    reviews = reviews.merge(
        orders[["order_id", "customer_id"]],
        on="order_id",
        how="left",
    )

    review_events = reviews.dropna(subset=["review_creation_date"]).copy()
    review_events["event_type"] = "REVIEW_CREATED"
    review_events["event_time_dt"] = review_events["review_creation_date"]
    review_events["event_time"] = to_event_time(review_events["event_time_dt"])
    review_events["event_id"] = (
        review_events["event_type"] + "_" + review_events["review_id"]
    )

    review_events = (
        review_events[
            [
                "event_id",
                "event_type",
                "event_time",
                "event_time_dt",
                "review_id",
                "order_id",
                "customer_id",
                "review_score",
            ]
        ]
        .sort_values("event_time_dt")
        .drop(columns=["event_time_dt"])
    )

    # =========================
    # 4. SAVE
    # =========================

    save_jsonl(order_events, OUTPUT_PATH / "order_events.jsonl")
    save_jsonl(delivery_events, OUTPUT_PATH / "delivery_events.jsonl")
    save_jsonl(review_events, OUTPUT_PATH / "review_events.jsonl")

    print("이벤트 생성 완료")
    print(f"order_events: {len(order_events):,}")
    print(f"delivery_events: {len(delivery_events):,}")
    print(f"review_events: {len(review_events):,}")


if __name__ == "__main__":
    main()