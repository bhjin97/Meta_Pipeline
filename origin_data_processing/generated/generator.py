import argparse
import json
import os
import re
import shutil
import tempfile
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .distributions import (
    DELAYED_RATE,
    DELIVERED_RATE,
    REVIEW_RATE,
    sample_answer_delay,
    sample_cancel_delay,
    sample_empirical_delay,
    sample_estimated_delivery,
    sample_installments,
    sample_item_count,
    sample_payment_type,
    sample_review_delay,
    sample_review_score,
)
from .schemas import ENTITY_SCHEMAS, EVENT_FIELDS_BY_TOPIC, EVENT_PRIORITY
from .validation import validate_run


RUN_ID_PATTERN = re.compile(r"^gen_\d{8}_\d{3}$")
REFERENCE_FILES = {
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
EXPECTED_OUTPUT_FILES = (
    "entities/orders.parquet",
    "entities/order_items.parquet",
    "entities/payments.parquet",
    "entities/reviews.parquet",
    "events/order_events.jsonl",
    "events/delivery_events.jsonl",
    "events/review_events.jsonl",
    "events/all_events_sorted.jsonl",
)


def parse_args():
    parser = argparse.ArgumentParser(description="Generate one immutable local run dataset")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--order-count", required=True, type=int)
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    parser.add_argument("--reference-root", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--output-root", type=Path,
        default=Path("origin_data_processing/data/generated/runs"),
    )
    return parser.parse_args()


def validate_inputs(args):
    if not RUN_ID_PATTERN.fullmatch(args.run_id):
        raise ValueError("invalid run_id: expected gen_YYYYMMDD_NNN")
    try:
        datetime.strptime(args.run_id[4:12], "%Y%m%d")
    except ValueError as exc:
        raise ValueError("invalid run_id: YYYYMMDD must be a valid date") from exc
    if args.seed < 0:
        raise ValueError("invalid seed: must be non-negative")
    if args.order_count <= 0:
        raise ValueError("invalid order_count: must be greater than zero")
    if args.start_date > args.end_date:
        raise ValueError("invalid date range: start_date must be <= end_date")
    run_dir = args.output_root / args.run_id
    if run_dir.exists():
        raise FileExistsError(f"immutable run directory already exists: {run_dir}")
    for file_name in REFERENCE_FILES.values():
        path = args.reference_root / file_name
        if not path.is_file():
            raise FileNotFoundError(f"reference file missing: {path}")


def _positive_seconds(later, earlier):
    values = (later - earlier).dt.total_seconds()
    result = np.sort(values[values > 0].astype(np.int64).to_numpy())
    if result.size == 0:
        raise ValueError("reference lifecycle duration pool is empty")
    return result


def load_reference_data(reference_root):
    frames = {
        name: pd.read_csv(reference_root / file_name)
        for name, file_name in REFERENCE_FILES.items()
    }
    orders = frames["orders"]
    for column in (
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
    ):
        orders[column] = pd.to_datetime(orders[column], errors="coerce")

    customers = (
        frames["customers"].dropna(subset=["customer_id"])
        .sort_values(["customer_id"], kind="mergesort").reset_index(drop=True)
    )
    items = (
        frames["order_items"]
        .dropna(subset=["product_id", "seller_id", "price", "freight_value"])
        .sort_values(
            ["order_id", "order_item_id", "product_id", "seller_id"],
            kind="mergesort",
        ).reset_index(drop=True)
    )
    products = (
        frames["products"].dropna(subset=["product_id"])
        .sort_values(["product_id"], kind="mergesort").reset_index(drop=True)
    )
    sellers = (
        frames["sellers"].dropna(subset=["seller_id"])
        .sort_values(["seller_id"], kind="mergesort").reset_index(drop=True)
    )
    installments = np.sort(
        frames["payments"].loc[
            (frames["payments"]["payment_type"] == "credit_card")
            & frames["payments"]["payment_installments"].between(1, 10),
            "payment_installments",
        ].astype(np.int64).to_numpy()
    )
    if any(frame.empty for frame in (customers, items, products, sellers)):
        raise ValueError("one or more canonical reference pools are empty")
    if installments.size == 0:
        raise ValueError("credit-card installment reference pool is empty")

    return {
        "customers": customers["customer_id"].astype(str).to_numpy(),
        "items": items[["product_id", "seller_id", "price", "freight_value"]],
        "products": set(products["product_id"].astype(str)),
        "sellers": set(sellers["seller_id"].astype(str)),
        "installments": installments,
        "approval_seconds": _positive_seconds(
            orders["order_approved_at"], orders["order_purchase_timestamp"]
        ),
        "carrier_seconds": _positive_seconds(
            orders["order_delivered_carrier_date"], orders["order_approved_at"]
        ),
        "delivery_seconds": _positive_seconds(
            orders["order_delivered_customer_date"],
            orders["order_delivered_carrier_date"],
        ),
    }


def _event(topic, event_type, event_time, order, **extra):
    event_id_subject = extra.get("review_id", order["order_id"])
    payload = {
        "event_id": f"{event_type}_{event_id_subject}",
        "event_type": event_type,
        "event_time": event_time,
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "order_status": order["order_status"],
        **extra,
    }
    fields = EVENT_FIELDS_BY_TOPIC[topic]
    return {"topic": topic, "event": {field: payload[field] for field in fields}}


def generate_run(run_id, seed, order_count, start_date, end_date, references):
    rng = np.random.default_rng(seed)
    entities = {name: [] for name in ENTITY_SCHEMAS}
    events = []
    review_sequence = 0
    start_dt = datetime.combine(start_date, time.min)
    end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min)
    range_seconds = int((end_exclusive - start_dt).total_seconds())

    for sequence in range(1, order_count + 1):
        order_id = f"{run_id}_order_{sequence:08d}"
        customer_id = str(rng.choice(references["customers"]))
        purchase = start_dt + timedelta(seconds=int(rng.integers(0, range_seconds)))
        delivered_lifecycle = bool(rng.random() < DELIVERED_RATE)

        if delivered_lifecycle:
            approved = purchase + sample_empirical_delay(rng, references["approval_seconds"])
            carrier = approved + sample_empirical_delay(rng, references["carrier_seconds"])
            delivered = carrier + sample_empirical_delay(rng, references["delivery_seconds"])
            is_delayed = bool(rng.random() < DELAYED_RATE)
            status = "delivered"
        else:
            approved = carrier = delivered = None
            is_delayed = False
            status = "canceled"
        estimated = sample_estimated_delivery(
            rng, purchase, delivered, is_delayed
        )
        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": status,
            "order_purchase_timestamp": purchase,
            "order_approved_at": approved,
            "order_delivered_carrier_date": carrier,
            "order_delivered_customer_date": delivered,
            "order_estimated_delivery_date": estimated,
        }
        entities["orders"].append(order)

        item_count = sample_item_count(rng)
        order_total = 0.0
        for item_id in range(1, item_count + 1):
            source = references["items"].iloc[int(rng.integers(0, len(references["items"])))]
            price = float(source["price"])
            freight = float(source["freight_value"])
            order_total += price + freight
            entities["order_items"].append({
                "order_id": order_id,
                "order_item_id": item_id,
                "product_id": str(source["product_id"]),
                "seller_id": str(source["seller_id"]),
                "shipping_limit_date": purchase + timedelta(days=7),
                "price": price,
                "freight_value": freight,
            })

        payment_type = sample_payment_type(rng)
        entities["payments"].append({
            "order_id": order_id,
            "payment_sequential": 1,
            "payment_type": payment_type,
            "payment_installments": sample_installments(
                rng, payment_type, references["installments"]
            ),
            "payment_value": round(order_total, 2),
        })

        events.append(_event("order-events", "ORDER_CREATED", purchase, order))
        if status == "canceled":
            events.append(_event(
                "order-events", "ORDER_CANCELED",
                purchase + sample_cancel_delay(rng), order,
            ))
            continue

        events.extend([
            _event("order-events", "ORDER_APPROVED", approved, order),
            _event(
                "delivery-events", "DELIVERY_STARTED", carrier, order,
                order_estimated_delivery_date=estimated,
            ),
            _event(
                "delivery-events", "DELIVERY_COMPLETED", delivered, order,
                order_estimated_delivery_date=estimated,
            ),
        ])
        if rng.random() < REVIEW_RATE:
            review_sequence += 1
            review_id = f"{run_id}_review_{review_sequence:08d}"
            creation = delivered + sample_review_delay(rng)
            answer = creation + sample_answer_delay(rng)
            score = sample_review_score(rng)
            entities["reviews"].append({
                "review_id": review_id,
                "order_id": order_id,
                "review_score": score,
                "review_comment_title": None,
                "review_comment_message": None,
                "review_creation_date": creation,
                "review_answer_timestamp": answer,
            })
            events.append(_event(
                "review-events", "REVIEW_CREATED", creation, order,
                review_id=review_id, review_score=str(score),
            ))

    events.sort(key=lambda row: (
        row["event"]["event_time"],
        EVENT_PRIORITY[row["event"]["event_type"]],
        row["event"]["event_id"],
    ))
    return entities, events


def _json_default(value):
    if isinstance(value, datetime):
        return value.strftime(DATETIME_FORMAT)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def _write_jsonl(rows, path):
    try:
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            for row in rows:
                handle.write(json.dumps(
                    row, ensure_ascii=False, separators=(",", ":"),
                    default=_json_default,
                ) + "\n")
    except Exception as exc:
        raise RuntimeError(f"JSONL write failed: {path}") from exc


def write_run(entities, events, run_dir):
    entity_dir = run_dir / "entities"
    event_dir = run_dir / "events"
    try:
        entity_dir.mkdir(parents=False, exist_ok=False)
        event_dir.mkdir(parents=False, exist_ok=False)
    except Exception as exc:
        raise RuntimeError(f"run directory creation failed: {run_dir}") from exc

    for name, schema in ENTITY_SCHEMAS.items():
        path = entity_dir / f"{name}.parquet"
        try:
            table = pa.Table.from_pylist(entities[name], schema=schema)
            pq.write_table(table, path)
        except Exception as exc:
            raise RuntimeError(f"Parquet write failed: {path}") from exc

    topic_files = {
        "order-events": "order_events.jsonl",
        "delivery-events": "delivery_events.jsonl",
        "review-events": "review_events.jsonl",
    }
    for topic, file_name in topic_files.items():
        _write_jsonl(
            (row["event"] for row in events if row["topic"] == topic),
            event_dir / file_name,
        )
    _write_jsonl(events, event_dir / "all_events_sorted.jsonl")


def write_run_atomically(entities, events, final_run_dir):
    output_root = final_run_dir.parent
    try:
        output_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise RuntimeError(f"output root creation failed: {output_root}") from exc

    if final_run_dir.exists():
        raise FileExistsError(
            f"immutable run directory already exists: {final_run_dir}"
        )

    try:
        staging_dir = Path(tempfile.mkdtemp(
            prefix=f".tmp-{final_run_dir.name}-",
            dir=output_root,
        ))
    except Exception as exc:
        raise RuntimeError(
            f"staging directory creation failed under: {output_root}"
        ) from exc

    try:
        staging_dir.chmod(0o755)
        write_run(entities, events, staging_dir)
        missing_files = [
            relative_path for relative_path in EXPECTED_OUTPUT_FILES
            if not (staging_dir / relative_path).is_file()
        ]
        if missing_files:
            raise RuntimeError(
                f"staging output is incomplete: missing={missing_files}"
            )
        if final_run_dir.exists():
            raise FileExistsError(
                f"immutable run directory already exists: {final_run_dir}"
            )
        try:
            os.rename(staging_dir, final_run_dir)
        except Exception as exc:
            raise RuntimeError(
                f"run finalization failed: {staging_dir} -> {final_run_dir}"
            ) from exc
    except Exception:
        shutil.rmtree(staging_dir, ignore_errors=True)
        raise


def main():
    args = parse_args()
    validate_inputs(args)
    references = load_reference_data(args.reference_root)
    entities, events = generate_run(
        args.run_id, args.seed, args.order_count,
        args.start_date, args.end_date, references,
    )
    validate_run(entities, events, {
        "customers": set(references["customers"]),
        "products": references["products"],
        "sellers": references["sellers"],
    })
    run_dir = args.output_root / args.run_id
    write_run_atomically(entities, events, run_dir)

    status_counts = pd.Series(
        [row["order_status"] for row in entities["orders"]]
    ).value_counts()
    delayed = sum(
        row["order_status"] == "delivered"
        and row["order_delivered_customer_date"]
        > row["order_estimated_delivery_date"]
        for row in entities["orders"]
    )
    print(f"run_dir: {run_dir}")
    print(f"orders: {len(entities['orders'])}")
    print(f"order_items: {len(entities['order_items'])}")
    print(f"payments: {len(entities['payments'])}")
    print(f"reviews: {len(entities['reviews'])}")
    print(f"events: {len(events)}")
    print(f"delivered: {int(status_counts.get('delivered', 0))}")
    print(f"canceled: {int(status_counts.get('canceled', 0))}")
    print(f"delayed: {delayed}")
    print("validation: PASS")


if __name__ == "__main__":
    main()
