from collections import Counter, defaultdict
from math import isclose

from .schemas import EVENT_FIELDS_BY_TOPIC


EVENT_TYPES_BY_TOPIC = {
    "order-events": {"ORDER_CREATED", "ORDER_APPROVED", "ORDER_CANCELED"},
    "delivery-events": {"DELIVERY_STARTED", "DELIVERY_COMPLETED"},
    "review-events": {"REVIEW_CREATED"},
}


class RunValidationError(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise RunValidationError(message)


def _unique(rows, fields, name):
    keys = [tuple(row.get(field) for field in fields) for row in rows]
    _require(all(all(value is not None for value in key) for key in keys),
             f"{name} contains a NULL key")
    _require(len(keys) == len(set(keys)), f"{name} grain is not unique")


def validate_run(entities, events, reference_ids):
    orders = entities["orders"]
    items = entities["order_items"]
    payments = entities["payments"]
    reviews = entities["reviews"]

    _unique(orders, ("order_id",), "orders")
    _unique(items, ("order_id", "order_item_id"), "order_items")
    _unique(payments, ("order_id", "payment_sequential"), "payments")
    _unique(reviews, ("review_id", "order_id"), "reviews")

    order_by_id = {row["order_id"]: row for row in orders}
    order_ids = set(order_by_id)

    event_payloads = []
    for wrapper in events:
        _require(isinstance(wrapper, dict) and "topic" in wrapper and "event" in wrapper,
                 "event wrapper must contain topic and event")
        topic = wrapper["topic"]
        event = wrapper["event"]
        _require(topic in EVENT_FIELDS_BY_TOPIC,
                 f"unknown event topic: {topic}")
        _require(isinstance(event, dict),
                 f"event payload for topic {topic} must be an object")
        expected_fields = set(EVENT_FIELDS_BY_TOPIC[topic])
        actual_fields = set(event)
        _require(actual_fields == expected_fields,
                 f"{topic} event fields mismatch: "
                 f"missing={sorted(expected_fields - actual_fields)}, "
                 f"unexpected={sorted(actual_fields - expected_fields)}")
        _require(event["event_type"] in EVENT_TYPES_BY_TOPIC[topic],
                 f"invalid topic/event_type combination: "
                 f"{topic}/{event['event_type']}")
        _require(event["order_id"] in order_ids,
                 f"event references an unknown order_id: {event['order_id']}")
        event_payloads.append(event)

    _unique(event_payloads, ("event_id",), "events")

    _require(all(row["customer_id"] in reference_ids["customers"] for row in orders),
             "orders contains an unknown customer_id")
    _require(all(row["order_id"] in order_ids for row in items),
             "order_items contains an unknown order_id")
    _require(all(row["product_id"] in reference_ids["products"] for row in items),
             "order_items contains an unknown product_id")
    _require(all(row["seller_id"] in reference_ids["sellers"] for row in items),
             "order_items contains an unknown seller_id")
    _require(all(row["order_id"] in order_ids for row in payments),
             "payments contains an unknown order_id")
    _require(all(row["order_id"] in order_ids for row in reviews),
             "reviews contains an unknown order_id")

    items_by_order = defaultdict(list)
    payments_by_order = defaultdict(list)
    reviews_by_order = defaultdict(list)
    events_by_order = defaultdict(list)
    for row in items:
        items_by_order[row["order_id"]].append(row)
    for row in payments:
        payments_by_order[row["order_id"]].append(row)
    for row in reviews:
        reviews_by_order[row["order_id"]].append(row)
    for wrapper in events:
        events_by_order[wrapper["event"]["order_id"]].append(wrapper["event"])

    review_event_by_id = {
        row["event"]["review_id"]: row["event"]
        for row in events if row["event"]["event_type"] == "REVIEW_CREATED"
    }

    for order_id, order in order_by_id.items():
        _require(order["order_estimated_delivery_date"] is not None,
                 f"{order_id} has no estimated delivery timestamp")
        order_items = items_by_order[order_id]
        order_payments = payments_by_order[order_id]
        _require(len(order_items) >= 1, f"{order_id} has no items")
        _require(len(order_payments) == 1, f"{order_id} must have one payment")
        payment = order_payments[0]
        _require(payment["payment_sequential"] == 1,
                 f"{order_id} has invalid payment_sequential")
        expected_total = round(sum(
            row["price"] + row["freight_value"] for row in order_items
        ), 2)
        _require(isclose(payment["payment_value"], expected_total, abs_tol=0.005),
                 f"{order_id} payment total does not match its items")

        typed_events = defaultdict(list)
        for event in events_by_order[order_id]:
            typed_events[event["event_type"]].append(event)
            _require(event["customer_id"] == order["customer_id"],
                     f"{event['event_id']} customer_id mismatch")
            if "order_status" in event:
                _require(event["order_status"] == order["order_status"],
                         f"{event['event_id']} order_status mismatch")

        if order["order_status"] == "delivered":
            _require(len(reviews_by_order[order_id]) <= 1,
                     f"{order_id} has more than one review")
            _require(
                order["order_purchase_timestamp"]
                <= order["order_approved_at"]
                <= order["order_delivered_carrier_date"]
                <= order["order_delivered_customer_date"],
                f"{order_id} delivered lifecycle is out of order",
            )
            expected_events = Counter({
                "ORDER_CREATED": 1,
                "ORDER_APPROVED": 1,
                "DELIVERY_STARTED": 1,
                "DELIVERY_COMPLETED": 1,
            })
            if reviews_by_order[order_id]:
                expected_events["REVIEW_CREATED"] = 1
            actual_events = Counter(
                event["event_type"] for event in events_by_order[order_id]
            )
            _require(actual_events == expected_events,
                     f"{order_id} delivered lifecycle has invalid events: "
                     f"expected={dict(expected_events)}, actual={dict(actual_events)}")
        elif order["order_status"] == "canceled":
            _require(
                order["order_approved_at"] is None
                and order["order_delivered_carrier_date"] is None
                and order["order_delivered_customer_date"] is None,
                f"{order_id} canceled lifecycle has delivery timestamps",
            )
            _require(Counter(event["event_type"] for event in events_by_order[order_id])
                     == Counter({"ORDER_CREATED": 1, "ORDER_CANCELED": 1}),
                     f"{order_id} canceled lifecycle has invalid events")
            _require(not reviews_by_order[order_id],
                     f"{order_id} canceled order has a review")
        else:
            raise RunValidationError(f"{order_id} has invalid order_status")

        timestamp_pairs = {
            "ORDER_CREATED": order["order_purchase_timestamp"],
            "ORDER_APPROVED": order["order_approved_at"],
            "DELIVERY_STARTED": order["order_delivered_carrier_date"],
            "DELIVERY_COMPLETED": order["order_delivered_customer_date"],
        }
        for event_type, timestamp in timestamp_pairs.items():
            if timestamp is not None:
                _require(typed_events[event_type][0]["event_time"] == timestamp,
                         f"{order_id} {event_type} timestamp mismatch")
        for event_type in ("DELIVERY_STARTED", "DELIVERY_COMPLETED"):
            if typed_events[event_type]:
                _require(
                    typed_events[event_type][0]["order_estimated_delivery_date"]
                    == order["order_estimated_delivery_date"],
                    f"{order_id} {event_type} estimated delivery mismatch",
                )

    for review in reviews:
        order = order_by_id[review["order_id"]]
        _require(order["order_status"] == "delivered",
                 f"{review['review_id']} references a non-delivered order")
        _require(review["review_score"] in {1, 2, 3, 4, 5},
                 f"{review['review_id']} has an invalid score")
        _require(review["review_creation_date"] >= order["order_delivered_customer_date"],
                 f"{review['review_id']} was created before delivery")
        _require(review["review_answer_timestamp"] >= review["review_creation_date"],
                 f"{review['review_id']} was answered before creation")
        event = review_event_by_id.get(review["review_id"])
        _require(event is not None,
                 f"{review['review_id']} has no REVIEW_CREATED event")
        _require(event["event_time"] == review["review_creation_date"],
                 f"{review['review_id']} event timestamp mismatch")
        _require(event["review_score"] == str(review["review_score"]),
                 f"{review['review_id']} event score mismatch")

    _require(len(review_event_by_id) == len(reviews),
             "REVIEW_CREATED event count does not match reviews")
