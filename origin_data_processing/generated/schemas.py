import pyarrow as pa


TIMESTAMP_TYPE = pa.timestamp("us")

ORDERS_SCHEMA = pa.schema([
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("customer_id", pa.string(), nullable=False),
    pa.field("order_status", pa.string(), nullable=False),
    pa.field("order_purchase_timestamp", TIMESTAMP_TYPE, nullable=False),
    pa.field("order_approved_at", TIMESTAMP_TYPE),
    pa.field("order_delivered_carrier_date", TIMESTAMP_TYPE),
    pa.field("order_delivered_customer_date", TIMESTAMP_TYPE),
    pa.field("order_estimated_delivery_date", TIMESTAMP_TYPE, nullable=False),
])

ORDER_ITEMS_SCHEMA = pa.schema([
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("order_item_id", pa.int64(), nullable=False),
    pa.field("product_id", pa.string(), nullable=False),
    pa.field("seller_id", pa.string(), nullable=False),
    pa.field("shipping_limit_date", TIMESTAMP_TYPE, nullable=False),
    pa.field("price", pa.float64(), nullable=False),
    pa.field("freight_value", pa.float64(), nullable=False),
])

PAYMENTS_SCHEMA = pa.schema([
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("payment_sequential", pa.int64(), nullable=False),
    pa.field("payment_type", pa.string(), nullable=False),
    pa.field("payment_installments", pa.int64(), nullable=False),
    pa.field("payment_value", pa.float64(), nullable=False),
])

REVIEWS_SCHEMA = pa.schema([
    pa.field("review_id", pa.string(), nullable=False),
    pa.field("order_id", pa.string(), nullable=False),
    pa.field("review_score", pa.int64(), nullable=False),
    pa.field("review_comment_title", pa.string()),
    pa.field("review_comment_message", pa.string()),
    pa.field("review_creation_date", TIMESTAMP_TYPE, nullable=False),
    pa.field("review_answer_timestamp", TIMESTAMP_TYPE, nullable=False),
])

ENTITY_SCHEMAS = {
    "orders": ORDERS_SCHEMA,
    "order_items": ORDER_ITEMS_SCHEMA,
    "payments": PAYMENTS_SCHEMA,
    "reviews": REVIEWS_SCHEMA,
}

ORDER_EVENT_FIELDS = (
    "event_id", "event_type", "event_time", "order_id",
    "customer_id", "order_status",
)
DELIVERY_EVENT_FIELDS = ORDER_EVENT_FIELDS + (
    "order_estimated_delivery_date",
)
REVIEW_EVENT_FIELDS = (
    "event_id", "event_type", "event_time", "review_id", "order_id",
    "customer_id", "review_score",
)

EVENT_FIELDS_BY_TOPIC = {
    "order-events": ORDER_EVENT_FIELDS,
    "delivery-events": DELIVERY_EVENT_FIELDS,
    "review-events": REVIEW_EVENT_FIELDS,
}

EVENT_PRIORITY = {
    "ORDER_CREATED": 1,
    "ORDER_APPROVED": 2,
    "ORDER_CANCELED": 3,
    "DELIVERY_STARTED": 4,
    "DELIVERY_COMPLETED": 5,
    "REVIEW_CREATED": 6,
}
