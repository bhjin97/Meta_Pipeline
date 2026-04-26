from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat_ws

RAW_PATH = "data/raw"
OUTPUT_PATH = "data/event_source/olist_order_status_events"


def main():
    spark = (
        SparkSession.builder
        .appName("Make Olist Order Status Events")
        .getOrCreate()
    )

    # Orders
    orders_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_orders_dataset.csv")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_carrier_date",
            "order_delivered_customer_date"
        )
    )

    # 1️⃣ ORDER_CREATED
    created_df = (
        orders_df
        .filter(col("order_purchase_timestamp").isNotNull())
        .select(
            "order_id",
            "customer_id",
            col("order_purchase_timestamp").alias("event_time")
        )
        .withColumn("event_type", lit("ORDER_CREATED"))
    )

    # 2️⃣ ORDER_SHIPPED
    shipped_df = (
        orders_df
        .filter(col("order_delivered_carrier_date").isNotNull())
        .select(
            "order_id",
            "customer_id",
            col("order_delivered_carrier_date").alias("event_time")
        )
        .withColumn("event_type", lit("ORDER_SHIPPED"))
    )

    # 3️⃣ ORDER_DELIVERED
    delivered_df = (
        orders_df
        .filter(col("order_delivered_customer_date").isNotNull())
        .select(
            "order_id",
            "customer_id",
            col("order_delivered_customer_date").alias("event_time")
        )
        .withColumn("event_type", lit("ORDER_DELIVERED"))
    )

    # 4️⃣ ORDER_CANCELED (optional)
    canceled_df = (
        orders_df
        .filter(col("order_status") == "canceled")
        .select(
            "order_id",
            "customer_id",
            col("order_purchase_timestamp").alias("event_time")
        )
        .withColumn("event_type", lit("ORDER_CANCELED"))
    )

    # 5️⃣ union
    event_df = (
        created_df
        .unionByName(shipped_df)
        .unionByName(delivered_df)
        .unionByName(canceled_df)
    )

    # 6️⃣ event_id 생성
    event_df = event_df.withColumn(
        "event_id",
        concat_ws("_", col("event_type"), col("order_id"))
    )

    # 7️⃣ 정렬
    event_df = event_df.orderBy("event_time")

    # 8️⃣ 저장
    (
        event_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(OUTPUT_PATH)
    )

    print("Olist order status events created successfully.")
    print(f"Output path: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()