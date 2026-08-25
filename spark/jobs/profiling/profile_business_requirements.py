from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    max as _max,
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Profile Business Requirements")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def pct(part, total):
    if total == 0:
        return 0.0
    return round(part / total * 100, 2)


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # ------------------------------------------------------------------
    # Read Silver
    # ------------------------------------------------------------------
    fact_order_item = spark.read.parquet(
        "s3a://ecommerce/silver/fact_order_item/"
    )
    fact_delivery = spark.read.parquet(
        "s3a://ecommerce/silver/fact_delivery/"
    )
    fact_review = spark.read.parquet(
        "s3a://ecommerce/silver/fact_review/"
    )
    dim_customer = spark.read.parquet(
        "s3a://ecommerce/silver/dim_customer/"
    )

    results = []

    # ==================================================================
    # P0-1. fact_order_item Grain
    # ==================================================================
    order_item_rows = fact_order_item.count()

    order_item_count = (
        fact_order_item
        .select("order_id", "order_item_id")
        .dropDuplicates()
        .count()
    )

    order_item_event_count = (
        fact_order_item
        .select("order_id", "order_item_id", "event_type")
        .dropDuplicates()
        .count()
    )

    results.append(
        (
            "P0-1 Order Item Grain",
            f"{order_item_rows:,}",
            f"{order_item_count:,}",
            (
                f"event-grain={order_item_event_count:,}, "
                f"row/item={order_item_rows / order_item_count:.2f}x"
                if order_item_count
                else "-"
            ),
        )
    )

    # ==================================================================
    # P0-2. Customer 반복 구매 구조
    # ==================================================================
    customer_orders = (
        fact_order_item
        .select("order_id", "customer_id")
        .dropDuplicates()
        .join(
            dim_customer.select(
                "customer_id",
                "customer_unique_id",
            ),
            on="customer_id",
            how="inner",
        )
        .groupBy("customer_unique_id")
        .agg(
            countDistinct("order_id").alias("order_count")
        )
    )

    customer_count = customer_orders.count()

    one_time_count = (
        customer_orders
        .filter(col("order_count") == 1)
        .count()
    )

    repeat_count = (
        customer_orders
        .filter(col("order_count") >= 2)
        .count()
    )

    max_orders = (
        customer_orders
        .agg(_max("order_count").alias("max_order_count"))
        .first()["max_order_count"]
    )

    results.append(
        (
            "P0-2 Customer Orders",
            f"{customer_count:,}",
            f"repeat={repeat_count:,}",
            (
                f"1-time={pct(one_time_count, customer_count)}%, "
                f"repeat={pct(repeat_count, customer_count)}%, "
                f"max={max_orders}"
            ),
        )
    )

    # ==================================================================
    # P0-3. Seller per Order
    # ==================================================================
    sellers_per_order = (
        fact_order_item
        .select("order_id", "seller_id")
        .dropDuplicates()
        .groupBy("order_id")
        .agg(
            countDistinct("seller_id").alias("seller_count")
        )
    )

    seller_order_count = sellers_per_order.count()

    multi_seller_orders = (
        sellers_per_order
        .filter(col("seller_count") > 1)
        .count()
    )

    max_sellers = (
        sellers_per_order
        .agg(_max("seller_count").alias("max_sellers"))
        .first()["max_sellers"]
    )

    results.append(
        (
            "P0-3 Sellers per Order",
            f"{seller_order_count:,}",
            f"multi={multi_seller_orders:,}",
            (
                f"multi-rate={pct(multi_seller_orders, seller_order_count)}%, "
                f"max={max_sellers}"
            ),
        )
    )

    # ==================================================================
    # P0-4. fact_delivery Grain
    # ==================================================================
    delivery_rows = fact_delivery.count()

    delivery_orders = (
        fact_delivery
        .select("order_id")
        .dropDuplicates()
    )

    delivery_order_count = delivery_orders.count()

    delivery_event_count = (
        fact_delivery
        .select("order_id", "event_type")
        .dropDuplicates()
        .count()
    )

    delivery_rows_per_order = (
        delivery_rows / delivery_order_count
        if delivery_order_count
        else 0
    )

    results.append(
        (
            "P0-4 Delivery Grain",
            f"{delivery_rows:,}",
            f"{delivery_order_count:,}",
            (
                f"event-grain={delivery_event_count:,}, "
                f"row/order={delivery_rows_per_order:.2f}x"
            ),
        )
    )

    # ==================================================================
    # P0-5. Review per Order
    # ==================================================================
    review_rows = fact_review.count()

    reviews_per_order = (
        fact_review
        .groupBy("order_id")
        .agg(
            countDistinct("review_id").alias("review_count")
        )
    )

    review_order_count = reviews_per_order.count()

    multi_review_orders = (
        reviews_per_order
        .filter(col("review_count") > 1)
        .count()
    )

    max_reviews = (
        reviews_per_order
        .agg(_max("review_count").alias("max_reviews"))
        .first()["max_reviews"]
    )

    results.append(
        (
            "P0-5 Reviews per Order",
            f"{review_order_count:,}",
            f"multi={multi_review_orders:,}",
            (
                f"multi-rate={pct(multi_review_orders, review_order_count)}%, "
                f"max={max_reviews}, "
                f"review-rows={review_rows:,}"
            ),
        )
    )

    # ==================================================================
    # P0-6. Delivery ↔ Review Join Cardinality
    # ==================================================================
    review_orders = (
        fact_review
        .select("order_id")
        .dropDuplicates()
    )

    common_order_count = (
        delivery_orders
        .join(
            review_orders,
            on="order_id",
            how="inner",
        )
        .count()
    )

    direct_join_rows = (
        fact_delivery
        .select("order_id", "event_type")
        .join(
            fact_review.select("order_id", "review_id"),
            on="order_id",
            how="inner",
        )
        .count()
    )

    join_factor = (
        direct_join_rows / common_order_count
        if common_order_count
        else 0
    )

    results.append(
        (
            "P0-6 Delivery x Review",
            f"{common_order_count:,}",
            f"{direct_join_rows:,}",
            (
                f"join-factor={join_factor:.2f}x, "
                f"coverage={pct(common_order_count, delivery_order_count)}%"
            ),
        )
    )

    # ==================================================================
    # Summary
    # ==================================================================
    summary_df = spark.createDataFrame(
        results,
        [
            "check",
            "base_count",
            "comparison_count",
            "result",
        ],
    )

    print("\n")
    print("=" * 100)
    print("P0 BUSINESS REQUIREMENT PROFILING SUMMARY")
    print("=" * 100)

    summary_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()