from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Fact Order Item")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    order_events_path = "s3a://ecommerce/bronze/events/order_events/"
    order_items_path = "s3a://ecommerce/bronze/olist/order_items/"
    payments_path = "s3a://ecommerce/bronze/olist/payments/"
    output_path = "s3a://ecommerce/silver/fact_order_item/"

    order_events_df = spark.read.parquet(order_events_path)
    order_items_df = spark.read.parquet(order_items_path)
    payments_df = spark.read.parquet(payments_path)

    latest_order_events = (
        order_events_df
        .select(
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            col("event_time"),
            col("order_status"),
        )
        .dropDuplicates(["order_id", "event_type"])
    )

    payments_agg = (
        payments_df
        .groupBy("order_id")
        .agg(
            {"payment_value": "sum"}
        )
        .withColumnRenamed("sum(payment_value)", "payment_total_value")
    )

    fact_order_item = (
        latest_order_events
        .join(order_items_df, on="order_id", how="inner")
        .join(payments_agg, on="order_id", how="left")
        .select(
            col("order_id"),
            col("order_item_id"),
            col("customer_id"),
            col("product_id"),
            col("seller_id"),
            col("event_type"),
            col("event_time"),
            col("order_status"),
            col("shipping_limit_date"),
            col("price").alias("item_price"),
            col("freight_value").alias("item_freight_value"),
            (col("price") + col("freight_value")).alias("item_total_amount"),
            col("payment_total_value"),
            to_date(col("event_time")).alias("order_event_date"),
        )
    )

    fact_order_item.write.mode("overwrite").parquet(output_path)

    print("fact_order_item build completed")
    print(f"row count: {fact_order_item.count()}")

    spark.stop()


if __name__ == "__main__":
    main()