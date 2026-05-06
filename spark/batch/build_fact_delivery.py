from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, datediff, when


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Fact Delivery")
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

    delivery_events_path = "s3a://ecommerce/bronze/events/delivery_events/"
    orders_path = "s3a://ecommerce/bronze/olist/orders/"
    output_path = "s3a://ecommerce/silver/fact_delivery/"

    delivery_events_df = spark.read.parquet(delivery_events_path)
    orders_df = spark.read.parquet(orders_path)

    delivery_events_df = (
        delivery_events_df
        .select(
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            to_timestamp(col("event_time")).alias("event_time"),
            col("delivery_status"),
        )
        .dropDuplicates(["order_id", "event_type"])
    )

    orders_df = (
        orders_df
        .select(
            col("order_id"),
            to_timestamp(col("order_purchase_timestamp")).alias("order_purchase_timestamp"),
            to_timestamp(col("order_delivered_carrier_date")).alias("order_delivered_carrier_date"),
            to_timestamp(col("order_delivered_customer_date")).alias("order_delivered_customer_date"),
            to_timestamp(col("order_estimated_delivery_date")).alias("order_estimated_delivery_date"),
        )
    )

    fact_delivery = (
        delivery_events_df
        .join(orders_df, on="order_id", how="left")
        .select(
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            col("event_time"),
            col("delivery_status"),

            col("order_purchase_timestamp"),
            col("order_delivered_carrier_date"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date"),

            datediff(
                col("order_delivered_carrier_date"),
                col("order_purchase_timestamp")
            ).alias("shipping_days"),

            datediff(
                col("order_delivered_customer_date"),
                col("order_purchase_timestamp")
            ).alias("delivery_days"),

            when(
                col("order_delivered_customer_date").isNotNull(),
                True
            ).otherwise(False).alias("is_delivered"),

            when(
                col("order_delivered_customer_date") > col("order_estimated_delivery_date"),
                True
            ).otherwise(False).alias("is_delayed"),

            to_date(col("event_time")).alias("delivery_event_date"),
        )
    )

    fact_delivery.write.mode("overwrite").parquet(output_path)

    print("fact_delivery build completed")
    print(f"row count: {fact_delivery.count()}")

    spark.stop()


if __name__ == "__main__":
    main()