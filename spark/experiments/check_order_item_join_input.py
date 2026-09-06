from pyspark.sql.functions import col, to_timestamp

from common.spark_session import create_spark_session


ORDER_EVENTS_PATH = "s3a://ecommerce/bronze/events/order_events/"
ORDER_ITEMS_PATH = "s3a://ecommerce/bronze/olist/order_items/"


def main():
    spark = create_spark_session(
        "Check Order Item Join Input"
    )
    spark.sparkContext.setLogLevel("WARN")

    order_created_df = (
        spark.read
        .parquet(ORDER_EVENTS_PATH)
        .filter(col("event_type") == "ORDER_CREATED")
        .select(
            "order_id",
            "customer_id",
            to_timestamp("event_time").alias("order_timestamp"),
        )
        .dropDuplicates(["order_id"])
    )

    order_items_df = (
        spark.read
        .parquet(ORDER_ITEMS_PATH)
    )

    print(
        "order_created partitions =",
        order_created_df.rdd.getNumPartitions()
    )
    print(
        "order_items partitions =",
        order_items_df.rdd.getNumPartitions()
    )

    print(
        "order_created rows =",
        order_created_df.count()
    )
    print(
        "order_items rows =",
        order_items_df.count()
    )

    spark.stop()


if __name__ == "__main__":
    main()