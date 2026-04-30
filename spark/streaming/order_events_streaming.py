from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "order-events"

OUTPUT_PATH = "/app/data/streaming/order_metrics"
CHECKPOINT_PATH = "/app/data/checkpoints/order_metrics"


def main():
    spark = (
        SparkSession.builder
        .appName("Order Event Metrics Streaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    order_event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
    ])

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), order_event_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("event_time").isNotNull())
        .filter(col("event_type").isNotNull())
    )

    order_metrics_df = (
        parsed_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("event_type")
        )
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("count").alias("event_count")
        )
    )

    query = (
        order_metrics_df
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()