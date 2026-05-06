import json
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "delivery-events"

MINIO_OUTPUT_PATH = "s3a://ecommerce/bronze/events/delivery_events/"
MINIO_CHECKPOINT_PATH = "s3a://ecommerce/checkpoints/events/delivery_events_raw/"

REDIS_CHECKPOINT_PATH = "/app/data/checkpoints/delivery_metrics"
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_KEY_PREFIX = "streaming:delivery"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Delivery Events Streaming")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_metrics_to_redis(df, batch_id):
    if df.isEmpty():
        return

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    for row in df.collect():
        key = f"{REDIS_KEY_PREFIX}:{row['event_type']}"
        value = {
            "window_start": str(row["window_start"]),
            "window_end": str(row["window_end"]),
            "event_type": row["event_type"],
            "event_count": row["event_count"],
            "batch_id": batch_id,
        }
        r.set(key, json.dumps(value, ensure_ascii=False))

    print(f"[BATCH {batch_id}] delivery metrics saved to Redis")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("delivery_status", StringType(), True),
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
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("event_id").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("event_type").isNotNull())
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["event_id"])
    )

    raw_query = (
        parsed_df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", MINIO_OUTPUT_PATH)
        .option("checkpointLocation", MINIO_CHECKPOINT_PATH)
        .start()
    )

    metrics_df = (
        parsed_df
        .groupBy(window(col("event_time"), "1 minute"), col("event_type"))
        .count()
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("count").alias("event_count"),
        )
    )

    metrics_query = (
        metrics_df.writeStream
        .outputMode("update")
        .foreachBatch(write_metrics_to_redis)
        .option("checkpointLocation", REDIS_CHECKPOINT_PATH)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()