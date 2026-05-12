import os
import json
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])

TOPIC_NAME = "delivery-events"

MINIO_OUTPUT_PATH = "s3a://ecommerce/bronze/events/delivery_events/"
MINIO_CHECKPOINT_PATH = "s3a://ecommerce/checkpoints/events/delivery_events_raw/"

REDIS_CHECKPOINT_PATH = "/app/data/checkpoints/delivery_metrics"
REDIS_KEY_PREFIX = "streaming:delivery"
TIMESERIES_TTL_SECONDS = 60 * 60 * 24


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Delivery Events Streaming")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_metrics_to_redis(df, batch_id):
    if df.isEmpty():
        return

    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )

    for row in df.collect():
        event_type = row["event_type"]
        window_start = str(row["window_start"])
        window_end = str(row["window_end"])
        event_count = int(row["event_count"])

        value = {
            "window_start": window_start,
            "window_end": window_end,
            "event_type": event_type,
            "event_count": event_count,
            "batch_id": batch_id,
        }

        latest_key = f"{REDIS_KEY_PREFIX}:latest:{event_type}"
        timeseries_key = f"{REDIS_KEY_PREFIX}:timeseries:{event_type}:{window_start}"
        total_key = f"{REDIS_KEY_PREFIX}:total:{event_type}"

        r.set(latest_key, json.dumps(value, ensure_ascii=False))
        r.setex(
            timeseries_key,
            TIMESERIES_TTL_SECONDS,
            json.dumps(value, ensure_ascii=False),
        )
        r.incrby(total_key, event_count)

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