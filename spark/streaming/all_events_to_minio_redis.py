import os
import json
import redis
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])

STREAM_MODE = os.getenv("STREAM_MODE", "prod")

if STREAM_MODE == "load_test":
    TOPIC_PREFIX = "load-test-"
    DEFAULT_CHECKPOINT_BASE_PATH = "s3a://ecommerce/checkpoints/load_test/load_test_001/events"
    DEFAULT_METRICS_CHECKPOINT_BASE_PATH = "/app/data/checkpoints/load_test/load_test_001"
    OUTPUT_BASE_PATH = "s3a://ecommerce/bronze/load_test/load_test_001/events"
    REDIS_PREFIX_BASE = "streaming:load_test"
else:
    TOPIC_PREFIX = ""
    DEFAULT_CHECKPOINT_BASE_PATH = "s3a://ecommerce/checkpoints/events"
    DEFAULT_METRICS_CHECKPOINT_BASE_PATH = "/app/data/checkpoints"
    OUTPUT_BASE_PATH = "s3a://ecommerce/bronze/events"
    REDIS_PREFIX_BASE = "streaming"

CHECKPOINT_BASE_PATH = os.getenv(
    "CHECKPOINT_BASE_PATH",
    DEFAULT_CHECKPOINT_BASE_PATH
)

METRICS_CHECKPOINT_BASE_PATH = os.getenv(
    "METRICS_CHECKPOINT_BASE_PATH",
    DEFAULT_METRICS_CHECKPOINT_BASE_PATH
)

ORDER_TOPIC = f"{TOPIC_PREFIX}order-events"
DELIVERY_TOPIC = f"{TOPIC_PREFIX}delivery-events"
REVIEW_TOPIC = f"{TOPIC_PREFIX}review-events"

TOPIC_NAMES = ",".join([
    ORDER_TOPIC,
    DELIVERY_TOPIC,
    REVIEW_TOPIC,
])

TIMESERIES_TTL_SECONDS = 60 * 60 * 24
TRIGGER_SECONDS = 20
EVENTS_PER_MIN_MULTIPLIER = 60 // TRIGGER_SECONDS


EVENT_CONFIGS = {
    ORDER_TOPIC: {
        "app_name": "Order Events Streaming",
        "output_path": f"{OUTPUT_BASE_PATH}/order_events/",
        "redis_prefix": f"{REDIS_PREFIX_BASE}:order",
        "schema": StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
        ]),
    },
    DELIVERY_TOPIC: {
        "app_name": "Delivery Events Streaming",
        "output_path": f"{OUTPUT_BASE_PATH}/delivery_events/",
        "redis_prefix": f"{REDIS_PREFIX_BASE}:delivery",
        "schema": StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_estimated_delivery_date", StringType(), True),
        ]),
    },
    REVIEW_TOPIC: {
        "app_name": "Review Events Streaming",
        "output_path": f"{OUTPUT_BASE_PATH}/review_events/",
        "redis_prefix": f"{REDIS_PREFIX_BASE}:review",
        "schema": StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_time", StringType(), True),
            StructField("review_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("review_score", StringType(), True),
        ]),
    },
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName(f"All Events Streaming - {STREAM_MODE}")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def write_metrics_to_redis(redis_prefix):
    def _write(df, batch_id):
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
        )

        now = datetime.now()
        window_start = now.strftime("%Y-%m-%d %H:%M:%S")

        if df.isEmpty():
            return

        metrics_df = (
            df.groupBy("event_type")
            .count()
            .withColumnRenamed("count", "batch_count")
        )

        for row in metrics_df.collect():
            event_type = row["event_type"]
            batch_count = int(row["batch_count"])
            event_count = batch_count * EVENTS_PER_MIN_MULTIPLIER

            value = {
                "processed_at": window_start,
                "event_type": event_type,
                "event_count": event_count,
                "batch_count": batch_count,
                "trigger_seconds": TRIGGER_SECONDS,
                "batch_id": batch_id,
                "stream_mode": STREAM_MODE,
            }

            latest_key = f"{redis_prefix}:latest:{event_type}"
            timeseries_key = f"{redis_prefix}:timeseries:{event_type}:{window_start}"
            total_key = f"{redis_prefix}:total:{event_type}"

            r.set(latest_key, json.dumps(value, ensure_ascii=False))
            r.setex(
                timeseries_key,
                TIMESERIES_TTL_SECONDS,
                json.dumps(value, ensure_ascii=False),
            )
            r.incrby(total_key, batch_count)

        print(f"[BATCH {batch_id}] metrics saved to Redis: {redis_prefix}")

    return _write


def build_topic_stream(kafka_df, topic_name, config):
    topic_key = topic_name.replace("-", "_")

    parsed_df = (
        kafka_df
        .filter(col("topic") == topic_name)
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), config["schema"]).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("event_id").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("event_type").isNotNull())
        #.dropDuplicates(["event_id"])
    )

    raw_query = (
        parsed_df.writeStream
        .queryName(f"{topic_key}_raw")
        .format("parquet")
        .outputMode("append")
        .option("path", config["output_path"])
        .option(
            "checkpointLocation",
            f"{CHECKPOINT_BASE_PATH}/{topic_key}_raw/"
        )
        .start()
    )

    metrics_query = (
        parsed_df.writeStream
        .queryName(f"{topic_key}_metrics")
        .foreachBatch(write_metrics_to_redis(config["redis_prefix"]))
        .option(
            "checkpointLocation",
            f"{METRICS_CHECKPOINT_BASE_PATH}/{topic_key}_metrics"
        )
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    return raw_query, metrics_query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[CONFIG] STREAM_MODE={STREAM_MODE}")
    print(f"[CONFIG] TOPIC_NAMES={TOPIC_NAMES}")
    print(f"[CONFIG] CHECKPOINT_BASE_PATH={CHECKPOINT_BASE_PATH}")
    print(f"[CONFIG] METRICS_CHECKPOINT_BASE_PATH={METRICS_CHECKPOINT_BASE_PATH}")
    print(f"[CONFIG] OUTPUT_BASE_PATH={OUTPUT_BASE_PATH}")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_NAMES)
        .option("startingOffsets", "earliest")
        .load()
    )

    queries = []

    for topic_name, config in EVENT_CONFIGS.items():
        raw_query, metrics_query = build_topic_stream(
            kafka_df=kafka_df,
            topic_name=topic_name,
            config=config,
        )
        queries.extend([raw_query, metrics_query])

    print("[STARTED] all event streaming queries started")

    for query in queries:
        print(f"[WAITING] query={query.name}, id={query.id}")
        query.awaitTermination()


if __name__ == "__main__":
    main()