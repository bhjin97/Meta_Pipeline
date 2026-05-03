import json
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC_NAME = "order-events"
CHECKPOINT_PATH = "/app/data/checkpoints/order_metrics"

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_KEY_PREFIX = "streaming:order"


def write_to_redis(df, batch_id):
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

    print(f"[BATCH {batch_id}] order metrics saved to Redis")


def main():
    spark = (
        SparkSession.builder
        .appName("Order Event Metrics Streaming")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
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
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", to_timestamp(col("event_time")))
        .filter(col("event_id").isNotNull())
        .filter(col("event_time").isNotNull())
        .filter(col("event_type").isNotNull())
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["event_id"])
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

    query = (
        metrics_df.writeStream
        .outputMode("append")
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()