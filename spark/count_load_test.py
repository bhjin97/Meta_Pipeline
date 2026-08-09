import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Count Load Test Bronze")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_SECRET_KEY"])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

base = "s3a://ecommerce/bronze/load_test/load_test_1000000/events"

counts = {}

for name in ["order_events", "delivery_events", "review_events"]:
    counts[name] = spark.read.parquet(f"{base}/{name}/").count()

for name, count in counts.items():
    print(f"{name}: {count:,}")

print(f"TOTAL: {sum(counts.values()):,}")

spark.stop()