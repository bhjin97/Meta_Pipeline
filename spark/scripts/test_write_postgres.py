from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test Write Postgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

orders_df = spark.read.parquet("s3a://ecommerce/bronze/olist/orders/")

(
    orders_df.limit(100)
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/ecommerce")
    .option("dbtable", "test_orders")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print("Postgres write test completed.")

spark.stop()