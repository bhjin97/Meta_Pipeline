from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Check Silver Tables")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

tables = {
    "dim_customer": "s3a://ecommerce/silver/dim_customer/",
    "dim_product": "s3a://ecommerce/silver/dim_product/",
    "dim_seller": "s3a://ecommerce/silver/dim_seller/",
    "fact_order_item": "s3a://ecommerce/silver/fact_order_item/",
    "fact_delivery": "s3a://ecommerce/silver/fact_delivery/",
    "fact_review": "s3a://ecommerce/silver/fact_review/",
}

for name, path in tables.items():
    print(f"\n===== {name} =====")
    df = spark.read.parquet(path)
    print(f"rows: {df.count()}")
    df.printSchema()
    df.show(5, truncate=False)

spark.stop()