from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Test Read MinIO")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

paths = {
    "orders": "s3a://ecommerce/bronze/olist/orders/",
    "customers": "s3a://ecommerce/bronze/olist/customers/",
    "products": "s3a://ecommerce/bronze/olist/products/",
    "sellers": "s3a://ecommerce/bronze/olist/sellers/",
    "persona": "s3a://ecommerce/bronze/persona/nemotron_korea/",
    "category_translation": "s3a://ecommerce/bronze/reference/category_translation/",
}

for name, path in paths.items():
    print(f"\n===== {name} =====")
    df = spark.read.parquet(path)
    df.printSchema()
    print(f"count = {df.count()}")
    df.show(3, truncate=False)

spark.stop()