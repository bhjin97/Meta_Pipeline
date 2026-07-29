import os

from pyspark.sql import SparkSession


SOURCE_PATH = os.getenv(
    "FACT_ORDER_ITEM_SOURCE_PATH",
    "s3a://ecommerce/silver/fact_order_item/",
)

JDBC_URL = os.getenv(
    "POSTGRES_JDBC_URL",
    "jdbc:postgresql://postgres:5432/ecommerce",
)

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

TARGET_TABLE = os.getenv(
    "POSTGRES_TARGET_TABLE",
    "public.serving_fact_order_item",
)

# 현재 Spark Worker가 2코어이므로 PostgreSQL에 과도한 동시 쓰기를 하지 않도록 제한
JDBC_WRITE_PARTITIONS = int(
    os.getenv("JDBC_WRITE_PARTITIONS", "2")
)

JDBC_BATCH_SIZE = int(
    os.getenv("JDBC_BATCH_SIZE", "5000")
)


def create_spark_session() -> SparkSession:
    minio_endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")

    if not minio_access_key or not minio_secret_key:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set."
        )

    return (
        SparkSession.builder
        .appName("Load Serving Fact Order Item")
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true",
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false",
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print(f"Source path : {SOURCE_PATH}")
    print(f"Target table: {TARGET_TABLE}")
    print(f"JDBC URL    : {JDBC_URL}")
    print("=" * 80)

    # Silver Parquet 읽기
    fact_order_item_df = spark.read.parquet(SOURCE_PATH)

    print("[1/4] Silver schema")
    fact_order_item_df.printSchema()

    source_count = fact_order_item_df.count()
    print(f"[2/4] Source row count: {source_count:,}")

    if source_count == 0:
        raise ValueError(
            f"No rows found in Silver path: {SOURCE_PATH}"
        )

    # PostgreSQL 컨테이너의 리소스가 크지 않으므로 동시 연결 수를 제한
    write_df = fact_order_item_df.coalesce(JDBC_WRITE_PARTITIONS)

    print(
        f"[3/4] Writing to PostgreSQL "
        f"with {JDBC_WRITE_PARTITIONS} partitions..."
    )

    (
        write_df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TARGET_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", str(JDBC_BATCH_SIZE))
        .option("isolationLevel", "READ_COMMITTED")
        .mode("overwrite")
        .save()
    )

    # PostgreSQL 적재 결과 검증
    loaded_df = (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", TARGET_TABLE)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    target_count = loaded_df.count()

    print(f"[4/4] Target row count: {target_count:,}")

    if source_count != target_count:
        raise RuntimeError(
            "Row count validation failed: "
            f"source={source_count:,}, target={target_count:,}"
        )

    print("=" * 80)
    print("PostgreSQL serving table load completed successfully.")
    print(f"Validated rows: {target_count:,}")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()