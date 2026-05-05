from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Dim Seller")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    sellers_path = "s3a://ecommerce/bronze/olist/sellers/"
    output_path = "s3a://ecommerce/silver/dim_seller/"

    sellers_df = spark.read.parquet(sellers_path)

    dim_seller = (
        sellers_df
        .select(
            col("seller_id"),
            col("seller_zip_code_prefix"),
            col("seller_city"),
            col("seller_state"),
        )
    )

    dim_seller.write.mode("overwrite").parquet(output_path)

    print("dim_seller build completed")
    print(f"row count: {dim_seller.count()}")

    spark.stop()


if __name__ == "__main__":
    main()