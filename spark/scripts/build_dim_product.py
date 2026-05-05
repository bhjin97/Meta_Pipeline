from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Dim Product")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    products_path = "s3a://ecommerce/bronze/olist/products/"
    translation_path = "s3a://ecommerce/bronze/reference/category_translation/"
    output_path = "s3a://ecommerce/silver/dim_product/"

    products_df = spark.read.parquet(products_path)
    translation_df = spark.read.parquet(translation_path)

    dim_product = (
        products_df
        .join(translation_df, on="product_category_name", how="left")
        .select(
            col("product_id"),
            coalesce(col("product_category_name"), lit("unknown")).alias("product_category_name"),
            coalesce(col("product_category_name_english"), lit("unknown")).alias("product_category_name_english"),
            coalesce(col("product_photos_qty"), lit(0)).alias("product_photos_qty"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
        )
    )

    dim_product.write.mode("overwrite").parquet(output_path)

    print("dim_product build completed")
    print(f"row count: {dim_product.count()}")

    spark.stop()


if __name__ == "__main__":
    main()