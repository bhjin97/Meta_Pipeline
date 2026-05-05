from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, monotonically_increasing_id


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Dim Customer")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def add_row_idx(df, order_col_name="tmp_id"):
    window = Window.orderBy(col(order_col_name))
    return (
        df.withColumn(order_col_name, monotonically_increasing_id())
          .withColumn("row_idx", row_number().over(window))
          .drop(order_col_name)
    )


def main():
    spark = create_spark_session()

    customers_path = "s3a://ecommerce/bronze/olist/customers/"
    persona_path = "s3a://ecommerce/bronze/persona/nemotron_korea/"
    output_path = "s3a://ecommerce/silver/dim_customer/"

    customers_df = spark.read.parquet(customers_path)
    persona_df = spark.read.parquet(persona_path)

    customers_df = add_row_idx(customers_df)
    persona_df = add_row_idx(persona_df)

    dim_customer = (
        customers_df
        .join(persona_df, on="row_idx", how="inner")
        .select(
            col("customer_id"),
            col("customer_unique_id"),

            col("customer_name"),
            col("sex"),
            col("age"),
            col("age_group"),
            col("occupation"),
            col("marital_status"),
            col("education_level"),
            col("family_type"),
            col("housing_type"),
            col("province"),
            col("district"),
            col("persona"),
        )
    )

    dim_customer.write.mode("overwrite").parquet(output_path)

    print("dim_customer build completed")
    print(f"row count: {dim_customer.count()}")

    spark.stop()


if __name__ == "__main__":
    main()