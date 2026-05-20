from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("Upload Persona Parquet To MinIO")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    input_path = "/app/data/raw/nemotron_persona_korea_sample.parquet"
    output_path = "s3a://ecommerce/bronze/persona/nemotron_korea/"

    df = spark.read.parquet(input_path)

    df.write.mode("overwrite").parquet(output_path)

    print("upload completed")
    print(f"row count: {df.count()}")
    df.groupBy("sex").count().show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()