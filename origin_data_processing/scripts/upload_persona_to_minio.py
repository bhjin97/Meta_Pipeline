from common.spark_session import create_spark_session


def main():
    spark = create_spark_session(
        "Upload Persona Pool To MinIO"
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = (
        "/app/origin_data_processing/scripts/data/raw/"
        "nemotron_persona_korea_pool.parquet"
    )

    output_path = (
        "s3a://ecommerce/bronze/persona/nemotron_korea/"
    )

    df = spark.read.parquet(input_path)

    print("[INFO] source schema")
    df.printSchema()

    print(f"[INFO] source row count: {df.count()}")

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print("[SUCCESS] persona pool upload completed")
    print(f"[INFO] output_path={output_path}")

    spark.stop()


if __name__ == "__main__":
    main()