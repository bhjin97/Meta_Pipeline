from common.spark_session import create_spark_session


STAGING_PATH = "s3a://ecommerce/staging/olist/reviews/"
BRONZE_PATH = "s3a://ecommerce/bronze/olist/reviews/"


def main():
    spark = create_spark_session(
        "Promote Reviews To Bronze"
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] staging_path={STAGING_PATH}")
    print(f"[INFO] bronze_path={BRONZE_PATH}")

    df = spark.read.parquet(STAGING_PATH)

    row_count = df.count()

    print(f"[INFO] staging_row_count={row_count}")

    if row_count != 99224:
        raise RuntimeError(
            f"Unexpected staging row count: {row_count}"
        )

    (
        df.write
        .mode("overwrite")
        .parquet(BRONZE_PATH)
    )

    print(
        f"[SUCCESS] promoted_rows={row_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()