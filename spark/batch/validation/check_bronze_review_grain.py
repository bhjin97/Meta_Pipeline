from pyspark.sql.functions import col, count

from common.spark_session import create_spark_session


REVIEWS_PATH = (
    "s3a://ecommerce/bronze/olist/reviews/"
)


def main():
    spark = create_spark_session(
        "Check Bronze Review Grain"
    )

    spark.sparkContext.setLogLevel("WARN")

    reviews_df = (
        spark.read
        .parquet(REVIEWS_PATH)
    )

    total_rows = reviews_df.count()

    distinct_pair_count = (
        reviews_df
        .select(
            "review_id",
            "order_id",
        )
        .distinct()
        .count()
    )

    duplicate_df = (
        reviews_df
        .groupBy(
            "review_id",
            "order_id",
        )
        .agg(
            count("*").alias("cnt")
        )
        .filter(
            col("cnt") > 1
        )
    )

    duplicate_pair_count = (
        duplicate_df.count()
    )

    print(
        f"[INFO] total_rows="
        f"{total_rows}"
    )

    print(
        f"[INFO] distinct_pair_count="
        f"{distinct_pair_count}"
    )

    print(
        f"[INFO] duplicate_pair_count="
        f"{duplicate_pair_count}"
    )

    print(
        "[INFO] Duplicate pair samples:"
    )

    (
        duplicate_df
        .orderBy(
            col("cnt").desc()
        )
        .show(
            30,
            truncate=False,
        )
    )

    # 중복 pair에 해당하는 실제 원본 행 확인
    duplicate_keys_df = (
        duplicate_df
        .select(
            "review_id",
            "order_id",
        )
    )

    print(
        "[INFO] Duplicate source rows:"
    )

    (
        reviews_df
        .join(
            duplicate_keys_df,
            on=[
                "review_id",
                "order_id",
            ],
            how="inner",
        )
        .orderBy(
            "review_id",
            "order_id",
        )
        .show(
            50,
            truncate=False,
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()