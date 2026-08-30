import argparse
import sys

from pyspark.sql.functions import col, count, regexp_extract, sum as spark_sum, when

from common.spark_session import create_spark_session


DEFAULT_REVIEWS_PATH = "s3a://ecommerce/bronze/olist/reviews/"
REVIEW_ID_PATTERN = r"^[0-9a-f]{32}$"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Validate the Olist Bronze reviews dataset."
    )
    parser.add_argument(
        "--reviews-path",
        default=DEFAULT_REVIEWS_PATH,
        help=f"Reviews Parquet path (default: {DEFAULT_REVIEWS_PATH}).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    spark = create_spark_session("Validate Bronze Reviews")
    spark.sparkContext.setLogLevel("WARN")

    reviews = spark.read.parquet(args.reviews_path)
    review_score = col("review_score").cast("int")

    metrics = reviews.agg(
        count("*").alias("total_rows"),
        spark_sum(
            when(
                col("review_id").isNull()
                | (regexp_extract(col("review_id"), REVIEW_ID_PATTERN, 0) == ""),
                1,
            ).otherwise(0)
        ).alias("invalid_review_id_count"),
        spark_sum(
            when(
                col("review_score").isNull()
                | review_score.isNull()
                | (~review_score.between(1, 5)),
                1,
            ).otherwise(0)
        ).alias("invalid_review_score_count"),
        spark_sum(
            when(col("review_creation_date").isNull(), 1).otherwise(0)
        ).alias("null_review_creation_date_count"),
    ).first().asDict()

    duplicate_ids = (
        reviews.groupBy("review_id")
        .count()
        .filter(col("count") > 1)
        .agg(
            count("*").alias("duplicate_review_id_count"),
            spark_sum(col("count") - 1).alias("duplicate_review_row_count"),
        )
        .first()
        .asDict()
    )
    metrics.update({key: value or 0 for key, value in duplicate_ids.items()})

    print(f"[INFO] reviews_path={args.reviews_path}")
    for name, value in metrics.items():
        print(f"[RESULT] {name}={value}")

    failed_metrics = [
        "invalid_review_id_count",
        "invalid_review_score_count",
        "null_review_creation_date_count",
    ]
    has_failure = any(metrics[name] != 0 for name in failed_metrics)

    spark.stop()
    if has_failure:
        sys.exit(1)


if __name__ == "__main__":
    main()
