from pyspark.sql.functions import (
    col,
    count,
    date_format,
    datediff,
    to_date,
)

from common.spark_session import create_spark_session


FACT_REVIEW_PATH = (
    "s3a://ecommerce/silver/fact_review/"
)


EXPECTED_COLUMNS = {
    "review_id",
    "order_id",
    "customer_sk",
    "date_key",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
    "review_answer_days",
    "review_date",
    "review_month",
}


def validate_schema(df):
    """
    예상 컬럼과 실제 컬럼이 일치하는지 확인한다.
    """

    actual_columns = set(df.columns)

    missing_columns = (
        EXPECTED_COLUMNS - actual_columns
    )

    unexpected_columns = (
        actual_columns - EXPECTED_COLUMNS
    )

    if missing_columns:
        raise RuntimeError(
            "Missing columns detected. "
            f"missing_columns="
            f"{sorted(missing_columns)}"
        )

    if unexpected_columns:
        raise RuntimeError(
            "Unexpected columns detected. "
            f"unexpected_columns="
            f"{sorted(unexpected_columns)}"
        )

    print(
        "[PASS] Schema validation"
    )


def validate_grain(df):
    """
    Grain:
        (review_id, order_id)

    동일 pair가 두 번 이상 존재하면 실패한다.
    """

    duplicate_df = (
        df.groupBy(
            "review_id",
            "order_id",
        )
        .agg(
            count("*").alias(
                "row_count"
            )
        )
        .filter(
            col("row_count") > 1
        )
    )

    duplicate_pair_count = (
        duplicate_df.count()
    )

    if duplicate_pair_count > 0:
        print(
            "[ERROR] Duplicate grain samples"
        )

        duplicate_df.show(
            20,
            truncate=False,
        )

        raise RuntimeError(
            "Duplicate "
            "(review_id, order_id) "
            "grain detected. "
            f"duplicate_pair_count="
            f"{duplicate_pair_count}"
        )

    print(
        "[PASS] Grain validation "
        "(review_id, order_id)"
    )


def validate_required_keys(df):
    """
    Fact Grain 및 Dimension Key 검증.
    """

    null_review_id_count = (
        df.filter(
            col("review_id").isNull()
        )
        .count()
    )

    null_order_id_count = (
        df.filter(
            col("order_id").isNull()
        )
        .count()
    )

    null_customer_sk_count = (
        df.filter(
            col("customer_sk").isNull()
        )
        .count()
    )

    null_date_key_count = (
        df.filter(
            col("date_key").isNull()
        )
        .count()
    )

    if (
        null_review_id_count > 0
        or null_order_id_count > 0
        or null_customer_sk_count > 0
        or null_date_key_count > 0
    ):
        raise RuntimeError(
            "Required key contains NULL. "
            f"review_id="
            f"{null_review_id_count}, "
            f"order_id="
            f"{null_order_id_count}, "
            f"customer_sk="
            f"{null_customer_sk_count}, "
            f"date_key="
            f"{null_date_key_count}"
        )

    print(
        "[PASS] Required key validation"
    )


def validate_review_score(df):
    """
    review_score는 1~5 범위여야 한다.
    """

    invalid_score_count = (
        df.filter(
            col("review_score").isNull()
            | (col("review_score") < 1)
            | (col("review_score") > 5)
        )
        .count()
    )

    if invalid_score_count > 0:
        raise RuntimeError(
            "Invalid review_score detected. "
            f"invalid_score_count="
            f"{invalid_score_count}"
        )

    print(
        "[PASS] review_score validation"
    )


def validate_review_dates(df):
    """
    review_creation_date 및
    review_date 관계를 검증한다.
    """

    null_creation_count = (
        df.filter(
            col(
                "review_creation_date"
            ).isNull()
        )
        .count()
    )

    null_review_date_count = (
        df.filter(
            col("review_date").isNull()
        )
        .count()
    )

    if (
        null_creation_count > 0
        or null_review_date_count > 0
    ):
        raise RuntimeError(
            "Required review date "
            "contains NULL. "
            f"review_creation_date="
            f"{null_creation_count}, "
            f"review_date="
            f"{null_review_date_count}"
        )

    invalid_review_date_count = (
        df.filter(
            col("review_date")
            != to_date(
                col(
                    "review_creation_date"
                )
            )
        )
        .count()
    )

    if invalid_review_date_count > 0:
        raise RuntimeError(
            "review_date mapping failed. "
            f"invalid_count="
            f"{invalid_review_date_count}"
        )

    print(
        "[PASS] review date validation"
    )


def validate_answer_days(df):
    """
    review_answer_days 계산 검증.

    answer timestamp가 NULL이면
    비교 대상에서 제외한다.
    """

    expected_days = (
        datediff(
            col(
                "review_answer_timestamp"
            ),
            col(
                "review_creation_date"
            ),
        )
    )

    invalid_answer_days_count = (
        df.filter(
            col(
                "review_answer_timestamp"
            ).isNotNull()
            & (
                col("review_answer_days")
                != expected_days
            )
        )
        .count()
    )

    if invalid_answer_days_count > 0:
        raise RuntimeError(
            "review_answer_days "
            "calculation failed. "
            f"invalid_count="
            f"{invalid_answer_days_count}"
        )

    negative_answer_days_count = (
        df.filter(
            col("review_answer_days") < 0
        )
        .count()
    )

    if negative_answer_days_count > 0:
        raise RuntimeError(
            "Negative review_answer_days "
            "detected. "
            f"negative_count="
            f"{negative_answer_days_count}"
        )

    print(
        "[PASS] review_answer_days validation"
    )


def validate_date_key(df):
    """
    date_key = yyyyMMdd(review_date)
    """

    expected_date_key = (
        date_format(
            col("review_date"),
            "yyyyMMdd",
        )
        .cast("int")
    )

    invalid_date_key_count = (
        df.filter(
            col("date_key")
            != expected_date_key
        )
        .count()
    )

    if invalid_date_key_count > 0:
        raise RuntimeError(
            "date_key mapping failed. "
            f"invalid_count="
            f"{invalid_date_key_count}"
        )

    print(
        "[PASS] date_key validation"
    )


def validate_review_month(df):
    """
    review_month = yyyy-MM(review_date)
    """

    expected_month = (
        date_format(
            col("review_date"),
            "yyyy-MM",
        )
    )

    invalid_month_count = (
        df.filter(
            col("review_month")
            != expected_month
        )
        .count()
    )

    if invalid_month_count > 0:
        raise RuntimeError(
            "review_month mapping failed. "
            f"invalid_count="
            f"{invalid_month_count}"
        )

    print(
        "[PASS] review_month validation"
    )


def main():
    spark = create_spark_session(
        "Validate Fact Review"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"[INFO] fact_review_path="
        f"{FACT_REVIEW_PATH}"
    )

    df = (
        spark.read
        .parquet(
            FACT_REVIEW_PATH
        )
    )

    total_rows = (
        df.count()
    )

    distinct_grain_count = (
        df.select(
            "review_id",
            "order_id",
        )
        .distinct()
        .count()
    )

    print(
        f"[INFO] total_rows="
        f"{total_rows}"
    )

    print(
        f"[INFO] distinct_grain_count="
        f"{distinct_grain_count}"
    )

    validate_schema(df)

    validate_grain(df)

    validate_required_keys(df)

    validate_review_score(df)

    validate_review_dates(df)

    validate_answer_days(df)

    validate_date_key(df)

    validate_review_month(df)

    print(
        "[SUCCESS] "
        "fact_review validation passed"
    )

    spark.stop()


if __name__ == "__main__":
    main()