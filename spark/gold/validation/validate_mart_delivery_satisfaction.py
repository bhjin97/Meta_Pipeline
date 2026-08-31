from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    count,
    lit,
    row_number,
)

from common.postgres import read_from_postgres
from common.spark_session import create_spark_session


FACT_DELIVERY_PATH = "s3a://ecommerce/silver/fact_delivery/"
FACT_REVIEW_PATH = "s3a://ecommerce/silver/fact_review/"
FACT_ORDER_EVENT_PATH = "s3a://ecommerce/silver/fact_order_event/"
REQUIRED_DELIVERY_COLUMNS = {
    "order_id",
    "is_delivered",
    "is_delayed",
    "delivery_days",
    "delay_days",
}

REQUIRED_REVIEW_COLUMNS = {
    "review_id",
    "order_id",
    "review_score",
    "review_answer_timestamp",
}

REQUIRED_ORDER_EVENT_COLUMNS = {
    "order_id",
    "event_type",
}

REQUIRED_MART_COLUMNS = {
    "order_id",
    "is_delivered",
    "is_delayed",
    "delivery_days",
    "delay_days",
    "delivery_status",
    "review_id",
    "review_score",
    "review_answer_timestamp",
    "has_review",
}


def validate_required_columns(
    df: DataFrame,
    required_columns: set,
    dataframe_name: str,
):
    missing_columns = (
        required_columns
        - set(df.columns)
    )

    if missing_columns:
        raise RuntimeError(
            f"{dataframe_name} missing columns: "
            f"{sorted(missing_columns)}"
        )


def assert_equal(
    actual,
    expected,
    label: str,
):
    if actual != expected:
        raise RuntimeError(
            f"[FAIL] {label}: "
            f"actual={actual}, expected={expected}"
        )

    print(
        f"[PASS] {label}: "
        f"actual={actual}, expected={expected}"
    )


def get_valid_deliveries(
    fact_delivery_df: DataFrame,
    fact_order_event_df: DataFrame,
) -> DataFrame:

    canceled_orders_df = (
        fact_order_event_df
        .filter(
            col("event_type") == lit("ORDER_CANCELED")
        )
        .select("order_id")
        .dropDuplicates(["order_id"])
    )

    return (
        fact_delivery_df
        .join(
            canceled_orders_df,
            on="order_id",
            how="left_anti",
        )
    )


def get_latest_review_per_order(
    fact_review_df: DataFrame,
) -> DataFrame:

    review_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col(
                "review_answer_timestamp"
            ).desc_nulls_last(),
            col("review_id").desc(),
        )
    )

    return (
        fact_review_df
        .withColumn(
            "review_rank",
            row_number().over(
                review_window
            ),
        )
        .filter(
            col("review_rank") == 1
        )
        .drop("review_rank")
    )


def main():
    spark = create_spark_session(
        "Validate BR04 Delivery Satisfaction Mart"
    )

    spark.sparkContext.setLogLevel("WARN")

    fact_delivery_df = spark.read.parquet(
        FACT_DELIVERY_PATH
    )

    fact_review_df = spark.read.parquet(
        FACT_REVIEW_PATH
    )

    fact_order_event_df = spark.read.parquet(
        FACT_ORDER_EVENT_PATH
    )

    mart_delivery_df = read_from_postgres(
        spark,
        "mart_delivery_satisfaction",
    )

    validate_required_columns(
        fact_delivery_df,
        REQUIRED_DELIVERY_COLUMNS,
        "fact_delivery",
    )

    validate_required_columns(
        fact_review_df,
        REQUIRED_REVIEW_COLUMNS,
        "fact_review",
    )

    validate_required_columns(
        fact_order_event_df,
        REQUIRED_ORDER_EVENT_COLUMNS,
        "fact_order_event",
    )

    validate_required_columns(
        mart_delivery_df,
        REQUIRED_MART_COLUMNS,
        "mart_delivery_satisfaction",
    )

    valid_delivery_df = get_valid_deliveries(
        fact_delivery_df,
        fact_order_event_df,
    )

    latest_review_df = get_latest_review_per_order(
        fact_review_df
    )

    valid_delivery_df.cache()
    latest_review_df.cache()

    # 1. Gold Grain 검증
    duplicate_order_count = (
        mart_delivery_df
        .groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert_equal(
        duplicate_order_count,
        0,
        "duplicate order_id",
    )

    # 2. NULL order_id 검증
    null_order_count = (
        mart_delivery_df
        .filter(
            col("order_id").isNull()
        )
        .count()
    )

    assert_equal(
        null_order_count,
        0,
        "null order_id",
    )

    # 3. Delivery 행 수 보존
    valid_delivery_count = (
        valid_delivery_df.count()
    )

    mart_count = (
        mart_delivery_df.count()
    )

    assert_equal(
        valid_delivery_count,
        mart_count,
        "valid delivery vs gold row count",
    )

    # 4. Source delivery order set과 Gold order set 비교
    missing_delivery_count = (
        valid_delivery_df
        .select("order_id")
        .join(
            mart_delivery_df
            .select("order_id"),
            on="order_id",
            how="left_anti",
        )
        .count()
    )

    assert_equal(
        missing_delivery_count,
        0,
        "missing delivery orders in gold",
    )

    unexpected_gold_order_count = (
        mart_delivery_df
        .select("order_id")
        .join(
            valid_delivery_df
            .select("order_id"),
            on="order_id",
            how="left_anti",
        )
        .count()
    )

    assert_equal(
        unexpected_gold_order_count,
        0,
        "unexpected orders in gold",
    )

    # 5. latest review 자체가 order당 1행인지
    duplicate_latest_review_count = (
        latest_review_df
        .groupBy("order_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    assert_equal(
        duplicate_latest_review_count,
        0,
        "duplicate latest review order_id",
    )

    # 6. Gold review가 실제 latest review와 일치하는지
    review_mismatch_count = (
        mart_delivery_df
        .alias("g")
        .join(
            latest_review_df
            .select(
                "order_id",
                "review_id",
                "review_score",
                "review_answer_timestamp",
            )
            .alias("r"),
            on="order_id",
            how="left",
        )
        .filter(
            (
                col("g.review_id").isNotNull()
                & col("r.review_id").isNull()
            )
            |
            (
                col("g.review_id").isNull()
                & col("r.review_id").isNotNull()
            )
            |
            (
                col("g.review_id")
                != col("r.review_id")
            )
        )
        .count()
    )

    assert_equal(
        review_mismatch_count,
        0,
        "latest review mismatch",
    )

    # 7. has_review 검증
    invalid_has_review_count = (
        mart_delivery_df
        .filter(
            (
                col("review_id").isNotNull()
                & (col("has_review") != True)
            )
            |
            (
                col("review_id").isNull()
                & (col("has_review") != False)
            )
        )
        .count()
    )

    assert_equal(
        invalid_has_review_count,
        0,
        "invalid has_review flag",
    )

    # 8. delivery_status 검증
    invalid_delivery_status_count = (
        mart_delivery_df
        .filter(
            (
                (col("is_delivered") == False)
                & (
                    col("delivery_status")
                    != lit("not_delivered")
                )
            )
            |
            (
                (col("is_delivered") == True)
                & (col("is_delayed") == True)
                & (
                    col("delivery_status")
                    != lit("delayed")
                )
            )
            |
            (
                (col("is_delivered") == True)
                & (col("is_delayed") == False)
                & (
                    col("delivery_status")
                    != lit("on_time")
                )
            )
        )
        .count()
    )

    assert_equal(
        invalid_delivery_status_count,
        0,
        "invalid delivery_status",
    )

    # 9. reviewed order count reconciliation
    expected_reviewed_order_count = (
        valid_delivery_df
        .select("order_id")
        .join(
            latest_review_df
            .select("order_id"),
            on="order_id",
            how="inner",
        )
        .select("order_id")
        .distinct()
        .count()
    )

    gold_reviewed_order_count = (
        mart_delivery_df
        .filter(
            col("has_review") == True
        )
        .count()
    )

    assert_equal(
        expected_reviewed_order_count,
        gold_reviewed_order_count,
        "reviewed order count",
    )

    print(
        "[SUCCESS] BR-04 delivery satisfaction "
        "mart validation completed"
    )

    valid_delivery_df.unpersist()
    latest_review_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
