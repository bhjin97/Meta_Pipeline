from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    lit,
    sum as spark_sum,
)

from common.postgres import read_from_postgres
from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"
FACT_ORDER_EVENT_PATH = "s3a://ecommerce/silver/fact_order_event/"
REQUIRED_ORDER_ITEM_COLUMNS = {
    "order_id",
    "order_item_id",
    "item_price",
    "item_freight_value",
}

REQUIRED_ORDER_EVENT_COLUMNS = {
    "order_id",
    "event_type",
}

REQUIRED_MART_COLUMNS = {
    "date_key",
    "order_date",
    "category_name",
    "product_revenue",
    "freight_revenue",
    "total_revenue",
    "order_count",
    "item_count",
    "avg_item_price",
}


def validate_required_columns(
    df: DataFrame,
    required_columns: set,
    dataframe_name: str,
):
    actual_columns = set(df.columns)

    missing_columns = (
        required_columns
        - actual_columns
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
    tolerance: float = 0.001,
):
    if actual is None or expected is None:
        if actual != expected:
            raise RuntimeError(
                f"[FAIL] {label}: "
                f"actual={actual}, "
                f"expected={expected}"
            )

        print(
            f"[PASS] {label}: "
            f"actual={actual}, "
            f"expected={expected}"
        )
        return

    if isinstance(actual, (int, float)) and isinstance(
        expected,
        (int, float),
    ):
        if abs(actual - expected) > tolerance:
            raise RuntimeError(
                f"[FAIL] {label}: "
                f"actual={actual}, "
                f"expected={expected}"
            )

    else:
        if actual != expected:
            raise RuntimeError(
                f"[FAIL] {label}: "
                f"actual={actual}, "
                f"expected={expected}"
            )

    print(
        f"[PASS] {label}: "
        f"actual={actual}, "
        f"expected={expected}"
    )


def get_valid_order_items(
    fact_order_item_df: DataFrame,
    fact_order_event_df: DataFrame,
) -> DataFrame:

    canceled_orders_df = (
        fact_order_event_df
        .filter(
            col("event_type")
            == lit("ORDER_CANCELED")
        )
        .select("order_id")
        .dropDuplicates(["order_id"])
    )

    valid_order_items_df = (
        fact_order_item_df
        .join(
            canceled_orders_df,
            on="order_id",
            how="left_anti",
        )
    )

    return valid_order_items_df


def main():
    spark = create_spark_session(
        "Validate BR02 Category Mart"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"[INFO] fact_order_item_path="
        f"{FACT_ORDER_ITEM_PATH}"
    )

    print(
        f"[INFO] fact_order_event_path="
        f"{FACT_ORDER_EVENT_PATH}"
    )

    print(
        f"[INFO] mart_category_path="
        f"gold_staging.mart_category_daily"
    )

    fact_order_item_df = (
        spark.read.parquet(
            FACT_ORDER_ITEM_PATH
        )
    )

    fact_order_event_df = (
        spark.read.parquet(
            FACT_ORDER_EVENT_PATH
        )
    )

    mart_category_df = (
        read_from_postgres(
            spark,
            "mart_category_daily",
        )
    )

    validate_required_columns(
        fact_order_item_df,
        REQUIRED_ORDER_ITEM_COLUMNS,
        "fact_order_item",
    )

    validate_required_columns(
        fact_order_event_df,
        REQUIRED_ORDER_EVENT_COLUMNS,
        "fact_order_event",
    )

    validate_required_columns(
        mart_category_df,
        REQUIRED_MART_COLUMNS,
        "mart_category_daily",
    )

    valid_order_items_df = (
        get_valid_order_items(
            fact_order_item_df,
            fact_order_event_df,
        )
    )

    valid_order_items_df.cache()

    # ------------------------------------
    # 1. Grain 중복 검증
    # date_key × category_name
    # ------------------------------------

    duplicate_grain_count = (
        mart_category_df
        .groupBy(
            "date_key",
            "category_name",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    assert_equal(
        duplicate_grain_count,
        0,
        "duplicate date_key x category_name",
    )

    # ------------------------------------
    # 2. Key NULL 검증
    # ------------------------------------

    null_key_count = (
        mart_category_df
        .filter(
            col("date_key").isNull()
            | col("category_name").isNull()
        )
        .count()
    )

    assert_equal(
        null_key_count,
        0,
        "null mart keys",
    )

    # ------------------------------------
    # 3. Silver 기준 집계
    # ------------------------------------

    silver_agg = (
        valid_order_items_df
        .agg(
            spark_sum(
                "item_price"
            ).alias(
                "product_revenue"
            ),

            spark_sum(
                "item_freight_value"
            ).alias(
                "freight_revenue"
            ),

            count(
                "*"
            ).alias(
                "item_count"
            ),
        )
        .first()
    )

    silver_product_revenue = (
        float(
            silver_agg["product_revenue"]
        )
    )

    silver_freight_revenue = (
        float(
            silver_agg["freight_revenue"]
        )
    )

    silver_total_revenue = (
        silver_product_revenue
        + silver_freight_revenue
    )

    silver_item_count = (
        silver_agg["item_count"]
    )

    # ------------------------------------
    # 4. Gold 기준 집계
    # ------------------------------------

    gold_agg = (
        mart_category_df
        .agg(
            spark_sum(
                "product_revenue"
            ).alias(
                "product_revenue"
            ),

            spark_sum(
                "freight_revenue"
            ).alias(
                "freight_revenue"
            ),

            spark_sum(
                "total_revenue"
            ).alias(
                "total_revenue"
            ),

            spark_sum(
                "item_count"
            ).alias(
                "item_count"
            ),
        )
        .first()
    )

    gold_product_revenue = (
        float(
            gold_agg["product_revenue"]
        )
    )

    gold_freight_revenue = (
        float(
            gold_agg["freight_revenue"]
        )
    )

    gold_total_revenue = (
        float(
            gold_agg["total_revenue"]
        )
    )

    gold_item_count = (
        gold_agg["item_count"]
    )

    # ------------------------------------
    # 5. Silver ↔ Gold 정합성 검증
    # ------------------------------------

    assert_equal(
        silver_product_revenue,
        gold_product_revenue,
        "silver vs gold product_revenue",
    )

    assert_equal(
        silver_freight_revenue,
        gold_freight_revenue,
        "silver vs gold freight_revenue",
    )

    assert_equal(
        silver_total_revenue,
        gold_total_revenue,
        "silver vs gold total_revenue",
    )

    assert_equal(
        silver_item_count,
        gold_item_count,
        "silver vs gold item_count",
    )

    # ------------------------------------
    # 6. total_revenue 계산식 검증
    # ------------------------------------

    invalid_total_revenue_count = (
        mart_category_df
        .filter(
            col("total_revenue").isNull()
            | (
                spark_abs(
                    col("total_revenue")
                    - (
                        col("product_revenue")
                        + col("freight_revenue")
                    )
                )
                > 0.001
            )
        )
        .count()
    )

    assert_equal(
        invalid_total_revenue_count,
        0,
        "invalid total_revenue",
    )

    # ------------------------------------
    # 7. avg_item_price 계산식 검증
    # ------------------------------------

    invalid_avg_item_price_count = (
        mart_category_df
        .filter(
            (col("item_count") <= 0)
            | col("avg_item_price").isNull()
            | (
                spark_abs(
                    (
                        col("product_revenue")
                        / col("item_count")
                    )
                    - col("avg_item_price")
                )
                > 0.001
            )
        )
        .count()
    )

    assert_equal(
        invalid_avg_item_price_count,
        0,
        "invalid avg_item_price",
    )

    print(
        "[SUCCESS] BR-02 category mart "
        "validation completed"
    )

    valid_order_items_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
