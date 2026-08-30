from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    countDistinct,
    lit,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"
FACT_ORDER_EVENT_PATH = "s3a://ecommerce/silver/fact_order_event/"
DIM_CUSTOMER_PATH = "s3a://ecommerce/silver/dim_customer/"
MART_CUSTOMER_PATH = "s3a://ecommerce/gold/mart_customer_summary/"


REQUIRED_ORDER_ITEM_COLUMNS = {
    "order_id",
    "order_item_id",
    "customer_sk",
    "item_price",
    "item_freight_value",
}

REQUIRED_ORDER_EVENT_COLUMNS = {
    "order_id",
    "event_type",
}

REQUIRED_CUSTOMER_COLUMNS = {
    "customer_sk",
    "customer_unique_id",
    "is_current",
}

REQUIRED_MART_COLUMNS = {
    "customer_unique_id",
    "customer_sk",
    "first_order_date",
    "last_order_date",
    "order_count",
    "item_count",
    "product_revenue",
    "freight_revenue",
    "total_revenue",
    "aov",
    "is_repeat_customer",
}


def validate_required_columns(
    df: DataFrame,
    required_columns: set,
    dataframe_name: str,
):
    missing_columns = required_columns - set(df.columns)

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
    if isinstance(actual, (int, float)) and isinstance(
        expected,
        (int, float),
    ):
        if abs(actual - expected) > tolerance:
            raise RuntimeError(
                f"[FAIL] {label}: "
                f"actual={actual}, expected={expected}"
            )
    else:
        if actual != expected:
            raise RuntimeError(
                f"[FAIL] {label}: "
                f"actual={actual}, expected={expected}"
            )

    print(
        f"[PASS] {label}: "
        f"actual={actual}, expected={expected}"
    )


def get_valid_order_items(
    fact_order_item_df: DataFrame,
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
        fact_order_item_df
        .join(
            canceled_orders_df,
            on="order_id",
            how="left_anti",
        )
    )


def main():
    spark = create_spark_session(
        "Validate BR03 Customer Summary Mart"
    )

    spark.sparkContext.setLogLevel("WARN")

    fact_order_item_df = spark.read.parquet(
        FACT_ORDER_ITEM_PATH
    )

    fact_order_event_df = spark.read.parquet(
        FACT_ORDER_EVENT_PATH
    )

    dim_customer_df = spark.read.parquet(
        DIM_CUSTOMER_PATH
    )

    mart_customer_df = spark.read.parquet(
        MART_CUSTOMER_PATH
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
        dim_customer_df,
        REQUIRED_CUSTOMER_COLUMNS,
        "dim_customer",
    )

    validate_required_columns(
        mart_customer_df,
        REQUIRED_MART_COLUMNS,
        "mart_customer_summary",
    )

    valid_order_items_df = get_valid_order_items(
        fact_order_item_df,
        fact_order_event_df,
    )

    valid_order_items_df.cache()

    # 1. Grain 검증
    duplicate_customer_count = (
        mart_customer_df
        .groupBy("customer_unique_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert_equal(
        duplicate_customer_count,
        0,
        "duplicate customer_unique_id",
    )

    # 2. NULL 고객키 검증
    null_customer_count = (
        mart_customer_df
        .filter(
            col("customer_unique_id").isNull()
        )
        .count()
    )

    assert_equal(
        null_customer_count,
        0,
        "null customer_unique_id",
    )

    # 3. current customer 중복 검증
    duplicate_current_customer_count = (
        dim_customer_df
        .filter(col("is_current") == True)
        .groupBy("customer_unique_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    assert_equal(
        duplicate_current_customer_count,
        0,
        "duplicate current dim_customer",
    )

    # 4. Silver 기준 집계
    silver_agg = (
        valid_order_items_df
        .agg(
            spark_sum("item_price").alias(
                "product_revenue"
            ),
            spark_sum("item_freight_value").alias(
                "freight_revenue"
            ),
            count("*").alias(
                "item_count"
            ),
            countDistinct("order_id").alias(
                "order_count"
            ),
        )
        .first()
    )

    silver_product_revenue = float(
        silver_agg["product_revenue"]
    )

    silver_freight_revenue = float(
        silver_agg["freight_revenue"]
    )

    silver_total_revenue = (
        silver_product_revenue
        + silver_freight_revenue
    )

    silver_item_count = silver_agg["item_count"]
    silver_order_count = silver_agg["order_count"]

    # 5. Gold 기준 집계
    gold_agg = (
        mart_customer_df
        .agg(
            spark_sum("product_revenue").alias(
                "product_revenue"
            ),
            spark_sum("freight_revenue").alias(
                "freight_revenue"
            ),
            spark_sum("total_revenue").alias(
                "total_revenue"
            ),
            spark_sum("item_count").alias(
                "item_count"
            ),
            spark_sum("order_count").alias(
                "order_count"
            ),
        )
        .first()
    )

    gold_product_revenue = float(
        gold_agg["product_revenue"]
    )

    gold_freight_revenue = float(
        gold_agg["freight_revenue"]
    )

    gold_total_revenue = float(
        gold_agg["total_revenue"]
    )

    gold_item_count = gold_agg["item_count"]
    gold_order_count = gold_agg["order_count"]

    # 6. Silver ↔ Gold reconciliation
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

    assert_equal(
        silver_order_count,
        gold_order_count,
        "silver vs gold order_count",
    )

    # 7. total_revenue 계산식
    invalid_total_revenue_count = (
        mart_customer_df
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

    # 8. AOV 계산식
    invalid_aov_count = (
        mart_customer_df
        .filter(
            (col("order_count") <= 0)
            | col("aov").isNull()
            | (
                spark_abs(
                    col("aov")
                    - (
                        col("total_revenue")
                        / col("order_count")
                    )
                )
                > 0.001
            )
        )
        .count()
    )

    assert_equal(
        invalid_aov_count,
        0,
        "invalid aov",
    )

    # 9. repeat flag 검증
    invalid_repeat_flag_count = (
        mart_customer_df
        .filter(
            (
                (col("order_count") >= 2)
                & (col("is_repeat_customer") != True)
            )
            |
            (
                (col("order_count") == 1)
                & (col("is_repeat_customer") != False)
            )
        )
        .count()
    )

    assert_equal(
        invalid_repeat_flag_count,
        0,
        "invalid repeat customer flag",
    )

    print(
        "[SUCCESS] BR-03 customer summary "
        "mart validation completed"
    )

    valid_order_items_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()