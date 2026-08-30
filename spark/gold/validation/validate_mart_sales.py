from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"
FACT_ORDER_EVENT_PATH = "s3a://ecommerce/silver/fact_order_event/"

DAILY_PATH = "s3a://ecommerce/gold/mart_sales_daily/"
MONTHLY_PATH = "s3a://ecommerce/gold/mart_sales_monthly/"


def assert_equal(name, actual, expected, tolerance=0.001):
    if actual is None or expected is None:
        if actual != expected:
            raise RuntimeError(
                f"[FAIL] {name}: actual={actual}, expected={expected}"
            )
        return

    if isinstance(actual, float) or isinstance(expected, float):
        if abs(actual - expected) > tolerance:
            raise RuntimeError(
                f"[FAIL] {name}: actual={actual}, expected={expected}"
            )
    else:
        if actual != expected:
            raise RuntimeError(
                f"[FAIL] {name}: actual={actual}, expected={expected}"
            )

    print(
        f"[PASS] {name}: "
        f"actual={actual}, expected={expected}"
    )


def validate_grain(daily_df, monthly_df):
    daily_duplicate_count = (
        daily_df
        .groupBy("date_key")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    monthly_duplicate_count = (
        monthly_df
        .groupBy("year_month")
        .agg(count("*").alias("cnt"))
        .filter(col("cnt") > 1)
        .count()
    )

    assert_equal(
        "daily duplicate date_key",
        daily_duplicate_count,
        0,
    )

    assert_equal(
        "monthly duplicate year_month",
        monthly_duplicate_count,
        0,
    )


def main():
    spark = create_spark_session(
        "Validate BR01 Sales Mart"
    )

    spark.sparkContext.setLogLevel("WARN")

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

    daily_df = (
        spark.read.parquet(
            DAILY_PATH
        )
    )

    monthly_df = (
        spark.read.parquet(
            MONTHLY_PATH
        )
    )

    # ------------------------------------
    # 1. 취소 주문 제외
    # ------------------------------------

    canceled_orders_df = (
        fact_order_event_df
        .filter(
            col("event_type")
            == "ORDER_CANCELED"
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

    valid_order_items_df.cache()

    # ------------------------------------
    # 2. Silver 기준값 계산
    # ------------------------------------

    silver_metrics = (
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

            countDistinct(
                "order_id"
            ).alias(
                "order_count"
            ),

            count(
                "*"
            ).alias(
                "item_count"
            ),
        )
        .collect()[0]
    )

    silver_product_revenue = float(
        silver_metrics[
            "product_revenue"
        ]
    )

    silver_freight_revenue = float(
        silver_metrics[
            "freight_revenue"
        ]
    )

    silver_revenue = (
        silver_product_revenue
        + silver_freight_revenue
    )

    silver_order_count = (
        silver_metrics[
            "order_count"
        ]
    )

    silver_item_count = (
        silver_metrics[
            "item_count"
        ]
    )

    # ------------------------------------
    # 3. Gold Daily 합계
    # ------------------------------------

    daily_metrics = (
        daily_df
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
                "revenue"
            ).alias(
                "revenue"
            ),

            spark_sum(
                "order_count"
            ).alias(
                "order_count"
            ),

            spark_sum(
                "item_count"
            ).alias(
                "item_count"
            ),
        )
        .collect()[0]
    )

    # ------------------------------------
    # 4. Gold Monthly 합계
    # ------------------------------------

    monthly_metrics = (
        monthly_df
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
                "revenue"
            ).alias(
                "revenue"
            ),

            spark_sum(
                "order_count"
            ).alias(
                "order_count"
            ),

            spark_sum(
                "item_count"
            ).alias(
                "item_count"
            ),
        )
        .collect()[0]
    )

    # ------------------------------------
    # 5. Grain 검증
    # ------------------------------------

    validate_grain(
        daily_df,
        monthly_df,
    )

    # ------------------------------------
    # 6. Silver ↔ Daily
    # ------------------------------------

    assert_equal(
        "silver vs daily product_revenue",
        float(
            daily_metrics[
                "product_revenue"
            ]
        ),
        silver_product_revenue,
    )

    assert_equal(
        "silver vs daily freight_revenue",
        float(
            daily_metrics[
                "freight_revenue"
            ]
        ),
        silver_freight_revenue,
    )

    assert_equal(
        "silver vs daily revenue",
        float(
            daily_metrics[
                "revenue"
            ]
        ),
        silver_revenue,
    )

    assert_equal(
        "silver vs daily order_count",
        daily_metrics[
            "order_count"
        ],
        silver_order_count,
    )

    assert_equal(
        "silver vs daily item_count",
        daily_metrics[
            "item_count"
        ],
        silver_item_count,
    )

    # ------------------------------------
    # 7. Daily ↔ Monthly
    # ------------------------------------

    assert_equal(
        "daily vs monthly revenue",
        float(
            monthly_metrics[
                "revenue"
            ]
        ),
        float(
            daily_metrics[
                "revenue"
            ]
        ),
    )

    assert_equal(
        "daily vs monthly product_revenue",
        float(
            monthly_metrics[
                "product_revenue"
            ]
        ),
        float(
            daily_metrics[
                "product_revenue"
            ]
        ),
    )

    assert_equal(
        "daily vs monthly freight_revenue",
        float(
            monthly_metrics[
                "freight_revenue"
            ]
        ),
        float(
            daily_metrics[
                "freight_revenue"
            ]
        ),
    )

    assert_equal(
        "daily vs monthly order_count",
        monthly_metrics[
            "order_count"
        ],
        daily_metrics[
            "order_count"
        ],
    )

    assert_equal(
        "daily vs monthly item_count",
        monthly_metrics[
            "item_count"
        ],
        daily_metrics[
            "item_count"
        ],
    )

    # ------------------------------------
    # 8. AOV 검증
    # ------------------------------------

    invalid_daily_aov_count = (
        daily_df
        .filter(
            col("order_count") > 0
        )
        .filter(
            spark_abs(
                col("revenue")
                / col("order_count")
                - col("aov")
            ) > 0.001
        )
        .count()
    )

    invalid_monthly_aov_count = (
        monthly_df
        .filter(
            col("order_count") > 0
        )
        .filter(
            spark_abs(
                col("revenue")
                / col("order_count")
                - col("aov")
            ) > 0.001
        )
        .count()
    )

    assert_equal(
        "daily invalid AOV",
        invalid_daily_aov_count,
        0,
    )

    assert_equal(
        "monthly invalid AOV",
        invalid_monthly_aov_count,
        0,
    )

    print(
        "[SUCCESS] BR-01 sales mart validation completed"
    )

    valid_order_items_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()