from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    date_format,
    lag,
    lit,
    round,
    sum as spark_sum,
    when,
)
from pyspark.sql.window import Window

from common.postgres import write_to_postgres
from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = (
    "s3a://ecommerce/silver/fact_order_item/"
)

FACT_ORDER_EVENT_PATH = (
    "s3a://ecommerce/silver/fact_order_event/"
)

REQUIRED_ORDER_ITEM_COLUMNS = {
    "order_id",
    "order_item_id",
    "date_key",
    "order_date",
    "item_price",
    "item_freight_value",
}

REQUIRED_ORDER_EVENT_COLUMNS = {
    "order_id",
    "event_type",
}


def validate_required_columns(
    df: DataFrame,
    required_columns: set,
    dataframe_name: str,
):
    """
    필요한 Silver 컬럼이 존재하는지 사전 검증한다.
    """

    actual_columns = set(df.columns)

    missing_columns = (
        required_columns - actual_columns
    )

    if missing_columns:
        raise RuntimeError(
            f"{dataframe_name} missing columns: "
            f"{sorted(missing_columns)}"
        )


def get_valid_order_items(
    fact_order_item_df: DataFrame,
    fact_order_event_df: DataFrame,
) -> DataFrame:
    """
    취소 주문을 제외한 주문 상품 Fact를 반환한다.
    """

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


def build_daily_mart(
    valid_order_items_df: DataFrame,
) -> DataFrame:
    """
    Grain:
        date_key 당 1행
    """

    daily_df = (
        valid_order_items_df
        .groupBy(
            "date_key",
            "order_date",
        )
        .agg(
            spark_sum(
                col("item_price")
            ).alias(
                "product_revenue"
            ),

            spark_sum(
                col("item_freight_value")
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
        .withColumn(
            "revenue",
            col("product_revenue")
            + col("freight_revenue"),
        )
        .withColumn(
            "aov",
            when(
                col("order_count") > 0,
                col("revenue")
                / col("order_count"),
            ),
        )
        .withColumn(
            "avg_item_value",
            when(
                col("item_count") > 0,
                col("product_revenue")
                / col("item_count"),
            ),
        )
        .withColumn(
            "year",
            date_format(
                col("order_date"),
                "yyyy",
            ).cast("int"),
        )
        .withColumn(
            "month",
            date_format(
                col("order_date"),
                "MM",
            ).cast("int"),
        )
        .withColumn(
            "year_month",
            date_format(
                col("order_date"),
                "yyyy-MM",
            ),
        )
        .select(
            "date_key",
            "order_date",
            "year",
            "month",
            "year_month",
            "revenue",
            "product_revenue",
            "freight_revenue",
            "order_count",
            "item_count",
            "aov",
            "avg_item_value",
        )
    )

    return daily_df


def build_monthly_mart(
    daily_df: DataFrame,
) -> DataFrame:
    """
    Grain:
        year_month 당 1행

    전월 대비 성장률까지 계산한다.
    """

    monthly_df = (
        daily_df
        .groupBy(
            "year_month"
        )
        .agg(
            spark_sum(
                "revenue"
            ).alias(
                "revenue"
            ),

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
        .withColumn(
            "aov",
            when(
                col("order_count") > 0,
                col("revenue")
                / col("order_count"),
            ),
        )
    )

    monthly_window = (
        Window.orderBy(
            "year_month"
        )
    )

    monthly_df = (
        monthly_df
        .withColumn(
            "previous_revenue",
            lag(
                "revenue"
            ).over(
                monthly_window
            ),
        )
        .withColumn(
            "previous_order_count",
            lag(
                "order_count"
            ).over(
                monthly_window
            ),
        )
        .withColumn(
            "previous_aov",
            lag(
                "aov"
            ).over(
                monthly_window
            ),
        )
        .withColumn(
            "revenue_growth_rate",
            when(
                col("previous_revenue") > 0,
                (
                    col("revenue")
                    - col("previous_revenue")
                )
                / col("previous_revenue"),
            ),
        )
        .withColumn(
            "order_growth_rate",
            when(
                col("previous_order_count") > 0,
                (
                    col("order_count")
                    - col("previous_order_count")
                )
                / col("previous_order_count"),
            ),
        )
        .withColumn(
            "aov_growth_rate",
            when(
                col("previous_aov") > 0,
                (
                    col("aov")
                    - col("previous_aov")
                )
                / col("previous_aov"),
            ),
        )
        .select(
            "year_month",
            "revenue",
            "product_revenue",
            "freight_revenue",
            "order_count",
            "item_count",
            "aov",
            "previous_revenue",
            "revenue_growth_rate",
            "previous_order_count",
            "order_growth_rate",
            "previous_aov",
            "aov_growth_rate",
        )
    )

    return monthly_df


def validate_daily_mart(
    daily_df: DataFrame,
):
    """
    Gold Daily Mart 최소 검증.
    """

    duplicate_date_count = (
        daily_df
        .groupBy("date_key")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_date_count > 0:
        raise RuntimeError(
            "Duplicate date_key detected in "
            "mart_sales_daily. "
            f"duplicate_date_count="
            f"{duplicate_date_count}"
        )

    null_key_count = (
        daily_df
        .filter(
            col("date_key").isNull()
            | col("order_date").isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise RuntimeError(
            "NULL key detected in "
            "mart_sales_daily. "
            f"null_key_count={null_key_count}"
        )

    negative_revenue_count = (
        daily_df
        .filter(
            col("revenue") < 0
        )
        .count()
    )

    if negative_revenue_count > 0:
        raise RuntimeError(
            "Negative revenue detected. "
            f"negative_revenue_count="
            f"{negative_revenue_count}"
        )


def validate_monthly_mart(
    monthly_df: DataFrame,
):
    duplicate_month_count = (
        monthly_df
        .groupBy("year_month")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_month_count > 0:
        raise RuntimeError(
            "Duplicate year_month detected in "
            "mart_sales_monthly. "
            f"duplicate_month_count="
            f"{duplicate_month_count}"
        )


def main():
    spark = create_spark_session(
        "Build BR01 Sales Mart"
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

    fact_order_item_df = (
        spark.read
        .parquet(
            FACT_ORDER_ITEM_PATH
        )
    )

    fact_order_event_df = (
        spark.read
        .parquet(
            FACT_ORDER_EVENT_PATH
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

    valid_order_items_df = (
        get_valid_order_items(
            fact_order_item_df,
            fact_order_event_df,
        )
    )

    valid_order_items_df.cache()

    valid_item_count = (
        valid_order_items_df.count()
    )

    print(
        f"[INFO] valid_item_count="
        f"{valid_item_count}"
    )

    daily_df = (
        build_daily_mart(
            valid_order_items_df
        )
    )

    validate_daily_mart(
        daily_df
    )

    monthly_df = (
        build_monthly_mart(
            daily_df
        )
    )

    validate_monthly_mart(
        monthly_df
    )

    daily_count = (
        daily_df.count()
    )

    monthly_count = (
        monthly_df.count()
    )

    print(
        f"[INFO] daily_row_count="
        f"{daily_count}"
    )

    print(
        f"[INFO] monthly_row_count="
        f"{monthly_count}"
    )

    write_to_postgres(
        daily_df,
        "mart_sales_daily",
    )

    write_to_postgres(
        monthly_df,
        "mart_sales_monthly",
    )

    print(
        "[SUCCESS] BR-01 sales marts "
        "build completed"
    )

    print(
        f"[INFO] daily_output="
        f"gold_staging.mart_sales_daily"
    )

    print(
        f"[INFO] monthly_output="
        f"gold_staging.mart_sales_monthly"
    )

    valid_order_items_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
