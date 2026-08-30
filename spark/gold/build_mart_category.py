from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    count,
    countDistinct,
    date_format,
    lit,
    sum as spark_sum,
    when,
)

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"
FACT_ORDER_EVENT_PATH = "s3a://ecommerce/silver/fact_order_event/"
DIM_PRODUCT_PATH = "s3a://ecommerce/silver/dim_product/"

OUTPUT_PATH = "s3a://ecommerce/gold/mart_category_daily/"


REQUIRED_ORDER_ITEM_COLUMNS = {
    "order_id",
    "order_item_id",
    "product_id",
    "date_key",
    "order_date",
    "item_price",
    "item_freight_value",
}

REQUIRED_ORDER_EVENT_COLUMNS = {
    "order_id",
    "event_type",
}

REQUIRED_PRODUCT_COLUMNS = {
    "product_id",
    "product_category_name",
    "product_category_name_english",
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


def build_category_mart(
    valid_order_items_df: DataFrame,
    dim_product_df: DataFrame,
) -> DataFrame:

    product_lookup_df = (
        dim_product_df
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
        )
    )

    joined_df = (
        valid_order_items_df
        .join(
            product_lookup_df,
            on="product_id",
            how="left",
        )
        .withColumn(
            "category_name",
            coalesce(
                col("product_category_name_english"),
                col("product_category_name"),
                lit("unknown"),
            ),
        )
    )

    mart_df = (
        joined_df
        .groupBy(
            "date_key",
            "order_date",
            "category_name",
        )
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
        .withColumn(
            "total_revenue",
            col("product_revenue")
            + col("freight_revenue"),
        )
        .withColumn(
            "avg_item_price",
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
            "category_name",
            "product_revenue",
            "freight_revenue",
            "total_revenue",
            "order_count",
            "item_count",
            "avg_item_price",
        )
    )

    return mart_df


def validate_mart(
    mart_df: DataFrame,
):
    duplicate_grain_count = (
        mart_df
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

    if duplicate_grain_count > 0:
        raise RuntimeError(
            "Duplicate category mart grain detected. "
            f"duplicate_grain_count="
            f"{duplicate_grain_count}"
        )

    null_key_count = (
        mart_df
        .filter(
            col("date_key").isNull()
            | col("order_date").isNull()
            | col("category_name").isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise RuntimeError(
            "NULL key detected in "
            "mart_category_daily. "
            f"null_key_count={null_key_count}"
        )

    negative_revenue_count = (
        mart_df
        .filter(
            col("product_revenue") < 0
        )
        .count()
    )

    if negative_revenue_count > 0:
        raise RuntimeError(
            "Negative product revenue detected. "
            f"negative_revenue_count="
            f"{negative_revenue_count}"
        )


def main():
    spark = create_spark_session(
        "Build BR02 Category Mart"
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
        f"[INFO] dim_product_path="
        f"{DIM_PRODUCT_PATH}"
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

    dim_product_df = (
        spark.read.parquet(
            DIM_PRODUCT_PATH
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
        dim_product_df,
        REQUIRED_PRODUCT_COLUMNS,
        "dim_product",
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

    mart_df = (
        build_category_mart(
            valid_order_items_df,
            dim_product_df,
        )
    )

    validate_mart(
        mart_df
    )

    mart_count = (
        mart_df.count()
    )

    category_count = (
        mart_df
        .select(
            "category_name"
        )
        .distinct()
        .count()
    )

    print(
        f"[INFO] mart_row_count="
        f"{mart_count}"
    )

    print(
        f"[INFO] category_count="
        f"{category_count}"
    )

    (
        mart_df
        .write
        .mode("overwrite")
        .partitionBy(
            "year_month"
        )
        .parquet(
            OUTPUT_PATH
        )
    )

    print(
        "[SUCCESS] BR-02 category mart "
        "build completed"
    )

    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    valid_order_items_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()