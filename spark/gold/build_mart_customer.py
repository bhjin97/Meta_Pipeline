from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = (
    "s3a://ecommerce/silver/fact_order_item/"
)

FACT_ORDER_EVENT_PATH = (
    "s3a://ecommerce/silver/fact_order_event/"
)

DIM_CUSTOMER_PATH = (
    "s3a://ecommerce/silver/dim_customer/"
)

OUTPUT_PATH = (
    "s3a://ecommerce/gold/mart_customer_summary/"
)


REQUIRED_ORDER_ITEM_COLUMNS = {
    "order_id",
    "order_item_id",
    "customer_sk",
    "order_date",
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
    "sex",
    "age",
    "age_group",
    "occupation",
    "marital_status",
    "education_level",
    "family_type",
    "housing_type",
    "province",
    "district",
    "persona",
    "is_current",
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

    return (
        fact_order_item_df
        .join(
            canceled_orders_df,
            on="order_id",
            how="left_anti",
        )
    )


def build_customer_metrics(
    valid_order_items_df: DataFrame,
    dim_customer_df: DataFrame,
) -> DataFrame:

    # ------------------------------------
    # Fact의 customer_sk를
    # customer_unique_id로 변환
    #
    # customer_sk는 SCD2 버전 키
    # customer_unique_id는 실제 고객 단위 키
    # ------------------------------------

    customer_key_lookup_df = (
        dim_customer_df
        .select(
            "customer_sk",
            "customer_unique_id",
        )
        .dropDuplicates(
            ["customer_sk"]
        )
    )

    customer_order_items_df = (
        valid_order_items_df
        .join(
            customer_key_lookup_df,
            on="customer_sk",
            how="left",
        )
    )

    # ------------------------------------
    # 고객 lifetime 집계
    # Grain = customer_unique_id
    # ------------------------------------

    customer_metrics_df = (
        customer_order_items_df
        .groupBy(
            "customer_unique_id"
        )
        .agg(
            spark_min(
                "order_date"
            ).alias(
                "first_order_date"
            ),

            spark_max(
                "order_date"
            ).alias(
                "last_order_date"
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
        )
        .withColumn(
            "total_revenue",
            col("product_revenue")
            + col("freight_revenue"),
        )
        .withColumn(
            "aov",
            when(
                col("order_count") > 0,
                col("total_revenue")
                / col("order_count"),
            ),
        )
        .withColumn(
            "is_repeat_customer",
            col("order_count") >= 2,
        )
    )

    return customer_metrics_df


def build_current_customer_attributes(
    dim_customer_df: DataFrame,
) -> DataFrame:

    # ------------------------------------
    # Mart는 고객당 1행이어야 하므로
    # 현재 SCD2 버전만 사용
    # ------------------------------------

    current_customer_df = (
        dim_customer_df
        .filter(
            col("is_current") == True
        )
        .select(
            "customer_unique_id",
            "customer_sk",
            "sex",
            "age",
            "age_group",
            "occupation",
            "marital_status",
            "education_level",
            "family_type",
            "housing_type",
            "province",
            "district",
            "persona",
        )
    )

    return current_customer_df


def build_customer_mart(
    valid_order_items_df: DataFrame,
    dim_customer_df: DataFrame,
) -> DataFrame:

    customer_metrics_df = (
        build_customer_metrics(
            valid_order_items_df,
            dim_customer_df,
        )
    )

    current_customer_df = (
        build_current_customer_attributes(
            dim_customer_df
        )
    )

    mart_df = (
        customer_metrics_df
        .join(
            current_customer_df,
            on="customer_unique_id",
            how="left",
        )
        .select(
            "customer_unique_id",
            "customer_sk",

            "sex",
            "age",
            "age_group",
            "occupation",
            "marital_status",
            "education_level",
            "family_type",
            "housing_type",
            "province",
            "district",
            "persona",

            "first_order_date",
            "last_order_date",

            "order_count",
            "item_count",

            "product_revenue",
            "freight_revenue",
            "total_revenue",

            "aov",

            "is_repeat_customer",
        )
    )

    return mart_df


def validate_mart(
    mart_df: DataFrame,
    dim_customer_df: DataFrame,
):
    # ------------------------------------
    # 1. Mart Grain 검증
    # customer_unique_id 당 1행
    # ------------------------------------

    duplicate_customer_count = (
        mart_df
        .groupBy(
            "customer_unique_id"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_customer_count > 0:
        raise RuntimeError(
            "Duplicate customer mart grain detected. "
            f"duplicate_customer_count="
            f"{duplicate_customer_count}"
        )

    # ------------------------------------
    # 2. customer_unique_id NULL 검증
    # ------------------------------------

    null_customer_count = (
        mart_df
        .filter(
            col(
                "customer_unique_id"
            ).isNull()
        )
        .count()
    )

    if null_customer_count > 0:
        raise RuntimeError(
            "NULL customer_unique_id detected. "
            f"null_customer_count="
            f"{null_customer_count}"
        )

    # ------------------------------------
    # 3. 현재 Customer Dimension 중복 검증
    #
    # 한 customer_unique_id에
    # is_current=True가 여러 개면
    # Gold Join에서 fan-out 발생
    # ------------------------------------

    duplicate_current_customer_count = (
        dim_customer_df
        .filter(
            col("is_current") == True
        )
        .groupBy(
            "customer_unique_id"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_current_customer_count > 0:
        raise RuntimeError(
            "Duplicate current dim_customer detected. "
            f"duplicate_current_customer_count="
            f"{duplicate_current_customer_count}"
        )

    # ------------------------------------
    # 4. 잘못된 주문 수 검증
    # ------------------------------------

    invalid_order_count = (
        mart_df
        .filter(
            col("order_count") <= 0
        )
        .count()
    )

    if invalid_order_count > 0:
        raise RuntimeError(
            "Invalid order_count detected. "
            f"invalid_order_count="
            f"{invalid_order_count}"
        )

    # ------------------------------------
    # 5. 음수 매출 검증
    # ------------------------------------

    negative_revenue_count = (
        mart_df
        .filter(
            col("total_revenue") < 0
        )
        .count()
    )

    if negative_revenue_count > 0:
        raise RuntimeError(
            "Negative customer revenue detected. "
            f"negative_revenue_count="
            f"{negative_revenue_count}"
        )


def main():
    spark = create_spark_session(
        "Build BR03 Customer Summary Mart"
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
        f"[INFO] dim_customer_path="
        f"{DIM_CUSTOMER_PATH}"
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

    dim_customer_df = (
        spark.read.parquet(
            DIM_CUSTOMER_PATH
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
        dim_customer_df,
        REQUIRED_CUSTOMER_COLUMNS,
        "dim_customer",
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
        build_customer_mart(
            valid_order_items_df,
            dim_customer_df,
        )
    )

    validate_mart(
        mart_df,
        dim_customer_df,
    )

    customer_count = (
        mart_df.count()
    )

    repeat_customer_count = (
        mart_df
        .filter(
            col("is_repeat_customer") == True
        )
        .count()
    )

    one_time_customer_count = (
        mart_df
        .filter(
            col("is_repeat_customer") == False
        )
        .count()
    )

    print(
        f"[INFO] customer_count="
        f"{customer_count}"
    )

    print(
        f"[INFO] repeat_customer_count="
        f"{repeat_customer_count}"
    )

    print(
        f"[INFO] one_time_customer_count="
        f"{one_time_customer_count}"
    )

    (
        mart_df
        .write
        .mode("overwrite")
        .parquet(
            OUTPUT_PATH
        )
    )

    print(
        "[SUCCESS] BR-03 customer summary mart "
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