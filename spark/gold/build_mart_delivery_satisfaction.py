from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    count,
    lit,
    row_number,
    when,
)

from common.postgres import write_to_postgres
from common.spark_session import create_spark_session


FACT_DELIVERY_PATH = (
    "s3a://ecommerce/silver/fact_delivery/"
)

FACT_REVIEW_PATH = (
    "s3a://ecommerce/silver/fact_review/"
)

FACT_ORDER_EVENT_PATH = (
    "s3a://ecommerce/silver/fact_order_event/"
)

REQUIRED_DELIVERY_COLUMNS = {
    "order_id",
    "customer_sk",
    "date_key",
    "order_date",
    "shipping_days",
    "delivery_days",
    "delay_days",
    "is_delivered",
    "is_delayed",
}

REQUIRED_REVIEW_COLUMNS = {
    "review_id",
    "order_id",
    "review_score",
    "review_answer_timestamp",
    "review_answer_days",
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


def get_valid_deliveries(
    fact_delivery_df: DataFrame,
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

    valid_delivery_df = (
        fact_delivery_df
        .join(
            canceled_orders_df,
            on="order_id",
            how="left_anti",
        )
    )

    return valid_delivery_df


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

    latest_review_df = (
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
        .drop(
            "review_rank"
        )
        .select(
            "order_id",
            "review_id",
            "review_score",
            "review_answer_timestamp",
            "review_answer_days",
        )
    )

    return latest_review_df


def build_delivery_satisfaction_mart(
    valid_delivery_df: DataFrame,
    latest_review_df: DataFrame,
) -> DataFrame:

    mart_df = (
        valid_delivery_df
        .join(
            latest_review_df,
            on="order_id",
            how="left",
        )
        .withColumn(
            "has_review",
            col("review_id").isNotNull(),
        )
        .withColumn(
            "delivery_status",
            when(
                col("is_delivered") == False,
                lit("not_delivered"),
            )
            .when(
                col("is_delayed") == True,
                lit("delayed"),
            )
            .otherwise(
                lit("on_time"),
            ),
        )
        .select(
            "order_id",
            "customer_sk",
            "date_key",
            "order_date",

            "shipping_days",
            "delivery_days",
            "delay_days",

            "is_delivered",
            "is_delayed",
            "delivery_status",

            "review_id",
            "review_score",
            "review_answer_timestamp",
            "review_answer_days",
            "has_review",
        )
    )

    return mart_df


def validate_mart(
    mart_df: DataFrame,
    valid_delivery_df: DataFrame,
):
    # ------------------------------------
    # 1. Grain
    # order_id 당 1행
    # ------------------------------------

    duplicate_order_count = (
        mart_df
        .groupBy(
            "order_id"
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_order_count > 0:
        raise RuntimeError(
            "Duplicate delivery satisfaction "
            "mart grain detected. "
            f"duplicate_order_count="
            f"{duplicate_order_count}"
        )

    # ------------------------------------
    # 2. order_id NULL
    # ------------------------------------

    null_order_count = (
        mart_df
        .filter(
            col("order_id").isNull()
        )
        .count()
    )

    if null_order_count > 0:
        raise RuntimeError(
            "NULL order_id detected. "
            f"null_order_count="
            f"{null_order_count}"
        )

    # ------------------------------------
    # 3. Delivery 행 보존
    #
    # latest review LEFT JOIN이므로
    # valid delivery와 Gold 행 수가
    # 동일해야 함
    # ------------------------------------

    source_count = (
        valid_delivery_df.count()
    )

    mart_count = (
        mart_df.count()
    )

    if source_count != mart_count:
        raise RuntimeError(
            "Delivery row count mismatch. "
            f"source_count={source_count}, "
            f"mart_count={mart_count}"
        )


def main():
    spark = create_spark_session(
        "Build BR04 Delivery Satisfaction Mart"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"[INFO] fact_delivery_path="
        f"{FACT_DELIVERY_PATH}"
    )

    print(
        f"[INFO] fact_review_path="
        f"{FACT_REVIEW_PATH}"
    )

    print(
        f"[INFO] fact_order_event_path="
        f"{FACT_ORDER_EVENT_PATH}"
    )

    fact_delivery_df = (
        spark.read.parquet(
            FACT_DELIVERY_PATH
        )
    )

    fact_review_df = (
        spark.read.parquet(
            FACT_REVIEW_PATH
        )
    )

    fact_order_event_df = (
        spark.read.parquet(
            FACT_ORDER_EVENT_PATH
        )
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

    valid_delivery_df = (
        get_valid_deliveries(
            fact_delivery_df,
            fact_order_event_df,
        )
    )

    valid_delivery_df.cache()

    latest_review_df = (
        get_latest_review_per_order(
            fact_review_df
        )
    )

    latest_review_df.cache()

    valid_delivery_count = (
        valid_delivery_df.count()
    )

    latest_review_count = (
        latest_review_df.count()
    )

    print(
        f"[INFO] valid_delivery_count="
        f"{valid_delivery_count}"
    )

    print(
        f"[INFO] latest_review_count="
        f"{latest_review_count}"
    )

    mart_df = (
        build_delivery_satisfaction_mart(
            valid_delivery_df,
            latest_review_df,
        )
    )

    validate_mart(
        mart_df,
        valid_delivery_df,
    )

    mart_count = (
        mart_df.count()
    )

    reviewed_order_count = (
        mart_df
        .filter(
            col("has_review") == True
        )
        .count()
    )

    delayed_order_count = (
        mart_df
        .filter(
            col("is_delayed") == True
        )
        .count()
    )

    delivered_order_count = (
        mart_df
        .filter(
            col("is_delivered") == True
        )
        .count()
    )

    print(
        f"[INFO] mart_row_count="
        f"{mart_count}"
    )

    print(
        f"[INFO] reviewed_order_count="
        f"{reviewed_order_count}"
    )

    print(
        f"[INFO] delivered_order_count="
        f"{delivered_order_count}"
    )

    print(
        f"[INFO] delayed_order_count="
        f"{delayed_order_count}"
    )

    write_to_postgres(
        mart_df,
        "mart_delivery_satisfaction",
    )

    print(
        "[SUCCESS] BR-04 delivery satisfaction "
        "mart build completed"
    )

    print(
        f"[INFO] output_path="
        f"gold_staging.mart_delivery_satisfaction"
    )

    valid_delivery_df.unpersist()
    latest_review_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
