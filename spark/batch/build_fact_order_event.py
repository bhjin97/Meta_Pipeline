from pyspark.sql.functions import (
    col,
    date_format,
    to_date,
    to_timestamp,
)
from pyspark.sql.utils import AnalysisException

from common.spark_session import create_spark_session


ORDER_EVENTS_PATH = (
    "s3a://ecommerce/bronze/events/order_events/"
)

CUSTOMERS_PATH = (
    "s3a://ecommerce/bronze/olist/customers/"
)

DIM_CUSTOMER_PATH = (
    "s3a://ecommerce/silver/dim_customer/"
)

OUTPUT_PATH = (
    "s3a://ecommerce/silver/fact_order_event/"
)


def read_existing_keys(spark):
    try:
        existing_df = (
            spark.read
            .parquet(OUTPUT_PATH)
        )

        print(
            "[INFO] Existing fact_order_event found. "
            "Running incremental load."
        )

        return (
            existing_df
            .select(
                "order_id",
                "event_type",
                "event_time",
            )
            .dropDuplicates(
                [
                    "order_id",
                    "event_type",
                    "event_time",
                ]
            )
        )

    except AnalysisException as e:
        if "PATH_NOT_FOUND" not in str(e):
            raise

        print(
            "[INFO] No existing fact_order_event found. "
            "Running initial load."
        )

        return spark.createDataFrame(
            [],
            """
            order_id string,
            event_type string,
            event_time timestamp
            """,
        )


def build_customer_lookup(spark):
    customers_df = (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select(
            "customer_id",
            "customer_unique_id",
        )
        .dropDuplicates(
            ["customer_id"]
        )
    )

    dim_customer_df = (
        spark.read
        .parquet(DIM_CUSTOMER_PATH)
        .select(
            "customer_sk",
            "customer_unique_id",
            "valid_from",
            "valid_to",
        )
    )

    return (
        customers_df,
        dim_customer_df,
    )


def build_source_df(
    spark,
    customers_df,
    dim_customer_df,
):
    order_events_df = (
        spark.read
        .parquet(ORDER_EVENTS_PATH)
        .select(
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            to_timestamp(
                col("event_time")
            ).alias("event_time"),
            col("order_status"),
        )
        .withColumn(
            "event_date",
            to_date("event_time"),
        )
    )

    return (
        order_events_df.alias("event")
        .join(
            customers_df.alias("customer"),
            col("event.customer_id")
            == col("customer.customer_id"),
            how="left",
        )
        .join(
            dim_customer_df.alias("dim"),
            (
                col(
                    "customer.customer_unique_id"
                )
                == col(
                    "dim.customer_unique_id"
                )
            )
            & (
                col("event.event_date")
                >= col("dim.valid_from")
            )
            & (
                col("dim.valid_to").isNull()
                | (
                    col("event.event_date")
                    <= col("dim.valid_to")
                )
            ),
            how="left",
        )
        .select(
            col("event.order_id")
            .alias("order_id"),

            col("dim.customer_sk")
            .alias("customer_sk"),

            col("event.event_type")
            .alias("event_type"),

            col("event.event_time")
            .alias("event_time"),

            date_format(
                col("event.event_date"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("date_key"),

            col("event.order_status")
            .alias("order_status"),

            col("event.event_date")
            .alias("event_date"),

            date_format(
                col("event.event_date"),
                "yyyy-MM",
            )
            .alias("event_month"),
        )
        .dropDuplicates(
            [
                "order_id",
                "event_type",
                "event_time",
            ]
        )
    )


def validate_before_write(df):
    duplicate_count = (
        df.groupBy(
            "order_id",
            "event_type",
            "event_time",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Duplicate order event grain detected. "
            f"duplicate_count={duplicate_count}"
        )

    null_customer_sk_count = (
        df.filter(
            col("customer_sk").isNull()
        )
        .count()
    )

    if null_customer_sk_count > 0:
        raise RuntimeError(
            "customer_sk mapping failed. "
            f"null_customer_sk_count="
            f"{null_customer_sk_count}"
        )


def main():
    spark = create_spark_session(
        "Build Fact Order Event"
    )
    spark.sparkContext.setLogLevel("WARN")

    (
        customers_df,
        dim_customer_df,
    ) = build_customer_lookup(spark)

    source_df = build_source_df(
        spark,
        customers_df,
        dim_customer_df,
    )

    validate_before_write(source_df)

    source_count = source_df.count()

    print(
        f"[INFO] source_row_count="
        f"{source_count}"
    )

    processed_keys_df = (
        read_existing_keys(spark)
    )

    new_event_df = (
        source_df
        .join(
            processed_keys_df,
            on=[
                "order_id",
                "event_type",
                "event_time",
            ],
            how="left_anti",
        )
    )

    new_row_count = (
        new_event_df.count()
    )

    print(
        f"[INFO] new_row_count="
        f"{new_row_count}"
    )

    if new_row_count == 0:
        print(
            "[INFO] No new order events "
            "to process"
        )

        spark.stop()
        return

    (
        new_event_df.write
        .mode("append")
        .partitionBy(
            "event_month"
        )
        .parquet(
            OUTPUT_PATH
        )
    )

    print(
        "[SUCCESS] fact_order_event "
        "build completed"
    )

    print(
        f"[INFO] written_rows="
        f"{new_row_count}"
    )

    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()