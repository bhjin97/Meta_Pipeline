from pyspark.sql.functions import (
    col,
    date_format,
    datediff,
    lit,
    max as spark_max,
    to_date,
    to_timestamp,
    when,
)

from common.spark_session import create_spark_session


DELIVERY_EVENTS_PATH = (
    "s3a://ecommerce/bronze/events/delivery_events/"
)

ORDERS_PATH = (
    "s3a://ecommerce/bronze/olist/orders/"
)

CUSTOMERS_PATH = (
    "s3a://ecommerce/bronze/olist/customers/"
)

DIM_CUSTOMER_PATH = (
    "s3a://ecommerce/silver/dim_customer/"
)

OUTPUT_PATH = (
    "s3a://ecommerce/silver/fact_delivery/"
)


def build_delivery_events(spark):
    delivery_events_df = (
        spark.read
        .parquet(DELIVERY_EVENTS_PATH)
        .select(
            "order_id",
            "event_type",
            to_timestamp(
                col("event_time")
            ).alias("event_time"),
        )
    )

    # 여러 Delivery Event를 주문당 1행으로 변환
    return (
        delivery_events_df
        .groupBy("order_id")
        .agg(
            spark_max(
                when(
                    col("event_type")
                    == "DELIVERY_STARTED",
                    col("event_time"),
                )
            ).alias(
                "order_delivered_carrier_date"
            ),

            spark_max(
                when(
                    col("event_type")
                    == "DELIVERY_COMPLETED",
                    col("event_time"),
                )
            ).alias(
                "order_delivered_customer_date"
            ),
        )
    )


def build_order_lookup(spark):
    return (
        spark.read
        .parquet(ORDERS_PATH)
        .select(
            "order_id",
            "customer_id",

            to_timestamp(
                col("order_purchase_timestamp")
            ).alias(
                "order_purchase_timestamp"
            ),

            to_timestamp(
                col("order_estimated_delivery_date")
            ).alias(
                "order_estimated_delivery_date"
            ),
        )
        .dropDuplicates(["order_id"])
    )


def build_customer_lookup(spark):
    customers_df = (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select(
            "customer_id",
            "customer_unique_id",
        )
        .dropDuplicates(["customer_id"])
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


def attach_customer_sk(
    delivery_df,
    customers_df,
    dim_customer_df,
):
    return (
        delivery_df.alias("fact")
        .join(
            customers_df.alias("customer"),
            col("fact.customer_id")
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
                col("fact.order_date")
                >= col("dim.valid_from")
            )
            & (
                col("dim.valid_to").isNull()
                | (
                    col("fact.order_date")
                    <= col("dim.valid_to")
                )
            ),
            how="left",
        )
    )


def validate_before_write(df):
    duplicate_order_count = (
        df.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_order_count > 0:
        raise RuntimeError(
            "Duplicate delivery grain detected. "
            f"duplicate_order_count="
            f"{duplicate_order_count}"
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
        "Build Fact Delivery"
    )
    spark.sparkContext.setLogLevel("WARN")

    delivery_events_df = (
        build_delivery_events(spark)
    )

    orders_df = (
        build_order_lookup(spark)
    )

    (
        customers_df,
        dim_customer_df,
    ) = build_customer_lookup(spark)

    # Delivery Event가 존재하는 주문만 배송 Fact 생성
    delivery_source_df = (
        delivery_events_df.alias("delivery")
        .join(
            orders_df.alias("orders"),
            on="order_id",
            how="left",
        )
        .select(
            col("order_id"),

            col("orders.customer_id")
            .alias("customer_id"),

            col(
                "orders.order_purchase_timestamp"
            ),

            col(
                "delivery.order_delivered_carrier_date"
            ),

            col(
                "delivery.order_delivered_customer_date"
            ),

            col(
                "orders.order_estimated_delivery_date"
            ),

            to_date(
                col(
                    "orders.order_purchase_timestamp"
                )
            ).alias("order_date"),
        )
    )

    joined_df = attach_customer_sk(
        delivery_source_df,
        customers_df,
        dim_customer_df,
    )

    fact_delivery_df = (
        joined_df
        .select(
            col("fact.order_id")
            .alias("order_id"),

            col("dim.customer_sk")
            .alias("customer_sk"),

            date_format(
                col("fact.order_date"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("date_key"),

            col(
                "fact.order_purchase_timestamp"
            ),

            col(
                "fact.order_delivered_carrier_date"
            ),

            col(
                "fact.order_delivered_customer_date"
            ),

            col(
                "fact.order_estimated_delivery_date"
            ),

            datediff(
                col(
                    "fact.order_delivered_carrier_date"
                ),
                col(
                    "fact.order_purchase_timestamp"
                ),
            ).alias("shipping_days"),

            datediff(
                col(
                    "fact.order_delivered_customer_date"
                ),
                col(
                    "fact.order_purchase_timestamp"
                ),
            ).alias("delivery_days"),

            datediff(
                col(
                    "fact.order_delivered_customer_date"
                ),
                col(
                    "fact.order_estimated_delivery_date"
                ),
            ).alias("delay_days"),

            col(
                "fact.order_delivered_customer_date"
            )
            .isNotNull()
            .alias("is_delivered"),

            when(
                col(
                    "fact.order_delivered_customer_date"
                ).isNull(),
                lit(None).cast("boolean"),
            )
            .otherwise(
                col(
                    "fact.order_delivered_customer_date"
                )
                > col(
                    "fact.order_estimated_delivery_date"
                )
            )
            .alias("is_delayed"),

            col("fact.order_date")
            .alias("order_date"),

            date_format(
                col("fact.order_date"),
                "yyyy-MM",
            ).alias("order_month"),
        )
    )

    validate_before_write(
        fact_delivery_df
    )

    row_count = (
        fact_delivery_df.count()
    )

    print(
        f"[INFO] source_row_count="
        f"{row_count}"
    )

    (
        fact_delivery_df.write
        .mode("overwrite")
        .partitionBy("order_month")
        .parquet(OUTPUT_PATH)
    )

    print(
        "[SUCCESS] fact_delivery "
        "build completed"
    )
    print(
        f"[INFO] row_count={row_count}"
    )
    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()