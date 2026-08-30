from pyspark.sql.functions import (
    col,
    date_format,
    datediff,
)

from common.spark_session import create_spark_session


FACT_PATH = "s3a://ecommerce/silver/fact_delivery/"


def main():
    spark = create_spark_session(
        "Validate Fact Delivery"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(FACT_PATH)

    total_rows = df.count()

    distinct_orders = (
        df.select("order_id")
        .distinct()
        .count()
    )

    duplicate_order_count = (
        df.groupBy("order_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_order_id = (
        df.filter(col("order_id").isNull())
        .count()
    )

    null_customer_sk = (
        df.filter(col("customer_sk").isNull())
        .count()
    )

    null_date_key = (
        df.filter(col("date_key").isNull())
        .count()
    )

    invalid_shipping_days = (
        df.filter(
            col("shipping_days").isNotNull()
            & (
                col("shipping_days")
                != datediff(
                    col("order_delivered_carrier_date"),
                    col("order_purchase_timestamp"),
                )
            )
        )
        .count()
    )

    invalid_delivery_days = (
        df.filter(
            col("delivery_days").isNotNull()
            & (
                col("delivery_days")
                != datediff(
                    col("order_delivered_customer_date"),
                    col("order_purchase_timestamp"),
                )
            )
        )
        .count()
    )

    invalid_delay_days = (
        df.filter(
            col("delay_days").isNotNull()
            & (
                col("delay_days")
                != datediff(
                    col("order_delivered_customer_date"),
                    col("order_estimated_delivery_date"),
                )
            )
        )
        .count()
    )

    invalid_is_delivered = (
        df.filter(
            (
                col("order_delivered_customer_date").isNotNull()
                & (col("is_delivered") != True)
            )
            |
            (
                col("order_delivered_customer_date").isNull()
                & (col("is_delivered") != False)
            )
        )
        .count()
    )

    invalid_is_delayed = (
        df.filter(
            col("order_delivered_customer_date").isNotNull()
            & (
                col("is_delayed")
                != (
                    col("order_delivered_customer_date")
                    > col("order_estimated_delivery_date")
                )
            )
        )
        .count()
    )

    invalid_date_key = (
        df.filter(
            col("date_key")
            != date_format(
                col("order_date"),
                "yyyyMMdd",
            ).cast("int")
        )
        .count()
    )

    invalid_order_month = (
        df.filter(
            col("order_month")
            != date_format(
                col("order_date"),
                "yyyy-MM",
            )
        )
        .count()
    )

    print("\n[VALIDATION] Grain")
    print(f"total_rows={total_rows}")
    print(f"distinct_orders={distinct_orders}")
    print(
        f"duplicate_order_count="
        f"{duplicate_order_count}"
    )

    print("\n[VALIDATION] NULL")
    print(f"null_order_id={null_order_id}")
    print(
        f"null_customer_sk={null_customer_sk}"
    )
    print(f"null_date_key={null_date_key}")

    print("\n[VALIDATION] Measures")
    print(
        f"invalid_shipping_days="
        f"{invalid_shipping_days}"
    )
    print(
        f"invalid_delivery_days="
        f"{invalid_delivery_days}"
    )
    print(
        f"invalid_delay_days="
        f"{invalid_delay_days}"
    )
    print(
        f"invalid_is_delivered="
        f"{invalid_is_delivered}"
    )
    print(
        f"invalid_is_delayed="
        f"{invalid_is_delayed}"
    )

    print("\n[VALIDATION] Date")
    print(
        f"invalid_date_key="
        f"{invalid_date_key}"
    )
    print(
        f"invalid_order_month="
        f"{invalid_order_month}"
    )

    checks = {
        "order_grain_unique": (
            total_rows == distinct_orders
            and duplicate_order_count == 0
        ),
        "order_id_not_null": (
            null_order_id == 0
        ),
        "customer_sk_not_null": (
            null_customer_sk == 0
        ),
        "date_key_not_null": (
            null_date_key == 0
        ),
        "shipping_days_correct": (
            invalid_shipping_days == 0
        ),
        "delivery_days_correct": (
            invalid_delivery_days == 0
        ),
        "delay_days_correct": (
            invalid_delay_days == 0
        ),
        "is_delivered_correct": (
            invalid_is_delivered == 0
        ),
        "is_delayed_correct": (
            invalid_is_delayed == 0
        ),
        "date_key_consistent": (
            invalid_date_key == 0
        ),
        "order_month_consistent": (
            invalid_order_month == 0
        ),
    }

    print("\n[RESULT]")

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check_name}: {status}")

    if all(checks.values()):
        print(
            "\n[SUCCESS] "
            "fact_delivery validation passed"
        )
    else:
        failed_checks = [
            name
            for name, passed in checks.items()
            if not passed
        ]

        raise RuntimeError(
            "fact_delivery validation failed: "
            + ", ".join(failed_checks)
        )

    spark.stop()


if __name__ == "__main__":
    main()