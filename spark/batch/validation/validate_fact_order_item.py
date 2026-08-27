from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


FACT_PATH = "s3a://ecommerce/silver/fact_order_item/"


def main():
    spark = create_spark_session(
        "Validate Fact Order Item"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(FACT_PATH)

    print("[INFO] fact_order_item schema")
    df.printSchema()

    total_rows = df.count()

    distinct_grain = (
        df.select(
            "order_id",
            "order_item_id",
        )
        .distinct()
        .count()
    )

    duplicate_grain_count = (
        df.groupBy(
            "order_id",
            "order_item_id",
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    null_result = (
        df.agg(
            spark_sum(
                col("customer_sk").isNull().cast("int")
            ).alias("null_customer_sk"),

            spark_sum(
                col("product_id").isNull().cast("int")
            ).alias("null_product_id"),

            spark_sum(
                col("seller_id").isNull().cast("int")
            ).alias("null_seller_id"),

            spark_sum(
                col("date_key").isNull().cast("int")
            ).alias("null_date_key"),

            spark_sum(
                col("item_price").isNull().cast("int")
            ).alias("null_item_price"),

            spark_sum(
                col("item_freight_value").isNull().cast("int")
            ).alias("null_item_freight_value"),

            spark_sum(
                col("item_total_amount").isNull().cast("int")
            ).alias("null_item_total_amount"),
        )
        .first()
    )

    invalid_amount_count = (
        df.filter(
            spark_abs(
                col("item_total_amount")
                - (
                    col("item_price")
                    + col("item_freight_value")
                )
            ) > 0.000001
        )
        .count()
    )

    forbidden_columns = {
        "event_type",
        "payment_total_value",
        "customer_id",
    }

    existing_forbidden_columns = (
        forbidden_columns.intersection(
            set(df.columns)
        )
    )

    print("\n[VALIDATION] Grain")
    print(f"total_rows={total_rows}")
    print(f"distinct_grain={distinct_grain}")
    print(
        f"duplicate_grain_count="
        f"{duplicate_grain_count}"
    )

    print("\n[VALIDATION] NULL")
    print(
        f"null_customer_sk="
        f"{null_result['null_customer_sk']}"
    )
    print(
        f"null_product_id="
        f"{null_result['null_product_id']}"
    )
    print(
        f"null_seller_id="
        f"{null_result['null_seller_id']}"
    )
    print(
        f"null_date_key="
        f"{null_result['null_date_key']}"
    )
    print(
        f"null_item_price="
        f"{null_result['null_item_price']}"
    )
    print(
        f"null_item_freight_value="
        f"{null_result['null_item_freight_value']}"
    )
    print(
        f"null_item_total_amount="
        f"{null_result['null_item_total_amount']}"
    )

    print("\n[VALIDATION] Measures")
    print(
        f"invalid_item_total_amount="
        f"{invalid_amount_count}"
    )

    print("\n[VALIDATION] Schema")
    print(
        "forbidden_columns_present="
        f"{sorted(existing_forbidden_columns)}"
    )

    checks = {
        "grain_unique": (
            total_rows == distinct_grain
            and duplicate_grain_count == 0
        ),
        "customer_sk_not_null": (
            null_result["null_customer_sk"] == 0
        ),
        "product_id_not_null": (
            null_result["null_product_id"] == 0
        ),
        "seller_id_not_null": (
            null_result["null_seller_id"] == 0
        ),
        "date_key_not_null": (
            null_result["null_date_key"] == 0
        ),
        "item_price_not_null": (
            null_result["null_item_price"] == 0
        ),
        "item_freight_value_not_null": (
            null_result["null_item_freight_value"] == 0
        ),
        "item_total_amount_not_null": (
            null_result["null_item_total_amount"] == 0
        ),
        "item_total_amount_correct": (
            invalid_amount_count == 0
        ),
        "legacy_columns_removed": (
            len(existing_forbidden_columns) == 0
        ),
    }

    print("\n[RESULT]")

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"{check_name}: {status}")

    if all(checks.values()):
        print(
            "\n[SUCCESS] "
            "fact_order_item validation passed"
        )
    else:
        raise RuntimeError(
            "fact_order_item validation failed"
        )

    spark.stop()


if __name__ == "__main__":
    main()