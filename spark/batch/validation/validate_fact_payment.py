from pyspark.sql.functions import col

from common.spark_session import create_spark_session


FACT_PAYMENT_PATH = (
    "s3a://ecommerce/silver/fact_payment/"
)

EXPECTED_COLUMNS = {
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
}


def main():
    spark = create_spark_session(
        "Validate Fact Payment"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(
        FACT_PAYMENT_PATH
    )

    # --------------------------------------------------
    # 1. Schema
    # --------------------------------------------------

    actual_columns = set(df.columns)

    missing_columns = (
        EXPECTED_COLUMNS - actual_columns
    )

    unexpected_columns = (
        actual_columns - EXPECTED_COLUMNS
    )

    # --------------------------------------------------
    # 2. Row / Grain
    # --------------------------------------------------

    total_rows = df.count()

    distinct_grain = (
        df.select(
            "order_id",
            "payment_sequential",
        )
        .distinct()
        .count()
    )

    duplicate_grain_count = (
        df.groupBy(
            "order_id",
            "payment_sequential",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    # --------------------------------------------------
    # 3. NULL
    # --------------------------------------------------

    null_order_id = (
        df.filter(
            col("order_id").isNull()
        )
        .count()
    )

    null_payment_sequential = (
        df.filter(
            col("payment_sequential").isNull()
        )
        .count()
    )

    null_payment_type = (
        df.filter(
            col("payment_type").isNull()
        )
        .count()
    )

    null_payment_installments = (
        df.filter(
            col("payment_installments").isNull()
        )
        .count()
    )

    null_payment_value = (
        df.filter(
            col("payment_value").isNull()
        )
        .count()
    )

    # --------------------------------------------------
    # 4. Value validation
    # --------------------------------------------------

    invalid_payment_sequential = (
        df.filter(
            col("payment_sequential") <= 0
        )
        .count()
    )

    invalid_payment_installments = (
        df.filter(
            col("payment_installments") < 0
        )
        .count()
    )

    negative_payment_value = (
        df.filter(
            col("payment_value") < 0
        )
        .count()
    )

    # --------------------------------------------------
    # Print
    # --------------------------------------------------

    print("\n[VALIDATION] Schema")
    print(
        f"missing_columns="
        f"{sorted(missing_columns)}"
    )
    print(
        f"unexpected_columns="
        f"{sorted(unexpected_columns)}"
    )

    print("\n[VALIDATION] Grain")
    print(
        f"total_rows={total_rows}"
    )
    print(
        f"distinct_grain={distinct_grain}"
    )
    print(
        f"duplicate_grain_count="
        f"{duplicate_grain_count}"
    )

    print("\n[VALIDATION] NULL")
    print(
        f"null_order_id="
        f"{null_order_id}"
    )
    print(
        f"null_payment_sequential="
        f"{null_payment_sequential}"
    )
    print(
        f"null_payment_type="
        f"{null_payment_type}"
    )
    print(
        f"null_payment_installments="
        f"{null_payment_installments}"
    )
    print(
        f"null_payment_value="
        f"{null_payment_value}"
    )

    print("\n[VALIDATION] Values")
    print(
        f"invalid_payment_sequential="
        f"{invalid_payment_sequential}"
    )
    print(
        f"invalid_payment_installments="
        f"{invalid_payment_installments}"
    )
    print(
        f"negative_payment_value="
        f"{negative_payment_value}"
    )

    # --------------------------------------------------
    # Result
    # --------------------------------------------------

    checks = {
        "schema_matches_target": (
            len(missing_columns) == 0
            and len(unexpected_columns) == 0
        ),

        "payment_grain_unique": (
            total_rows == distinct_grain
            and duplicate_grain_count == 0
        ),

        "order_id_not_null": (
            null_order_id == 0
        ),

        "payment_sequential_not_null": (
            null_payment_sequential == 0
        ),

        "payment_type_not_null": (
            null_payment_type == 0
        ),

        "payment_installments_not_null": (
            null_payment_installments == 0
        ),

        "payment_value_not_null": (
            null_payment_value == 0
        ),

        "payment_sequential_valid": (
            invalid_payment_sequential == 0
        ),

        "payment_installments_valid": (
            invalid_payment_installments == 0
        ),

        "payment_value_non_negative": (
            negative_payment_value == 0
        ),
    }

    print("\n[RESULT]")

    for check_name, passed in checks.items():
        status = (
            "PASS"
            if passed
            else "FAIL"
        )

        print(
            f"{check_name}: {status}"
        )

    if all(checks.values()):
        print(
            "\n[SUCCESS] "
            "fact_payment validation passed"
        )

    else:
        failed_checks = [
            check_name
            for check_name, passed
            in checks.items()
            if not passed
        ]

        raise RuntimeError(
            "fact_payment validation failed: "
            + ", ".join(failed_checks)
        )

    spark.stop()


if __name__ == "__main__":
    main()