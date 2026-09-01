from pyspark.sql.functions import col

from common.spark_session import create_spark_session
from pyspark.sql.functions import col, date_format


FACT_PATH = (
    "s3a://ecommerce/silver/fact_order_event/"
)

EXPECTED_COLUMNS = {
    "order_id",
    "customer_sk",
    "event_type",
    "event_time",
    "date_key",
    "order_status",
    "event_date",
    "event_month",
}

ALLOWED_EVENT_TYPES = {
    "ORDER_CREATED",
    "ORDER_APPROVED",
    "ORDER_CANCELED",
}


def main():
    spark = create_spark_session(
        "Validate Fact Order Event"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(FACT_PATH)

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
    # 2. Grain
    # order_id + event_type + event_time
    # --------------------------------------------------

    total_rows = df.count()

    distinct_grain = (
        df.select(
            "order_id",
            "event_type",
            "event_time",
        )
        .distinct()
        .count()
    )

    duplicate_grain_count = (
        df.groupBy(
            "order_id",
            "event_type",
            "event_time",
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    # --------------------------------------------------
    # 3. Required NULL checks
    # --------------------------------------------------

    required_columns = [
        "order_id",
        "customer_sk",
        "event_type",
        "event_time",
        "date_key",
        "event_date",
        "event_month",
    ]

    null_counts = {}

    for column_name in required_columns:
        null_counts[column_name] = (
            df.filter(
                col(column_name).isNull()
            )
            .count()
        )

    # --------------------------------------------------
    # 4. Event type
    # --------------------------------------------------

    invalid_event_type_count = (
        df.filter(
            ~col("event_type").isin(
                list(ALLOWED_EVENT_TYPES)
            )
        )
        .count()
    )

    # --------------------------------------------------
    # 5. Date consistency
    # --------------------------------------------------

    invalid_date_key_count = (
        df.filter(
            col("date_key")
            != date_format(col("event_date"), "yyyyMMdd").cast("int")
        )
        .count()
    )

    invalid_event_month_count = (
        df.filter(
            col("event_month")
            != date_format(
                col("event_date"),
                "yyyy-MM",
            )
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
    print(f"total_rows={total_rows}")
    print(
        f"distinct_grain="
        f"{distinct_grain}"
    )
    print(
        f"duplicate_grain_count="
        f"{duplicate_grain_count}"
    )

    print("\n[VALIDATION] NULL")

    for column_name in required_columns:
        print(
            f"null_{column_name}="
            f"{null_counts[column_name]}"
        )

    print("\n[VALIDATION] Event Type")
    print(
        f"invalid_event_type_count="
        f"{invalid_event_type_count}"
    )

    print("\n[VALIDATION] Date")
    print(
        f"invalid_date_key_count="
        f"{invalid_date_key_count}"
    )
    print(
        f"invalid_event_month_count="
        f"{invalid_event_month_count}"
    )

    # --------------------------------------------------
    # Result
    # --------------------------------------------------

    checks = {
        "schema_matches_target": (
            len(missing_columns) == 0
            and len(unexpected_columns) == 0
        ),

        "event_grain_unique": (
            total_rows == distinct_grain
            and duplicate_grain_count == 0
        ),

        "required_columns_not_null": (
            all(
                count == 0
                for count in null_counts.values()
            )
        ),

        "event_type_valid": (
            invalid_event_type_count == 0
        ),

        "date_key_consistent": (
            invalid_date_key_count == 0
        ),

        "event_month_consistent": (
            invalid_event_month_count == 0
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
            "fact_order_event validation passed"
        )

    else:
        failed_checks = [
            name
            for name, passed
            in checks.items()
            if not passed
        ]

        raise RuntimeError(
            "fact_order_event validation failed: "
            + ", ".join(failed_checks)
        )

    spark.stop()


if __name__ == "__main__":
    main()
    