from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


DIM_CUSTOMER_PATH = "s3a://ecommerce/silver/dim_customer/"
INITIAL_VALID_FROM = "2016-01-01"


def main():
    spark = create_spark_session(
        "Validate Dim Customer"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(DIM_CUSTOMER_PATH)

    print("[INFO] dim_customer schema")
    df.printSchema()

    # -----------------------------------------
    # 1. 기본 행 수 / Key / NULL 검증
    # -----------------------------------------

    result = (
        df.agg(
            count("*").alias("total_rows"),

            countDistinct("customer_sk").alias(
                "distinct_customer_sk"
            ),

            countDistinct("customer_unique_id").alias(
                "distinct_customer_unique_id"
            ),

            spark_sum(
                col("customer_sk")
                .isNull()
                .cast("int")
            ).alias("null_customer_sk"),

            spark_sum(
                col("customer_unique_id")
                .isNull()
                .cast("int")
            ).alias("null_customer_unique_id"),

            spark_min("customer_sk").alias(
                "min_customer_sk"
            ),

            spark_max("customer_sk").alias(
                "max_customer_sk"
            ),
        )
        .first()
    )

    print("\n[VALIDATION] Keys")
    print(
        f"total_rows="
        f"{result['total_rows']}"
    )
    print(
        f"distinct_customer_sk="
        f"{result['distinct_customer_sk']}"
    )
    print(
        f"distinct_customer_unique_id="
        f"{result['distinct_customer_unique_id']}"
    )
    print(
        f"null_customer_sk="
        f"{result['null_customer_sk']}"
    )
    print(
        f"null_customer_unique_id="
        f"{result['null_customer_unique_id']}"
    )
    print(
        f"customer_sk_range="
        f"{result['min_customer_sk']} "
        f"~ {result['max_customer_sk']}"
    )

    # -----------------------------------------
    # 2. customer_sk 중복 검증
    # -----------------------------------------

    duplicate_sk_count = (
        df.groupBy("customer_sk")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("\n[VALIDATION] SK Uniqueness")
    print(
        f"duplicate_customer_sk="
        f"{duplicate_sk_count}"
    )

    # -----------------------------------------
    # 3. SCD2 current 상태 검증
    # -----------------------------------------

    print("\n[VALIDATION] SCD2 Current Status")

    (
        df.groupBy("is_current")
        .count()
        .orderBy("is_current")
        .show()
    )

    current_duplicate_count = (
        df
        .filter(col("is_current") == True)
        .groupBy("customer_unique_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(
        f"customers_with_multiple_current_rows="
        f"{current_duplicate_count}"
    )

    # -----------------------------------------
    # 4. 최초 valid_from 검증
    # -----------------------------------------

    invalid_initial_valid_from = (
        df
        .filter(
            col("valid_from")
            != INITIAL_VALID_FROM
        )
        .count()
    )

    print("\n[VALIDATION] Validity Period")
    print(
        f"invalid_initial_valid_from="
        f"{invalid_initial_valid_from}"
    )

    # 최초 적재에서는 모든 row의 valid_to가 NULL
    non_null_valid_to = (
        df
        .filter(
            col("valid_to").isNotNull()
        )
        .count()
    )

    print(
        f"non_null_valid_to="
        f"{non_null_valid_to}"
    )

    # -----------------------------------------
    # 5. Persona UUID 검증
    # -----------------------------------------

    persona_result = (
        df.agg(
            countDistinct("persona_uuid").alias(
                "distinct_persona_uuid"
            ),
            spark_sum(
                col("persona_uuid")
                .isNull()
                .cast("int")
            ).alias(
                "null_persona_uuid"
            ),
        )
        .first()
    )

    print("\n[VALIDATION] Persona")
    print(
        f"distinct_persona_uuid="
        f"{persona_result['distinct_persona_uuid']}"
    )
    print(
        f"null_persona_uuid="
        f"{persona_result['null_persona_uuid']}"
    )

    # -----------------------------------------
    # 6. 최종 PASS / FAIL
    # -----------------------------------------

    checks = {
        "customer_sk_unique": (
            result["total_rows"]
            == result["distinct_customer_sk"]
        ),
        "customer_unique_id_unique_initially": (
            result["total_rows"]
            == result["distinct_customer_unique_id"]
        ),
        "customer_sk_not_null": (
            result["null_customer_sk"] == 0
        ),
        "customer_unique_id_not_null": (
            result["null_customer_unique_id"] == 0
        ),
        "duplicate_customer_sk": (
            duplicate_sk_count == 0
        ),
        "one_current_row_per_customer": (
            current_duplicate_count == 0
        ),
        "initial_valid_from": (
            invalid_initial_valid_from == 0
        ),
        "initial_valid_to_null": (
            non_null_valid_to == 0
        ),
        "persona_uuid_not_null": (
            persona_result["null_persona_uuid"] == 0
        ),
    }

    print("\n[RESULT]")

    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(
            f"{check_name}: {status}"
        )

    if all(checks.values()):
        print(
            "\n[SUCCESS] "
            "dim_customer validation passed"
        )
    else:
        print(
            "\n[FAIL] "
            "dim_customer validation failed"
        )

    spark.stop()


if __name__ == "__main__":
    main()