from pyspark.sql.functions import col, lit, when

from common.spark_session import create_spark_session
from scripts.build_dim_customer import (
    PROCESS_DATE,
    PROFILE_COLUMNS,
    build_scd2_dimension,
)


PRODUCTION_DIM_PATH = "s3a://ecommerce/silver/dim_customer/"
TEST_OUTPUT_PATH = "s3a://ecommerce/silver/test/dim_customer/"
ALLOWED_TEST_PREFIX = "s3a://ecommerce/silver/test/"

EXPECTED_PRODUCTION_ROWS = 96_096
EXPECTED_TEST_ROWS = 96_097
TEST_PROVINCE = "__SCD2_TEST_PROVINCE__"
ALTERNATE_TEST_PROVINCE = "__SCD2_TEST_PROVINCE_ALT__"


def assert_safe_test_path(path):
    if not path.startswith(ALLOWED_TEST_PREFIX):
        raise ValueError(
            "Test output must be under "
            f"{ALLOWED_TEST_PREFIX}: {path}"
        )

    if path == PRODUCTION_DIM_PATH:
        raise ValueError(
            "Production dim_customer cannot be a test output"
        )


def main():
    assert_safe_test_path(TEST_OUTPUT_PATH)

    spark = create_spark_session(
        "Test Dim Customer SCD2 Change"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        production_dim_df = (
            spark.read
            .parquet(PRODUCTION_DIM_PATH)
            .cache()
        )

        production_rows_before = production_dim_df.count()

        if production_rows_before != EXPECTED_PRODUCTION_ROWS:
            raise RuntimeError(
                "Unexpected production row count before test: "
                f"expected={EXPECTED_PRODUCTION_ROWS}, "
                f"actual={production_rows_before}"
            )

        current_df = production_dim_df.filter(
            col("is_current") == True
        )

        target = (
            current_df
            .orderBy("customer_unique_id")
            .select(
                "customer_unique_id",
                "province",
                "customer_sk",
            )
            .first()
        )

        if target is None:
            raise RuntimeError(
                "No current customer is available for the test"
            )

        target_customer_id = target["customer_unique_id"]
        old_province = target["province"]

        new_province_expression = when(
            col("province") == TEST_PROVINCE,
            lit(ALTERNATE_TEST_PROVINCE),
        ).otherwise(lit(TEST_PROVINCE))

        test_source_df = (
            current_df
            .select(
                "customer_unique_id",
                "persona_uuid",
                *PROFILE_COLUMNS,
            )
            .withColumn(
                "province",
                when(
                    col("customer_unique_id")
                    == target_customer_id,
                    new_province_expression,
                ).otherwise(col("province")),
            )
        )

        result_df = (
            build_scd2_dimension(
                spark,
                test_source_df,
                production_dim_df,
            )
            .cache()
        )

        result_rows = result_df.count()

        if result_rows != EXPECTED_TEST_ROWS:
            raise RuntimeError(
                "Unexpected test result row count; refusing to write: "
                f"expected={EXPECTED_TEST_ROWS}, actual={result_rows}"
            )

        (
            result_df.write
            .mode("overwrite")
            .parquet(TEST_OUTPUT_PATH)
        )

        # Re-read production after the test write to prove that the
        # production path was not used as the output destination.
        production_rows_after = (
            spark.read
            .parquet(PRODUCTION_DIM_PATH)
            .count()
        )

        if production_rows_after != production_rows_before:
            raise RuntimeError(
                "Production row count changed during the test: "
                f"before={production_rows_before}, "
                f"after={production_rows_after}"
            )

        print("[SUCCESS] SCD2 test data build completed")
        print(f"[INFO] process_date={PROCESS_DATE}")
        print(f"[INFO] target_customer_unique_id={target_customer_id}")
        print(f"[INFO] old_customer_sk={target['customer_sk']}")
        print(f"[INFO] old_province={old_province!r}")
        new_province = (
            ALTERNATE_TEST_PROVINCE
            if old_province == TEST_PROVINCE
            else TEST_PROVINCE
        )
        print(
            "[INFO] new_province="
            f"{new_province!r}"
        )
        print(
            "[INFO] production_rows_before="
            f"{production_rows_before}"
        )
        print(f"[INFO] test_result_rows={result_rows}")
        print(
            "[INFO] production_rows_after="
            f"{production_rows_after}"
        )
        print(f"[INFO] output_path={TEST_OUTPUT_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
