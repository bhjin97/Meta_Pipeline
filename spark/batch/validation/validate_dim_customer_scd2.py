from datetime import date, timedelta

from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max as spark_max,
)

from common.spark_session import create_spark_session
from scripts.build_dim_customer import PROCESS_DATE


PRODUCTION_DIM_PATH = "s3a://ecommerce/silver/dim_customer/"
TEST_DIM_PATH = "s3a://ecommerce/silver/test/dim_customer/"

EXPECTED_PRODUCTION_ROWS = 96_096
EXPECTED_TEST_ROWS = 96_097


def main():
    spark = create_spark_session(
        "Validate Dim Customer SCD2 Change"
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        production_df = (
            spark.read
            .parquet(PRODUCTION_DIM_PATH)
            .cache()
        )
        test_df = (
            spark.read
            .parquet(TEST_DIM_PATH)
            .cache()
        )

        production_rows = production_df.count()
        production_max_sk = (
            production_df
            .agg(spark_max("customer_sk").alias("max_sk"))
            .first()["max_sk"]
        )

        totals = (
            test_df
            .agg(
                count("*").alias("total_rows"),
                countDistinct("customer_sk").alias(
                    "distinct_customer_sk"
                ),
            )
            .first()
        )

        duplicate_customer_sk = (
            test_df
            .groupBy("customer_sk")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        version_counts_df = (
            test_df
            .groupBy("customer_unique_id")
            .count()
            .cache()
        )
        changed_customers = (
            version_counts_df
            .filter(col("count") == 2)
            .select("customer_unique_id")
            .collect()
        )
        customers_with_more_than_two_versions = (
            version_counts_df
            .filter(col("count") > 2)
            .count()
        )

        changed_customer_count = len(changed_customers)
        changed_customer_id = (
            changed_customers[0]["customer_unique_id"]
            if changed_customer_count == 1
            else None
        )

        if changed_customer_id is None:
            changed_versions_df = test_df.limit(0)
        else:
            changed_versions_df = test_df.filter(
                col("customer_unique_id")
                == changed_customer_id
            )

        old_version = (
            changed_versions_df
            .filter(col("is_current") == False)
            .orderBy(col("valid_from").desc())
            .first()
        )
        new_version = (
            changed_versions_df
            .filter(col("is_current") == True)
            .first()
        )

        changed_current_rows = (
            changed_versions_df
            .filter(col("is_current") == True)
            .count()
        )
        customers_with_multiple_current_rows = (
            test_df
            .filter(col("is_current") == True)
            .groupBy("customer_unique_id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        process_date = date.fromisoformat(PROCESS_DATE)
        expected_old_valid_to = process_date - timedelta(days=1)

        old_valid_to_matches = (
            old_version is not None
            and old_version["valid_to"] == expected_old_valid_to
        )
        new_valid_from_matches = (
            new_version is not None
            and new_version["valid_from"] == process_date
        )
        new_valid_to_is_null = (
            new_version is not None
            and new_version["valid_to"] is None
        )
        new_sk_is_greater = (
            new_version is not None
            and new_version["customer_sk"] > production_max_sk
        )

        old_row_matches_production = False
        province_changed_only = False

        if old_version is not None and new_version is not None:
            production_old = (
                production_df
                .filter(
                    col("customer_sk")
                    == old_version["customer_sk"]
                )
                .first()
            )
            old_row_matches_production = (
                production_old is not None
                and production_old["province"]
                == old_version["province"]
            )

            excluded_columns = {
                "customer_sk",
                "province",
                "valid_from",
                "valid_to",
                "is_current",
            }
            stable_columns = [
                column
                for column in test_df.columns
                if column not in excluded_columns
            ]
            province_changed_only = (
                old_version["province"] != new_version["province"]
                and all(
                    old_version[column] == new_version[column]
                    for column in stable_columns
                )
            )

        unchanged_customers_with_new_versions = (
            version_counts_df
            .filter(col("customer_unique_id") != changed_customer_id)
            .filter(col("count") > 1)
            .count()
            if changed_customer_id is not None
            else version_counts_df.filter(col("count") > 1).count()
        )

        checks = {
            "total_rows_96097": (
                totals["total_rows"] == EXPECTED_TEST_ROWS
            ),
            "distinct_customer_sk_96097": (
                totals["distinct_customer_sk"]
                == EXPECTED_TEST_ROWS
            ),
            "duplicate_customer_sk_zero": (
                duplicate_customer_sk == 0
            ),
            "one_changed_customer_with_two_versions": (
                changed_customer_count == 1
                and customers_with_more_than_two_versions == 0
            ),
            "changed_customer_one_current_row": (
                changed_current_rows == 1
            ),
            "no_customer_has_multiple_current_rows": (
                customers_with_multiple_current_rows == 0
            ),
            "old_valid_to_is_process_date_minus_one": (
                old_valid_to_matches
            ),
            "new_valid_from_is_process_date": (
                new_valid_from_matches
            ),
            "new_valid_to_is_null": new_valid_to_is_null,
            "new_customer_sk_above_production_max": (
                new_sk_is_greater
            ),
            "old_sk_and_province_are_preserved": (
                old_row_matches_production
            ),
            "province_is_the_only_profile_change": (
                province_changed_only
            ),
            "unchanged_customers_have_no_new_version": (
                unchanged_customers_with_new_versions == 0
            ),
            "production_rows_remain_96096": (
                production_rows == EXPECTED_PRODUCTION_ROWS
            ),
        }

        print("[VALIDATION] dim_customer SCD2 change")
        print(f"production_rows={production_rows}")
        print(f"test_total_rows={totals['total_rows']}")
        print(
            "distinct_customer_sk="
            f"{totals['distinct_customer_sk']}"
        )
        print(
            "duplicate_customer_sk="
            f"{duplicate_customer_sk}"
        )
        print(
            "changed_customer_unique_id="
            f"{changed_customer_id}"
        )
        print(f"process_date={PROCESS_DATE}")

        print("\n[RESULT]")
        for check_name, passed in checks.items():
            status = "PASS" if passed else "FAIL"
            print(f"{check_name}: {status}")

        if not all(checks.values()):
            failed_checks = [
                name
                for name, passed in checks.items()
                if not passed
            ]
            raise RuntimeError(
                "SCD2 validation failed: "
                + ", ".join(failed_checks)
            )

        print("\n[SUCCESS] dim_customer SCD2 validation passed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
