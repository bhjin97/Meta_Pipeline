from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


BRONZE_SELLERS_PATH = (
    "s3a://ecommerce/bronze/olist/sellers/"
)

DIM_SELLER_PATH = (
    "s3a://ecommerce/silver/dim_seller/"
)


def main():
    spark = create_spark_session(
        "Validate Dim Seller"
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = (
        spark.read
        .parquet(BRONZE_SELLERS_PATH)
    )

    dim_df = (
        spark.read
        .parquet(DIM_SELLER_PATH)
    )

    print("[INFO] dim_seller schema")
    dim_df.printSchema()

    bronze_count = bronze_df.count()
    dim_count = dim_df.count()

    print("\n[VALIDATION] Row Count")
    print(
        f"bronze_seller_count="
        f"{bronze_count}"
    )
    print(
        f"dim_seller_count="
        f"{dim_count}"
    )

    key_result = (
        dim_df
        .agg(
            count("*").alias("total_rows"),

            countDistinct(
                "seller_id"
            ).alias(
                "distinct_seller_id"
            ),

            spark_sum(
                col("seller_id")
                .isNull()
                .cast("int")
            ).alias(
                "null_seller_id"
            ),
        )
        .first()
    )

    duplicate_seller_id = (
        dim_df
        .groupBy("seller_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print("\n[VALIDATION] Natural Key")

    print(
        f"total_rows="
        f"{key_result['total_rows']}"
    )

    print(
        f"distinct_seller_id="
        f"{key_result['distinct_seller_id']}"
    )

    print(
        f"null_seller_id="
        f"{key_result['null_seller_id']}"
    )

    print(
        f"duplicate_seller_id="
        f"{duplicate_seller_id}"
    )

    attribute_columns = [
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ]

    null_exprs = [
        spark_sum(
            col(column)
            .isNull()
            .cast("int")
        ).alias(column)
        for column in attribute_columns
    ]

    null_result = (
        dim_df
        .agg(*null_exprs)
        .first()
        .asDict()
    )

    print("\n[VALIDATION] Attribute NULL")

    for column, null_count in null_result.items():
        print(
            f"{column}={null_count}"
        )

    checks = {
        "row_count_matches_bronze": (
            bronze_count
            == dim_count
        ),

        "seller_id_unique": (
            key_result["total_rows"]
            == key_result[
                "distinct_seller_id"
            ]
        ),

        "seller_id_not_null": (
            key_result[
                "null_seller_id"
            ]
            == 0
        ),

        "duplicate_seller_id_zero": (
            duplicate_seller_id
            == 0
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
            "dim_seller validation passed"
        )

    else:
        raise RuntimeError(
            "dim_seller validation failed"
        )

    spark.stop()


if __name__ == "__main__":
    main()