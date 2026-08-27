from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


BRONZE_PRODUCTS_PATH = (
    "s3a://ecommerce/bronze/olist/products/"
)

DIM_PRODUCT_PATH = (
    "s3a://ecommerce/silver/dim_product/"
)


def main():
    spark = create_spark_session(
        "Validate Dim Product"
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze_df = (
        spark.read
        .parquet(BRONZE_PRODUCTS_PATH)
    )

    dim_df = (
        spark.read
        .parquet(DIM_PRODUCT_PATH)
    )

    print("[INFO] dim_product schema")
    dim_df.printSchema()

    # -----------------------------------------
    # 1. Bronze / Silver row count
    # -----------------------------------------

    bronze_count = bronze_df.count()
    dim_count = dim_df.count()

    print("\n[VALIDATION] Row Count")
    print(
        f"bronze_product_count={bronze_count}"
    )
    print(
        f"dim_product_count={dim_count}"
    )

    # -----------------------------------------
    # 2. Natural Key 검증
    # -----------------------------------------

    key_result = (
        dim_df
        .agg(
            count("*").alias(
                "total_rows"
            ),

            countDistinct(
                "product_id"
            ).alias(
                "distinct_product_id"
            ),

            spark_sum(
                col("product_id")
                .isNull()
                .cast("int")
            ).alias(
                "null_product_id"
            ),
        )
        .first()
    )

    duplicate_product_id_count = (
        dim_df
        .groupBy("product_id")
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
        f"distinct_product_id="
        f"{key_result['distinct_product_id']}"
    )

    print(
        f"null_product_id="
        f"{key_result['null_product_id']}"
    )

    print(
        f"duplicate_product_id="
        f"{duplicate_product_id_count}"
    )

    # -----------------------------------------
    # 3. Category NULL 정책
    # -----------------------------------------

    category_result = (
        dim_df
        .agg(
            spark_sum(
                col(
                    "product_category_name"
                )
                .isNull()
                .cast("int")
            ).alias(
                "null_category"
            ),

            spark_sum(
                col(
                    "product_category_name_english"
                )
                .isNull()
                .cast("int")
            ).alias(
                "null_category_english"
            ),
        )
        .first()
    )

    print(
        "\n[VALIDATION] Category"
    )

    print(
        f"null_category="
        f"{category_result['null_category']}"
    )

    print(
        f"null_category_english="
        f"{category_result['null_category_english']}"
    )

    # -----------------------------------------
    # 4. 수치형 NULL 보존 확인
    # -----------------------------------------

    numeric_columns = [
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]

    null_exprs = [
        spark_sum(
            col(column)
            .isNull()
            .cast("int")
        ).alias(column)
        for column in numeric_columns
    ]

    numeric_null_result = (
        dim_df
        .agg(*null_exprs)
        .first()
        .asDict()
    )

    print(
        "\n[VALIDATION] Numeric NULL"
    )

    for column, null_count in (
        numeric_null_result.items()
    ):
        print(
            f"{column}={null_count}"
        )

    # -----------------------------------------
    # 5. 최종 PASS / FAIL
    # -----------------------------------------

    checks = {
        "row_count_matches_bronze": (
            bronze_count
            == dim_count
        ),

        "product_id_unique": (
            key_result["total_rows"]
            == key_result[
                "distinct_product_id"
            ]
        ),

        "product_id_not_null": (
            key_result[
                "null_product_id"
            ]
            == 0
        ),

        "duplicate_product_id_zero": (
            duplicate_product_id_count
            == 0
        ),

        "category_not_null_after_normalization": (
            category_result[
                "null_category"
            ]
            == 0
        ),

        "category_english_not_null_after_normalization": (
            category_result[
                "null_category_english"
            ]
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
            "dim_product validation passed"
        )

    else:
        raise RuntimeError(
            "dim_product validation failed"
        )

    spark.stop()


if __name__ == "__main__":
    main()