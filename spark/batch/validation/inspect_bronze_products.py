from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


PRODUCTS_PATH = "s3a://ecommerce/bronze/olist/products/"


def main():
    spark = create_spark_session(
        "Inspect Bronze Products"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(PRODUCTS_PATH)

    print("\n[SCHEMA]")
    df.printSchema()

    print("\n[ROW COUNT]")
    print(f"total_rows={df.count()}")

    print("\n[PRODUCT ID CHECK]")

    key_result = (
        df.agg(
            count("*").alias("total_rows"),
            countDistinct("product_id").alias(
                "distinct_product_id"
            ),
            spark_sum(
                col("product_id")
                .isNull()
                .cast("int")
            ).alias("null_product_id"),
        )
        .first()
    )

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

    print("\n[NULL COUNTS]")

    null_exprs = [
        spark_sum(
            col(column)
            .isNull()
            .cast("int")
        ).alias(column)
        for column in df.columns
    ]

    null_result = (
        df.agg(*null_exprs)
        .first()
        .asDict()
    )

    for column, null_count in null_result.items():
        print(
            f"{column}: {null_count}"
        )

    print("\n[DUPLICATE PRODUCT ID]")

    duplicate_count = (
        df.groupBy("product_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(
        f"duplicate_product_id="
        f"{duplicate_count}"
    )

    print("\n[SAMPLE]")
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()