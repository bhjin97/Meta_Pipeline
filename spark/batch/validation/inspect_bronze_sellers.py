from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


SELLERS_PATH = "s3a://ecommerce/bronze/olist/sellers/"


def main():
    spark = create_spark_session(
        "Inspect Bronze Sellers"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(SELLERS_PATH)

    print("\n[SCHEMA]")
    df.printSchema()

    result = (
        df.agg(
            count("*").alias("total_rows"),
            countDistinct("seller_id").alias(
                "distinct_seller_id"
            ),
            spark_sum(
                col("seller_id")
                .isNull()
                .cast("int")
            ).alias("null_seller_id"),
        )
        .first()
    )

    print("\n[SELLER ID CHECK]")
    print(
        f"total_rows="
        f"{result['total_rows']}"
    )
    print(
        f"distinct_seller_id="
        f"{result['distinct_seller_id']}"
    )
    print(
        f"null_seller_id="
        f"{result['null_seller_id']}"
    )

    duplicate_seller_id = (
        df.groupBy("seller_id")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print(
        f"duplicate_seller_id="
        f"{duplicate_seller_id}"
    )

    null_columns = [
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
        for column in null_columns
    ]

    null_result = (
        df.agg(*null_exprs)
        .first()
        .asDict()
    )

    print("\n[NULL COUNTS]")

    for column, null_count in null_result.items():
        print(
            f"{column}={null_count}"
        )

    print("\n[SAMPLE]")
    df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()