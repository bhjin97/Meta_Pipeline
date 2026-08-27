from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    sum as spark_sum,
)

from common.spark_session import create_spark_session


MAPPING_PATH = "s3a://ecommerce/silver/customer_persona_mapping/"


def main():
    spark = create_spark_session(
        "Validate Customer Persona Mapping"
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(MAPPING_PATH)

    result = (
        df.agg(
            count("*").alias("total_rows"),
            countDistinct("customer_unique_id").alias(
                "distinct_customers"
            ),
            countDistinct("persona_uuid").alias(
                "distinct_personas"
            ),
            spark_sum(
                col("customer_unique_id").isNull().cast("int")
            ).alias("null_customer_ids"),
            spark_sum(
                col("persona_uuid").isNull().cast("int")
            ).alias("null_persona_uuids"),
        )
        .first()
    )

    print("[VALIDATION] customer_persona_mapping")
    print(f"total_rows={result['total_rows']}")
    print(
        f"distinct_customers="
        f"{result['distinct_customers']}"
    )
    print(
        f"distinct_personas="
        f"{result['distinct_personas']}"
    )
    print(
        f"null_customer_ids="
        f"{result['null_customer_ids']}"
    )
    print(
        f"null_persona_uuids="
        f"{result['null_persona_uuids']}"
    )

    # 중복 샘플 확인
    duplicate_customers_df = (
        df.groupBy("customer_unique_id")
        .count()
        .filter(col("count") > 1)
    )

    duplicate_personas_df = (
        df.groupBy("persona_uuid")
        .count()
        .filter(col("count") > 1)
    )

    print(
        f"duplicate_customer_count="
        f"{duplicate_customers_df.count()}"
    )
    print(
        f"duplicate_persona_count="
        f"{duplicate_personas_df.count()}"
    )

    spark.stop()


if __name__ == "__main__":
    main()