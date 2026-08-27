from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    coalesce,
    date_sub,
    lit,
    max as spark_max,
    row_number,
    sha2,
    when,
)

from common.spark_session import create_spark_session


PERSONA_PATH = "s3a://ecommerce/bronze/persona/nemotron_korea/"
MAPPING_PATH = "s3a://ecommerce/silver/customer_persona_mapping/"
OUTPUT_PATH = "s3a://ecommerce/silver/dim_customer/"

INITIAL_VALID_FROM = "2016-01-01"
PROCESS_DATE = "2026-08-27"

PROFILE_COLUMNS = [
    "customer_name",
    "sex",
    "age",
    "age_group",
    "occupation",
    "marital_status",
    "education_level",
    "family_type",
    "housing_type",
    "province",
    "district",
    "persona",
]


def path_exists(spark, path):
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()

    fs = jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.java.net.URI(path),
        hadoop_conf,
    )

    return fs.exists(
        jvm.org.apache.hadoop.fs.Path(path)
    )


def add_profile_hash(df):
    hash_columns = [
        coalesce(
            col(column).cast("string"),
            lit("__NULL__"),
        )
        for column in PROFILE_COLUMNS
    ]

    return df.withColumn(
        "profile_hash",
        sha2(
            concat_ws("||", *hash_columns),
            256,
        ),
    )


def build_customer_profile(mapping_df, persona_df):
    persona_profile_df = (
        persona_df
        .select(
            col("uuid").alias("persona_uuid"),
            *PROFILE_COLUMNS,
        )
        .dropDuplicates(["persona_uuid"])
    )

    return (
        mapping_df
        .join(
            persona_profile_df,
            on="persona_uuid",
            how="inner",
        )
        .select(
            "customer_unique_id",
            "persona_uuid",
            *PROFILE_COLUMNS,
        )
    )


def build_initial_dimension(customer_profile_df):
    window = Window.orderBy(
        "customer_unique_id"
    )

    return (
        customer_profile_df
        .withColumn(
            "customer_sk",
            row_number()
            .over(window)
            .cast("long"),
        )
        .withColumn(
            "valid_from",
            lit(INITIAL_VALID_FROM)
            .cast("date"),
        )
        .withColumn(
            "valid_to",
            lit(None)
            .cast("date"),
        )
        .withColumn(
            "is_current",
            lit(True),
        )
        .select(
            "customer_sk",
            "customer_unique_id",
            "persona_uuid",
            *PROFILE_COLUMNS,
            "valid_from",
            "valid_to",
            "is_current",
        )
    )


def build_scd2_dimension(
    spark,
    customer_profile_df,
    existing_dim_df,
):
    source_df = (
        add_profile_hash(
            customer_profile_df
        )
        .alias("source")
    )

    current_df = (
        add_profile_hash(
            existing_dim_df
            .filter(
                col("is_current") == True
            )
        )
        .alias("current")
    )

    current_hash_df = (
        current_df
        .select(
            col("customer_unique_id"),
            col("profile_hash").alias(
                "current_profile_hash"
            ),
        )
    )

    comparison_df = (
        source_df
        .join(
            current_hash_df,
            on="customer_unique_id",
            how="left",
        )
    )

    changed_df = (
        comparison_df
        .filter(
            col("current_profile_hash").isNull()
            | (
                col("profile_hash")
                != col("current_profile_hash")
            )
        )
        .drop(
            "profile_hash",
            "current_profile_hash",
        )
    )

    changed_customer_ids = (
        changed_df
        .select(
            "customer_unique_id"
        )
        .dropDuplicates()
    )

    closed_existing_df = (
        existing_dim_df
        .join(
            changed_customer_ids
            .withColumn(
                "_changed",
                lit(True),
            ),
            on="customer_unique_id",
            how="left",
        )
        .withColumn(
            "valid_to",
            when(
                (col("_changed") == True)
                & (
                    col("is_current")
                    == True
                ),
                date_sub(
                    lit(PROCESS_DATE)
                    .cast("date"),
                    1,
                ),
            )
            .otherwise(
                col("valid_to")
            ),
        )
        .withColumn(
            "is_current",
            when(
                (col("_changed") == True)
                & (
                    col("is_current")
                    == True
                ),
                lit(False),
            )
            .otherwise(
                col("is_current")
            ),
        )
        .drop("_changed")
    )

    max_sk = (
        existing_dim_df
        .agg(
            spark_max(
                "customer_sk"
            ).alias("max_sk")
        )
        .first()["max_sk"]
    )

    max_sk = max_sk or 0

    window = Window.orderBy(
        "customer_unique_id"
    )

    new_versions_df = (
        changed_df
        .withColumn(
            "customer_sk",
            (
                row_number()
                .over(window)
                + lit(max_sk)
            )
            .cast("long"),
        )
        .withColumn(
            "valid_from",
            lit(PROCESS_DATE)
            .cast("date"),
        )
        .withColumn(
            "valid_to",
            lit(None)
            .cast("date"),
        )
        .withColumn(
            "is_current",
            lit(True),
        )
        .select(
            "customer_sk",
            "customer_unique_id",
            "persona_uuid",
            *PROFILE_COLUMNS,
            "valid_from",
            "valid_to",
            "is_current",
        )
    )

    return (
        closed_existing_df
        .unionByName(
            new_versions_df
        )
    )


def main():
    spark = create_spark_session(
        "Build Dim Customer"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    persona_df = (
        spark.read
        .parquet(PERSONA_PATH)
    )

    mapping_df = (
        spark.read
        .parquet(MAPPING_PATH)
    )

    customer_profile_df = (
        build_customer_profile(
            mapping_df,
            persona_df,
        )
        .cache()
    )

    profile_count = (
        customer_profile_df.count()
    )

    print(
        f"[INFO] customer_profile_count="
        f"{profile_count}"
    )

    if path_exists(
        spark,
        OUTPUT_PATH,
    ):
        existing_dim_df = (
            spark.read
            .parquet(OUTPUT_PATH)
            .cache()
        )

        existing_dim_count = (
            existing_dim_df.count()
        )

        print(
            f"[INFO] existing_dimension_count="
            f"{existing_dim_count}"
        )

        dim_customer_df = (
            build_scd2_dimension(
                spark,
                customer_profile_df,
                existing_dim_df,
            )
        )

    else:
        print(
            "[INFO] initial dimension build"
        )

        dim_customer_df = (
            build_initial_dimension(
                customer_profile_df
            )
        )

    dim_customer_df = (
        dim_customer_df.cache()
    )

    row_count = (
        dim_customer_df.count()
    )

    (
        dim_customer_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(
        "[SUCCESS] dim_customer build completed"
    )
    print(
        f"[INFO] row_count="
        f"{row_count}"
    )
    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()