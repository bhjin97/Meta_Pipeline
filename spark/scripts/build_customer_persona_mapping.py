from pyspark.sql import Window
from pyspark.sql.functions import col, rand, row_number

from common.spark_session import create_spark_session


CUSTOMERS_PATH = "s3a://ecommerce/bronze/olist/customers/"
PERSONA_PATH = "s3a://ecommerce/bronze/persona/nemotron_korea/"
OUTPUT_PATH = "s3a://ecommerce/silver/customer_persona_mapping/"

RANDOM_SEED = 42


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


def main():
    spark = create_spark_session(
        "Build Customer Persona Mapping"
    )
    spark.sparkContext.setLogLevel("WARN")

    customers_df = (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select("customer_unique_id")
        .where(col("customer_unique_id").isNotNull())
        .dropDuplicates(["customer_unique_id"])
    )

    persona_df = (
        spark.read
        .parquet(PERSONA_PATH)
        .select(
            col("uuid").alias("persona_uuid")
        )
        .where(col("uuid").isNotNull())
        .dropDuplicates(["persona_uuid"])
    )

    if path_exists(spark, OUTPUT_PATH):
        existing_mapping_df = (
            spark.read
            .parquet(OUTPUT_PATH)
            .cache()
        )

        existing_mapping_count = (
            existing_mapping_df.count()
        )

        print(
            f"[INFO] existing_mapping_count="
            f"{existing_mapping_count}"
        )

        new_customers_df = (
            customers_df
            .join(
                existing_mapping_df.select(
                    "customer_unique_id"
                ),
                on="customer_unique_id",
                how="left_anti",
            )
        )

        used_persona_df = (
            existing_mapping_df
            .select("persona_uuid")
            .dropDuplicates()
        )

        available_persona_df = (
            persona_df
            .join(
                used_persona_df,
                on="persona_uuid",
                how="left_anti",
            )
        )

    else:
        existing_mapping_df = None
        new_customers_df = customers_df
        available_persona_df = persona_df

    new_customer_count = new_customers_df.count()
    available_persona_count = available_persona_df.count()

    print(
        f"[INFO] new_customers={new_customer_count}"
    )
    print(
        f"[INFO] available_personas="
        f"{available_persona_count}"
    )

    if new_customer_count == 0:
        print("[INFO] no new customers")
        spark.stop()
        return

    if new_customer_count > available_persona_count:
        raise RuntimeError(
            "Not enough persona records. "
            f"new_customers={new_customer_count}, "
            f"available_personas={available_persona_count}"
        )

    customer_window = Window.orderBy(
        col("customer_unique_id")
    )

    new_customers_ranked_df = (
        new_customers_df
        .withColumn(
            "mapping_idx",
            row_number().over(customer_window),
        )
    )

    persona_window = Window.orderBy(
        rand(RANDOM_SEED)
    )

    available_persona_ranked_df = (
        available_persona_df
        .withColumn(
            "mapping_idx",
            row_number().over(persona_window),
        )
        .where(
            col("mapping_idx") <= new_customer_count
        )
    )

    new_mapping_df = (
        new_customers_ranked_df
        .join(
            available_persona_ranked_df,
            on="mapping_idx",
            how="inner",
        )
        .select(
            "customer_unique_id",
            "persona_uuid",
        )
        .cache()
    )

    new_mapping_count = new_mapping_df.count()

    if existing_mapping_df is not None:
        final_mapping_df = (
            existing_mapping_df
            .unionByName(new_mapping_df)
            .cache()
        )
    else:
        final_mapping_df = new_mapping_df

    total_mapping_count = final_mapping_df.count()

    (
        final_mapping_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(
        "[SUCCESS] customer persona mapping completed"
    )
    print(
        f"[INFO] new_mapping_count="
        f"{new_mapping_count}"
    )
    print(
        f"[INFO] total_mapping_count="
        f"{total_mapping_count}"
    )

    spark.stop()


if __name__ == "__main__":
    main()