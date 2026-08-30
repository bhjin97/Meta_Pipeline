from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

from common.spark_session import create_spark_session


PAYMENTS_PATH = (
    "s3a://ecommerce/bronze/olist/payments/"
)

OUTPUT_PATH = (
    "s3a://ecommerce/silver/fact_payment/"
)


def read_existing_keys(spark):
    try:
        existing_df = (
            spark.read
            .parquet(OUTPUT_PATH)
        )

        print(
            "[INFO] Existing fact_payment found. "
            "Running incremental load."
        )

        return (
            existing_df
            .select(
                "order_id",
                "payment_sequential",
            )
            .dropDuplicates(
                [
                    "order_id",
                    "payment_sequential",
                ]
            )
        )

    except AnalysisException as e:
        if "PATH_NOT_FOUND" not in str(e):
            raise

        print(
            "[INFO] No existing fact_payment found. "
            "Running initial load."
        )

        return spark.createDataFrame(
            [],
            """
            order_id string,
            payment_sequential long
            """,
        )


def build_source_df(spark):
    payments_df = (
        spark.read
        .parquet(PAYMENTS_PATH)
    )

    return (
        payments_df
        .select(
            col("order_id"),

            col("payment_sequential")
            .cast("long")
            .alias("payment_sequential"),

            col("payment_type"),

            col("payment_installments")
            .cast("long")
            .alias("payment_installments"),

            col("payment_value")
            .cast("double")
            .alias("payment_value"),
        )
        .dropDuplicates(
            [
                "order_id",
                "payment_sequential",
            ]
        )
    )


def validate_before_write(df):
    duplicate_count = (
        df.groupBy(
            "order_id",
            "payment_sequential",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Duplicate payment grain detected. "
            f"duplicate_count={duplicate_count}"
        )

    null_key_count = (
        df.filter(
            col("order_id").isNull()
            | col(
                "payment_sequential"
            ).isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise RuntimeError(
            "Payment key contains NULL. "
            f"null_key_count={null_key_count}"
        )


def main():
    spark = create_spark_session(
        "Build Fact Payment"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    source_df = (
        build_source_df(spark)
    )

    validate_before_write(
        source_df
    )

    source_count = (
        source_df.count()
    )

    print(
        f"[INFO] source_row_count="
        f"{source_count}"
    )

    processed_keys_df = (
        read_existing_keys(spark)
    )

    new_payment_df = (
        source_df
        .join(
            processed_keys_df,
            on=[
                "order_id",
                "payment_sequential",
            ],
            how="left_anti",
        )
    )

    new_row_count = (
        new_payment_df.count()
    )

    print(
        f"[INFO] new_row_count="
        f"{new_row_count}"
    )

    if new_row_count == 0:
        print(
            "[INFO] No new payments "
            "to process"
        )

        spark.stop()
        return

    (
        new_payment_df.write
        .mode("append")
        .parquet(OUTPUT_PATH)
    )

    print(
        "[SUCCESS] fact_payment "
        "build completed"
    )

    print(
        f"[INFO] written_rows="
        f"{new_row_count}"
    )

    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()