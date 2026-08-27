import argparse

from pyspark.sql.functions import (
    col,
    date_format,
    lit,
    to_date,
    to_timestamp,
)
from pyspark.sql.utils import AnalysisException

from common.spark_session import create_spark_session


ORDER_EVENTS_PATH = "s3a://ecommerce/bronze/events/order_events/"
ORDER_ITEMS_PATH = "s3a://ecommerce/bronze/olist/order_items/"
CUSTOMERS_PATH = "s3a://ecommerce/bronze/olist/customers/"
DIM_CUSTOMER_PATH = "s3a://ecommerce/silver/dim_customer/"
OUTPUT_PATH = "s3a://ecommerce/silver/fact_order_item/"


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        default="prod",
        choices=["prod", "load_test"],
        help="Execution mode",
    )

    parser.add_argument(
        "--test-run-id",
        default=None,
        help="Load test run id",
    )

    return parser.parse_args()


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


def get_paths(args):
    if args.mode == "prod":
        return (
            ORDER_EVENTS_PATH,
            ORDER_ITEMS_PATH,
            OUTPUT_PATH,
        )

    if not args.test_run_id:
        raise ValueError(
            "--test-run-id is required "
            "when mode=load_test"
        )

    base_path = (
        "s3a://ecommerce/bronze/load_test/"
        f"{args.test_run_id}"
    )

    output_path = (
        "s3a://ecommerce/silver/load_test/"
        f"{args.test_run_id}/fact_order_item/"
    )

    return (
        f"{base_path}/events/order_events/",
        f"{base_path}/order_items/",
        output_path,
    )


def build_order_created_df(
    spark,
    order_events_path,
):
    order_events_df = spark.read.parquet(
        order_events_path
    )

    return (
        order_events_df
        .filter(
            col("event_type") == "ORDER_CREATED"
        )
        .select(
            "order_id",
            "customer_id",
            to_timestamp(
                col("event_time")
            ).alias("order_timestamp"),
        )
        .dropDuplicates(["order_id"])
    )


def build_customer_lookup_df(spark):
    customers_df = (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select(
            "customer_id",
            "customer_unique_id",
        )
        .dropDuplicates(["customer_id"])
    )

    dim_customer_df = (
        spark.read
        .parquet(DIM_CUSTOMER_PATH)
        .select(
            "customer_sk",
            "customer_unique_id",
            "valid_from",
            "valid_to",
        )
    )

    return customers_df, dim_customer_df


def build_source_fact_df(
    spark,
    order_created_df,
    order_items_path,
):
    order_items_df = (
        spark.read
        .parquet(order_items_path)
    )

    return (
        order_created_df.alias("orders")
        .join(
            order_items_df.alias("items"),
            on="order_id",
            how="inner",
        )
        .select(
            col("order_id"),
            col("items.order_item_id")
            .alias("order_item_id"),

            col("orders.customer_id")
            .alias("customer_id"),

            col("items.product_id")
            .alias("product_id"),

            col("items.seller_id")
            .alias("seller_id"),

            to_timestamp(
                col("items.shipping_limit_date")
            ).alias("shipping_limit_date"),

            col("items.price")
            .cast("double")
            .alias("item_price"),

            col("items.freight_value")
            .cast("double")
            .alias("item_freight_value"),

            (
                col("items.price").cast("double")
                + col("items.freight_value")
                .cast("double")
            ).alias("item_total_amount"),

            col("orders.order_timestamp")
            .alias("order_timestamp"),

            to_date(
                col("orders.order_timestamp")
            ).alias("order_date"),
        )
        .dropDuplicates(
            [
                "order_id",
                "order_item_id",
            ]
        )
    )


def attach_customer_sk(
    source_fact_df,
    customers_df,
    dim_customer_df,
):
    fact_with_customer_df = (
        source_fact_df.alias("fact")
        .join(
            customers_df.alias("customers"),
            col("fact.customer_id")
            == col("customers.customer_id"),
            how="left",
        )
        .join(
            dim_customer_df.alias("dim"),
            (
                col("customers.customer_unique_id")
                == col("dim.customer_unique_id")
            )
            & (
                col("fact.order_date")
                >= col("dim.valid_from")
            )
            & (
                col("dim.valid_to").isNull()
                | (
                    col("fact.order_date")
                    <= col("dim.valid_to")
                )
            ),
            how="left",
        )
    )

    return (
        fact_with_customer_df
        .select(
            col("fact.order_id")
            .alias("order_id"),

            col("fact.order_item_id")
            .alias("order_item_id"),

            col("dim.customer_sk")
            .alias("customer_sk"),

            col("fact.product_id")
            .alias("product_id"),

            col("fact.seller_id")
            .alias("seller_id"),

            date_format(
                col("fact.order_date"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("date_key"),

            col("fact.shipping_limit_date")
            .alias("shipping_limit_date"),

            col("fact.item_price")
            .alias("item_price"),

            col("fact.item_freight_value")
            .alias("item_freight_value"),

            col("fact.item_total_amount")
            .alias("item_total_amount"),

            col("fact.order_timestamp")
            .alias("order_timestamp"),

            col("fact.order_date")
            .alias("order_date"),

            date_format(
                col("fact.order_date"),
                "yyyy-MM",
            ).alias("order_month"),
        )
    )


def validate_before_write(df):
    duplicate_count = (
        df.groupBy(
            "order_id",
            "order_item_id",
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_count > 0:
        raise RuntimeError(
            "Duplicate fact grain detected. "
            f"duplicate_key_count={duplicate_count}"
        )

    null_customer_sk_count = (
        df.filter(
            col("customer_sk").isNull()
        )
        .count()
    )

    if null_customer_sk_count > 0:
        raise RuntimeError(
            "customer_sk mapping failed. "
            f"null_customer_sk_count="
            f"{null_customer_sk_count}"
        )


def remove_existing_rows(
    source_df,
    existing_df,
):
    processed_keys_df = (
        existing_df
        .select(
            "order_id",
            "order_item_id",
        )
        .dropDuplicates()
    )

    return (
        source_df
        .join(
            processed_keys_df,
            on=[
                "order_id",
                "order_item_id",
            ],
            how="left_anti",
        )
    )


def main():
    args = parse_args()

    spark = create_spark_session(
        "Build Fact Order Item"
    )
    spark.sparkContext.setLogLevel("WARN")

    (
        order_events_path,
        order_items_path,
        output_path,
    ) = get_paths(args)

    print(f"[INFO] mode={args.mode}")
    print(
        f"[INFO] order_events_path="
        f"{order_events_path}"
    )
    print(
        f"[INFO] order_items_path="
        f"{order_items_path}"
    )
    print(
        f"[INFO] output_path={output_path}"
    )

    order_created_df = (
        build_order_created_df(
            spark,
            order_events_path,
        )
    )

    source_fact_df = (
        build_source_fact_df(
            spark,
            order_created_df,
            order_items_path,
        )
    )

    (
        customers_df,
        dim_customer_df,
    ) = build_customer_lookup_df(spark)

    fact_order_item_df = (
        attach_customer_sk(
            source_fact_df,
            customers_df,
            dim_customer_df,
        )
    )

    validate_before_write(
        fact_order_item_df
    )

    source_count = (
        fact_order_item_df.count()
    )

    print(
        f"[INFO] source_row_count="
        f"{source_count}"
    )

    if path_exists(spark, output_path):
        existing_df = spark.read.parquet(
            output_path
        )

        required_columns = {
            "order_id",
            "order_item_id",
            "customer_sk",
        }

        if not required_columns.issubset(
            set(existing_df.columns)
        ):
            raise RuntimeError(
                "Legacy fact_order_item schema "
                "detected. "
                "Remove the existing output "
                "before the first rebuild."
            )

        new_fact_df = remove_existing_rows(
            fact_order_item_df,
            existing_df,
        )

    else:
        print(
            "[INFO] Initial fact_order_item build"
        )
        new_fact_df = fact_order_item_df

    new_row_count = new_fact_df.count()

    print(
        f"[INFO] new_row_count="
        f"{new_row_count}"
    )

    if new_row_count == 0:
        print(
            "[INFO] No new order items "
            "to process"
        )
        spark.stop()
        return

    (
        new_fact_df.write
        .mode("append")
        .partitionBy("order_month")
        .parquet(output_path)
    )

    print(
        "[SUCCESS] fact_order_item "
        "build completed"
    )
    print(
        f"[INFO] written_rows="
        f"{new_row_count}"
    )
    print(
        f"[INFO] output_path="
        f"{output_path}"
    )

    spark.stop()


if __name__ == "__main__":
    main()