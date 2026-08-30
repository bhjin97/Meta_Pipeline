import argparse

from pyspark.sql import SparkSession

from common.spark_session import create_spark_session


DATASET_CONFIG = {
    "orders": {
        "file_name": "olist_orders_dataset.csv",
    },
    "order_items": {
        "file_name": "olist_order_items_dataset.csv",
    },
    "payments": {
        "file_name": "olist_order_payments_dataset.csv",
    },
    "reviews": {
        "file_name": "olist_order_reviews_dataset.csv",
        "multiline": True,
    },
    "customers": {
        "file_name": "olist_customers_dataset.csv",
    },
    "products": {
        "file_name": "olist_products_dataset.csv",
    },
    "sellers": {
        "file_name": "olist_sellers_dataset.csv",
    },
    "category_translation": {
        "file_name": (
            "product_category_name_translation.csv"
        ),
    },
}


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--dataset",
        required=True,
        choices=DATASET_CONFIG.keys(),
        help="Dataset to convert",
    )

    parser.add_argument(
        "--input-root",
        default="/app/origin_data_processing/data/raw",
        help="Directory containing raw Olist CSV files",
    )

    parser.add_argument(
        "--output-root",
        default="s3a://ecommerce/bronze/olist",
        help="Root directory for output Parquet",
    )

    return parser.parse_args()


def read_csv(
    spark: SparkSession,
    input_path: str,
    multiline: bool = False,
):
    reader = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("encoding", "UTF-8")
        .option("mode", "FAILFAST")
    )

    # reviews에는 comment 내부의 줄바꿈/쉼표/따옴표가 존재할 수 있음
    if multiline:
        reader = (
            reader
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
        )

    return reader.csv(input_path)


def main():
    args = parse_args()

    spark = create_spark_session(
        "Convert Raw Olist To Parquet"
    )

    spark.sparkContext.setLogLevel("WARN")

    config = DATASET_CONFIG[args.dataset]

    input_path = (
        f"{args.input_root.rstrip('/')}/"
        f"{config['file_name']}"
    )

    output_path = (
        f"{args.output_root.rstrip('/')}/"
        f"{args.dataset}/"
    )

    print(
        f"[INFO] dataset={args.dataset}"
    )
    print(
        f"[INFO] input_path={input_path}"
    )
    print(
        f"[INFO] output_path={output_path}"
    )

    df = read_csv(
        spark=spark,
        input_path=input_path,
        multiline=config.get(
            "multiline",
            False,
        ),
    )

    row_count = df.count()

    print(
        f"[INFO] source_row_count={row_count}"
    )

    print(
        "[INFO] schema:"
    )
    df.printSchema()

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(
        f"[SUCCESS] dataset={args.dataset}"
    )
    print(
        f"[SUCCESS] written_rows={row_count}"
    )
    print(
        f"[SUCCESS] output_path={output_path}"
    )

    spark.stop()


if __name__ == "__main__":
    main()