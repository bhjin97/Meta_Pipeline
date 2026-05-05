from pyspark.sql import SparkSession

RAW_PATH = "/app/data/raw"
OUTPUT_PATH = "/app/data/parquet"

DATASETS = {
    "olist/orders": "olist_orders_dataset.csv",
    "olist/order_items": "olist_order_items_dataset.csv",
    "olist/customers": "olist_customers_dataset.csv",
    "olist/products": "olist_products_dataset.csv",
    "olist/sellers": "olist_sellers_dataset.csv",
    "olist/payments": "olist_order_payments_dataset.csv",
    "olist/reviews": "olist_order_reviews_dataset.csv",

    "reference/category_translation": "product_category_name_translation.csv",

    "persona/nemotron_korea": "nemotron_persona_korea_sample.csv",
}


def main():
    spark = (
        SparkSession.builder
        .appName("Convert Raw CSV to Parquet")
        .getOrCreate()
    )

    for target_path, csv_file in DATASETS.items():
        input_path = f"{RAW_PATH}/{csv_file}"
        output_path = f"{OUTPUT_PATH}/{target_path}"

        print(f"Converting {input_path} -> {output_path}")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(input_path)
        )

        df.write.mode("overwrite").parquet(output_path)

    spark.stop()


if __name__ == "__main__":
    main()