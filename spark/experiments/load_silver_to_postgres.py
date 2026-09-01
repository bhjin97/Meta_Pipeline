from common.spark_session import create_spark_session
from common.postgres import write_to_postgres


TABLES = {
    "fact_order_item": "s3a://ecommerce/silver/fact_order_item/",
    "fact_order_event": "s3a://ecommerce/silver/fact_order_event/",
    "dim_product": "s3a://ecommerce/silver/dim_product/",
    "dim_customer": "s3a://ecommerce/silver/dim_customer/",
}


def main():
    spark = create_spark_session("Load Silver To PostgreSQL")

    for table_name, path in TABLES.items():
        print(f"[INFO] loading table={table_name}")
        print(f"[INFO] source_path={path}")

        df = spark.read.parquet(path)

        row_count = df.count()
        print(f"[INFO] source_row_count={row_count}")

        write_to_postgres(
            df=df,
            table_name=table_name,
            schema="silver",
            mode="overwrite",
        )

        print(f"[SUCCESS] loaded table={table_name}")

    spark.stop()


if __name__ == "__main__":
    main()