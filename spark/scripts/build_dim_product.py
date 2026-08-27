from pyspark.sql.functions import col, coalesce, lit

from common.spark_session import create_spark_session


PRODUCTS_PATH = "s3a://ecommerce/bronze/olist/products/"
CATEGORY_TRANSLATION_PATH = (
    "s3a://ecommerce/bronze/reference/category_translation/"
)
OUTPUT_PATH = "s3a://ecommerce/silver/dim_product/"


def main():
    spark = create_spark_session(
        "Build Dim Product"
    )
    spark.sparkContext.setLogLevel("WARN")

    products_df = (
        spark.read
        .parquet(PRODUCTS_PATH)
    )

    category_translation_df = (
        spark.read
        .parquet(CATEGORY_TRANSLATION_PATH)
    )

    dim_product_df = (
        products_df.alias("p")
        .join(
            category_translation_df.alias("t"),
            on="product_category_name",
            how="left",
        )
        .select(
            col("p.product_id"),

            coalesce(
                col("p.product_category_name"),
                lit("unknown"),
            ).alias("product_category_name"),

            coalesce(
                col("t.product_category_name_english"),
                lit("unknown"),
            ).alias(
                "product_category_name_english"
            ),

            # NULL은 "0개"가 아니라
            # "정보 없음"일 수 있으므로 그대로 유지
            col("p.product_photos_qty"),

            col("p.product_weight_g"),
            col("p.product_length_cm"),
            col("p.product_height_cm"),
            col("p.product_width_cm"),
        )
    )

    row_count = dim_product_df.count()

    (
        dim_product_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print(
        "[SUCCESS] dim_product build completed"
    )
    print(
        f"[INFO] row_count={row_count}"
    )
    print(
        f"[INFO] output_path={OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()