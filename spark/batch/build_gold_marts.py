import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as _sum,
    count,
    countDistinct,
    avg,
    round,
    when,
    date_format,
    DataFrame,
)


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Gold Marts")
        .getOrCreate()
    )


POSTGRES_HOST = "postgres"
POSTGRES_PORT = 5432
POSTGRES_DB = "ecommerce"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"

POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def write_to_postgres(df: DataFrame, table_name: str):
    (
        df.write
        .mode("overwrite")
        .jdbc(
            url=POSTGRES_URL,
            table=table_name,
            properties=POSTGRES_PROPERTIES,
        )
    )


def create_postgres_indexes():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )

    cur = conn.cursor()

    index_sqls = [
        """
        CREATE INDEX IF NOT EXISTS idx_mart_daily_sales_order_event_date
        ON mart_daily_sales (order_event_date);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_mart_category_sales_category
        ON mart_category_sales (product_category_name_english);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_mart_customer_segment_age_sex_occupation
        ON mart_customer_segment_sales (age_group, sex, occupation);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_mart_delivery_kpi_delivery_event_date
        ON mart_delivery_kpi (delivery_event_date);
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_mart_review_kpi_review_event_date
        ON mart_review_kpi (review_event_date);
        """,
    ]

    for sql in index_sqls:
        cur.execute(sql)

    conn.commit()
    cur.close()
    conn.close()

    print("postgres indexes created")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    fact_order_item_path = "s3a://ecommerce/silver/fact_order_item/"
    fact_delivery_path = "s3a://ecommerce/silver/fact_delivery/"
    fact_review_path = "s3a://ecommerce/silver/fact_review/"
    dim_product_path = "s3a://ecommerce/silver/dim_product/"
    dim_customer_path = "s3a://ecommerce/silver/dim_customer/"

    daily_sales_output = "s3a://ecommerce/gold/mart_daily_sales/"
    category_sales_output = "s3a://ecommerce/gold/mart_category_sales/"
    customer_segment_output = "s3a://ecommerce/gold/mart_customer_segment_sales/"
    delivery_kpi_output = "s3a://ecommerce/gold/mart_delivery_kpi/"
    review_kpi_output = "s3a://ecommerce/gold/mart_review_kpi/"

    fact_order_item = spark.read.parquet(fact_order_item_path)
    fact_delivery = spark.read.parquet(fact_delivery_path)
    fact_review = spark.read.parquet(fact_review_path)
    dim_product = spark.read.parquet(dim_product_path)
    dim_customer = spark.read.parquet(dim_customer_path)

    # 1. Daily Sales Mart
    mart_daily_sales = (
        fact_order_item
        .groupBy("order_event_date")
        .agg(
            round(_sum("item_total_amount"), 2).alias("total_sales_amount"),
            round(_sum("item_price"), 2).alias("total_product_sales_amount"),
            round(_sum("item_freight_value"), 2).alias("total_freight_amount"),
            countDistinct("order_id").alias("order_count"),
            count("*").alias("item_count"),
            countDistinct("customer_id").alias("customer_count"),
        )
        .withColumn(
            "avg_order_amount",
            round(col("total_sales_amount") / col("order_count"), 2)
        )
        .withColumn(
            "order_month",
            date_format(col("order_event_date"), "yyyy-MM")
        )
        .orderBy("order_event_date")
    )

    mart_daily_sales.write.mode("overwrite") \
        .partitionBy("order_month") \
        .parquet(daily_sales_output)

    write_to_postgres(mart_daily_sales, "mart_daily_sales")

    # 2. Category Sales Mart
    mart_category_sales = (
        fact_order_item
        .join(dim_product, on="product_id", how="left")
        .groupBy("product_category_name_english")
        .agg(
            round(_sum("item_total_amount"), 2).alias("total_sales_amount"),
            round(_sum("item_price"), 2).alias("total_product_sales_amount"),
            round(_sum("item_freight_value"), 2).alias("total_freight_amount"),
            countDistinct("order_id").alias("order_count"),
            count("*").alias("item_count"),
            round(avg("item_price"), 2).alias("avg_item_price"),
        )
        .orderBy(col("total_sales_amount").desc())
    )

    mart_category_sales.write.mode("overwrite").parquet(category_sales_output)

    write_to_postgres(mart_category_sales, "mart_category_sales")

    # 3. Customer Segment Sales Mart
    mart_customer_segment_sales = (
        fact_order_item
        .join(dim_customer, on="customer_id", how="left")
        .groupBy("age_group", "sex", "occupation")
        .agg(
            round(_sum("item_total_amount"), 2).alias("total_sales_amount"),
            round(_sum("item_price"), 2).alias("total_product_sales_amount"),
            round(_sum("item_freight_value"), 2).alias("total_freight_amount"),
            countDistinct("order_id").alias("order_count"),
            countDistinct("customer_id").alias("customer_count"),
            count("*").alias("item_count"),
        )
        .withColumn(
            "avg_customer_amount",
            round(col("total_sales_amount") / col("customer_count"), 2)
        )
        .orderBy(col("total_sales_amount").desc())
    )

    mart_customer_segment_sales.write.mode("overwrite").parquet(
        customer_segment_output
    )

    write_to_postgres(
        mart_customer_segment_sales,
        "mart_customer_segment_sales"
    )

    # 4. Delivery KPI Mart
    mart_delivery_kpi = (
        fact_delivery
        .groupBy("delivery_event_date")
        .agg(
            count("*").alias("delivery_event_count"),
            _sum(
                when(col("event_type") == "DELIVERY_COMPLETED", 1).otherwise(0)
            ).alias("completed_delivery_count"),
            _sum(
                when(col("is_delayed") == True, 1).otherwise(0)
            ).alias("delayed_delivery_count"),
            round(avg("shipping_days"), 2).alias("avg_days_to_carrier"),
            round(avg("delivery_days"), 2).alias("avg_total_delivery_days"),
        )
        .withColumn(
            "delay_rate",
            round(col("delayed_delivery_count") / col("delivery_event_count"), 4)
        )
        .withColumn(
            "delivery_month",
            date_format(col("delivery_event_date"), "yyyy-MM")
        )
        .orderBy("delivery_event_date")
    )

    mart_delivery_kpi.write.mode("overwrite") \
        .partitionBy("delivery_month") \
        .parquet(delivery_kpi_output)

    write_to_postgres(mart_delivery_kpi, "mart_delivery_kpi")

    # 5. Review KPI Mart
    mart_review_kpi = (
        fact_review
        .groupBy("review_event_date")
        .agg(
            count("*").alias("review_count"),
            round(avg("review_score"), 2).alias("avg_review_score"),
            _sum(when(col("review_score") == 1, 1).otherwise(0)).alias("score_1_count"),
            _sum(when(col("review_score") == 2, 1).otherwise(0)).alias("score_2_count"),
            _sum(when(col("review_score") == 3, 1).otherwise(0)).alias("score_3_count"),
            _sum(when(col("review_score") == 4, 1).otherwise(0)).alias("score_4_count"),
            _sum(when(col("review_score") == 5, 1).otherwise(0)).alias("score_5_count"),
            round(avg("review_answer_days"), 2).alias("avg_review_answer_days"),
        )
        .withColumn(
            "review_month",
            date_format(col("review_event_date"), "yyyy-MM")
        )
        .orderBy("review_event_date")
    )

    mart_review_kpi.write.mode("overwrite") \
        .partitionBy("review_month") \
        .parquet(review_kpi_output)

    write_to_postgres(mart_review_kpi, "mart_review_kpi")

    create_postgres_indexes()

    print("gold marts build completed")
    print(f"mart_daily_sales rows: {mart_daily_sales.count()}")
    print(f"mart_category_sales rows: {mart_category_sales.count()}")
    print(f"mart_customer_segment_sales rows: {mart_customer_segment_sales.count()}")
    print(f"mart_delivery_kpi rows: {mart_delivery_kpi.count()}")
    print(f"mart_review_kpi rows: {mart_review_kpi.count()}")

    spark.stop()


if __name__ == "__main__":
    main()