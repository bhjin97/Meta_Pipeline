from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from faker import Faker
import random


RAW_PATH = "data/raw"
OUTPUT_PATH = "data/event_source/olist_item_events"


def create_fake_customer_profiles(spark, customers_df):
    fake = Faker("pt_BR")
    random.seed(42)
    Faker.seed(42)

    unique_customers = (
        customers_df
        .select("customer_unique_id")
        .distinct()
        .collect()
    )

    rows = []

    for row in unique_customers:
        customer_unique_id = row["customer_unique_id"]

        gender = random.choice(["male", "female"])
        age = random.randint(18, 70)

        rows.append((
            customer_unique_id,
            fake.name(),
            fake.email(),
            fake.phone_number(),
            age,
            gender
        ))

    return spark.createDataFrame(
        rows,
        [
            "customer_unique_id",
            "customer_name",
            "customer_email",
            "customer_phone",
            "customer_age",
            "customer_gender"
        ]
    )


def main():
    spark = (
        SparkSession.builder
        .appName("Make Olist Item Events")
        .getOrCreate()
    )

    # 1. Customers
    customers_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_customers_dataset.csv")
        .select(
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        )
    )

    # Faker 고객 프로필 생성 후 customer_unique_id 기준으로 붙이기
    fake_customer_profiles_df = create_fake_customer_profiles(
        spark,
        customers_df
    )

    customers_df = (
        customers_df
        .join(fake_customer_profiles_df, "customer_unique_id", "left")
    )

    # 2. Orders
    orders_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_orders_dataset.csv")
        .select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_carrier_date",
            "order_delivered_customer_date"
        )
    )

    # 3. Order Items
    order_items_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_order_items_dataset.csv")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value"
        )
    )

    # 4. Products
    products_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_products_dataset.csv")
        .select(
            "product_id",
            "product_category_name"
        )
    )

    # 5. Category Translation
    category_translation_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/product_category_name_translation.csv")
    )

    products_df = (
        products_df
        .join(category_translation_df, "product_category_name", "left")
        .select(
            "product_id",
            col("product_category_name_english").alias("product_category")
        )
    )

    #  Sellers
    sellers_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_PATH}/olist_sellers_dataset.csv")
        .select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
    )

    # 6. Join
    # 상품 단위 이벤트이므로 payments 테이블은 조인하지 않음
    event_df = (
        orders_df
        .join(customers_df, "customer_id", "left")
        .join(order_items_df, "order_id", "inner")
        .join(products_df, "product_id", "left")
        .join(sellers_df, "seller_id", "left")
    )

    # 7. 이벤트 메타데이터 생성
    event_df = (
        event_df
        .withColumn(
            "event_type",
            lit("ITEM_PURCHASED")
        )
        .withColumn(
            "event_time",
            col("order_purchase_timestamp")
        )
        .withColumn(
            "event_id",
            concat_ws(
                "_",
                lit("ITEM_PURCHASED"),
                col("order_id"),
                col("order_item_id")
            )
        )
    )

    # 8. Kafka 이벤트 시간 기준 정렬
    event_df = event_df.orderBy("order_purchase_timestamp")

    # 9. Kafka Producer가 읽기 좋은 JSON Lines 형태로 저장
    (
        event_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .json(OUTPUT_PATH)
    )

    print("Olist item event source data created successfully.")
    print(f"Output path: {OUTPUT_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()