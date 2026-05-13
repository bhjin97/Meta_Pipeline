from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, to_timestamp, datediff, date_format


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Build Fact Review")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    review_events_path = "s3a://ecommerce/bronze/events/review_events/"
    reviews_path = "s3a://ecommerce/bronze/olist/reviews/"
    output_path = "s3a://ecommerce/silver/fact_review/"

    review_events_df = spark.read.parquet(review_events_path)
    reviews_df = spark.read.parquet(reviews_path)

    processed_review_df = (
        spark.read.parquet(output_path)
        .select("review_id", "event_type")
        .dropDuplicates(["review_id", "event_type"])
    )

    review_events_df = (
        review_events_df
        .select(
            col("review_id"),
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            to_timestamp(col("event_time")).alias("event_time"),
            col("review_score").cast("int").alias("review_score"),
        )
        .dropDuplicates(["review_id", "event_type"])
        .join(
            processed_review_df,
            on=["review_id", "event_type"],
            how="left_anti"
        )
    )

    if review_events_df.rdd.isEmpty():
        print("No new review events to process")
        spark.stop()
        return

    reviews_df = (
        reviews_df
        .select(
            col("review_id"),
            to_timestamp(col("review_creation_date")).alias("review_creation_date"),
            to_timestamp(col("review_answer_timestamp")).alias("review_answer_timestamp"),
        )
    )

    fact_review = (
        review_events_df
        .join(reviews_df, on="review_id", how="left")
        .select(
            col("review_id"),
            col("order_id"),
            col("customer_id"),
            col("event_type"),
            col("event_time"),
            col("review_score"),
            col("review_answer_timestamp"),
            datediff(
                col("review_answer_timestamp"),
                col("review_creation_date")
            ).alias("review_answer_days"),
            to_date(col("event_time")).alias("review_event_date"),
        )
    )

    fact_review = fact_review.withColumn(
        "review_month",
        date_format(col("review_event_date"), "yyyy-MM")
    )

    new_count = fact_review.count()

    fact_review.write.mode("append") \
        .partitionBy("review_month") \
        .parquet(output_path)

    print("fact_review incremental build completed")
    print(f"new row count: {new_count}")

    spark.stop()


if __name__ == "__main__":
    main()