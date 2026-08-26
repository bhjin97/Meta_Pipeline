from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    dayofmonth,
    dayofweek,
    dayofyear,
    explode,
    expr,
    lit,
    month,
    quarter,
    sequence,
    weekofyear,
    when,
    year,
)
from common.spark_session import create_spark_session

START_DATE = "2016-01-01"
END_DATE = "2030-12-31"

OUTPUT_PATH = "s3a://ecommerce/silver/dim_date/"


def main():
    spark = create_spark_session("Build Dim Date")
    spark.sparkContext.setLogLevel("WARN")

    date_df = (
        spark.range(1)
        .select(
            explode(
                sequence(
                    lit(START_DATE).cast("date"),
                    lit(END_DATE).cast("date"),
                    expr("interval 1 day"),
                )
            ).alias("full_date")
        )
    )

    dim_date_df = (
        date_df
        .select(
            date_format(col("full_date"), "yyyyMMdd")
            .cast("int")
            .alias("date_key"),

            col("full_date"),

            year(col("full_date")).alias("year"),
            quarter(col("full_date")).alias("quarter"),
            month(col("full_date")).alias("month"),

            date_format(
                col("full_date"),
                "MMMM",
            ).alias("month_name"),

            date_format(
                col("full_date"),
                "yyyy-MM",
            ).alias("year_month"),

            dayofmonth(
                col("full_date")
            ).alias("day"),

            dayofweek(
                col("full_date")
            ).alias("day_of_week"),

            date_format(
                col("full_date"),
                "EEEE",
            ).alias("day_name"),

            weekofyear(
                col("full_date")
            ).alias("week_of_year"),

            dayofyear(
                col("full_date")
            ).alias("day_of_year"),

            when(
                dayofweek(col("full_date")).isin(1, 7),
                True,
            )
            .otherwise(False)
            .alias("is_weekend"),
        )
    )

    (
        dim_date_df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print("[SUCCESS] dim_date build completed")
    print(f"[INFO] output_path={OUTPUT_PATH}")
    print(f"[INFO] date_range={START_DATE} ~ {END_DATE}")

    spark.stop()


if __name__ == "__main__":
    main()