import time

from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"

SHUFFLE_PARTITIONS = [25, 50, 100, 200]


def print_spark_config(spark):
    print("\n" + "=" * 80)
    print("SPARK CONFIG")
    print("=" * 80)

    print(
        "AQE enabled =",
        spark.conf.get("spark.sql.adaptive.enabled"),
    )
    # print(
    #     "AQE coalesce partitions enabled =",
    #     spark.conf.get(
    #         "spark.sql.adaptive.coalescePartitions.enabled",
    #         "true",
    #     ),
    # )
    print(
        "Default shuffle partitions =",
        spark.conf.get("spark.sql.shuffle.partitions"),
    )


def build_aggregation(fact_df):
    """
    customer_sk 기준으로 데이터를 집계해
    Shuffle을 의도적으로 발생시킨다.
    """

    return (
        fact_df
        .groupBy("customer_sk")
        .agg(
            F.count("*").alias("order_item_count"),
            F.sum("item_price").alias("total_price"),
            F.sum("item_freight_value").alias("total_freight"),
        )
    )


def consume_result(aggregated_df):
    """
    단순 count()만 수행하면 optimizer가 일부 집계 계산을
    제거할 가능성이 있으므로 실제 집계 결과를 다시 사용한다.
    """

    final_df = (
        aggregated_df
        .agg(
            F.sum("order_item_count").alias("total_order_items"),
            F.sum("total_price").alias("grand_total_price"),
            F.sum("total_freight").alias("grand_total_freight"),
        )
    )

    return final_df.collect()[0]


def run_experiment(spark, fact_df, shuffle_partitions):
    print("\n\n" + "#" * 100)
    print(
        f"EXPERIMENT: spark.sql.shuffle.partitions = "
        f"{shuffle_partitions}"
    )
    print("#" * 100)

    spark.conf.set(
        "spark.sql.shuffle.partitions",
        str(shuffle_partitions),
    )

    print(
        "Current shuffle partitions =",
        spark.conf.get("spark.sql.shuffle.partitions"),
    )

    aggregated_df = build_aggregation(fact_df)

    print("\n[PHYSICAL PLAN]")
    aggregated_df.explain("formatted")

    start_time = time.perf_counter()

    result = consume_result(aggregated_df)

    elapsed_time = time.perf_counter() - start_time

    print("\n[RESULT]")
    print(
        f"shuffle_partitions = {shuffle_partitions}"
    )
    print(
        f"total_order_items = {result['total_order_items']}"
    )
    print(
        f"grand_total_price = {result['grand_total_price']}"
    )
    print(
        f"grand_total_freight = {result['grand_total_freight']}"
    )
    print(
        f"elapsed_seconds = {elapsed_time:.3f}"
    )

    return {
        "shuffle_partitions": shuffle_partitions,
        "elapsed_seconds": elapsed_time,
    }


def main():
    spark = create_spark_session(
        "Compare Shuffle Partitions AQE OFF"
    )

    spark.sparkContext.setLogLevel("WARN")

    # 이번 1차 실험에서는 AQE를 켠 상태로 고정
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true",
    )

    print_spark_config(spark)

    fact_df = (
        spark.read
        .parquet(FACT_ORDER_ITEM_PATH)
        .select(
            "customer_sk",
            "item_price",
            "item_freight_value",
        )
        .filter(
            F.col("customer_sk").isNotNull()
        )
    )

    print("\n" + "=" * 80)
    print("MATERIALIZE INPUT")
    print("=" * 80)

    # 각 실험에서 원본 Parquet Scan 비용이 반복되지 않도록
    # 입력 DataFrame을 동일한 상태로 재사용한다.
    fact_df = fact_df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    input_count = fact_df.count()

    print(f"input_rows = {input_count}")
    print(
        f"input_partitions = "
        f"{fact_df.rdd.getNumPartitions()}"
    )

    results = []

    for shuffle_partitions in SHUFFLE_PARTITIONS:
        result = run_experiment(
            spark,
            fact_df,
            shuffle_partitions,
        )

        results.append(result)

    print("\n\n" + "=" * 100)
    print("SUMMARY")
    print("=" * 100)

    print(
        f"{'Shuffle Partitions':<25}"
        f"{'Elapsed Seconds':>20}"
    )

    for result in results:
        print(
            f"{result['shuffle_partitions']:<25}"
            f"{result['elapsed_seconds']:>20.3f}"
        )

    fact_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()