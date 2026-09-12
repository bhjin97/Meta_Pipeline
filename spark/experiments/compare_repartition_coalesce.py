import time

from pyspark import StorageLevel
from pyspark.sql.functions import spark_partition_id, count

from common.spark_session import create_spark_session


FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"

BASE_PARTITIONS = 10
TARGET_PARTITIONS = 2


def print_partition_distribution(spark, df, label):
    print(f"\n===== {label} Partition Distribution =====")

    spark.sparkContext.setJobDescription(
        f"{label}_DISTRIBUTION"
    )

    distribution_df = (
        df.withColumn(
            "partition_id",
            spark_partition_id(),
        )
        .groupBy("partition_id")
        .agg(
            count("*").alias("row_count")
        )
        .orderBy("partition_id")
    )

    distribution_df.show(100, truncate=False)


def run_count_test(spark, df, label):
    print(f"\n===== {label} =====")

    partition_count = df.rdd.getNumPartitions()
    print(f"Partitions: {partition_count}")

    spark.sparkContext.setJobDescription(
        f"{label}_COUNT"
    )

    start = time.perf_counter()

    row_count = df.count()

    elapsed = time.perf_counter() - start

    print(f"Rows: {row_count}")
    print(f"Elapsed: {elapsed:.3f} sec")

    return {
        "label": label,
        "partitions": partition_count,
        "rows": row_count,
        "elapsed": elapsed,
    }


def main():
    spark = create_spark_session(
        "Compare Repartition vs Coalesce Phase 3"
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n==============================================")
    print(" Repartition vs Coalesce Experiment - Phase 3")
    print("==============================================")

    # --------------------------------------------------
    # 1. Read source
    # --------------------------------------------------
    fact_df = spark.read.parquet(
        FACT_ORDER_ITEM_PATH
    )

    original_partitions = (
        fact_df.rdd.getNumPartitions()
    )

    print(f"\nOriginal Partitions: {original_partitions}")
    print(f"Base Partitions:     {BASE_PARTITIONS}")
    print(f"Target Partitions:   {TARGET_PARTITIONS}")

    # --------------------------------------------------
    # 2. Create common starting point
    #
    # 2 partitions
    #      ↓
    # repartition(10)
    #      ↓
    # persist
    #      ↓
    # materialize
    #      ↓
    # cached 10-partition DataFrame
    # --------------------------------------------------
    print("\n===== MATERIALIZE BASE DATAFRAME =====")

    base_df = (
        fact_df
        .repartition(BASE_PARTITIONS)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    spark.sparkContext.setJobDescription(
        f"BASE_REPARTITION_{BASE_PARTITIONS}_MATERIALIZE"
    )

    start = time.perf_counter()

    base_row_count = base_df.count()

    base_materialize_elapsed = (
        time.perf_counter() - start
    )

    print(
        f"Base Partitions: "
        f"{base_df.rdd.getNumPartitions()}"
    )
    print(f"Rows: {base_row_count}")
    print(
        f"Materialize Elapsed: "
        f"{base_materialize_elapsed:.3f} sec"
    )

    # --------------------------------------------------
    # Optional: verify common base distribution
    # --------------------------------------------------
    print_partition_distribution(
        spark,
        base_df,
        f"BASE_REPARTITION_{BASE_PARTITIONS}",
    )

    # --------------------------------------------------
    # 3. Experiment A
    # repartition(10) -> repartition(2)
    #
    # Cached base
    #      ↓
    # repartition(2)
    #      ↓
    # Shuffle
    # --------------------------------------------------
    repartition_df = base_df.repartition(
        TARGET_PARTITIONS
    )

    repartition_result = run_count_test(
        spark,
        repartition_df,
        f"REPARTITION_{TARGET_PARTITIONS}",
    )

    print_partition_distribution(
        spark,
        repartition_df,
        f"REPARTITION_{TARGET_PARTITIONS}",
    )

    # --------------------------------------------------
    # 4. Experiment B
    # repartition(10) -> coalesce(2)
    #
    # Cached base
    #      ↓
    # coalesce(2)
    #      ↓
    # No full shuffle
    # --------------------------------------------------
    coalesce_df = base_df.coalesce(
        TARGET_PARTITIONS
    )

    coalesce_result = run_count_test(
        spark,
        coalesce_df,
        f"COALESCE_{TARGET_PARTITIONS}",
    )

    print_partition_distribution(
        spark,
        coalesce_df,
        f"COALESCE_{TARGET_PARTITIONS}",
    )

    # --------------------------------------------------
    # 5. Summary
    # --------------------------------------------------
    print("\n==============================================")
    print(" Result Summary")
    print("==============================================")

    print(
        f"{'BASE MATERIALIZE':25s} | "
        f"Partitions={BASE_PARTITIONS:4d} | "
        f"Rows={base_row_count:8d} | "
        f"Elapsed={base_materialize_elapsed:.3f}s"
    )

    for result in [
        repartition_result,
        coalesce_result,
    ]:
        print(
            f"{result['label']:25s} | "
            f"Partitions={result['partitions']:4d} | "
            f"Rows={result['rows']:8d} | "
            f"Elapsed={result['elapsed']:.3f}s"
        )

    # --------------------------------------------------
    # 6. Release cached blocks
    # --------------------------------------------------
    base_df.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()