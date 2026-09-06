import time

from pyspark.sql.functions import (
    broadcast,
    col,
    to_date,
    to_timestamp,
)
from pyspark.storagelevel import StorageLevel

from common.spark_session import create_spark_session


# ============================================================
# Paths
# ============================================================

ORDER_EVENTS_PATH = "s3a://ecommerce/bronze/events/order_events/"
ORDER_ITEMS_PATH = "s3a://ecommerce/bronze/olist/order_items/"
CUSTOMERS_PATH = "s3a://ecommerce/bronze/olist/customers/"
DIM_CUSTOMER_PATH = "s3a://ecommerce/silver/dim_customer/"
FACT_ORDER_ITEM_PATH = "s3a://ecommerce/silver/fact_order_item/"


# ============================================================
# Common helpers
# ============================================================

def print_section(title):
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)


def materialize(df, name):
    """
    Join 자체의 성능을 비교하기 위해
    입력 DataFrame을 먼저 cache + count하여 materialize한다.

    MEMORY_AND_DISK:
    메모리에 다 못 올라가면 디스크를 사용할 수 있도록 한다.
    """
    print(f"[MATERIALIZE] {name}")

    cached_df = df.persist(
        StorageLevel.MEMORY_AND_DISK
    )

    start = time.perf_counter()

    row_count = cached_df.count()

    elapsed = time.perf_counter() - start

    print(
        f"[MATERIALIZE] {name} "
        f"rows={row_count:,}, "
        f"time={elapsed:.3f}s"
    )

    return cached_df


def run_experiment(
    spark,
    experiment_name,
    build_join_df,
    broadcast_threshold,
):
    """
    하나의 Join 실험 실행.

    build_join_df:
        Join DataFrame을 생성하는 함수

    broadcast_threshold:
        auto  -> 원래 Spark 설정 사용
        -1    -> Auto Broadcast 비활성화
    """

    print_section(
        f"EXPERIMENT: {experiment_name}"
    )

    if broadcast_threshold == "auto":
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            "10485760b",
        )
    else:
        spark.conf.set(
            "spark.sql.autoBroadcastJoinThreshold",
            broadcast_threshold,
        )

    print(
        "autoBroadcastJoinThreshold =",
        spark.conf.get(
            "spark.sql.autoBroadcastJoinThreshold"
        )
    )

    result_df = build_join_df()

    print()
    print("----- Physical Plan -----")
    result_df.explain("formatted")

    print()
    print(
        "result partitions =",
        result_df.rdd.getNumPartitions(),
    )

    start = time.perf_counter()

    row_count = result_df.count()

    elapsed = time.perf_counter() - start

    print()
    print("----- Result -----")

    print(
        f"experiment={experiment_name}"
    )

    print(
        f"rows={row_count:,}"
    )

    print(
        f"elapsed_seconds={elapsed:.3f}"
    )

    return {
        "experiment": experiment_name,
        "rows": row_count,
        "elapsed_seconds": elapsed,
    }


def print_summary(results):
    print_section("FINAL SUMMARY")

    print(
        f"{'Experiment':<45}"
        f"{'Rows':>15}"
        f"{'Seconds':>15}"
    )

    print("-" * 75)

    for result in results:
        print(
            f"{result['experiment']:<45}"
            f"{result['rows']:>15,}"
            f"{result['elapsed_seconds']:>15.3f}"
        )


# ============================================================
# Base DataFrames
# ============================================================

def build_order_created_df(spark):

    return (
        spark.read
        .parquet(ORDER_EVENTS_PATH)
        .filter(
            col("event_type") == "ORDER_CREATED"
        )
        .select(
            "order_id",
            "customer_id",
            to_timestamp(
                col("event_time")
            ).alias("order_timestamp"),
        )
        .dropDuplicates(["order_id"])
    )


def build_order_items_df(spark):

    return (
        spark.read
        .parquet(ORDER_ITEMS_PATH)
    )


def build_customers_df(spark):

    return (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select(
            "customer_id",
            "customer_unique_id",
        )
        .dropDuplicates(["customer_id"])
    )


def build_dim_customer_df(spark):

    return (
        spark.read
        .parquet(DIM_CUSTOMER_PATH)
        .select(
            "customer_sk",
            "customer_unique_id",
            "valid_from",
            "valid_to",
        )
    )


# ============================================================
# Experiment 1
# order_created ↔ order_items
# Inner Equi Join
# ============================================================

def experiment_inner_join(
    spark,
    results,
):

    print_section(
        "JOIN 1 - order_created ↔ order_items"
    )

    order_created_df = materialize(
        build_order_created_df(spark),
        "order_created",
    )

    order_items_df = materialize(
        build_order_items_df(spark),
        "order_items",
    )

    print(
        "order_created partitions =",
        order_created_df.rdd.getNumPartitions(),
    )

    print(
        "order_items partitions =",
        order_items_df.rdd.getNumPartitions(),
    )

    # --------------------------------------------------------
    # 1-A Auto
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "INNER / AUTO"
            ),
            broadcast_threshold="auto",
            build_join_df=lambda: (
                order_created_df
                .alias("orders")
                .join(
                    order_items_df.alias("items"),
                    on="order_id",
                    how="inner",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 1-B Auto Broadcast disabled
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "INNER / BROADCAST_DISABLED"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                order_created_df
                .alias("orders")
                .join(
                    order_items_df.alias("items"),
                    on="order_id",
                    how="inner",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 1-C Force Broadcast order_items
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "INNER / FORCE_BROADCAST_ITEMS"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                order_created_df
                .alias("orders")
                .join(
                    broadcast(
                        order_items_df
                    ).alias("items"),
                    on="order_id",
                    how="inner",
                )
            ),
        )
    )

    order_created_df.unpersist()
    order_items_df.unpersist()

    spark.catalog.clearCache()


# ============================================================
# Experiment 2
# fact/customer ↔ dim_customer
# SCD2 Left Join
# ============================================================

def experiment_scd2_join(
    spark,
    results,
):

    print_section(
        "JOIN 2 - customer SCD2 Join"
    )

    order_created_df = build_order_created_df(
        spark
    )

    customers_df = build_customers_df(
        spark
    )

    # SCD2 Join 직전 입력 생성
    fact_customer_df = (
        order_created_df
        .select(
            "order_id",
            "customer_id",
            to_date(
                col("order_timestamp")
            ).alias("order_date"),
        )
        .alias("fact")
        .join(
            customers_df.alias("customers"),
            col("fact.customer_id")
            == col("customers.customer_id"),
            how="left",
        )
        .select(
            col("fact.order_id")
            .alias("order_id"),
            col("fact.order_date")
            .alias("order_date"),
            col(
                "customers.customer_unique_id"
            ).alias(
                "customer_unique_id"
            ),
        )
    )

    fact_customer_df = materialize(
        fact_customer_df,
        "fact_customer_before_scd2",
    )

    dim_customer_df = materialize(
        build_dim_customer_df(spark),
        "dim_customer",
    )

    def scd2_condition(
        fact_alias,
        dim_alias,
    ):
        return (
            (
                col(
                    f"{fact_alias}."
                    "customer_unique_id"
                )
                ==
                col(
                    f"{dim_alias}."
                    "customer_unique_id"
                )
            )
            &
            (
                col(
                    f"{fact_alias}.order_date"
                )
                >=
                col(
                    f"{dim_alias}.valid_from"
                )
            )
            &
            (
                col(
                    f"{dim_alias}.valid_to"
                ).isNull()
                |
                (
                    col(
                        f"{fact_alias}.order_date"
                    )
                    <=
                    col(
                        f"{dim_alias}.valid_to"
                    )
                )
            )
        )

    # --------------------------------------------------------
    # 2-A Auto
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "SCD2 LEFT / AUTO"
            ),
            broadcast_threshold="auto",
            build_join_df=lambda: (
                fact_customer_df
                .alias("fact")
                .join(
                    dim_customer_df
                    .alias("dim"),
                    scd2_condition(
                        "fact",
                        "dim",
                    ),
                    how="left",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 2-B Broadcast disabled
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "SCD2 LEFT / "
                "BROADCAST_DISABLED"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                fact_customer_df
                .alias("fact")
                .join(
                    dim_customer_df
                    .alias("dim"),
                    scd2_condition(
                        "fact",
                        "dim",
                    ),
                    how="left",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 2-C Force Broadcast dim_customer
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "SCD2 LEFT / "
                "FORCE_BROADCAST_DIM"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                fact_customer_df
                .alias("fact")
                .join(
                    broadcast(
                        dim_customer_df
                    ).alias("dim"),
                    scd2_condition(
                        "fact",
                        "dim",
                    ),
                    how="left",
                )
            ),
        )
    )

    fact_customer_df.unpersist()
    dim_customer_df.unpersist()

    spark.catalog.clearCache()


# ============================================================
# Experiment 3
# fact_order_item ↔ existing keys
# Left Anti Join
# ============================================================

def experiment_left_anti_join(
    spark,
    results,
):

    print_section(
        "JOIN 3 - Left Anti Join"
    )

    fact_df = (
        spark.read
        .parquet(FACT_ORDER_ITEM_PATH)
    )

    existing_keys_df = (
        spark.read
        .parquet(FACT_ORDER_ITEM_PATH)
        .select(
            "order_id",
            "order_item_id",
        )
        .dropDuplicates()
    )

    fact_df = materialize(
        fact_df,
        "fact_order_item",
    )

    existing_keys_df = materialize(
        existing_keys_df,
        "existing_fact_keys",
    )

    join_keys = [
        "order_id",
        "order_item_id",
    ]

    # --------------------------------------------------------
    # 3-A Auto
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "LEFT ANTI / AUTO"
            ),
            broadcast_threshold="auto",
            build_join_df=lambda: (
                fact_df.join(
                    existing_keys_df,
                    on=join_keys,
                    how="left_anti",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 3-B Broadcast disabled
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "LEFT ANTI / "
                "BROADCAST_DISABLED"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                fact_df.join(
                    existing_keys_df,
                    on=join_keys,
                    how="left_anti",
                )
            ),
        )
    )

    # --------------------------------------------------------
    # 3-C Force Broadcast existing keys
    # --------------------------------------------------------

    results.append(
        run_experiment(
            spark=spark,
            experiment_name=(
                "LEFT ANTI / "
                "FORCE_BROADCAST_KEYS"
            ),
            broadcast_threshold=-1,
            build_join_df=lambda: (
                fact_df.join(
                    broadcast(
                        existing_keys_df
                    ),
                    on=join_keys,
                    how="left_anti",
                )
            ),
        )
    )

    fact_df.unpersist()
    existing_keys_df.unpersist()

    spark.catalog.clearCache()


# ============================================================
# Main
# ============================================================

def main():

    spark = create_spark_session(
        "Compare Fact Order Item Joins"
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print_section(
        "SPARK CONFIG"
    )

    print(
        "AQE enabled =",
        spark.conf.get(
            "spark.sql.adaptive.enabled"
        )
    )

    print(
        "shuffle partitions =",
        spark.conf.get(
            "spark.sql.shuffle.partitions"
        )
    )

    print(
        "preferSortMergeJoin =",
        spark.conf.get(
            "spark.sql.join.preferSortMergeJoin"
        )
    )

    print(
        "original "
        "autoBroadcastJoinThreshold =",
        spark.conf.get(
            "spark.sql.autoBroadcastJoinThreshold"
        )
    )

    results = []

    experiment_inner_join(
        spark,
        results,
    )

    experiment_scd2_join(
        spark,
        results,
    )

    experiment_left_anti_join(
        spark,
        results,
    )

    print_summary(
        results
    )

    spark.stop()


if __name__ == "__main__":
    main()