from pyspark.sql.functions import (
    col,
    date_format,
    datediff,
    to_date,
    to_timestamp,
)
from pyspark.sql.utils import AnalysisException

from common.spark_session import create_spark_session


REVIEW_EVENTS_PATH = (
    "s3a://ecommerce/bronze/events/review_events/"
)

REVIEWS_PATH = (
    "s3a://ecommerce/bronze/olist/reviews/"
)

ORDERS_PATH = (
    "s3a://ecommerce/bronze/olist/orders/"
)

CUSTOMERS_PATH = (
    "s3a://ecommerce/bronze/olist/customers/"
)

DIM_CUSTOMER_PATH = (
    "s3a://ecommerce/silver/dim_customer/"
)

OUTPUT_PATH = (
    "s3a://ecommerce/silver/fact_review/"
)


# ============================================================
# Existing Fact
# ============================================================

def read_existing_review_keys(spark):
    """
    기존 fact_review의 Grain Key를 읽는다.

    Grain:
        (review_id, order_id)

    반환:
        existing_keys_df
        is_initial_load
    """

    try:
        existing_df = (
            spark.read
            .parquet(OUTPUT_PATH)
        )

        print(
            "[INFO] Existing fact_review found. "
            "Running incremental load."
        )

        existing_keys_df = (
            existing_df
            .select(
                "review_id",
                "order_id",
            )
            .dropDuplicates(
                [
                    "review_id",
                    "order_id",
                ]
            )
        )

        return (
            existing_keys_df,
            False,
        )

    except AnalysisException as e:
        if "PATH_NOT_FOUND" not in str(e):
            raise

        print(
            "[INFO] No existing fact_review found. "
            "Running historical initial load."
        )

        empty_df = (
            spark.createDataFrame(
                [],
                """
                review_id string,
                order_id string
                """,
            )
        )

        return (
            empty_df,
            True,
        )


# ============================================================
# Bronze Review
# ============================================================

def build_review_details(spark):
    """
    Bronze reviews 전체를 읽는다.

    Historical Backfill에서는 이 데이터가
    authoritative source 역할을 한다.

    여기서는 dropDuplicates를 하지 않는다.
    원본 Grain 오류가 존재한다면 숨기지 않고
    validation 단계에서 실패시키기 위함이다.
    """

    return (
        spark.read
        .parquet(REVIEWS_PATH)
        .select(
            "review_id",
            "order_id",

            col("review_score")
            .cast("int")
            .alias("review_score"),

            to_timestamp(
                col("review_creation_date")
            ).alias(
                "review_creation_date"
            ),

            to_timestamp(
                col("review_answer_timestamp")
            ).alias(
                "review_answer_timestamp"
            ),
        )
    )


def validate_review_detail_grain(df):
    """
    Bronze review가 우리가 정의한 Grain을
    만족하는지 확인한다.

    Grain:
        (review_id, order_id)
    """

    duplicate_pair_count = (
        df.groupBy(
            "review_id",
            "order_id",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_pair_count > 0:
        raise RuntimeError(
            "Duplicate review pair detected "
            "in Bronze reviews. "
            f"duplicate_pair_count="
            f"{duplicate_pair_count}"
        )

    null_key_count = (
        df.filter(
            col("review_id").isNull()
            | col("order_id").isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise RuntimeError(
            "Review grain key contains NULL. "
            f"null_key_count="
            f"{null_key_count}"
        )


# ============================================================
# Review Event
# ============================================================

def build_review_events(spark):
    """
    REVIEW_CREATED 이벤트에서
    증분 처리 대상 Grain Key를 식별한다.

    이벤트 재전송 가능성이 있으므로
    이벤트 단계에서는 동일 pair를 dedup한다.
    """

    return (
        spark.read
        .parquet(REVIEW_EVENTS_PATH)
        .filter(
            col("event_type")
            == "REVIEW_CREATED"
        )
        .select(
            "review_id",
            "order_id",
        )
        .dropDuplicates(
            [
                "review_id",
                "order_id",
            ]
        )
    )


def build_incremental_review_details(
    new_review_events_df,
    review_details_df,
):
    """
    신규 REVIEW_CREATED 이벤트를
    Bronze review 상세정보와 결합한다.

    반드시 review_id + order_id
    두 컬럼으로 JOIN한다.
    """

    return (
        new_review_events_df.alias("event")
        .join(
            review_details_df.alias("review"),
            (
                col("event.review_id")
                == col("review.review_id")
            )
            & (
                col("event.order_id")
                == col("review.order_id")
            ),
            how="left",
        )
        .select(
            col("event.review_id")
            .alias("review_id"),

            col("event.order_id")
            .alias("order_id"),

            col("review.review_score")
            .alias("review_score"),

            col(
                "review.review_creation_date"
            ).alias(
                "review_creation_date"
            ),

            col(
                "review.review_answer_timestamp"
            ).alias(
                "review_answer_timestamp"
            ),
        )
    )


# ============================================================
# Lookup
# ============================================================

def build_order_customer_lookup(spark):
    """
    order_id -> customer_id
    """

    return (
        spark.read
        .parquet(ORDERS_PATH)
        .select(
            "order_id",
            "customer_id",
        )
        .dropDuplicates(
            ["order_id"]
        )
    )


def build_customer_lookup(spark):
    """
    customer_id -> customer_unique_id

    customer_unique_id + review_date
        -> customer_sk
    """

    customers_df = (
        spark.read
        .parquet(CUSTOMERS_PATH)
        .select(
            "customer_id",
            "customer_unique_id",
        )
        .dropDuplicates(
            ["customer_id"]
        )
    )

    dim_customer_df = (
        spark.read
        .parquet(DIM_CUSTOMER_PATH)
        .select(
            "customer_sk",
            "customer_unique_id",
            "valid_from",
            "valid_to",
        )
    )

    return (
        customers_df,
        dim_customer_df,
    )


# ============================================================
# Fact Build
# ============================================================

def build_source_df(
    review_source_df,
    orders_df,
    customers_df,
    dim_customer_df,
):
    """
    review_source_df를 기준으로
    customer_sk 및 분석 컬럼을 구성한다.

    review_source_df는 실행 모드에 따라:

    Initial:
        Bronze reviews 전체

    Incremental:
        신규 REVIEW_CREATED 이벤트에 해당하는
        Bronze review 상세정보
    """

    review_df = (
        review_source_df.alias("review")
        .join(
            orders_df.alias("orders"),
            col("review.order_id")
            == col("orders.order_id"),
            how="left",
        )
        .join(
            customers_df.alias("customer"),
            col("orders.customer_id")
            == col("customer.customer_id"),
            how="left",
        )
        .select(
            col("review.review_id")
            .alias("review_id"),

            col("review.order_id")
            .alias("order_id"),

            col(
                "customer.customer_unique_id"
            ).alias(
                "customer_unique_id"
            ),

            col("review.review_score")
            .alias("review_score"),

            col(
                "review.review_creation_date"
            ).alias(
                "review_creation_date"
            ),

            col(
                "review.review_answer_timestamp"
            ).alias(
                "review_answer_timestamp"
            ),

            to_date(
                col(
                    "review.review_creation_date"
                )
            ).alias(
                "review_date"
            ),
        )
    )

    # --------------------------------------------------------
    # SCD2 Customer Dimension
    #
    # 리뷰 생성일 당시 유효한 customer_sk 선택
    # --------------------------------------------------------

    return (
        review_df.alias("fact")
        .join(
            dim_customer_df.alias("dim"),
            (
                col(
                    "fact.customer_unique_id"
                )
                == col(
                    "dim.customer_unique_id"
                )
            )
            & (
                col("fact.review_date")
                >= col("dim.valid_from")
            )
            & (
                col("dim.valid_to").isNull()
                | (
                    col("fact.review_date")
                    <= col("dim.valid_to")
                )
            ),
            how="left",
        )
        .select(
            col("fact.review_id")
            .alias("review_id"),

            col("fact.order_id")
            .alias("order_id"),

            col("dim.customer_sk")
            .alias("customer_sk"),

            date_format(
                col("fact.review_date"),
                "yyyyMMdd",
            )
            .cast("int")
            .alias("date_key"),

            col("fact.review_score")
            .alias("review_score"),

            col(
                "fact.review_creation_date"
            ).alias(
                "review_creation_date"
            ),

            col(
                "fact.review_answer_timestamp"
            ).alias(
                "review_answer_timestamp"
            ),

            datediff(
                col(
                    "fact.review_answer_timestamp"
                ),
                col(
                    "fact.review_creation_date"
                ),
            ).alias(
                "review_answer_days"
            ),

            col("fact.review_date")
            .alias("review_date"),

            date_format(
                col("fact.review_date"),
                "yyyy-MM",
            ).alias(
                "review_month"
            ),
        )
    )


# ============================================================
# Validation
# ============================================================

def validate_before_write(df):
    """
    Write 전 최소 안전성 검증.

    Grain:
        (review_id, order_id)

    상세 품질 검증은
    validate_fact_review.py에서 수행한다.
    """

    # --------------------------------------------------------
    # Grain Validation
    # --------------------------------------------------------

    duplicate_pair_count = (
        df.groupBy(
            "review_id",
            "order_id",
        )
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    if duplicate_pair_count > 0:
        raise RuntimeError(
            "Duplicate review grain detected. "
            f"duplicate_pair_count="
            f"{duplicate_pair_count}"
        )

    # --------------------------------------------------------
    # Grain Key NULL
    # --------------------------------------------------------

    null_key_count = (
        df.filter(
            col("review_id").isNull()
            | col("order_id").isNull()
        )
        .count()
    )

    if null_key_count > 0:
        raise RuntimeError(
            "Review grain key contains NULL. "
            f"null_key_count="
            f"{null_key_count}"
        )

    # --------------------------------------------------------
    # Review Detail Mapping
    # --------------------------------------------------------

    null_review_detail_count = (
        df.filter(
            col("review_score").isNull()
            | col(
                "review_creation_date"
            ).isNull()
        )
        .count()
    )

    if null_review_detail_count > 0:
        raise RuntimeError(
            "Review detail mapping failed. "
            f"null_review_detail_count="
            f"{null_review_detail_count}"
        )

    # --------------------------------------------------------
    # Customer Dimension Mapping
    # --------------------------------------------------------

    null_customer_sk_count = (
        df.filter(
            col("customer_sk").isNull()
        )
        .count()
    )

    if null_customer_sk_count > 0:
        raise RuntimeError(
            "customer_sk mapping failed. "
            f"null_customer_sk_count="
            f"{null_customer_sk_count}"
        )


# ============================================================
# Main
# ============================================================

def main():
    spark = create_spark_session(
        "Build Fact Review"
    )

    spark.sparkContext.setLogLevel("WARN")

    print(
        f"[INFO] review_events_path="
        f"{REVIEW_EVENTS_PATH}"
    )

    print(
        f"[INFO] reviews_path="
        f"{REVIEWS_PATH}"
    )

    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    # --------------------------------------------------------
    # 1. Bronze review 상세 읽기
    # --------------------------------------------------------

    review_details_df = (
        build_review_details(spark)
    )

    validate_review_detail_grain(
        review_details_df
    )

    # --------------------------------------------------------
    # 2. Initial / Incremental 판단
    # --------------------------------------------------------

    (
        existing_review_keys_df,
        is_initial_load,
    ) = read_existing_review_keys(
        spark
    )

    # --------------------------------------------------------
    # 3. 처리 대상 결정
    # --------------------------------------------------------

    if is_initial_load:

        # ====================================================
        # Historical Initial Backfill
        #
        # 이벤트가 아니라 Bronze reviews 전체가 기준이다.
        # ====================================================

        print(
            "[INFO] Historical backfill source="
            "bronze/olist/reviews"
        )

        review_source_df = (
            review_details_df
        )

    else:

        # ====================================================
        # Incremental
        #
        # REVIEW_CREATED 이벤트에서 신규 pair만 처리한다.
        # ====================================================

        review_events_df = (
            build_review_events(spark)
        )

        new_review_events_df = (
            review_events_df
            .join(
                existing_review_keys_df,
                on=[
                    "review_id",
                    "order_id",
                ],
                how="left_anti",
            )
        )

        new_event_count = (
            new_review_events_df.count()
        )

        print(
            f"[INFO] new_review_count="
            f"{new_event_count}"
        )

        if new_event_count == 0:
            print(
                "[INFO] No new reviews "
                "to process."
            )

            spark.stop()
            return

        review_source_df = (
            build_incremental_review_details(
                new_review_events_df,
                review_details_df,
            )
        )

    # --------------------------------------------------------
    # 4. Lookup
    # --------------------------------------------------------

    orders_df = (
        build_order_customer_lookup(
            spark
        )
    )

    (
        customers_df,
        dim_customer_df,
    ) = build_customer_lookup(
        spark
    )

    # --------------------------------------------------------
    # 5. Fact 생성
    # --------------------------------------------------------

    fact_review_df = (
        build_source_df(
            review_source_df,
            orders_df,
            customers_df,
            dim_customer_df,
        )
    )

    # --------------------------------------------------------
    # 6. Validation
    # --------------------------------------------------------

    validate_before_write(
        fact_review_df
    )

    written_rows = (
        fact_review_df.count()
    )

    print(
        f"[INFO] rows_to_write="
        f"{written_rows}"
    )

    # --------------------------------------------------------
    # 7. Write
    # --------------------------------------------------------

    (
        fact_review_df.write
        .mode("append")
        .partitionBy(
            "review_month"
        )
        .parquet(
            OUTPUT_PATH
        )
    )

    print(
        "[SUCCESS] "
        "fact_review build completed"
    )

    print(
        f"[INFO] written_rows="
        f"{written_rows}"
    )

    print(
        f"[INFO] load_type="
        f"{'historical_backfill' if is_initial_load else 'incremental'}"
    )

    print(
        f"[INFO] output_path="
        f"{OUTPUT_PATH}"
    )

    spark.stop()


if __name__ == "__main__":
    main()