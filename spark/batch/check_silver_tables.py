from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, sum as spark_sum


TABLE_CONFIG = {
    "dim_customer": {
        "path": "s3a://ecommerce/silver/dim_customer/",
        "required_columns": [
            "customer_id",
            "customer_unique_id",
            "customer_name",
            "sex",
            "age",
            "age_group",
            "occupation",
            "marital_status",
            "education_level",
            "family_type",
            "housing_type",
            "province",
            "district",
            "persona",
        ],
        "not_null_columns": [
            "customer_id",
            "customer_unique_id",
            "sex",
            "age",
            "age_group",
            "occupation",
        ],
        "unique_key": [
            "customer_id",
        ],
        "allowed_values": {
            "age_group": [
                "20s",
                "30s",
                "40s",
                "50s",
                "60s",
                "70+",
            ],
        },
        "numeric_rules": {
            "age": {
                "min": 0,
            },
        },
    },

    "dim_product": {
        "path": "s3a://ecommerce/silver/dim_product/",
        "required_columns": [
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ],
        "not_null_columns": [
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_photos_qty",
        ],
        "unique_key": [
            "product_id",
        ],
        "allowed_values": {},
        "numeric_rules": {
            "product_photos_qty": {
                "min": 0,
            },
            "product_weight_g": {
                "min": 0,
                "allow_null": True,
            },
            "product_length_cm": {
                "min": 0,
                "allow_null": True,
            },
            "product_height_cm": {
                "min": 0,
                "allow_null": True,
            },
            "product_width_cm": {
                "min": 0,
                "allow_null": True,
            },
        },
    },

    "dim_seller": {
        "path": "s3a://ecommerce/silver/dim_seller/",
        "required_columns": [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ],
        "not_null_columns": [
            "seller_id",
        ],
        "unique_key": [
            "seller_id",
        ],
        "allowed_values": {},
        "numeric_rules": {},
    },

    "fact_order_item": {
        "path": "s3a://ecommerce/silver/fact_order_item/",
        "required_columns": [
            "order_id",
            "order_item_id",
            "event_type",
            "customer_id",
            "product_id",
            "seller_id",
            "event_time",
            "order_status",
            "shipping_limit_date",
            "item_price",
            "item_freight_value",
            "item_total_amount",
            "payment_total_value",
            "order_event_date",
            "order_month",
        ],
        "not_null_columns": [
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "seller_id",
            "event_type",
            "event_time",
            "item_price",
            "item_freight_value",
            "order_event_date",
            "order_month",
        ],
        "unique_key": [
            "order_id",
            "order_item_id",
            "event_type",
        ],
        "allowed_values": {
            "event_type": [
                "ORDER_CREATED",
                "ORDER_APPROVED",
                "ORDER_CANCELED",
            ],
        },
        "numeric_rules": {
            "item_price": {
                "min": 0,
            },
            "item_freight_value": {
                "min": 0,
            },
            "item_total_amount": {
                "min": 0,
            },
            "payment_total_value": {
                "min": 0,
                "allow_null": True,
            },
        },
    },

    "fact_delivery": {
        "path": "s3a://ecommerce/silver/fact_delivery/",
        "required_columns": [
            "order_id",
            "customer_id",
            "event_type",
            "event_time",
            "order_purchase_timestamp",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "shipping_days",
            "delivery_days",
            "is_delivered",
            "is_delayed",
            "delivery_event_date",
            "delivery_month",
        ],
        "not_null_columns": [
            "order_id",
            "customer_id",
            "event_type",
            "event_time",
            "delivery_event_date",
            "delivery_month",
        ],
        "unique_key": [
            "order_id",
            "event_type",
        ],
        "allowed_values": {
            "event_type": [
                "DELIVERY_STARTED",
                "DELIVERY_COMPLETED",
            ],
        },
        "numeric_rules": {
            "shipping_days": {
                "min": 0,
                "allow_null": True,
            },
            "delivery_days": {
                "min": 0,
                "allow_null": True,
            },
        },
    },

    "fact_review": {
        "path": "s3a://ecommerce/silver/fact_review/",
        "required_columns": [
            "review_id",
            "order_id",
            "customer_id",
            "event_type",
            "event_time",
            "review_score",
            "review_answer_timestamp",
            "review_answer_days",
            "review_event_date",
            "review_month",
        ],
        "not_null_columns": [
            "review_id",
            "order_id",
            "customer_id",
            "event_type",
            "event_time",
            "review_score",
            "review_event_date",
            "review_month",
        ],
        "unique_key": [
            "review_id",
            "event_type",
        ],
        "allowed_values": {
            "event_type": [
                "REVIEW_CREATED",
            ],
        },
        "numeric_rules": {
            "review_score": {
                "min": 1,
                "max": 5,
            },
            "review_answer_days": {
                "min": 0,
                "allow_null": True,
            },
        },
    },
}


def create_spark_session():
    return (
        SparkSession.builder
        .appName("Validate Silver Tables")
        .getOrCreate()
    )


def validate_required_columns(df, table_name, config):
    errors = []

    required_columns = set(config["required_columns"])
    actual_columns = set(df.columns)

    missing_columns = sorted(required_columns - actual_columns)

    if missing_columns:
        errors.append(
            f"[FAIL] {table_name} | missing columns: {missing_columns}"
        )
    else:
        print(f"[PASS] {table_name} | required columns")

    return errors


def validate_not_null(df, table_name, config):
    errors = []

    target_columns = [
        column_name
        for column_name in config["not_null_columns"]
        if column_name in df.columns
    ]

    if not target_columns:
        return errors

    null_count_expressions = [
        spark_sum(
            when(col(column_name).isNull(), 1).otherwise(0)
        ).alias(column_name)
        for column_name in target_columns
    ]

    null_counts = df.agg(*null_count_expressions).collect()[0].asDict()

    has_error = False

    for column_name, null_count in null_counts.items():
        null_count = int(null_count or 0)

        if null_count > 0:
            has_error = True
            errors.append(
                f"[FAIL] {table_name} | "
                f"null column={column_name}, count={null_count:,}"
            )

    if not has_error:
        print(f"[PASS] {table_name} | not-null check")

    return errors


def validate_unique_key(df, table_name, config):
    errors = []

    unique_key = config["unique_key"]

    if not all(column_name in df.columns for column_name in unique_key):
        return errors

    duplicate_groups = (
        df.groupBy(*unique_key)
        .agg(count("*").alias("row_count"))
        .filter(col("row_count") > 1)
    )

    duplicate_group_count = duplicate_groups.count()

    if duplicate_group_count > 0:
        errors.append(
            f"[FAIL] {table_name} | "
            f"duplicate key={unique_key}, "
            f"duplicate groups={duplicate_group_count:,}"
        )

        print(
            f"[INFO] {table_name} | duplicate key samples"
        )
        duplicate_groups.show(5, truncate=False)

    else:
        print(
            f"[PASS] {table_name} | unique key={unique_key}"
        )

    return errors


def validate_allowed_values(df, table_name, config):
    errors = []

    for column_name, allowed_values in config["allowed_values"].items():
        if column_name not in df.columns:
            continue

        invalid_df = df.filter(
            col(column_name).isNotNull()
            & (~col(column_name).isin(allowed_values))
        )

        invalid_count = invalid_df.count()

        if invalid_count > 0:
            errors.append(
                f"[FAIL] {table_name} | "
                f"invalid values column={column_name}, "
                f"count={invalid_count:,}, "
                f"allowed={allowed_values}"
            )

            print(
                f"[INFO] {table_name} | "
                f"invalid {column_name} samples"
            )

            (
                invalid_df
                .select(column_name)
                .distinct()
                .show(10, truncate=False)
            )

        else:
            print(
                f"[PASS] {table_name} | "
                f"allowed values column={column_name}"
            )

    return errors


def validate_numeric_rules(df, table_name, config):
    errors = []

    for column_name, rule in config["numeric_rules"].items():
        if column_name not in df.columns:
            continue

        condition = None

        if "min" in rule:
            min_condition = col(column_name) < rule["min"]
            condition = min_condition

        if "max" in rule:
            max_condition = col(column_name) > rule["max"]

            if condition is None:
                condition = max_condition
            else:
                condition = condition | max_condition

        if condition is None:
            continue

        allow_null = rule.get("allow_null", False)

        if allow_null:
            condition = (
                col(column_name).isNotNull()
                & condition
            )

        invalid_df = df.filter(condition)

        invalid_count = invalid_df.count()

        if invalid_count > 0:
            rule_description = []

            if "min" in rule:
                rule_description.append(
                    f"min={rule['min']}"
                )

            if "max" in rule:
                rule_description.append(
                    f"max={rule['max']}"
                )

            errors.append(
                f"[FAIL] {table_name} | "
                f"numeric rule column={column_name}, "
                f"count={invalid_count:,}, "
                f"rule={', '.join(rule_description)}"
            )

            print(
                f"[INFO] {table_name} | "
                f"invalid {column_name} samples"
            )

            (
                invalid_df
                .select(column_name)
                .show(10, truncate=False)
            )

        else:
            print(
                f"[PASS] {table_name} | "
                f"numeric rule column={column_name}"
            )

    return errors


def validate_table(spark, table_name, config):
    errors = []

    print()
    print("=" * 70)
    print(f"[VALIDATE] {table_name}")
    print(f"[PATH] {config['path']}")
    print("=" * 70)

    try:
        df = spark.read.parquet(config["path"])

    except Exception as e:
        errors.append(
            f"[FAIL] {table_name} | "
            f"unable to read table: {str(e)}"
        )
        return errors

    row_count = df.count()

    print(f"[INFO] {table_name} | rows={row_count:,}")

    if row_count == 0:
        errors.append(
            f"[FAIL] {table_name} | table is empty"
        )
        return errors

    print(f"[PASS] {table_name} | table is not empty")

    required_column_errors = validate_required_columns(
        df,
        table_name,
        config,
    )
    errors.extend(required_column_errors)

    # 필수 컬럼이 누락된 경우 이후 검증에서 잘못된 컬럼 참조를
    # 피하기 위해 존재하는 컬럼만 대상으로 각 검증을 수행한다.
    errors.extend(
        validate_not_null(
            df,
            table_name,
            config,
        )
    )

    errors.extend(
        validate_unique_key(
            df,
            table_name,
            config,
        )
    )

    errors.extend(
        validate_allowed_values(
            df,
            table_name,
            config,
        )
    )

    errors.extend(
        validate_numeric_rules(
            df,
            table_name,
            config,
        )
    )

    if not errors:
        print(f"[PASS] {table_name} | ALL CHECKS PASSED")
    else:
        print(
            f"[FAIL] {table_name} | "
            f"{len(errors)} validation error(s)"
        )

    return errors


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    all_errors = []

    print()
    print("#" * 70)
    print("SILVER DATA QUALITY VALIDATION START")
    print("#" * 70)

    for table_name, config in TABLE_CONFIG.items():
        table_errors = validate_table(
            spark,
            table_name,
            config,
        )

        all_errors.extend(table_errors)

    print()
    print("#" * 70)
    print("SILVER DATA QUALITY VALIDATION SUMMARY")
    print("#" * 70)

    if all_errors:
        print(
            f"[RESULT] FAILED | "
            f"{len(all_errors)} error(s)"
        )

        for error in all_errors:
            print(error)

        spark.stop()

        raise RuntimeError(
            f"Silver validation failed: "
            f"{len(all_errors)} error(s)"
        )

    print(
        f"[RESULT] PASSED | "
        f"{len(TABLE_CONFIG)} tables validated successfully"
    )

    spark.stop()


if __name__ == "__main__":
    main()