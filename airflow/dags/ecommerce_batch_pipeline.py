from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup


SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.7.3"
)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


with DAG(
    dag_id="ecommerce_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 7),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "spark", "batch"],
) as dag:

    with TaskGroup("silver_layer") as silver_layer:

        build_fact_order_item = SparkSubmitOperator(
            task_id="build_fact_order_item",
            application="/app/spark/batch/build_fact_order_item.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            verbose=True,
        )

        build_fact_delivery = SparkSubmitOperator(
            task_id="build_fact_delivery",
            application="/app/spark/batch/build_fact_delivery.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            verbose=True,
        )

        build_fact_review = SparkSubmitOperator(
            task_id="build_fact_review",
            application="/app/spark/batch/build_fact_review.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            verbose=True,
        )

    with TaskGroup("validation_layer") as validation_layer:

        check_silver_tables = SparkSubmitOperator(
            task_id="check_silver_tables",
            application="/app/spark/batch/check_silver_tables.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            verbose=True,
        )

    with TaskGroup("gold_layer") as gold_layer:

        build_gold_marts = SparkSubmitOperator(
            task_id="build_gold_marts",
            application="/app/spark/batch/build_gold_marts.py",
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            verbose=True,
        )

    silver_layer >> validation_layer >> gold_layer