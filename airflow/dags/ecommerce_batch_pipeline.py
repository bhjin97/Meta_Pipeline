from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

COMMON_JARS = (
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/airflow/jars/postgresql-42.7.3.jar"
)

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


def create_spark_task(task_id: str, script_name: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=f"/app/spark/batch/{script_name}",
        conn_id="spark_default",
        jars=COMMON_JARS,
        conf=SPARK_CONF,
        verbose=True,
    )


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
        build_fact_order_item = create_spark_task(
            task_id="build_fact_order_item",
            script_name="build_fact_order_item.py",
        )

        build_fact_delivery = create_spark_task(
            task_id="build_fact_delivery",
            script_name="build_fact_delivery.py",
        )

        build_fact_review = create_spark_task(
            task_id="build_fact_review",
            script_name="build_fact_review.py",
        )

    with TaskGroup("validation_layer") as validation_layer:
        check_silver_tables = create_spark_task(
            task_id="check_silver_tables",
            script_name="check_silver_tables.py",
        )

    with TaskGroup("gold_layer") as gold_layer:
        build_gold_marts = create_spark_task(
            task_id="build_gold_marts",
            script_name="build_gold_marts.py",
        )

    silver_layer >> validation_layer >> gold_layer