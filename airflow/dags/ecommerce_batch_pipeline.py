from datetime import datetime, timedelta
import pendulum
import os
import json
import requests
import docker

from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup

COMMON_JARS = (
    "/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
    "/opt/airflow/jars/postgresql-42.7.3.jar"
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SPARK_CONF = {
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key": MINIO_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key": MINIO_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}

KST = pendulum.timezone("Asia/Seoul")

def send_slack_alert(context, status):
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("SLACK_WEBHOOK_URL is not set.")
        return

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    if status == "success":
        emoji = "✅"
        title = "Airflow Task Success"
    else:
        emoji = "🚨"
        title = "Airflow Task Failed"

    message = {
        "text": (
            f"{emoji} *{title}*\n"
            f"*DAG*: `{dag_id}`\n"
            f"*Task*: `{task_id}`\n"
            f"*Execution Date*: `{execution_date}`\n"
            f"*Log*: {log_url}"
        )
    }

    response = requests.post(
        webhook_url,
        data=json.dumps(message),
        headers={"Content-Type": "application/json"},
        timeout=5
    )

    print(response.text)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": lambda context: send_slack_alert(context, "failed"),
}

STREAMING_CONTAINER_NAME = "spark-streaming"


def stop_streaming_container():
    client = docker.from_env()

    try:
        container = client.containers.get(STREAMING_CONTAINER_NAME)

        if container.status == "running":
            print(f"Stopping {STREAMING_CONTAINER_NAME}...")
            container.stop()
        else:
            print(f"{STREAMING_CONTAINER_NAME} is already stopped. status={container.status}")

    except docker.errors.NotFound:
        print(f"{STREAMING_CONTAINER_NAME} container not found. skip stop.")


def start_streaming_container():
    client = docker.from_env()

    try:
        container = client.containers.get(STREAMING_CONTAINER_NAME)

        container.reload()
        if container.status != "running":
            print(f"Starting {STREAMING_CONTAINER_NAME}...")
            container.start()
        else:
            print(f"{STREAMING_CONTAINER_NAME} is already running.")

    except docker.errors.NotFound:
        raise RuntimeError(f"{STREAMING_CONTAINER_NAME} container not found. cannot start streaming.")

def create_spark_task(task_id: str, application: str, on_success_callback=None) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        conn_id="spark_default",
        jars=COMMON_JARS,
        conf=SPARK_CONF,
        env_vars={"PYTHONPATH": "/app/spark"},
        verbose=True,
        on_success_callback=on_success_callback,
    )


with DAG(
    dag_id="ecommerce_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 5, 7, tzinfo=KST),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    template_searchpath=["/app/sql"],
    tags=["ecommerce", "spark", "batch"],
) as dag:

    with TaskGroup("silver_layer") as silver_layer:
        build_fact_order_item = create_spark_task(
            task_id="build_fact_order_item",
            application="/app/spark/batch/build_fact_order_item.py",
        )

        build_fact_order_event = create_spark_task(
            task_id="build_fact_order_event",
            application="/app/spark/batch/build_fact_order_event.py",
        )

        build_fact_payment = create_spark_task(
            task_id="build_fact_payment",
            application="/app/spark/batch/build_fact_payment.py",
        )

        build_fact_delivery = create_spark_task(
            task_id="build_fact_delivery",
            application="/app/spark/batch/build_fact_delivery.py",
        )

        build_fact_review = create_spark_task(
            task_id="build_fact_review",
            application="/app/spark/batch/build_fact_review.py",
        )

    with TaskGroup("validation_layer") as validation_layer:
        validate_fact_order_item = create_spark_task(
            task_id="validate_fact_order_item",
            application="/app/spark/batch/validation/validate_fact_order_item.py",
        )

        validate_fact_order_event = create_spark_task(
            task_id="validate_fact_order_event",
            application="/app/spark/batch/validation/validate_fact_order_event.py",
        )

        validate_fact_payment = create_spark_task(
            task_id="validate_fact_payment",
            application="/app/spark/batch/validation/validate_fact_payment.py",
        )

        validate_fact_delivery = create_spark_task(
            task_id="validate_fact_delivery",
            application="/app/spark/batch/validation/validate_fact_delivery.py",
        )

        validate_fact_review = create_spark_task(
            task_id="validate_fact_review",
            application="/app/spark/batch/validation/validate_fact_review.py",
        )

    with TaskGroup("gold_layer") as gold_layer:
        build_mart_sales = create_spark_task(
            task_id="build_mart_sales",
            application="/app/spark/gold/build_mart_sales.py",
        )

        build_mart_category = create_spark_task(
            task_id="build_mart_category",
            application="/app/spark/gold/build_mart_category.py",
        )

        build_mart_customer = create_spark_task(
            task_id="build_mart_customer",
            application="/app/spark/gold/build_mart_customer.py",
        )

        build_mart_delivery_satisfaction = create_spark_task(
            task_id="build_mart_delivery_satisfaction",
            application="/app/spark/gold/build_mart_delivery_satisfaction.py",
        )

    with TaskGroup("gold_validation_layer") as gold_validation_layer:
        validate_mart_sales = create_spark_task(
            task_id="validate_mart_sales",
            application="/app/spark/gold/validation/validate_mart_sales.py",
        )

        validate_mart_category = create_spark_task(
            task_id="validate_mart_category",
            application="/app/spark/gold/validation/validate_mart_category.py",
        )

        validate_mart_customer = create_spark_task(
            task_id="validate_mart_customer",
            application="/app/spark/gold/validation/validate_mart_customer.py",
        )

        validate_mart_delivery_satisfaction = create_spark_task(
            task_id="validate_mart_delivery_satisfaction",
            application="/app/spark/gold/validation/validate_mart_delivery_satisfaction.py",
        )

    with TaskGroup("serving_layer") as serving_layer:
        publish_gold = SQLExecuteQueryOperator(
            task_id="publish_gold",
            conn_id="ecommerce_postgres",
            sql="publish_gold.sql",
            on_success_callback=lambda context: send_slack_alert(context, "success"),
        )
    
    stop_streaming = PythonOperator(
        task_id="stop_streaming",
        python_callable=stop_streaming_container,
    )

    start_streaming = PythonOperator(
        task_id="start_streaming",
        python_callable=start_streaming_container,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    stop_streaming >> silver_layer

    build_fact_order_item >> validate_fact_order_item
    build_fact_order_event >> validate_fact_order_event
    build_fact_payment >> validate_fact_payment
    build_fact_delivery >> validate_fact_delivery
    build_fact_review >> validate_fact_review

    validation_layer >> gold_layer

    build_mart_sales >> validate_mart_sales
    build_mart_category >> validate_mart_category
    build_mart_customer >> validate_mart_customer
    build_mart_delivery_satisfaction >> validate_mart_delivery_satisfaction

    gold_validation_layer >> serving_layer >> start_streaming
