from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

BASE_CMD = """
spark-submit \
  --master spark://spark-master:7077 \
  --packages {packages} \
  /app/spark/batch/{script}
"""


with DAG(
    dag_id="ecommerce_batch_pipeline", 
    start_date=datetime(2026, 5, 7),
    schedule=None, 
    catchup=False,
    tags=["ecommerce", "spark", "batch"],
) as dag:

    build_fact_order_item = BashOperator(
        task_id="build_fact_order_item",
        bash_command=BASE_CMD.format(
            packages=SPARK_PACKAGES,
            script="build_fact_order_item.py",
        ),
    )

    build_fact_delivery = BashOperator(
        task_id="build_fact_delivery",
        bash_command=BASE_CMD.format(
            packages=SPARK_PACKAGES,
            script="build_fact_delivery.py",
        ),
    )

    build_fact_review = BashOperator(
        task_id="build_fact_review",
        bash_command=BASE_CMD.format(
            packages=SPARK_PACKAGES,
            script="build_fact_review.py",
        ),
    )

    check_silver_tables = BashOperator(
        task_id="check_silver_tables",
        bash_command=BASE_CMD.format(
            packages=SPARK_PACKAGES,
            script="check_silver_tables.py",
        ),
    )

    build_gold_marts = BashOperator(
        task_id="build_gold_marts",
        bash_command=BASE_CMD.format(
            packages=SPARK_PACKAGES,
            script="build_gold_marts.py",
        ),
    )

    [
        build_fact_order_item,
        build_fact_delivery,
        build_fact_review,
    ] >> check_silver_tables >> build_gold_marts

# 태스크 그룹 추가