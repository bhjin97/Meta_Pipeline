from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="spark_test_dag",
    start_date=datetime(2026, 5, 7),
    schedule=None,
    catchup=False,
    tags=["spark", "test"],
) as dag:

    spark_test = BashOperator(
        task_id="run_spark_test_job",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          /app/spark/jobs/test_spark_job.py
        """,
    )