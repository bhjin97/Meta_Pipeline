from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="hello_airflow_dag",
    start_date=datetime(2026, 5, 7),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    hello = BashOperator(
        task_id="hello_task",
        bash_command="echo 'Airflow DAG 실행 성공!' && date",
    )