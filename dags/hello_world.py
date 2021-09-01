import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="hello_world",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
    tags=["hello", "world"],
) as dag:
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")
    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))
    hello >> world
