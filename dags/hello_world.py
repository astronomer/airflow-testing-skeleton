import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(dag_id="hello_world", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily") as dag:
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")
    world = PythonOperator(task_id="world", python_callable=lambda: print("world"))
    hello >> world
