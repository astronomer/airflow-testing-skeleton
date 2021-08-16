"""Airflow unit tests using templated arguments."""

import datetime
import pathlib

from airflow.models import DAG
from airflow.operators.bash import BashOperator


def test_bash_operator(tmp_path: pathlib.PosixPath):
    """Validate a BashOperator using templated bash_command."""
    with DAG(dag_id="test_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily") as dag:
        output_file = tmp_path / "output.txt"
        test = BashOperator(task_id="test", bash_command="echo {{ ds_nodash }} > " + str(output_file))
        dag.clear()
        test.run(
            start_date=dag.start_date, end_date=dag.start_date, ignore_first_depends_on_past=True, ignore_ti_state=True
        )

        assert output_file.read_text() == "20210101\n"
