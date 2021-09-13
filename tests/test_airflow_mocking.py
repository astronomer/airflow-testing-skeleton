"""Examples for testing Airflow using mocking."""

import datetime
from pathlib import PosixPath
from typing import Dict
from unittest import mock

from _pytest.capture import CaptureFixture
from airflow.hooks.base import BaseHook
from airflow.models import DAG, BaseOperator, Connection
from airflow.operators.bash import BashOperator


def test_bash_operator_with_variable(tmp_path: PosixPath):
    """Mocking Variable.get() to avoid polluting the metastore for testing."""
    with DAG(dag_id="test_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily") as dag:
        with mock.patch("airflow.models.variable.Variable.get") as variable_get_mock:
            employees = ["Alice", "Bob", "Charlie"]
            variable_get_mock.return_value = employees
            output_file = tmp_path / "output.txt"
            test = BashOperator(task_id="test", bash_command="echo {{ var.json.employees }} > " + str(output_file))
            dag.clear()
            test.run(
                start_date=dag.start_date,
                end_date=dag.start_date,
                ignore_first_depends_on_past=True,
                ignore_ti_state=True,
            )

            variable_get_mock.assert_called_once()
            assert output_file.read_text() == f"[{', '.join(employees)}]\n"


@mock.patch.dict("os.environ", AIRFLOW_VAR_EMPLOYEES='["Alice", "Bob", "Charlie"]')
def test_bash_operator_with_variable_with_mock_decorator(tmp_path: PosixPath):
    """Mocking Variable.get() with a decorator & environment variable, to demonstrate different options."""
    with DAG(dag_id="test_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily") as dag:
        output_file = tmp_path / "output.txt"
        test = BashOperator(task_id="test", bash_command="echo {{ var.json.employees }} > " + str(output_file))
        dag.clear()
        test.run(
            start_date=dag.start_date,
            end_date=dag.start_date,
            ignore_first_depends_on_past=True,
            ignore_ti_state=True,
        )

        assert output_file.read_text() == "[Alice, Bob, Charlie]\n"


class FetchConnHostnameOperator(BaseOperator):
    """Sample operator to demonstrate testing & mocking a connection."""

    def __init__(self, *, conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self._conn_id = conn_id

    def execute(self, context: Dict):
        conn = BaseHook.get_connection(conn_id=self._conn_id)
        print(conn.host)


def test_custom_operator_with_connection(capsys: CaptureFixture):
    """Mocking BaseHook.get_connection() to avoid polluting the metastore for testing."""
    with DAG(dag_id="test_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily") as dag:
        with mock.patch("airflow.hooks.base.BaseHook.get_connection") as conn_mock:
            dummy_connection = Connection(host="localhost", login="airflow", password="airflow", port=1234)
            conn_mock.return_value = dummy_connection
            test = FetchConnHostnameOperator(task_id="test", conn_id="whatever")
            dag.clear()
            test.run(
                start_date=dag.start_date,
                end_date=dag.start_date,
                ignore_first_depends_on_past=True,
                ignore_ti_state=True,
            )

            conn_mock.assert_called_once()
            # capsys is a pytest builtin fixture to capture stdout & stderr
            output = capsys.readouterr()
            assert output.out == dummy_connection.host + "\n"
