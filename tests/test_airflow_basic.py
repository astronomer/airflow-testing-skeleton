"""Basic Airflow unit tests, by calling operator.execute()."""

import datetime

import pytest
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def test_bash_operator():
    """Validate a BashOperator"""
    test = BashOperator(task_id="test", bash_command="echo hello")
    result = test.execute(context={})
    assert result == "hello"


@pytest.mark.xfail(reason="Expect failure because outputs don't match")
def test_failure_bash_operator():
    """Validate a BashOperator"""
    test = BashOperator(task_id="test", bash_command="echo nothello")
    result = test.execute(context={})
    assert result == "hello"


def test_python_operator():
    def return_today(**context):
        return f"Today is {context['execution_date'].strftime('%d-%m-%Y')}"

    test = PythonOperator(task_id="test", python_callable=return_today)
    result = test.execute(context={"execution_date": datetime.datetime(2021, 1, 1)})
    assert result == "Today is 01-01-2021"
