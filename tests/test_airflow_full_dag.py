"""Tests demonstrating how to validate a complete DAG."""

import datetime

from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.state import State


def test_dag():
    """Validate a complete DAG."""
    with DAG(dag_id="test_dag", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@daily") as dag:
        start = BashOperator(task_id="start", bash_command="echo start")

        branch = BranchDayOfWeekOperator(
            task_id="branch",
            follow_task_ids_if_true="email_week_team",
            follow_task_ids_if_false="email_weekend_team",
            week_day={"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY"},
        )

        email_week_team = DummyOperator(task_id="email_week_team")
        email_weekend_team = DummyOperator(task_id="email_weekend_team")

        start >> branch >> [email_week_team, email_weekend_team]

        dag.clear()
        dag.run(executor=DebugExecutor(), start_date=dag.start_date, end_date=dag.start_date)

        # Validate DAG run was successful
        dagruns = DagRun.find(dag_id=dag.dag_id, execution_date=dag.start_date)
        assert len(dagruns) == 1
        assert dagruns[0].state == State.SUCCESS

        # Validate task states
        expected_task_states = {
            start: State.SUCCESS,
            branch: State.SUCCESS,
            email_week_team: State.SUCCESS if dag.start_date.weekday() < 5 else State.SKIPPED,
            email_weekend_team: State.SUCCESS if dag.start_date.weekday() >= 5 else State.SKIPPED,
        }
        for task, expected_state in expected_task_states.items():
            ti = TaskInstance(task, dag.start_date)
            ti.refresh_from_db()
            assert ti.state == expected_state
