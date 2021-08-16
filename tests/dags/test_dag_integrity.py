"""Test the validity of all DAGs."""

import glob
from os import path

import pytest
from airflow import models as airflow_models
from airflow.utils import dag_cycle_tester

# Select *.py files in /dags directory
DAG_PATHS = glob.glob(path.join(path.dirname(__file__), "..", "..", "dags", "*.py"))
FILE_NAMES = [path.basename(dag_path) for dag_path in DAG_PATHS]


@pytest.mark.parametrize("dag_path", DAG_PATHS, ids=FILE_NAMES)
def test_dag_integrity(dag_path):
    """Fetch DAG objects from *.py files in /dags."""
    dag_name = path.basename(dag_path)
    module = _import_file(dag_name, dag_path)
    dag_objects = [var for var in vars(module).values() if isinstance(var, airflow_models.DAG)]

    # For every DAG object, test for cycles
    for dag in dag_objects:
        dag_cycle_tester.test_cycle(dag)


def _import_file(module_name, module_path):
    """Python magic to load objects from a given Python script. Same effect as running 'python your_dag.py'."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
