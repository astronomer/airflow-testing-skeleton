"""Test the validity of all DAGs."""

from airflow.models import DagBag


def test_dagbag():
    """
    Validate DAG files using Airflow's DagBag.
    This includes sanity checks e.g. do tasks have required arguments, are DAG ids unique & do DAGs have no cycles.
    """
    dag_bag = DagBag(include_examples=False)
    assert not dag_bag.import_errors  # Import errors aren't raised but captured to ensure all DAGs are parsed

    # Additional project-specific checks can be added here, e.g. to enforce each DAG has a tag
    for dag_id, dag in dag_bag.dags.items():
        error_msg = f"{dag_id} in {dag.full_filepath} has no tags"
        assert dag.tags, error_msg
