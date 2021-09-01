import os
import shutil

import pytest

os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"  # Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"  # Don't want anything to "magically" work
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"  # Set default test settings, skip certain actions, etc.
os.environ["AIRFLOW_HOME"] = os.path.dirname(os.path.dirname(__file__))  # Hardcode AIRFLOW_HOME to root of this project


@pytest.fixture(autouse=True, scope="session")
def reset_db():
    """Reset the Airflow metastore for every test session."""
    from airflow.utils import db

    db.resetdb()
    yield

    # Cleanup temp files generated during tests
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.cfg"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "unittests.db"))
    os.remove(os.path.join(os.environ["AIRFLOW_HOME"], "webserver_config.py"))
    shutil.rmtree(os.path.join(os.environ["AIRFLOW_HOME"], "logs"))
