"""Unit tests using pytest_docker_tools to create Docker containers for testing."""

from unittest import mock

from airflow.models import Connection
from airflow.operators.sql import SQLValueCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pytest_docker_tools import fetch, container

# Pytest fixture for Postgres Docker image
postgres_image = fetch(repository="postgres:13-alpine")

# Pytest fixture for Postgres Docker container
postgres = container(
    image="{postgres_image.id}",
    environment={"POSTGRES_USER": "airflow", "POSTGRES_PASSWORD": "airflow"},
    ports={"5432/tcp": None},
)


# Mock decorators avoid using with ... context manager, which avoids indenting code
@mock.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.get_connection")
@mock.patch("airflow.hooks.base.BaseHook.get_connection")
def test_postgres_operator_with_docker(base_hook_mock, pg_hook_mock, postgres):
    """Validate the PostgresOperator with the help of Postgres in Docker using pytest_docker_tools."""
    postgres_connection = Connection(
        host="localhost",
        conn_type="postgres",
        login="airflow",
        password="airflow",
        port=postgres.ports["5432/tcp"][0],
        schema="postgres",
    )
    pg_hook_mock.return_value = postgres_connection
    base_hook_mock.return_value = postgres_connection

    # Create table and insert rows
    create_data_and_count = PostgresOperator(
        task_id="create_data_and_count",
        postgres_conn_id="foobar",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
                pet_id SERIAL PRIMARY KEY,
                name VARCHAR NOT NULL,
                pet_type VARCHAR NOT NULL,
                birth_date DATE NOT NULL,
                OWNER VARCHAR NOT NULL
            );

            INSERT INTO pet VALUES (DEFAULT, 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet VALUES (DEFAULT, 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet VALUES (DEFAULT, 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet VALUES (DEFAULT, 'Quincy', 'Parrot', '2013-08-11', 'Anne');
          """,
    )
    create_data_and_count.execute(context={})

    # Since you cannot fetch any data with the PostgresOperator,
    # use the SQLValueCheckOperator to verify the number of rows
    count_rows = SQLValueCheckOperator(
        task_id="count_rows", conn_id="foobar", sql="SELECT COUNT(*) FROM pet;", pass_value=4
    )
    count_rows.execute(context={})
