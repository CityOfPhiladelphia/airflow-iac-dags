from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pytz import timezone

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}

# https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-from-the-cli
with DAG(
    # Name of the DAG, must be globally unique
    dag_id="add_connections",
    default_args=default_args,
    schedule=None,
    catchup=False,
    # Tags are useful for filtering
    tags=["secrets"],
) as dag:
    command = [
        "airflow",
        "connections",
        "add",
        "'test-add-connection",
        "--conn-uri",
        "'https://admin:admin@phila.gov:443/test-schema?test-value=abcd'",
    ]

    BashOperator(task_id="add_test_connection", bash_command=" ".join(command))
