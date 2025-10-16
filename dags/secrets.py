from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pytz import timezone
from airflow.models import Connection
import json

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
    conn = Connection(
        conn_id="test-abcd",
        conn_type="http",
        host="phila.gov",
        login="admin",
        password="admin",
        port="443",
    )
    command = [
        "airflow",
        "connections",
        "add",
        "'test-add-connection'",
        "--conn-json",
        f"'{json.dumps(conn.to_dict())}'",
    ]

    BashOperator(task_id="add_test_connection", bash_command=" ".join(command))
