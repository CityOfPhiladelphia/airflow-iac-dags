from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone
import time

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}

# Create the DAG
with DAG(
    # Name of the DAG, must be globally unique
    dag_id="many_small_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    for i in range(20):
        hello_task = BashOperator(task_id=f"hello_task_{i}", bash_command="echo hello")
