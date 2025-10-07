from airflow import DAG
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone
import time

executor_config_very_big_resources = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "4",  # also valid, cpu: 0.4
                            "memory": "8Gi",
                        },
                        limits={
                            "memory": "8Gi",  # request_memory and limit_memory should always be the same
                        },
                    ),
                )
            ]
        )
    )
}

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}

# Create the DAG
with DAG(
    # Name of the DAG, must be globally unique
    dag_id="very_big_test",
    default_args=default_args,
    # Cron schedule, this runs every 15 minutes. Set to 'None' for manual only
    schedule=None,
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    hello_task_big = BashOperator(
        task_id="hello_task_big",
        command="echo hello",
        executor_config=executor_config_very_big_resources,
    )
