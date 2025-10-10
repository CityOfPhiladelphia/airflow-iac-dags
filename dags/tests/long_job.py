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

executor_config_small_resources = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "100m",  # also valid, cpu: 0.4
                            "memory": "300Mi",
                        },
                        limits={
                            "memory": "300Mi",  # request_memory and limit_memory should always be the same
                        },
                    ),
                )
            ]
        )
    )
}

# Create the DAG
with DAG(
    # Name of the DAG, must be globally unique
    dag_id="long_test",
    default_args=default_args,
    # Cron schedule, this runs every 20 minutes. Set to 'None' for manual only
    schedule="0/20 * * * *",
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    sleep_task = BashOperator(
        task_id="sleep",
        bash_command="sleep 600; echo completed",
        executor_config=executor_config_small_resources,
    )
