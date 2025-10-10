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
                            "cpu": "100m",
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
    # Cron schedule, this runs every  two hours. Set to 'None' for manual only
    schedule="0 */2 * * *",
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    sleep_5_task = BashOperator(
        task_id="sleep_5",
        bash_command="sleep 300; echo completed",  # Sleep for 15 mins
        executor_config=executor_config_small_resources,
    )
    sleep_10_task = BashOperator(
        task_id="sleep_10",
        bash_command="sleep 600; echo completed",  # Sleep for 15 mins
        executor_config=executor_config_small_resources,
    )
    sleep_15_task = BashOperator(
        task_id="sleep_15",
        bash_command="sleep 900; echo completed",  # Sleep for 15 mins
        executor_config=executor_config_small_resources,
    )
