from airflow.sdk import dag, task
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


@dag(tags=["example"], catchup=False)
def very_big_test():
    @task.bash
    def hello_world_bash():
        return 'echo "Hello world from Bash!"'

    hello_world_bash.override(executor_config=executor_config_very_big_resources)


very_big_test()
