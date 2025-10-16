from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone
from airflow.sdk import dag, task

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


@dag(tags=["example"], schedule="0 */2 * * *", catchup=False)
def long_test():
    @task.bash
    def sleep_task(seconds: int):
        return f"sleep {seconds}; echo completed"

    sleep_5 = sleep_task.override(
        executor_config=executor_config_small_resources, task_id="sleep_5"
    )(5 * 60)
    sleep_10 = sleep_task.override(
        executor_config=executor_config_small_resources, task_id="sleep_10"
    )(10 * 60)
    sleep_15 = sleep_task.override(
        executor_config=executor_config_small_resources, task_id="sleep_15"
    )(15 * 60)


long_test()
