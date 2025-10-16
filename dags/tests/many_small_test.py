from airflow.sdk import dag, task
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone
import time

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}


@dag(tags=["example"], catchup=False)
def many_small_test():
    @task.bash
    def hello_world_bash():
        return 'echo "Hello world from Bash!"'

    for i in range(20):
        hello_world_bash.override(task_id=f"hello_task_{i}")()


many_small_test()
