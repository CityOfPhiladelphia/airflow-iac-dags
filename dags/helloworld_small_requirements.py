from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from kubernetes.client import models as k8s
import time


k8s_exec_config_resource_requirements = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={
                            "cpu": "10m",
                            "memory": "128Mi",
                        },
                        limits={
                            "memory": "128Mi",
                        },
                    ),
                )
            ]
        )
    )
}


def wait_a_while():
    time.sleep(10)


def hello_world():
    print("Hello from KubernetesExecutor!")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

with DAG(
    dag_id="hello_big_kubernetes_executor",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    sleep_task = PythonOperator(
        task_id="wait_a_while_big", python_callable=wait_a_while
    )
    hello_task = PythonOperator(
        task_id="say_hello_big",
        python_callable=hello_world,
        executor_config=k8s_exec_config_resource_requirements,
    )
    bash_operator = BashOperator(
        task_id="databridge_etl_tools_pre",
        bash_command="echo hello world",
        executor_config=k8s_exec_config_resource_requirements,
    )
