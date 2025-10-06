from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from kubernetes.client import models as k8s
import time


def hello_world():
    print("Hello from KubernetesExecutor!")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}

for i in range(10):
    cpu_amt = 100 + i * 20
    with DAG(
        dag_id=f"hello_world_{cpu_amt}",
        default_args=default_args,
        schedule=None,
        catchup=False,
        tags=["example"],
    ) as dag:
        k8s_exec_config_resource_requirements = {
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    "cpu": f"{cpu_amt}m",
                                    "memory": "300Mi",
                                },
                                limits={
                                    # "cpu": f"{cpu_amt}m",
                                    "memory": "300Mi",
                                },
                            ),
                        )
                    ]
                )
            )
        }
        hello_task = PythonOperator(
            task_id="python",
            python_callable=hello_world,
            executor_config=k8s_exec_config_resource_requirements,
        )
        bash_operator = BashOperator(
            task_id="bash",
            bash_command="echo hello world",
            executor_config=k8s_exec_config_resource_requirements,
        )

# with DAG(
#    dag_id="hello_small_kubernetes_executor",
#    default_args=default_args,
#    schedule=None,
#    catchup=False,
#    tags=["example"],
# ) as dag:
#    for i in range(15):
#        cpu_amt = (i + 2) * 15
#        k8s_exec_config_resource_requirements = {
#            "pod_override": k8s.V1Pod(
#                spec=k8s.V1PodSpec(
#                    containers=[
#                        k8s.V1Container(
#                            name="base",
#                            resources=k8s.V1ResourceRequirements(
#                                requests={
#                                    "cpu": f"{cpu_amt}m",
#                                    "memory": "256Mi",
#                                },
#                                limits={
#                                    "cpu": f"{cpu_amt}m",
#                                    "memory": "256Mi",
#                                },
#                            ),
#                        )
#                    ]
#                )
#            )
#        }
#        hello_task = PythonOperator(
#            task_id=f"say_hello_world_python_small_{cpu_amt}",
#            python_callable=hello_world,
#            executor_config=k8s_exec_config_resource_requirements,
#        )
#        bash_operator = BashOperator(
#            task_id=f"say_hello_world_bash_small_{cpu_amt}",
#            bash_command="echo hello world",
#            executor_config=k8s_exec_config_resource_requirements,
#        )
