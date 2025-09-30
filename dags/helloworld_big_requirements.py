from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def wait_a_while():
    time.sleep(60)


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
        executor_config={
            "KubernetesExecutor": {
                "request_memory": "4Gi",
                "limit_memory": "4Gi",
                "request_cpu": "6",
            }
        },
    )
