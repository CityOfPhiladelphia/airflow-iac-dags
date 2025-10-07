from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kubernetes.client import models as k8s
from datetime import datetime
from pytz import timezone
import time

# Secret retrieval should be done at the DAG level, not individually in the Python functions
# This makes the tasks fast because the secret will already be availabile to them
databridge_test_conn = PostgresHook.get_connection("databridge-v2-testing")
# If your script takes a libql string, you can build it manually like this
databridge_test_conn_string = f"postgresql://{databridge_test_conn.login}:{databridge_test_conn.password}@{databridge_test_conn.host}:{databridge_test_conn.port}/{databridge_test_conn.schema}"


def wait_a_while():
    time.sleep(10)


def hello_world():
    print("Hello from KubernetesExecutor!")


default_args = {
    "owner": "airflow",
    "start_date": datetime.now(timezone("US/Eastern")),
}

with DAG(
    # Name of the DAG, must be globally unique
    dag_id="hello_kubernetes_executor",
    default_args=default_args,
    # Cron schedule, this runs every 15 minutes. Set to 'None' for manual only
    schedule="0/15 * * * *",
    catchup=False,
    # Tags are useful for filtering
    tags=["example"],
) as dag:
    # Simple python sleep task
    sleep_task = PythonOperator(
        # Name of task must be unique within the dag, not globally
        task_id="wait_a_while",
        python_callable=wait_a_while,  # This refers to `def wait_a_while()`
    )
    # Simple python hello world task
    hello_task_python = PythonOperator(
        task_id="hello_task_python",
        python_callable=hello_world,  # This refers to `def hello_world()`
    )
    # Simple bash hello world task
    hello_task_bash = BashOperator(
        task_id="hello_task_bash",
        bash_command="echo hello world",
    )
    # Task with bigger requirements
    # By default, tasks have maximum of 300Mi of Memory (Airflow itself needs about 256)
    # And 150m (0.15) cpu minimum
    # Since most tasks are just executing DB commands, they shouldn't need much more CPU,
    # but if your script runs a lot of loops, increasing that number is recommended.
    # Please check with Ryan or Roland before going higher than 0.3 CPU or 1Gi Memory
    hello_task_python_big = PythonOperator(
        task_id="hello_task_python_big",
        python_callable=hello_world,
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base",
                            resources=k8s.V1ResourceRequirements(
                                requests={
                                    "cpu": "400m",  # also valid, cpu: 0.4
                                    "memory": "800Mi",
                                },
                                limits={
                                    "memory": "800Mi",  # request_memory and limit_memory should always be the same
                                },
                            ),
                        )
                    ]
                )
            )
        },
    )

    db_test_command = [
        "databridge_etl_tools",
        "db2",
        "--table_name=ghactions_test1",
        "--account_name=citygeo",
        "--enterprise_schema=viewer_citygeo",
        "--copy_from_source_schema=citygeo",
        f"--libpq_conn_string={databridge_test_conn_string}",
        "--timeout=50",
        "copy-dept-to-enterprise",
    ]

    # This task uses the databridge_etl_tools cli
    # Notice how it is much cleaner to build the command out as a list
    # and join it by spaces, than trying to have a massive single line command
    databridge_etl_tools_sample_task = BashOperator(
        task_id="databridge_etl_tools_sample", bash_command=" ".join(db_test_command)
    )

    # Control flow
    (
        sleep_task
        >> [hello_task_python, hello_task_bash, hello_task_python_big]
        >> databridge_etl_tools_sample_task
    )
