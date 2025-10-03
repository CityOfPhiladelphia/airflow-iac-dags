from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta
from pytz import timezone
import json

databridge_test_conn = PostgresHook.get_connection("databridge-v2-testing")
# databridge_test_conn_string = f"{databridge_test_conn.login}/{databridge_test_conn.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={databridge_test_conn.host})(PORT={databridge_test_conn.port}))(CONNECT_DATA=(SID={databridge_test_conn.schema})))"
databridge_test_conn_string = f"postgresql://{databridge_test_conn.login}:{databridge_test_conn.password}@{databridge_test_conn.host}:{databridge_test_conn.port}/{databridge_test_conn.schema}"

eastern = timezone("US/Eastern")

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(eastern),
}

# Use databridge etl tools image
# k8s_exec_config_custom_image = {
#    "pod_override": k8s.V1Pod(
#        spec=k8s.V1PodSpec(
#            containers=[
#                k8s.V1Container(
#                    name="base",
#                    image="880708401960.dkr.ecr.us-east-1.amazonaws.com/databridge-etl-tools-v2-testing:latest",
#                )
#            ],
#            security_context=k8s.V1SecurityContext(run_as_user="1000"),
#        )
#    )
# }

k8s_exec_config_custom_image = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="base",
                    image="880708401960.dkr.ecr.us-east-1.amazonaws.com/databridge-etl-tools-v2-airflow:airflow-3.0.2-latest",
                )
            ]
        )
    )
}

with DAG(
    dag_id="databridge_etl_tools_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    bash_operator = BashOperator(
        task_id="databridge_etl_tools_pre",
        bash_command="echo hello123; sleep 10;",
        # bash_command=f'echo "{databridge_test_conn_string}"; databridge_etl_tools --help',
        # executor_config=k8s_exec_config_custom_image,
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

    bash_operator = BashOperator(
        task_id="databridge_etl_tools_help",
        # bash_command="echo hello; sleep 10; databridge_etl_tools --help",
        # bash_command=f'echo "{databridge_test_conn_string}"; databridge_etl_tools --help',
        bash_command=" ".join(db_test_command),
        executor_config=k8s_exec_config_custom_image,
    )
