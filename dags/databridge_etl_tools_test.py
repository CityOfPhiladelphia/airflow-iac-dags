from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pytz import timezone

databridge_test_conn = BaseHook.get_connection("databridge-v2-testing")
databridge_test_name = json.loads(databridge_conn.extra)["db_name"]
databridge_test_conn_string = f"{databridge_test_conn.login}/{databridge_test_conn.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={databridge_test_conn.host})(PORT={databridge_test_conn.port}))(CONNECT_DATA=(SID={databridge_test_name})))"

eastern = timezone("US/Eastern")

default_args = {
    "owner": "airflow",
    "start_date": datetime.now(eastern),
}

with DAG(
    dag_id="databridge_etl_tools_test",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    bash_operator = BashOperator(
        task_id="databridge_etl_tools_help",
        bash_command=f"echo {databridge_test_conn_string}; databridge_etl_tools --help",
    )
