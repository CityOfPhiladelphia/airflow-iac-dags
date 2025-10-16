# from airflow.sdk import DAG
# from airflow.sdk import task
# from airflow.operators.bash import BashOperator
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from pytz import timezone
# from airflow.sdk import Connection
# from airflow import settings
# import json
#
# default_args = {
#    "owner": "airflow",
#    "start_date": datetime.now(timezone("US/Eastern")),
# }
#
#
# @task(task_id="add_test_connection")
# def add_connection():
#    conn = Connection(
#        conn_id="test-abcd",
#        conn_type="http",
#        host="phila.gov",
#        login="admin",
#        password="adminpass",
#        schema="test-schema",
#        port="443",
#        extra={"abcd": "defg"},
#    )
#    session = settings.Session()
#    session.add(conn)
#    session.commit()
#
#
## https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-from-the-cli
# with DAG(
#    # Name of the DAG, must be globally unique
#    dag_id="add_connections",
#    default_args=default_args,
#    schedule=None,
#    catchup=False,
#    # Tags are useful for filtering
#    tags=["secrets"],
# ) as dag:
#    add_connection()
