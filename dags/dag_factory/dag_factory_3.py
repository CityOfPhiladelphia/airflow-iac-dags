from airflow import DAG
import os
import sys
import json
from pytz import timezone
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from packaging.version import Version
import re
import logging
from datetime import datetime, timedelta

sys.path.append("/opt/airflow/dags/shared")

from yaml_config import get_yaml_data, DagConfig
from scripts.checks import checks as checks_func


def databridge_dag_factory(dag_config, s3_bucket, dbv2_conn_id):
    # Extract creds and conn string
    dbv2_creds = PostgresHook.get_connection(dbv2_conn_id)
    dbv2_conn_string = f"postgresql://{dbv2_creds.login}:{dbv2_creds.password}@{dbv2_creds.host}:{dbv2_creds.port}/{dbv2_creds.schema}"

    # Set viewer account at the top level for use elsewhere
    if dag_config["override_viewer_account"] and isinstance(
        dag_config["override_viewer_account"], str
    ):
        viewer_account = (
            f"viewer_{dag_config['override_viewer_account'].replace('viewer_', '')}"
        )
    else:
        viewer_account = f"viewer_{dag_config['account_name']}"

    # Default args used for constructing/initializing operators, e.g. we can set
    # defaults for all the tasks below. Important ones being the retry amounts and execuption timeouts.
    # reference: https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html
    eastern = timezone("US/Eastern")

    default_args = {
        "owner": "airflow",
        "retries": 3 if os.environ["ENVIRONMENT"] == "prod-v2" else 1,
        "retry_delay": timedelta(seconds=15),
        "execution_timeout": dag_config["execution_timeout"],
    }

    if dag_config["dagrun_timeout"]:
        dag_timeout = timedelta(seconds=dag_config["dagrun_timeout"])
    else:
        dag_timeout = None

    #######################################################
    # Where we actually construct our DAG and its tasks.  #
    #######################################################
    with DAG(
        dag_id=dag_config["dag_id"],
        # now minus one week
        schedule=dag_config["schedule_interval"],
        default_args=default_args,
        max_active_runs=1,
        dagrun_timeout=dag_timeout,
        catchup=False,  # Don't queue up a dag run for every missed dag
        tags=dag_config["tags"],
    ) as dag:
        # Call the checks function
        checks = PythonOperator(
            task_id="checks",
            python_callable=checks_func,
            op_kwargs={
                "table_name": dag_config["table_name"],
                "table_schema": dag_config["account_name"],
                "conn_id": dbv2_conn_id,
                "target_table_schema": dag_config["source_schema"],
                "rowcount_difference_threshold": dag_config[
                    "rowcount_difference_threshold"
                ],
                "force_registered": dag_config["force_viewer_registered"],
            },
        )
        # Send department to viewer
        upsert_to_viewer_command = [
            "databridge_etl_tools",
            "postgres",
            f"--connection_string={dbv2_conn_string}",
            f"--table_name={dag_config['table_name']}",
            f"--table_schema={viewer_account}",
            "upsert-table",
            f"--staging_table={dag_config['table_name']}",
            f"--staging_schema={dag_config['account_name']}",
            f"--delete_stale={dag_config['delete_stale']}",
        ]
        send_dept_to_viewer = BashOperator(
            task_id="upsert_to_viewer", bash_command=" ".join(upsert_to_viewer_command)
        )
        checks >> send_dept_to_viewer


def run_dagfactory():
    print("Running dag factory")
    # Load OS config
    try:
        airflow_env = os.environ["ENVIRONMENT"]
    except KeyError:
        raise Exception("Environment variable $ENVIRONMENT missing")

    try:
        s3_bucket = os.environ["S3_NAME"]
    except KeyError:
        raise Exception("Environment variable $S3_NAME missing")

    # Establish databridge connection based on environment
    if airflow_env == "prod-v2":
        dbv2_conn_id = "databridge-v2"
        # TEMPORARY
        raise Exception("TEMP, AIRFLOW PROD IS DISABLED")
    elif airflow_env == "test-v2":
        dbv2_conn_id = "databridge-v2-testing"
    else:
        raise Exception(
            "Airflow env must be `prod-v2` or `test-v2`, currently " + airflow_env
        )

    # Loop through configs
    dag_configs_path = "/opt/airflow/dags/dag_factory/configs"
    dag_config_folders = os.listdir(dag_configs_path)
    for department in dag_config_folders:
        department_path = os.path.join(dag_configs_path, department)
        if os.path.isfile(department_path):
            print(f"{department} is not a folder.")
            continue
        for table_config_file_name in os.listdir(department_path):
            # only parse yaml files.
            if not table_config_file_name.endswith(
                ".yml"
            ) and not table_config_file_name.endswith(".yaml"):
                print(f"Not parsing: {table_config_file_name}")
                continue
            table_name = table_config_file_name.split(".")[0].lower()
            table_config_file_path = os.path.join(
                department_path, table_config_file_name
            )
            yaml_data = get_yaml_data(table_config_file_path)
            if not yaml_data:
                print(f"ERROR! Could not parse: {table_config_file_path}")
                continue

            config_data = {**yaml_data, "table_name": table_name}
            dag_config = DagConfig(config_data)

            if dag_config["status"] == "disabled":
                print(f"Config file disabled: {table_config_file_name}")
                continue
            if dag_config["status"] == "needs_review":
                print(f"Config file is in needs_review: {table_config_file_name}")
                continue
            if dag_config["is_view"] or dag_config["view_name"]:
                # Don't process view configs in this dag factory
                continue
            if dag_config["status"] == "enabled":
                print(f"Running dag factory for config: {table_config_file_name}")
                databridge_dag_factory(
                    dag_config, s3_bucket=s3_bucket, dbv2_conn_id=dbv2_conn_id
                )


# So long as this file is not being run by pytest, run the full dagfactory when called.
if "pytest" not in sys.modules:
    run_dagfactory()
