from airflow import DAG
import os
import sys
import json
from pytz import timezone
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from yaml_config import get_yaml_data, DagConfig
from scripts.checks import checks as checks_func

sys.path.append("/opt/airflow/dags/")


def databridge_dag_factory(dag_config, s3_bucket, dbv2_conn_id):
    # Extract creds and conn string
    dbv2_creds = PostgresHook(postgres_conn_id=dbv2_conn_id).get_connection()
    dbv2_conn_string = f"postgresql://{dbv2_creds.login}:{dbv2_creds.password}@{dbv2_creds.host}:{dbv2_creds.port}/{dbv2_creds.schema}"
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


def run_dagfactory():
    # Load OS config
    try:
        s3_bucket = os.environ["S3_NAME"]
    except KeyError:
        raise Exception("Environment variable $S3_NAME missing")

    try:
        airflow_env = os.environ["ENVIRONMENT"]
    except KeyError:
        raise Exception("Environment variable $ENVIRONMENT missing")

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

    # databridge_dag_factory(s3_bucket=s3_bucket, dbv2_conn_string=dbv2_conn_string)
