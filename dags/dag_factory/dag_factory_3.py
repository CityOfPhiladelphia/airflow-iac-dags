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

sys.path.append("/opt/airflow/dags/scripts")

from yaml_config import get_yaml_data, DagConfig


# Checks function
def checks_func(
    ti,
    table_name,
    table_schema,
    conn_id,
    target_table_schema,
    rowcount_difference_threshold,
    force_registered,
    **context,
):
    """
    Checks to determine if we're good to start the DAG.
    """
    # Version of the geodatabase I wrote this against. If we're newer than this,
    # then the below rows are probably supported now? Re-evaluate this when we update.
    # Check this page for more info: https://pro.arcgis.com/en/pro-app/latest/help/data/databases/dbms-data-types-supported.htm
    PINNED_GDB_VERSION = Version("10.9.0.2.8")
    # Data types that are not supported by our SDE geodatabase version, but probably for the next geodatabase version we upgrade to.
    FUTURE_SUPPORT_DATA_TYPES = ["int8"]
    # Always unsupported data types.
    UNSUPPORTED_DATA_TYPES = ["bool"]
    # Columns to always ignore when we do column comparisons.
    IGNORE_COLUMNS = ["objectid", "gdb_geomattr_data"]
    # Var to track whether dept and viewer tables are truly different.
    DIFFERENT = "false"
    # Vars to save current xmin and row count value for XCMON later
    CURRENT_XMIN = None
    CURRENT_ROW_COUNT = None

    ROWCOUNT_DIFFERENCE_THRESHOLD_ERROR = None
    SAVE_EXCEPTION = None

    REGISTRATION_ERROR = None

    ###############################
    # 1. Make DataBridge connection.
    db_hook = PostgresHook(postgres_conn_id=conn_id)
    conn = db_hook.get_conn()
    cur = conn.cursor()
    # Wrap all our checks in try block, so we can still save XCOM variables after in case someone wants to try to skip checks.
    try:
        ###############################
        # 2. Detect if registered. Do three checks to be sure.
        reg_stmt1 = f"select registration_id FROM sde.sde_table_registry where lower(table_name) = '{table_name.lower()}' and schema = '{table_schema}';"
        reg_stmt2 = f"select objectid FROM sde.gdb_items where lower(name) = 'databridge.{table_schema}.{table_name.lower()}';"
        # Note: this pulls in the XML definition made for the table when registered. It could be useful in the future to introspect this.
        reg_stmt3 = f"SELECT definition FROM sde.gdb_items WHERE lower(name) = 'databridge.{table_schema}.{table_name.lower()}';"

        cur.execute(reg_stmt1)
        registered_one = cur.fetchone()
        cur.execute(reg_stmt2)
        registered_two = cur.fetchone()
        cur.execute(reg_stmt3)
        registered_three = cur.fetchone()

        # all three should be true or false, otherwise this table is partially registered and we should
        if registered_one and registered_two and registered_three:
            is_registered = True
        elif registered_one or registered_two or registered_three:
            if not registered_one:
                print("No registration_id found in sde.sde_table_registry!")
                REGISTRATION_ERROR = "Inconsistent registration detected! Please remake the table. Reason: No registration_id found in sde.sde_table_registry!"
                print(REGISTRATION_ERROR)
            if not registered_two:
                REGISTRATION_ERROR = "Inconsistent registration detected! Please remake the table. Reason: No objectid found in sde.gdb_items!"
                print(REGISTRATION_ERROR)
            if not registered_three:
                REGISTRATION_ERROR = "Inconsistent registration detected! Please remake the table. Reason: No XML definition found in sde.gdb_items!"
                print(REGISTRATION_ERROR)
        else:
            is_registered = False
            print("***Source table detected as NOT registered!***")

        if is_registered:
            print("***Source table detected as registered with the SDE geodatabase.***")
            # Example of how we could search for values in the ESRI-made XML definition.
            # Left in here because it could be useful in the future.
            xml_def = registered_three[0]
            # find official registered name
            search_name = re.findall(r"<Name>(.*?)</Name>", xml_def)
            print(f"Registered name: {search_name[0]}")

            # Figure out what the official OBJECTID is (since there can be multiple like "OBJECTID_1")
            get_oid_column_stmt = f"""
                SELECT rowid_column FROM sde.sde_table_registry 
                WHERE table_name = '{table_name}' AND schema = '{table_schema}'
                """
            logging.info("Executing get_oid_column_stmt: " + str(get_oid_column_stmt))
            cur.execute(get_oid_column_stmt)
            oid_column = cur.fetchone()[0]

            # We really don't want objectid columns besides the default
            print(f"objectid column is: {oid_column}")
            assert oid_column.lower() == "objectid"

        ###############################
        # 3. Confirm tables exist
        dept_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = '{table_schema}' AND table_name = '{table_name}');"
        cur.execute(dept_exists_query)
        dept_exists = cur.fetchone()[0]
        assert dept_exists, (
            f'source table doesnt exist??: "{table_schema}.{table_name}"'
        )

        viewer_exists_query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables  WHERE table_schema = '{target_table_schema}' AND table_name = '{table_name}');"
        cur.execute(viewer_exists_query)
        viewer_exists = cur.fetchone()[0]
        assert viewer_exists, (
            f'viewer table doesnt exist??: "{target_table_schema}.{table_name}"'
        )

        ###############################
        # 4. Get columns from both tables and compare them
        get_dept_columns_stmt = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{table_schema}' AND table_name = '{table_name}';
        """
        get_viewer_columns_stmt = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{target_table_schema}' AND  table_name = '{table_name}';
        """

        cur.execute(get_dept_columns_stmt)
        dept_columns = cur.fetchall()
        assert dept_columns, (
            f'department columns are empty??: "{table_schema}.{table_name}"'
        )
        # convert to a dictionary for easier viewing/comparison
        dept_columns = {col[0]: col[1] for col in dept_columns}

        cur.execute(get_viewer_columns_stmt)
        viewer_columns = cur.fetchall()
        assert viewer_columns, (
            f'viewer columns are empty??: "{target_table_schema}.{table_name}"'
        )
        # convert to a dictionary for easier viewing/comparison
        viewer_columns = {col[0]: col[1] for col in viewer_columns}

        # Do not compare these columns as they are unreliable and will likely differ.
        for col in IGNORE_COLUMNS:
            if col in viewer_columns:
                del viewer_columns[col]
            if col in dept_columns:
                del dept_columns[col]

        # if it has a shape, then just make the data type "geometry" in case we're comparing against registered and non-registered tables.
        if "shape" in dept_columns:
            dept_columns["shape"] = "geometry"
        if "shape" in viewer_columns:
            viewer_columns["shape"] = "geometry"

        print("Comparing columns...")
        print(f"{table_schema}.{table_name} columns: {dept_columns}")
        print(f"{target_table_schema}.{table_name} columns: {viewer_columns}")
        # Differences between viewer and dept columns
        col_diff1 = set(dept_columns.items()) - set(viewer_columns.items())
        # Differences between dept and viewer columns
        col_diff2 = set(viewer_columns.items()) - set(dept_columns.items())

        assert dept_columns == viewer_columns, (
            f"Columns do not match!, diff in dept: {col_diff1}, diff in viewer: {col_diff2}"
        )

        ###############################
        # 4.5 Check for disallowed column types by our SDE geodatabase version.
        if is_registered or force_registered:
            get_gdb_version_stmt = "select description from sde.sde_version;"
            cur.execute(get_gdb_version_stmt)
            current_gdb_version = cur.fetchone()[0]
            current_gdb_version = current_gdb_version.split(" ")[0]
            print(f"SDE GDB version: {current_gdb_version}")

            data_type_url = "https://pro.arcgis.com/en/pro-app/latest/help/data/databases/dbms-data-types-supported.htm"
            # Hard to tell what's supported, but as of this writing and version of 10.9.0.2.8 these types aren't supported.
            for col, col_type in dept_columns.items():
                if col_type in FUTURE_SUPPORT_DATA_TYPES:
                    if Version(current_gdb_version) <= PINNED_GDB_VERSION:
                        raise AssertionError(
                            f'Column "{col}" is of type {col_type}, which (as of this writing) doesn\'t work in SDE geodatabase version {current_gdb_version}. Check this for supported data types: {data_type_url}'
                        )
                if col_type in UNSUPPORTED_DATA_TYPES:
                    raise AssertionError(
                        f'Column "{col}" is of type {col_type}, which (as of this writing) doesn\'t work in SDE geodatabase version {current_gdb_version}. Check this for supported data types: {data_type_url}'
                    )

        ###############################
        # 5. Determine if dept and viewer tables are different, either from a row count or from an EXCEPT statement.
        get_dept_count = f"select count(*) from {table_schema}.{table_name}"
        cur.execute(get_dept_count)
        dept_count = cur.fetchone()[0]

        assert dept_count > 0, "Department table is empty!!!"
        print(f"dept_count count: {dept_count}")

        print("Comparing row counts...")
        get_viewer_count = f"select count(*) from {target_table_schema}.{table_name}"
        cur.execute(get_viewer_count)  # count(*) is better than count(col)
        viewer_count = cur.fetchone()[0]
        print(f"dept_count count: {dept_count}, viewer_count: {viewer_count}")

        # Run a row count comparison check between dept and viewer, if viewer isn't 0 (e.g. first DAG run)
        if viewer_count == 0:
            print("Viewer table is empty! Must be new and this DAG is a first run?")
            DIFFERENT = "true"
        else:
            # Determine if tables are truly different (either row counts differ or row counts are the same but content differs)
            if dept_count != viewer_count:
                print(
                    f'Row counts are different {dept_count} vs {viewer_count}! Saving "tables_different" variable as true to XCOM.'
                )
                DIFFERENT = "true"
                # Perform our row count comparion to ensure we're within the row count difference thresholds we want.
                # Define our thresholds and ensure there's no big row count differences
                # Defaults are +/- 15%
                if not rowcount_difference_threshold:
                    # if row counts are under 50, then be way less strict
                    if dept_count < 50 and viewer_count < 50:
                        rowcount_difference_threshold = 200
                    else:
                        rowcount_difference_threshold = 15

                lower_pct = 1 - rowcount_difference_threshold * 0.01
                upper_pct = 1 + rowcount_difference_threshold * 0.01
                lower_threshold = int(
                    viewer_count * lower_pct
                )  # Ok if this is negative because we check for row count != 0 elsewhere
                upper_threshold = int(viewer_count * upper_pct)

                if dept_count < lower_threshold:
                    error_message = f"""Source dataset has {int(lower_pct * 100)}% or less rows than the enterprise dataset! Did a failed update occur?
                                    Enterprise: {viewer_count}, Source: {dept_count}"""
                    ROWCOUNT_DIFFERENCE_THRESHOLD_ERROR = error_message
                else:
                    print(
                        f"source count is within threshold of no lower than {int(lower_pct * 100)}% or {int(lower_threshold)} records. (based off target count: {viewer_count})"
                    )
                if dept_count > upper_threshold:
                    error_message = f"""Source dataset has {int(upper_pct * 100)}% or more rows than the enterprise dataset! Did a double update occur?
                                    Enterprise: {viewer_count}, Source: {dept_count}"""
                    ROWCOUNT_DIFFERENCE_THRESHOLD_ERROR = error_message
                else:
                    print(
                        f"source count is within threshold of no greater than {int(upper_pct * 100)}% or {int(upper_threshold)} records. (based off target count: {viewer_count}"
                    )
            else:
                print(
                    f"Row counts are the same {dept_count} vs {viewer_count}. Now running EXCEPT sql to determine if they're truly different."
                )
                # Compare them using EXCEPT if row counts are the same.
                # Only return a count of the rows that are different so we're not exploding ourselves with fetch results.
                # Keeps all work in the database and this script lightweight.

                # Pre-make our joined column string so python doesn't do anything funny and put them out of order each time it's made.
                # Also cast json as text so we can do a proper comparison
                quoted_columns = []
                for i in dept_columns.items():
                    if i[1] in ["json"]:
                        quoted_columns.append(f'"{i[0]}"::text')
                    else:
                        quoted_columns.append(f'"{i[0]}"')
                col_string = ",".join(quoted_columns)
                except_stmt = f"""
                SELECT COUNT(*)
                    FROM (
                        SELECT {col_string}
                        FROM {table_schema}.{table_name}
                        EXCEPT
                        SELECT {col_string}
                        FROM {target_table_schema}.{table_name}
                    ) differences;
                """
                print("Executing EXCEPT statement: " + str(except_stmt))
                cur.execute(except_stmt)
                different_rows_count = cur.fetchall()[0][0]
                print(f"Different except rows: {different_rows_count}")
                if different_rows_count > 0:
                    print(
                        f'Tables are different! Saving "tables_different" variable to XCOM.'
                    )
                    DIFFERENT = "true"
                elif different_rows_count == 0:
                    print(f"Tables are the same.")
                    DIFFERENT = "false"
                else:
                    print(
                        f"Huh?? EXCEPT statement returned a negative number of rows? {different_rows_count}!!"
                    )
                    raise AssertionError(
                        "EXCEPT statement returned a negative number of rows?"
                    )
    except Exception as e:
        print(f"***Checks Exception: {e}***")
        SAVE_EXCEPTION = e

    ###############################
    # 6. Finally, save results to XCOM. They'll be needed later in the DAG.

    # Save xcom variables with dag_run id in the name so we can track them across the DAG and ensure they're unique.
    dag_run_id = context["dag_run"].run_id

    # Save variable that tells us if the dept and viewer tables are truly different.
    assert DIFFERENT
    xcom_table_different_key = (
        f"{table_schema.lower()}_{table_name.lower()}__{dag_run_id}_TABLE_DIFFERENT"
    )
    print(f'Saving "table_different" XCOM key: {xcom_table_different_key}: {DIFFERENT}')
    ti.xcom_push(key=xcom_table_different_key, value=DIFFERENT)

    # Save XMIN
    stmt = f"""SELECT xmin::text::bigint as xminint FROM {table_schema}.{table_name} ORDER BY xminint desc limit 1;"""
    cur.execute(stmt)
    CURRENT_XMIN = cur.fetchone()[0]
    assert CURRENT_XMIN
    xcom_xmin_key = f"{table_schema.lower()}_{table_name.lower()}__{dag_run_id}_XMIN"
    print(f"Saving XMIN value {CURRENT_XMIN} to XCOM under key {xcom_xmin_key}")
    ti.xcom_push(key=xcom_xmin_key, value=CURRENT_XMIN)

    # Save current row count of department table.
    stmt = f"""select count(*) FROM {table_schema}.{table_name};"""
    cur.execute(stmt)
    CURRENT_ROW_COUNT = cur.fetchone()[0]
    assert CURRENT_ROW_COUNT
    xcom_row_count_key = (
        f"{table_schema.lower()}_{table_name.lower()}__{dag_run_id}_ROW_COUNT"
    )
    print(
        f"Saving Row count {CURRENT_ROW_COUNT} to XCOM under key {xcom_row_count_key}"
    )
    ti.xcom_push(key=xcom_row_count_key, value=CURRENT_ROW_COUNT)

    print("XCOM variables saved.")

    ###############################
    # 7. Raise errors if they were found earlier.
    # Do it here after we save XCOM variables, so someone can attempt to bypass checks if they think they need to.
    # XCOM variables are needed for the DAG to run successfully.
    if SAVE_EXCEPTION:
        raise SAVE_EXCEPTION

    if ROWCOUNT_DIFFERENCE_THRESHOLD_ERROR:
        raise AssertionError(ROWCOUNT_DIFFERENCE_THRESHOLD_ERROR)

    if REGISTRATION_ERROR:
        raise AssertionError(REGISTRATION_ERROR)

    conn.close()


def databridge_dag_factory(dag_config, s3_bucket, dbv2_conn_id):
    # Extract creds and conn string
    dbv2_creds = PostgresHook.get_connection(dbv2_conn_id)
    dbv2_conn_string = f"postgresql://{dbv2_creds.login}:{dbv2_creds.password}@{dbv2_creds.host}:{dbv2_creds.port}/{dbv2_creds.schema}"

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
