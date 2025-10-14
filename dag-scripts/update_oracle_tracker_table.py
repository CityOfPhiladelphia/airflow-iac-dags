from airflow.hooks.base_hook import BaseHook
import cx_Oracle
import json
import logging
import sys
from time import sleep
from datetime import datetime
import pytz


'''
If the dag is successful, update the latest Oracle SCN
'''
def update_oracle_tracker_table(account_name, table_name, conn_id, **context):

    # check if all past tasks successeful
    dr = context["dag_run"]
    ti = context["ti"]
    account_name = account_name.upper()
    table_name = table_name.upper()

    # Make a dictionary of all tasks in the dag where the key is the task name, and the value is it's state
    # state being 'failed', 'success', 'running', etc.
    dag_tasks = {task.task_id: task.state for task in dr.get_task_instances() if task.task_id != ti.task_id }

    dag_tasks.pop('email_data_stewards_on_fail', None)

    # Remove success/None states, anything left means we have prior task failure
    non_success_tasks = {}
    for k,v in dag_tasks.items():
        if v != 'success' and v != 'removed' and v is not None:
            non_success_tasks[k] = v

    if non_success_tasks:
        raise Exception(f'Some task(s) havent finished or failed, not updating Oracle SCN in tracker! Task(s): {non_success_tasks}')
    else:
        logging.info("No running, failed, scheduled, or retrying tasks, updating postgres xmin in tracker table..")
        try:
            db_conn = BaseHook.get_connection(conn_id)
            db_name = json.loads(db_conn.extra)['db_name']

            connection_string = f'{db_conn.login}/{db_conn.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={db_conn.host})(PORT={db_conn.port}))(CONNECT_DATA=(SID={db_name})))'

            conn = cx_Oracle.connect(connection_string)
            logging.info('Connected to %s' % conn)
            conn.autocommit = True

        except Exception as e:
            logging.info("Failed to connect to database! Exception: {}".format(e))
            sys.exit(1)


        cursor = conn.cursor()

        # Not getting the SCN here anymore, we are recording it right after the to_s3 task in XCOM.
        #stmt = f'''SELECT MAX(ora_rowscn) FROM {account_name.upper()}.{table_name.upper()}'''
        #cursor.execute(stmt)
        #current_scn = cursor.fetchone()[0]

        count_stmt = f'''SELECT count(*) FROM {account_name.upper()}.{table_name.upper()}'''
        cursor.execute(count_stmt)
        current_count = cursor.fetchone()[0]

        # Make our very unique xcom key to get the SCN we have stored in there from an earlier task.
        dag_run_id = context['dag_run'].run_id
        xcom_key = f'{account_name.lower()}_{table_name.lower()}__{dag_run_id}_SCN'

        logging.info(f'Pulling xcom key for SCN: {xcom_key}')
        current_scn = ti.xcom_pull(key=xcom_key)
        assert current_scn

        logging.info(f'Current SCN pulled from XCOM is: {current_scn}')

        logging.info('Grabbing SCN in table...')
        stmt=f'''
            SELECT SCN FROM GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY
            WHERE TABLE_OWNER = '{account_name.upper()}'
            AND TABLE_NAME = '{table_name.upper()}'
        '''
        logging.info('Executing stmt: ' + str(stmt))
        cursor.execute(stmt)
        old_scn = cursor.fetchone()

        # Also insert the time that we actually insert the record
        eastern = pytz.timezone('America/New_York')
        current_time_with_tz = datetime.now(eastern)

        # Convert datetime object to a string format that Oracle recognizes
        timestamp = current_time_with_tz.strftime('%Y-%m-%d %H:%M:%S %z')

        # Because Oracle is an outdated database product, we don't have upsert and need to do
        # either an insert or update depending if the row we want already exists.
        if old_scn is None:
            stmt = f'''
            INSERT INTO GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY (TABLE_OWNER, TABLE_NAME, SCN, ROW_COUNT, RECORDED)
                VALUES('{account_name.upper()}', '{table_name.upper()}', {current_scn}, {current_count}, TO_TIMESTAMP_TZ('{timestamp}', 'YYYY-MM-DD HH24:MI:SS TZHTZM'))
            '''
        elif old_scn:
            stmt = f'''
            UPDATE GIS_GSG.DB2_ORACLE_TRANSACTION_HISTORY SET SCN={current_scn}, ROW_COUNT={current_count}, RECORDED = TO_TIMESTAMP_TZ('{timestamp}', 'YYYY-MM-DD HH24:MI:SS TZHTZM')
                WHERE TABLE_OWNER = '{account_name.upper()}' AND TABLE_NAME = '{table_name.upper()}'
            '''

        print('Executing stmt: ' + str(stmt))

        try:
            cursor.execute(stmt)
        except Exception as e:
            cursor.close()
            conn.close()
            raise e

        try:
            assert cursor.rowcount != 0
        except:
            cursor.close()
            conn.close()
            raise AssertionError('No rows affected??')
        cursor.close()
        conn.close()


        #ti.xcom_push(key='testing', value='hi')
