from airflow.hooks.base_hook import BaseHook
import cx_Oracle
import json
import logging
import sys

'''
Save the latest Oracle SCN in Airflow XCOM
'''
def save_xcom_oracle_scn(ti, table_schema, table_name, conn_id, **context):

    table_schema = table_schema.upper()
    table_name = table_name.upper()

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

    stmt = f'''SELECT MAX(ora_rowscn) FROM {table_schema.upper()}.{table_name.upper()}'''
    cursor.execute(stmt)
    current_scn = cursor.fetchone()[0]

    # Make our very unique key in xcom
    dag_run_id = context['dag_run'].run_id
    xcom_key = f'{table_schema.lower()}_{table_name.lower()}__{dag_run_id}_SCN'
    print(f'Saving SCN value {current_scn} to XCOM under key {xcom_key}')
    ti.xcom_push(key=xcom_key, value=current_scn)
