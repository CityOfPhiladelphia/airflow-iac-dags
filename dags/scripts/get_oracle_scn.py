from airflow.hooks.base_hook import BaseHook
import os,sys
import json
import logging
import psycopg2
import cx_Oracle
import re

def get_oracle_scn(ti, **kwargs):

    logging.info("DEBUG kwargs: " + str(kwargs))
    account_name = kwargs['account_name'].upper()
    table_name = kwargs['table_name'].upper()
    oracle_conn_id = kwargs['oracle_conn_id']

    # Connect to Oracle
    db_conn = BaseHook.get_connection(oracle_conn_id)
    db_name = json.loads(db_conn.extra)['db_name']
    connection_string = f'{db_conn.login}/{db_conn.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={db_conn.host})(PORT={db_conn.port}))(CONNECT_DATA=(SID={db_name})))'
    oracle_conn = cx_Oracle.connect(connection_string)
    oracle_cursor = oracle_conn.cursor()

    stmt = ''' SELECT MAX(ora_rowscn) FROM {account_name.upper()}.{table_name.upper()} '''

    oracle_cursor.execute(stmt)
    result = oracle_cursor.fetchone()

    dag_id = '{}__{}'.format(oracle_account_name.lower(), table_name.lower())

    if not result:
        result = None
    else:
        result = result[0]

    ti.xcom_push(key=dag_id, value=result)

    return
