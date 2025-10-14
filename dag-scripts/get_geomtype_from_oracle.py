from airflow.hooks.base_hook import BaseHook
import cx_Oracle
import json
import logging
import sys

def get_geomtype_from_oracle(ti, **kwargs):

    logging.info("update_oracle_scn DEBUG kwargs: " + str(kwargs))
    # Parse kwargs passed to us via op_kwargs in the dag_factory task definition
    account_name = kwargs['account_name'].upper()
    table_name = kwargs['table_name'].upper()
    conn_id = kwargs['conn_id']
    xcom_task_id_key = kwargs['xcom_task_id_key'] + 'geomtype'


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

    is_geom_stmt = f'''
        SELECT GEOMETRY_TYPE FROM SDE.ALL_ST_GEOMETRY_COLUMNS_V
            WHERE OWNER = '{account_name}' AND
            TABLE_NAME = '{table_name}'
            '''
    logging.info('Executing is_geom_stmt: ' + str(is_geom_stmt))
    cursor.execute(is_geom_stmt)
    result = cursor.fetchone()
    if result is None:
        return

    geometric = result[0] 
    if geometric != 'ST_GEOMETRY':

        geom_type_stmt = f'''
            SELECT distinct st_geometrytype(shape) AS geom_type
            FROM {account_name}.{table_name}
            WHERE st_isempty(shape) != 1
            '''
        logging.info('Executing geom_type_stmt: ' + str(is_geom_stmt))
        cursor.execute(geom_type_stmt)
        geomtype = cursor.fetchone()[0]
        assert geomtype

        ti.xcom_push(key=xcom_task_id_key, value=geomtype)
        return

