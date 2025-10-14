from airflow.hooks.base_hook import BaseHook
import os,sys
import json
import logging
import psycopg2
import cx_Oracle
import re

def sync_indexes_from_oracle(**kwargs):

    logging.info("DEBUG kwargs: " + str(kwargs))
    postgres_account_name = kwargs['postgres_account_name'].upper()
    oracle_account_name = kwargs['oracle_account_name'].upper()
    table_name = kwargs['table_name'].upper()
    oracle_conn_id = kwargs['oracle_conn_id']
    postgres_conn_id = kwargs['postgres_conn_id']
    enterprise_schema = kwargs['enterprise_schema']

    # Connect to Oracle
    db_conn = BaseHook.get_connection(oracle_conn_id)
    db_name = json.loads(db_conn.extra)['db_name']
    connection_string = f'{db_conn.login}/{db_conn.password}@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={db_conn.host})(PORT={db_conn.port}))(CONNECT_DATA=(SID={db_name})))'
    oracle_conn = cx_Oracle.connect(connection_string)
    oracle_cursor = oracle_conn.cursor()


    # Connect to Postgres
    db_conn = BaseHook.get_connection(postgres_conn_id)
    conn_kwargs = {
        'user': db_conn.login,
        'password': db_conn.password,
        'host': db_conn.host,
        'port': db_conn.port,
        'database': json.loads(db_conn.extra)['db_name']
        }
    postgres_conn = psycopg2.connect(**conn_kwargs)
    postgres_cursor = postgres_conn.cursor()


    # Get all of the indices for this table, minus SDE nonsense
    index_stmt = f'''
    SELECT INDEX_NAME FROM all_indexes
    WHERE table_owner = '{oracle_account_name.upper()}'
    AND table_name = '{table_name.upper()}'
    '''

    oracle_cursor.execute(index_stmt)
    results = oracle_cursor.fetchmany()

    # All the indexes for this table.
    indices = [i[0] for i in results]


    if indices:
        for index_name in indices:
            ignore = ['ROWID', 'SYS_', 'SDE_', '_SDE', 'UUID', 'GDB_']
            if index_name in ignore:
                print(f'Ignoring {index_name}..')
                continue

            index_ddl_stmt = f'''
            SELECT dbms_metadata.get_ddl('INDEX', index_name, owner) FROM all_indexes
            WHERE owner in ('{oracle_account_name}') AND index_name IN ('{index_name}')
            '''
            oracle_cursor.execute(index_ddl_stmt)
            # comes in as a LOB, read into a string.
            result = oracle_cursor.fetchone()[0].read()

            # The first line is the actual index create statement, everything after is oracle specific junk
            index_ddl = result.split('\n')[1]
            print(f'Oracle original index: {index_ddl}')

            # Find the ON portion of the DDL to index ourselves
            after_on = index_ddl.split().index('ON')

            # Assert we actually have an index DDL.
            assert ('CREATE' in index_ddl.split())

            # Is it unique?
            unique = 'UNIQUE' in index_ddl.split()

            # Get everything after the ON (either index fields or a function)
            index_fields = index_ddl.split()[after_on+2:]
            index_fields = [i.lower() for i in index_fields]
            print(f'index_fields: {index_fields}')
            # If the only index field is a shape or objectid, then we can just ignore this, it's a default index
            # made by SDE.
            if 'shape' in index_fields[0] and len(index_fields) == 1:
                print(f'Ignoring {index_name}..')
                continue
            if 'objectid' in index_fields[0] and len(index_fields) == 1:
                print(f'Ignoring {index_name}..')
                continue

            if 'trunc' in index_fields[0]:
                # Get the index field we're truncating.
                trunc_field = re.search(r'"(.*?)"', index_fields[0]).group(1)
                # Format it for postgres
                # If it's not a bare trunc with an unspecificed 'fmt'
                # then this will likely fail and we'll figure it out then.
                # A trunc without a 'fmt' arg defaults to 'day'.
                # (ref: https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions201.htm)
                trunc_function = f"(date_trunc('day', {trunc_field}))"
                index_fields = None
            else:
                trunc_function = None

            if unique:
                db2_index = f'create unique index if not exists {index_name.lower()} on "{enterprise_schema}"."{table_name.lower()}" '
            else:
                db2_index = f'create index if not exists {index_name.lower()} on "{enterprise_schema}"."{table_name.lower()}" '

            if trunc_function:
                db2_index = db2_index + trunc_function
            elif index_fields:
                db2_index = db2_index + ''.join(index_fields)

            logging.info('Creating index: ' + db2_index)

            postgres_cursor.execute(db2_index)
            postgres_cursor.execute('COMMIT')
            print('')
    else:
        logging.info('No indices to make..')
    

    return
