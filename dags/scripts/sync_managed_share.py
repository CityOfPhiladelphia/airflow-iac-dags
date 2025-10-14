from airflow.hooks.base_hook import BaseHook
import psycopg2
import psycopg2.extras
import json
import logging
import os
import uuid

def sync_managed_share(**kwargs):
    # Parse kwargs passed to us via op_kwargs in the dag_factory task definition
    source_schema = kwargs['source_schema']
    table_name = kwargs['table_name']
    table_schema = kwargs['table_schema']
    conn_id = kwargs['conn_id']
    managed_share = kwargs['managed_share']

    db_conn = BaseHook.get_connection(conn_id)
    conn_kwargs = {
        'user': db_conn.login,
        'password': db_conn.password,
        'host': db_conn.host,
        'port': db_conn.port,
        'database': json.loads(db_conn.extra)['db_name']
        }
    database = json.loads(db_conn.extra)['db_name']

    # Make db2 connection:
    conn = psycopg2.connect(**conn_kwargs)
    #cur = conn.cursor()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Attempt to split by commas in case it's more than one managed share
    # If there's no comma, it will just loop once.
    managed_shares = managed_share.split(',')

    for share in managed_shares: 
        dest_dept = share.replace('to_','')

        # confirm dest_dept is real
        stmt = f"SELECT 1 FROM pg_roles WHERE rolname='{dest_dept}'"
        cur.execute(stmt)
        exists = cur.fetchone()
        if not exists:
            raise AssertionError(f'{dest_dept} does not exist! Please correct the managed_share value.')
        if exists:
            print(f'User "{dest_dept}" exists.')

        # Make the managed share schema if it doesn't exist
        # Make it owned by postgres so nobody can mess with it 
        share_schema = f'{table_schema}_to_{dest_dept}'
        stmt = f'''
            CREATE SCHEMA IF NOT EXISTS {share_schema} AUTHORIZATION postgres;
            ALTER DEFAULT PRIVILEGES IN SCHEMA {share_schema} GRANT SELECT ON TABLES TO {dest_dept}; 
            GRANT USAGE ON SCHEMA {share_schema} to {dest_dept};
        '''
        cur.execute(stmt)
        conn.commit()

        # Just delete the table if it exists so we can just run the next statement that copies it efficiently
        print(f'Deleting {share_schema}.{table_name}..')
        del_stmt = f'DROP TABLE IF EXISTS {share_schema}.{table_name}'

        # Make table in schema if not exists

        # If the dataset is being pushed to these special schemas,
        # the table name will be prefixed with the dept name and a dunder.
        # Also note this copies all of the data too.
        print('Copying into table from source..')
        if source_schema == 'import' or source_schema == 'viewer': 
            stmt = f'CREATE TABLE IF NOT EXISTS {share_schema}.{table_name} AS TABLE {source_schema}.{table_schema}__{table_name};'
        else:
            stmt = f'CREATE TABLE IF NOT EXISTS {share_schema}.{table_name} AS TABLE {source_schema}.{table_name};'
        cur.execute(del_stmt)
        cur.execute(stmt)
        conn.commit()
        print('Copy done.')

        #####################
        # Registration stuff
        #####################
        # This next part is a a bit of a doozy. It "registers" the newly made table with
        # SDE if it's not registered which is necessary because otherwise when someone
        # attempts to add the data into a new map in Pro or ArcCatalog, it will ask them to
        # set up stuff such as the objectid and shape field and SRID. For the sake of
        # usability and friction, we don't want this.

        # Normally to register a table/feature class, you need a connection with a the "data owner"
        # of the schema, and then run a registration tool from Arc Pro or Desktop.
        # Our on-the-fly made schema does not have a data owner user.

        # So to trick SDE into thinking we registered it, what we'll do is copy the row
        # for the current enterprise item from the 'sde.gdb_items' system table,
        # update schema in each of the values that references it somewhere, and insert
        # it as a new row ith a new "UUID".

        # I also thought it might be necessary to duplicate information in these tables
        # but apparently it is not. Might cause unforeseen issues but I haven't seen any yet.
        # probably important if the tables needed to be edited, but these don't.
        # - sde.sde_column_registry
        # - sde.sde_layers
        # - sde.table_registry

        # FINAL NOTE: tables made in this way can't be deleted by sharee or sharer, which we like.
        # Delete as sde or postgres user instead if needed.

        # First check if the table is already registered
        stmt = f"SELECT name FROM sde.gdb_items WHERE name = '{database}.{share_schema}.{table_name}'"
        print(stmt)
        cur.execute(stmt)
        # If it's empty it means the managed_share is not registered yet.
        managed_registered = cur.fetchone()

        # Check if source is registered. If not, assume that's on purpose and don't register it.
        stmt = f"select name from sde.gdb_items where name = '{database}.{source_schema}.{table_schema}__{table_name}'"
        cur.execute(stmt)
        source_registered = cur.fetchall()

        if not source_registered:
            print('Source table is not registered, assuming this is on purpose. Not registering managed shared table.')
        elif source_registered and managed_registered:
            print(f"Managed share table '{database}.{share_schema}.{table_name}' is already registered.")
        elif not source_registered and managed_registered:
            raise AssertionError('Source is not registered but managed is?? This shouldnt be like this.')
        elif source_registered and not managed_registered:
            print(f"Registering managed share table '{database}.{share_schema}.{table_name}'..")
            # First let's handle sde.gdb_items
            # Get registration info from already registered source table:
            stmt = f"select * from sde.gdb_items where name = '{database}.{source_schema}.{table_schema}__{table_name}'"
            cur.execute(stmt)
            results = cur.fetchall()
            if not results:
                raise Exception('Error, source table doesnt appear to be registered??')
            if len(results) != 1:
                raise Exception('Error, should have only gotten one row back with this query!')
            reg_row = results[0]

            if reg_row['definition']:
                definition = reg_row['definition']
                # replace old fully qualified name with new one
                # e.g. databridge.schema.table --> databridge.dept_to_dept.table
                definition = definition.replace(f'{database}.{source_schema}.{table_schema}__{table_name}', f'{database}.{share_schema}.{table_name}')
                # Double up single quotes so they insert
                definition = definition.replace("'","''")
                reg_row['definition'] = definition
            else:
                reg_row['definition'] = 'NULL'

            if reg_row['iteminfo']:
                iteminfo = reg_row['iteminfo']
                # replace old fully qualified name with new one
                # e.g. databridge.schema.table --> databridge.dept_to_dept.table
                iteminfo = iteminfo.replace(f'{database}.{source_schema}.{table_schema}__{table_name}', f'{database}.{share_schema}.{table_name}')
                # Double up single quotes so they insert
                iteminfo = iteminfo.replace("'","''")
                reg_row['iteminfo'] = iteminfo
            else:
                reg_row['iteminfo'] = 'NULL'

            name = reg_row['name']
            name = name.replace(f'{database}.{source_schema}.{table_schema}__{table_name}', f'{database}.{share_schema}.{table_name}')
            reg_row['name'] = name
            
            path = reg_row['path']
            path = name.replace(f'{database}.{source_schema}.{table_schema}__{table_name}', f'{database}.{share_schema}.{table_name}')
            # necessary to insert a backslash that this field wants. Without it, we can't get the backslash inserted.
            path = f"E'\\\{path}'"
            reg_row['path'] = path

            # Why is physicalname upper case? *shrug
            # Note: if it's not uppercase, SDE doesn't think it's registered lol
            physicalname = reg_row['physicalname']
            physicalname = physicalname.replace(f'{database}.{source_schema}.{table_schema}__{table_name}'.upper(), f'{database}.{share_schema}.{table_name}'.upper())
            reg_row['physicalname'] = physicalname

            # The unique UUID of the dataset for column "uuid"
            item_uuid = uuid.uuid3(uuid.NAMESPACE_DNS, f'{share_schema}{table_name}')
            item_uuid = '{' + str(item_uuid).upper() + '}'
            reg_row['uuid'] = item_uuid


            # Set None to NULL
            for k,v in reg_row.items():
                if v is None:
                    reg_row[k] = 'NULL'
            # if it's a string and not NULL, surround with quotes. Makes the below insert query easier to craft.
            # Avoid "path" because we have to do something really wierd to get a backslash into it.
            for k,v in reg_row.items():
                if isinstance(v, str) and v != 'NULL' and k != 'path':
                    reg_row[k] = f"'{v}'"

            # make an insert statement with our old and new values
            reg_stmt = f'''
                INSERT INTO {database}.sde.gdb_items
                    (uuid, type, name, physicalname, path, url, properties, defaults, datasetsubtype1,
                    datasetsubtype2, datasetinfo1, datasetinfo2, contingentvalues, definition,
                    documentation, iteminfo, shape, objectid)
                    VALUES
                    (
                    {reg_row['uuid']}, {reg_row['type']}, {reg_row['name']}, {reg_row['physicalname']}, {reg_row['path']},
                    {reg_row['url']}, {reg_row['properties']}, {reg_row['defaults']}, {reg_row['datasetsubtype1']},
                    {reg_row['datasetsubtype2']}, {reg_row['datasetinfo1']}, {reg_row['datasetinfo2']},
                    {reg_row['contingentvalues']}, {reg_row['definition']}, {reg_row['documentation']},
                    {reg_row['iteminfo']}, {reg_row['shape']}, sde.next_rowid('sde', 'gdb_items')
                    );
            '''
            print(reg_stmt)
            cur.execute(reg_stmt)
            conn.commit()

    conn.close()

