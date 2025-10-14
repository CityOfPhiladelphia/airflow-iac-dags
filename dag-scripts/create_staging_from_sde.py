from airflow.hooks.base_hook import BaseHook
import psycopg2
import json
import logging
import re
from pprint import pprint
#from config import get_dsn # update to read from Airflow conn_id

'''
This operates entirely in postgres databridge v2 and assumes the "enterprise" table already exists.
This will create a "staging" table based off what exists in the "enterprise" table.
1. Get our table columns from Oracle in get_table_column_info 
2. Get our geom type and SRID
3. Generate the table DDL.
4. Drop the table if it exists then recreate with our DDL.
'''
def create_staging_from_sde(ti, **kwargs):

    print("creating_staging_from_sde DEBUG kwargs: " + str(kwargs))
    # Parse kwargs passed to us via op_kwargs in the dag_factory task definition
    enterprise_schema = kwargs['enterprise_schema']
    table_schema = kwargs['table_schema']
    table_name = kwargs['table_name']
    conn_id = kwargs['conn_id']
    staging_schema = 'etl_staging'
    # entreprise in this context means destination schema/table
    # We should probably rename it at some point
    if enterprise_schema == 'viewer' or enterprise_schema == 'import':
        enterprise_table_name = '__'.join([table_schema, table_name])
    else:
        # If it's not either of the prior schemas, the table_name is just the table_name.
        enterprise_table_name = table_name

    staging_table_name = '__'.join([table_schema, table_name])

    data_type_map = {'character varying': 'text'}
    ddl = None
    column_info = None
    geom_info = None
    ignore_field_name = []

    db_conn = BaseHook.get_connection(conn_id)
    database =  json.loads(db_conn.extra)['db_name']
    print(f'Logging in to {db_conn.host}, database {database} as user {db_conn.login}')
    conn_kwargs = {
        'user': db_conn.login,
        'password': db_conn.password,
        'host': db_conn.host,
        'port': db_conn.port,
        'database': database
        }
    conn = psycopg2.connect(**conn_kwargs)
    conn.autocommit = True
    cur = conn.cursor()

    def confirm_table_existence():
        exist_stmt = f"SELECT to_regclass('{enterprise_schema}.{enterprise_table_name}');"
        print(f'Table exists statement: {exist_stmt}')
        cur.execute(exist_stmt)
        table_exists_check = cur.fetchone()[0]
        assert table_exists_check

    def get_table_column_info_from_enterprise():
        """Queries the information_schema.columns table to get column names and data types"""

        col_info_stmt = f'''
            SELECT column_name, data_type 
            FROM information_schema.columns
            WHERE table_schema = '{enterprise_schema}' and table_name = '{enterprise_table_name}'
        '''
        print('Running col_info_stmt: ' + col_info_stmt)
        cur.execute(col_info_stmt)

        # Format and transform data types:
        nonlocal column_info
        column_info = {i[0]: data_type_map.get(i[1], i[1]) for i in cur.fetchall()}

        print(f'column_info: {column_info}')

        # Metadata column added into postgres tables by arc programs, not needed.
        if 'gdb_geomattr_data' in column_info.keys():
            column_info.pop('gdb_geomattr_data')

        # If the table doesn't exist, the above query silently fails.
        assert column_info
        column_info = column_info
        return column_info

    def get_geom_column_info():
        """Queries the geometry_columns table to geom field and srid, then queries the sde table to get geom_type"""

        get_column_name_and_srid_stmt = f'''
            select f_geometry_column, srid from geometry_columns
            where f_table_schema = '{enterprise_schema}' and f_table_name = '{enterprise_table_name}'
        '''
        # Identify the geometry column values
        print('Running get_column_name_and_srid_stmt' + get_column_name_and_srid_stmt)
        cur.execute(get_column_name_and_srid_stmt)

        #col_name = cur.fetchall()[0]
        col_name1 = cur.fetchall()

        # If the result is empty, there is no shape field and this is a table.
        # return empty dict that will evaluate to False.
        if not col_name1:
            return {}
        print(f'Got shape field and SRID back as: {col_name1}')

        col_name = col_name1[0]
        # Grab the column names
        header = [h.name for h in cur.description]
        # zip column names with the values
        geom_column_and_srid = dict(zip(header, list(col_name)))
        geom_column = geom_column_and_srid['f_geometry_column']
        srid = geom_column_and_srid['srid']

        # Try to get the type of geometry in various ways, e.g. point, line, polygon.. etc.
        # docs on this SDE function: https://desktop.arcgis.com/en/arcmap/latest/manage-data/using-sql-with-gdbs/st-geometrytype.htm
        geom_type_stmt = f'''
            select public.st_geometrytype({geom_column}) as geom_type
            from {enterprise_schema}.{enterprise_table_name}
            where st_isempty({geom_column}) is False
            limit 1
        '''
        print('Running geom_type_stmt: ' + geom_type_stmt)
        cur.execute(geom_type_stmt)
        result = cur.fetchone()
        if not result:
            # If none, next try selecting from the source department table
            geom_type_stmt_2 = f'''
                select public.st_geometrytype({geom_column}) as geom_type
                from {table_schema}.{table_name}
                where st_isempty({geom_column}) is False
                limit 1
            '''
            print('Running geom_type_stmt_2: ' + geom_type_stmt_2)
            cur.execute(geom_type_stmt_2)
            result = cur.fetchone()
            # If we still don't have a result, finally try the postgis function to figure it out.
            if not result:
                geom_type_stmt_3 = f'''
                select geometry_type('{enterprise_schema}', '{enterprise_table_name}', '{geom_column}')
                '''
                print('Running geom_type_stmt_3: ' + geom_type_stmt_3)
                cur.execute(geom_type_stmt_3)
                result = cur.fetchone()
                # If we get to this point, this means we're in an odd situation where we have
                # an empty unregistered table (probably just made) and we got back every shape possible.
                # Simply try defaulting to multipolygon in hopes that it works.
                if isinstance(result, list):
                    result = result[0]
                if result == 'POINT LINESTRING POLYGON MULTIPOINT MULTILINESTRING MULTIPOLYGON':
                    result = 'MULTIPOLYGON'

        print(f'Final geom_type result: {result}')
        assert len(result) < 20, 'Geom type result is too long! Bogus result?'

        if isinstance(result, str):
            geom_type = result.replace('ST_', '').capitalize()
        else:
            geom_type = result[0].replace('ST_', '').capitalize()
        assert geom_type
        print(f'Formatted geom_type: {geom_type}')
            

        print('Checking for z or m values in sde.gdb_items.definition column..')
        # Assume innocence until proven guilty
        m = False
        z = False
        has_m_or_z_stmt = f'''
        SELECT definition FROM sde.gdb_items
        WHERE name = 'databridge.{table_schema}.{table_name}'
        '''
        print('Running has_m_or_z_stmt: ' + has_m_or_z_stmt)
        cur.execute(has_m_or_z_stmt)
        result = cur.fetchone()

        if not result:
            print('No XML file found sde.gdb_items definition col.')
        else:
            xml_def = result[0]
            if not xml_def:
                print('No XML file found sde.gdb_items definition col.')
            else:
                m_search = re.search("<HasM>\D*<\/HasM>", xml_def)
                if not m_search:
                    print('No <HasM> element found in xml definition, assuming False.')
                else:
                    if 'true' in m_search[0]:
                        print(m_search[0])
                        m = True
                z_search = re.search("<HasZ>\D*<\/HasZ>", xml_def)
                if not z_search:
                    print('No <HasZ> element found in xml definition, assuming False.')
                else:
                    if 'true' in z_search[0]:
                        print(z_search[0])
                        z = True

        # This will ultimately be the data type we create the table with,
        # example data type: 'shape geometry(MultipolygonZ, 2272)
        if m:
            geom_type = geom_type + 'M'
        if z:
            geom_type = geom_type + 'Z'
        # Except on this because we don't support these values
        if m or z:
            print('Error! Dataset is set to have an M or Z value in its geometry! This is unsupported, please re-make the dataset or talk to CityGeo.')
            raise Exception('Error! Dataset is set to have an M or Z value in its geometry! This is unsupported, please re-make the dataset or talk to CityGeo.')


        nonlocal geom_info

        if geom_type.lower() == 'point linestring polygon multipoint multilinestring multipolygon':
            print(f'Warning! Got a nonsense value returned for geom type. Defaulting to multipolygon.')
            geom_type = 'multipolygon'
        geom_info = {'geom_field': geom_column,
                          'geom_type': geom_type,
                          'srid': srid}
        print(f'geom_info: {geom_info}')

        #return {'geom_field': geom_column, 'geom_type': geom_type, 'srid': srid}

    def generate_ddl():
        """Builds the DDL based on the table's generic and geom column info"""
        # If geom_info is not None
        if geom_info:
            column_type_map = [f'{k} {v}' for k,v in column_info.items() if k not in ignore_field_name and k != geom_info['geom_field']]
            srid = geom_info['srid']
            geom_column = geom_info['geom_field']

            geom_type = geom_info['geom_type']
            geom_column_string = f'{geom_column} geometry({geom_type}, {srid})'
            column_type_map.append(geom_column_string)
            column_type_map_string = ', '.join(column_type_map)
        # If this is a table with no geometry column..
        else:
            column_type_map = [f'{k} {v}' for k,v in column_info.items() if k not in ignore_field_name]
            column_type_map_string = ', '.join(column_type_map)

        #print('DEBUG!!: ' + str(column_info))

        assert column_type_map_string

        nonlocal ddl
        ddl = f'''CREATE TABLE {staging_schema}.{staging_table_name}
            ({column_type_map_string})'''
        ddl = ddl


    def run_ddl():
        drop_stmt = f'DROP TABLE IF EXISTS {staging_schema}.{staging_table_name}'
        # drop first so we have a total refresh
        print('Running drop stmt: ' + drop_stmt)
        cur.execute(drop_stmt)
        cur.execute('COMMIT')
        # Identify the geometry column values
        print('Running ddl stmt: ' + ddl)
        cur.execute(ddl)
        cur.execute('COMMIT')
        # Make sure we were successful
        try:
            check_stmt = f'''
                SELECT EXISTS
                    (SELECT FROM pg_tables
                    WHERE schemaname = \'{staging_schema}\'
                    AND tablename = \'{staging_table_name}\');
                    '''
            print('Running check_stmt: ' + check_stmt)
            cur.execute(check_stmt)
            return_val = str(cur.fetchone()[0])
            assert (return_val == 'True' or return_val == 'False')
            if return_val == 'False':
                raise Exception('Table does not appear to have been created!')
            if return_val != 'True':
                raise Exception('This value from the check_stmt query is unexpected: ' + return_val)
            if return_val == 'True':
                print(f'Table "{staging_schema}.{staging_table_name}" created successfully.')
        except Exception as e:
            raise Exception("DEBUG: " + str(e) + " RETURN: " + str(return_val) + " DDL: " + ddl + " check query" + check_stmt)

    def register_table():
        """
        This next part is a a bit of a doozy. It "registers" the newly made table with
        SDE if it's not registered which is necessary because otherwise when someone
        attempts to add the data into a new map in Pro or ArcCatalog, it will ask them to
        set up stuff such as the objectid and shape field and SRID. For the sake of
        usability and friction, we don't want this.

        Normally to register a table/feature class, you need a connection with a the "data owner"
        of the schema, and then run a registration tool from Arc Pro or Desktop.
        Our on-the-fly made schema does not have a data owner user.

        So to trick SDE into thinking we registered it, what we'll do is copy the row
        for the current enterprise item from the 'sde.gdb_items' system table,
        update schema in each of the values that references it somewhere, and insert
        it as a new row ith a new "UUID".

        I also thought it might be necessary to duplicate information in these tables
        but apparently it is not. Might cause unforeseen issues but I haven't seen any yet.
        probably important if the tables needed to be edited, but these don't.
        - sde.sde_column_registry
        - sde.sde_layers
        - sde.table_registry

        FINAL NOTE: tables made in this way can't be deleted by sharee or sharer, which we like.
        Delete as sde or postgres user instead if needed.

        first check if the table is already registered
        """
        raise NotImplementedError('Dont use this, but keeping it around in the future in case its useful.')
        stmt = f"SELECT name FROM sde.gdb_items WHERE name = '{database}.{share_schema}.{table_name}'"
        print(stmt)
        cur.execute(stmt)
        registered = cur.fetchone()

        # empty list means it's not registered yet, let's register it.
        if not registered:
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


    confirm_table_existence()
    get_table_column_info_from_enterprise()
    get_geom_column_info()
    generate_ddl()
    run_ddl()


