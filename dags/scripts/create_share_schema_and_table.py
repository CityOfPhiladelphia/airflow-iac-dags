from airflow.hooks.base_hook import BaseHook
import psycopg2
import json
import logging
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

    logging.info("creating_staging_from_sde DEBUG kwargs: " + str(kwargs))
    # Parse kwargs passed to us via op_kwargs in the dag_factory task definition
    table_schema = kwargs['table_schema']
    table_name = kwargs['table_name']
    conn_id = kwargs['conn_id']
    staging_schema = 'etl_staging'
    xcom_task_id_key = kwargs['xcom_task_id_key']
    source_schema = kwargs['source_schema']
    enterprise_table_name = '__'.join([table_schema, table_name])


    db_conn = BaseHook.get_connection(conn_id)
    database =  json.loads(db_conn.extra)['db_name']
    logging.info(f'Logging in to {db_conn.host}, database {database} as user {db_conn.login}')
    conn_kwargs = {
        'user': db_conn.login,
        'password': db_conn.password,
        'host': db_conn.host,
        'port': db_conn.port,
        'database': database
        }


    #test_table_schema = 'oem'
    #test_table_name = 'high_rise_office_bldgs_pt'

    # SDE specific columns we don'twant
    #ignore_field_name = [
    #    'gdb_geomattr_data',
    #    'objectid'
    #]
    ignore_field_name = []

    # use this to transform specific to more general data types for staging table
    data_type_map = {
        'character varying': 'text'
    }

    # use this if there are any corrupted srid's from copying SDE
    srid_map = {}

    # Make db2 connection:
    # update to read from Airflow conn_id
    #params = get_dsn('betabridge')
    conn = psycopg2.connect(**conn_kwargs)
    conn.autocommit = True
    cur = conn.cursor()

    def check_source_table_exists():
        exist_stmt = f'''
            SELECT EXISTS (
                SELECT FROM
                    pg_tables
                WHERE
                    schemaname = '{source_schema}' AND
                    tablename  = '{enterprise_table_name}'
                );
                '''
        logging.info('Running exist_stmt: ' + exist_stmt)
        cur.execute(exist_stmt)
        result = cur.fetchone()[0]
        if not result:
            raise AssertionError(f'Source table doesnt exist?: {source_schema + "." + enterprise_table_name}')


    def get_table_column_info_from_source(schema_name, table_name):
        """Queries the information_schema.columns table to get column names and data types"""

        table_name = schema_name + '__' + table_name
        # Override for now, our SDE tables are here:
        col_info_stmt = f'''
        SELECT column_name, data_type 
        FROM information_schema.columns
        WHERE table_schema = '{source_schema}' and table_name = '{table_name}'
        '''
        logging.info('Running col_info_stmt: ' + col_info_stmt)
        cur.execute(col_info_stmt)
        # Format and transform data types:
        column_info = {i[0]: data_type_map.get(i[1], i[1]) for i in cur.fetchall()}

        logging.info(f'DEBUG! column_info: {column_info}')

        # Metadata column added into postgres tables by arc programs, not needed.
        if 'gdb_geomattr_data' in column_info.keys():
            column_info.pop('gdb_geomattr_data')

        # If the table doesn't exist, the above query silently fails.
        assert column_info
        return column_info


    def get_geom_column_info(schema_name, table_name):
        """Queries the geometry_columns table to geom field and srid, then queries the sde table to get geom_type"""
        get_column_name_and_srid_stmt = f'''
            select f_geometry_column, srid from geometry_columns
            where f_table_schema = '{source_schema}' and f_table_name = '{enterprise_table_name}'
        '''
        cur = conn.cursor()
        # Identify the geometry column values
        logging.info('Running get_column_name_and_srid_stmt' + get_column_name_and_srid_stmt)
        cur.execute(get_column_name_and_srid_stmt)

        #col_name = cur.fetchall()[0]
        col_name1 = cur.fetchall()
    
        # If the result is empty, there is no shape field and this is a table.
        # return empty dict that will evaluate to False.
        if not col_name1: 
            return {}
        logging.info(f'Got shape field and SRID back as: {col_name1}')

        col_name = col_name1[0]
        # Grab the column names
        header = [h.name for h in cur.description]
        # zip column names with the values
        geom_column_and_srid = dict(zip(header, list(col_name)))
        geom_column = geom_column_and_srid['f_geometry_column']
        srid = geom_column_and_srid['srid']

        # Get the type of geometry, e.g. point, line, polygon.. etc.
        # docs on this SDE function: https://desktop.arcgis.com/en/arcmap/latest/manage-data/using-sql-with-gdbs/st-geometrytype.htm
        # NOTE!: if phl is empty, e.g. this is a first run, this will fail, as a backup get the value from XCOM
        # Which will be populated by our "get_geomtype" task.
        geom_type_stmt = f'''
            select public.st_geometrytype({geom_column}) as geom_type 
            from {source_schema}.{enterprise_table_name}
            where st_isempty({geom_column}) is False
            limit 1
        '''
        logging.info('Running geom_type_stmt: ' + geom_type_stmt)
        cur.execute(geom_type_stmt)
        result = cur.fetchone()
        if result is None:
            geom_type_stmt = f'''
            select geometry_type('{source_schema}', '{enterprise_table_name}', '{geom_column}')
            '''
            logging.info('Running geom_type_stmt: ' + geom_type_stmt)
            cur.execute(geom_type_stmt)
            geom_type = cur.fetchone()[0]

            #geom_type = ti.xcom_pull(key=xcom_task_id_key + 'geomtype')
            assert geom_type
            #logging.info(f'Got our geom_type from xcom: {geom_type}') 
        else:
            geom_type = result[0]
        return {'geom_field': geom_column, 'geom_type': geom_type, 'srid': srid}


    def generate_ddl(schema_name, table_name, column_info, geom_info):
        """Builds the DDL based on the table's generic and geom column info"""
        # If we have a geometry column..
        if geom_info:
            column_type_map = [f'{k} {v}' for k,v in column_info.items() if k not in ignore_field_name and k != geom_info['geom_field']]
            srid = geom_info['srid']
            geom_column = geom_info['geom_field']
            # Think postgres calls for something like 'Point' vs 'POINT' ?
            geom_type = geom_info['geom_type'].replace('ST_', '').capitalize()
            geom_column_string = f'{geom_column} geometry({geom_type}, {srid})'
            column_type_map.append(geom_column_string)
            column_type_map_string = ', '.join(column_type_map)
        # If this is a table with no geometry column..
        else:
            column_type_map = [f'{k} {v}' for k,v in column_info.items() if k not in ignore_field_name]
            column_type_map_string = ', '.join(column_type_map)

        logging.info('DEBUG!!: ' + str(column_info))

        assert column_type_map_string

        ddl = f'''CREATE TABLE {staging_schema}.{enterprise_table_name}
            ({column_type_map_string})'''
        return ddl


    def run_ddl(ddl, schema_name, table_name):
        drop_stmt = f'DROP TABLE IF EXISTS {staging_schema}.{enterprise_table_name}'
        cur = conn.cursor()
        # drop first so we have a total refresh
        logging.info('Running drop stmt: ' + drop_stmt)
        cur.execute(drop_stmt)
        # Identify the geometry column values
        logging.info('Running ddl stmt: ' + ddl)
        cur.execute(ddl)
        # Make sure we were successful
        try:
            check_stmt = f'''
                SELECT EXISTS
                    (SELECT FROM pg_tables
                    WHERE schemaname = \'{staging_schema}\'
                    AND tablename = \'{enterprise_table_name}\');
                    '''
            logging.info('Running check_stmt: ' + check_stmt)
            cur.execute(check_stmt)
            return_val = str(cur.fetchone()[0])
            assert (return_val == 'True' or return_val == 'False')
            if return_val == 'False':
                raise Exception('Table does not appear to have been created!')
            if return_val != 'True':
                raise Exception('This value from the check_stmt query is unexpected: ' + return_val)
            if return_val == 'True':
                logging.info(f'Table "{staging_schema}.{enterprise_table_name}" created successfully.')
        except Exception as e:
            raise Exception("DEBUG: " + str(e) + " RETURN: " + str(return_val) + " DDL: " + ddl + " check query" + check_stmt)

    check_source_table_exists()

    column_info = get_table_column_info_from_source(table_schema, table_name)
    geom_info = get_geom_column_info(table_schema, table_name)
    ddl = generate_ddl(table_schema, table_name, column_info, geom_info)
    run_ddl(ddl, table_schema, table_name)

    #ti.xcom_push(key='testing', value='hi')
    #return 0
