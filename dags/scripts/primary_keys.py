from airflow.hooks.base_hook import BaseHook
import sqlalchemy as sa
import sys
from scripts.utils import create_engine_from_airflow_connection, print_sa_stmt


def set_primary_keys(conn_id: str, schema: str, dag_config, **context): 
    '''Set the (possibly compound) primary key of a table from the provided yaml_config file.
    
    Exit gracefully if the primary key already exists.
    #### Paramaters
    - `conn_id`: The name of the connection to use, which must already exist 
    in Airflow
    - `schema`: Name of the schema where the table to be altered resides
    - `dag_config` _DagConfig_: Instance of the DagConfig class that contains all 
    of the information about the yaml config file
    #### Exceptions
    - `AssertionError` - If a primary key already exists on the table that is different 
    from the fields specified in the yaml_config file
    - `AssertionError` - If a value provided for the primary key is not an 
    existing column name
    '''
    db_conn = BaseHook.get_connection(conn_id)
    engine = create_engine_from_airflow_connection(db_conn)        
    metadata = sa.MetaData()
    table_name = dag_config['table_name']
    print(f'{schema = }, {table_name = }')
    table = sa.Table(table_name, metadata, schema=schema, autoload_with=engine)
    
    existing_keys = [col.name for col in table.primary_key]
    primary_keys = dag_config['primary_keys']
    print(f'Existing primary keys for {schema}.{table_name}: {existing_keys}')
    if existing_keys: 
        assert existing_keys == primary_keys, f"Existing keys {existing_keys} do not match specified primary keys {primary_keys}"
        print(f"Existing keys {existing_keys} match specified primary keys {primary_keys}")
        sys.exit(0)

    # Because SQLAlchemy doesn't have a way to add a pk after a table has been created, 
    # the statement must be run as (insecure) text
    # Use this to hopefully prevent any SQL injection
    for key in primary_keys: 
        assert key in table.columns.keys(), f'Key "{key}" does not exist in {schema}.{table_name} columns: {table.columns.keys()}'
    
    keys = ', '.join(primary_keys)
    with engine.begin() as conn: 
        print('Adding primary key')
        stmt = f'ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {table_name}_pk PRIMARY KEY ({keys})'
        sa_stmt = sa.text(stmt)
        print_sa_stmt(sa_stmt)
        conn.execute(sa.text(stmt))
    
    print('Success')
