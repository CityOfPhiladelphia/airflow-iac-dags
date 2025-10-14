from airflow.hooks.base_hook import BaseHook
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from scripts.utils import create_engine_from_airflow_connection, print_sa_stmt

# This function requires sqlalchemy<2.0.0 because of a dependency in apache-airflow
# To refactor this function to use sqlalchemy>=2.0.0, replace `sa.engine` with `sa`

DEFAULT_DEL_TRANS = 1
DEFAULT_INS_TRANS = 1
DEFAULT_UPD_TRANS = 3


def create_audit_table(conn_id: str, dag_config, **context): 
    '''Create audit table and relevant indices if it does not already exist
    #### Paramaters
    - `conn_id`: The name of the connection to use, which must already exist 
    in Airflow
    - `dag_config` _DagConfig_: Instance of the DagConfig class that contains all 
    of the information from the yaml config file
    '''
    db_conn = BaseHook.get_connection(conn_id)
    engine = create_engine_from_airflow_connection(db_conn)      
    metadata = sa.MetaData()
    audit_table_name = f"{dag_config['table_name']}_history"
    audit_table = sa.Table(
        audit_table_name, metadata, 
        sa.Column('tabname', sa.String(), nullable=False),
        sa.Column('schemaname', sa.String(), nullable=False),
        sa.Column('primary_keys', postgresql.HSTORE(), nullable=False, index=True), 
        sa.Column('operation', sa.String(), nullable=False),
        sa.Column('new_val', postgresql.JSONB()),
        sa.Column('old_val', postgresql.JSONB()),
        sa.Column('updated_cols', postgresql.ARRAY(sa.String, dimensions=1)),
        sa.Column('etl_modified_timestamp', sa.TIMESTAMP(timezone=True), nullable=False, index=True), 
        schema=f"{dag_config['source_schema']}")
    metadata.create_all(bind=engine)
    print(f'CREATE IF NOT EXISTS {audit_table.schema}.{audit_table.name} statement executed')


def create_audit_trigger(conn_id: str, schema: str, dag_config, **context): 
    '''Create an audit_trigger on a table connected to a specified function
    #### Parameters
    - `conn_id`: The name of the connection to use, which must already exist 
    in Airflow
    - `schema`: Name of the schema where the table to be altered resides
    - `dag_config` _DagConfig_: Instance of the DagConfig class that contains all 
    of the information from the yaml config file
    '''
    db_conn = BaseHook.get_connection(conn_id)
    engine = create_engine_from_airflow_connection(db_conn)        
    table_name = dag_config['table_name']
    with engine.begin() as conn: 
        print('Running create trigger statement:')
        stmt = sa.text(f'''
            CREATE OR REPLACE TRIGGER audit_trigger AFTER INSERT OR DELETE OR UPDATE ON "{schema}"."{table_name}" FOR EACH ROW EXECUTE FUNCTION audit.audit_row_history()
        ''')
        print_sa_stmt(stmt)
        conn.execute(stmt)


def prune_audit_table(conn_id: str, dag_config, **context): 
    '''Prune the audit table using either the CityGeo standard algorithm or the 
    most recent number of user-specified transactions.
    
    The CityGeo standard algorithm leaves in place the last INSERT, last DELETE, and 
    last 3 UPDATE transactions per record. Otherwise, the user specifies the last
    X number of transactions to maintain per record. 

    #### Paramaters
    - `conn_id`: The name of the connection to use, which must already exist 
    in Airflow
    - `dag_config` _DagConfig_: Instance of the DagConfig class that contains all 
    of the information from the yaml config file
    '''
    db_conn = BaseHook.get_connection(conn_id)
    engine = create_engine_from_airflow_connection(db_conn)      
    metadata = sa.MetaData()
    audit_table_name = f"{dag_config['table_name']}_history"
    audit_table = sa.Table(audit_table_name, metadata, schema=f"{dag_config['source_schema']}", autoload_with=engine)
    print(f'audit_table = {audit_table.schema}.{audit_table.name}')
    
    if not dag_config['history_duration']: 
        print('Pruning using standard CityGeo Algorithm')
        print(f'Maintaining most recent {DEFAULT_DEL_TRANS} DELETE, {DEFAULT_INS_TRANS} INSERT, and {DEFAULT_UPD_TRANS} UPDATE transaction(s) per record')
        dr_subq = (
            sa.select(
                sa.func.dense_rank()
                    .over(
                        partition_by=[audit_table.c.primary_keys, audit_table.c.operation], 
                        order_by=sa.desc(audit_table.c.etl_modified_timestamp))
                    .label('dr'), 
                audit_table)
            .order_by(
                audit_table.c.primary_keys, audit_table.c.operation, sa.desc(audit_table.c.etl_modified_timestamp))
            .subquery('dr'))

        del_subq = (
            sa.select(
                dr_subq.c.primary_keys, dr_subq.c.operation, 
                dr_subq.c.etl_modified_timestamp)
                .where(sa.or_(
                    sa.and_(dr_subq.c.operation == 'DELETE', dr_subq.c.dr > DEFAULT_DEL_TRANS), 
                    sa.and_(dr_subq.c.operation == 'INSERT', dr_subq.c.dr > DEFAULT_INS_TRANS), 
                    sa.and_(dr_subq.c.operation == 'UPDATE', dr_subq.c.dr > DEFAULT_UPD_TRANS), 
            ))
            .subquery('del'))

        stmt = (sa.delete(audit_table)
                .where(
                    audit_table.c.primary_keys == del_subq.c.primary_keys, 
                    audit_table.c.operation == del_subq.c.operation, 
                    audit_table.c.etl_modified_timestamp == del_subq.c.etl_modified_timestamp))
        stmt = stmt.compile(dialect = postgresql.dialect()) # Make it a postgresql specific correlated delete

    else: 
        history_duration = int(dag_config['history_duration'])
        print('Pruning using user-defined most recent transactions')
        print(f'Maintaining most recent {history_duration} transaction(s) per record')
        dr_subq = (
            sa.select(
                sa.func.dense_rank()
                    .over(
                        partition_by=[audit_table.c.primary_keys], 
                        order_by=sa.desc(audit_table.c.etl_modified_timestamp))
                    .label('dr'), 
                audit_table)
            .order_by(
                audit_table.c.primary_keys, sa.desc(audit_table.c.etl_modified_timestamp))
            .subquery('dr'))

        del_subq = (
            sa.select(
                dr_subq.c.primary_keys, dr_subq.c.operation, 
                dr_subq.c.etl_modified_timestamp)
                .where(dr_subq.c.dr > history_duration)
            .subquery('del'))

        stmt = (sa.delete(audit_table)
                .where(
                    audit_table.c.primary_keys == del_subq.c.primary_keys, 
                    audit_table.c.operation == del_subq.c.operation, 
                    audit_table.c.etl_modified_timestamp == del_subq.c.etl_modified_timestamp))
        stmt = stmt.compile(dialect = postgresql.dialect()) # Make it a postgresql specific correlated delete

    with engine.begin() as conn: 
        print(f'Executing statement to prune audit records:')
        result = conn.execute(stmt)
        print_sa_stmt(stmt, result.rowcount)

    print('Success')


def protect_viewer_table(conn_id: str, dag_config, **context): 
    '''Protect the viewer_ version of an audited table by appending its name to 
    the table `audit.drop_protected_tables`. Doing so will allow an event trigger 
    and database function to prevent any `DROP TABLE` statements from succeeding, as 
    carelessly dropping, recreating, and re-upserting the department data from Airflow
    causes erroneous data to be added to the history table'''
    db_conn = BaseHook.get_connection(conn_id)
    engine = create_engine_from_airflow_connection(db_conn)      
    metadata = sa.MetaData()
    protection_table = sa.Table("drop_protected_tables", metadata, schema='audit', autoload_with=engine)

    with engine.begin() as conn: 
        stmt = sa.select(protection_table).where(
            protection_table.c.schema_name == dag_config['source_schema'], 
            protection_table.c.table_name == dag_config['table_name'])
        
        results = conn.execute(stmt).one_or_none()

        if results: 
            print(f'{dag_config["source_schema"]}.{dag_config["table_name"]} already protected from DROP TABLE statements')
            return None
        else: 
            stmt = sa.insert(protection_table).values(
                schema_name=dag_config['source_schema'], 
                table_name=dag_config['table_name']
            )
            conn.execute(stmt)
            print(f'{dag_config["source_schema"]}.{dag_config["table_name"]} successfully protected from DROP TABLE statements')
