import sqlalchemy as sa
import json

# This function requires sqlalchemy<2.0.0 because of a dependency in apache-airflow
# To refactor this function to use sqlalchemy>=2.0.0, replace `sa.engine` with `sa`
def create_engine(login: str, password: str, host: str, port: str, database: str) -> sa.engine.Engine:
    '''Compose the URL object, create engine, and test connection'''
    url_object = sa.engine.URL.create(
        drivername='postgresql+psycopg2',
        username=login,
        password=password,
        host=host,
        port=port,
        database=database
    )
    engine = sa.create_engine(url_object)
    engine.connect()
    return engine


def create_engine_from_airflow_connection(db_conn) -> sa.engine.Engine: 
    '''Pass the correct information from Airflow BaseHook connection to SQLAlchemy
    #### Returns
    - SQLAlchemy engine'''
    return create_engine(
        login=db_conn.login,
        password=db_conn.password,
        host=db_conn.host,
        port=db_conn.port,
        database=json.loads(db_conn.extra)['db_name']
    )


def print_sa_stmt(stmt: sa.sql, rowcount:int=None):
    '''Print out an SQLAlchemy statement'''
    print(stmt, '\n')
    if rowcount != None and rowcount >= 0:
        print(f'Rows affected: {rowcount:,}\n')