from airflow.hooks.base_hook import BaseHook
import json
import logging
import sys
import boto3
import psycopg2
from time import sleep
import os
from random import randint

def wait_for_big_dags(**context):
    batch_client = boto3.client('batch', region_name='us-east-1')

    #our_dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID').replace('gis_','')
    our_dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID')

    print(f"Dag id is: {our_dag_id}")

    # Make a connection to our meta db so we can see what's running.
    conn = BaseHook.get_connection('airflow_db')
    host = conn.host
    pw = conn.password
    pg_conn = psycopg2.connect(f"user=postgres host={host} password={pw} dbname=airflow")
    cursor = pg_conn.cursor()

    # get dags tagged with "large_dataset" which is informed by the dataset config
    stmt = "select dag_id from public.dag_tag where name = 'large_dataset'"
    cursor.execute(stmt)
    big_dags_set = {r[0] for r in cursor.fetchall()}

    # Remove our own dag id from big_dags_set so we don't block our own run.
    big_dags_set.remove(our_dag_id)

    def get_running_dags():
        # Get running dags, exclude wait_for_big_dags because then they're all stuck waiting for each other
        # Banking on our random staggers to have them not run simultaneously.
        #stmt = "select dag_id from public.task_instance where state = 'running' and task_id != 'wait_for_big_dags';"
        # Try selecting dags that are only running the oracle_to_s3 task, which is the problem one.
        stmt = """
        SELECT dag_id,task_id FROM public.task_instance
        WHERE (state = 'running' OR state = 'scheduled' OR state = 'queued')
        AND task_id = 'oracle_to_s3';
        """
        cursor.execute(stmt)
        #output = [r[0].replace('gis_','') for r in cursor.fetchall()]
        output = [r[0] for r in cursor.fetchall()]

        running_dags = set(output)
        if our_dag_id in running_dags:
            running_dags.remove(our_dag_id)

        # Get back only running dags that are in our big_dags set
        # Using a set intersection
        if running_dags:
            big_running_dags = running_dags.intersection(big_dags_set)
        else:
            big_running_dags = []
        print(f'Big Running dags: {big_running_dags}')
        return big_running_dags

    big_running_dags = get_running_dags()

    # randomize to decrease chance of dags starting at the exact same time.
    # If no big dags, sleep anyway and check again because for some reason the table
    # for task runs is cleared out occasionally. Checking again should make us
    # more resistant to false positives.
    if not big_running_dags:
        logging.info(f'Dag is runnable! No other running big dags.')
        sys.exit(0)
    # While there is at least 2 other running big dags, exit unsuccessfully so the task can be retried
    elif len(big_running_dags) >= 2:
        logging.info(f'More than 1 other big dags are running: {big_running_dags}')
        sys.exit(1)
    else:
        logging.info(f'Dag is runnable! Running big dags: {big_running_dags}')
        sys.exit(0)