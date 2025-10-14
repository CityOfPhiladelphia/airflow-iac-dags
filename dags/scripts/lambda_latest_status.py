from datetime import datetime, timedelta
import boto3
import logging
from airflow.hooks.base import BaseHook
import json

def lambda_latest_status(**kwargs):
    logging.info("DEBUG kwargs: " + str(kwargs))

    aws_conn = BaseHook.get_connection('aws')
    aws_access_key = aws_conn.login
    aws_secret_key = aws_conn.password
    aws_region = json.loads(aws_conn.extra)['region_name']

    function_name = kwargs['function_name']

    # Connect to cloudwatch to get the latest logs from our invocation.
    cloudwatch_client = boto3.client('logs',
                                    region_name=aws_region,
                                    aws_access_key_id=aws_access_key,
                                    aws_secret_access_key=aws_secret_key)

    log_group_name = f'/aws/lambda/{function_name}'

    streams = cloudwatch_client.describe_log_streams(
        logGroupName=log_group_name,
        orderBy='LastEventTime',
        descending=True,
        limit=1
    )
    
    if not streams['logStreams']:
        raise ValueError("No logstreams found for the given Lambda function.")
        
    latest_stream = streams['logStreams'][0]['logStreamName']
    print(f"Analyzing logstream's last 10 lines: {latest_stream}")

    logs = cloudwatch_client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=latest_stream,
        limit=20
    )

    #for event in logs['events']:
    #    message = event['message']
    #    if "Task timed out" in message or "Unhandled exception" in message:
    #        raise ValueError(f"Latest invocation of Lambda function {function_name} failed with error: {message}")
    #    else:
    #        print(message)
    #        print(f"Latest invocation of Lambda function {function_name} was successful")

    logs_failure_found = False
    for event in logs['events']:
        message = event['message']
        if 'Traceback (most recent call last):' in message:
            logs_failure_found = True
        print(message)

    # Initialize boto3 client for Lambda
    lambda_client = boto3.client('lambda',
                                region_name=aws_region,
                                aws_access_key_id=aws_access_key,
                                aws_secret_access_key=aws_secret_key)
    # Get the function's configuration
    response = lambda_client.get_function(FunctionName=function_name)

    # Extract LastUpdateStatus from the response
    last_update_status = response['Configuration'].get('LastUpdateStatus')

    # LastUpdateStatus for some reason doesn't detect raised exceptions in python.
    # If we found a traceback in the logs, then raise our own exception so the task fails.
    if logs_failure_found:
        raise ValueError(f"Latest invocation of Lambda function {function_name} failed!")

    if last_update_status == 'Successful':
        print(f"Latest invocation of Lambda function {function_name} was successful!")
    else:
        raise ValueError(f"Latest invocation of Lambda function {function_name} failed!")

