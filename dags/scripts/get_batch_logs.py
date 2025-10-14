import boto3
from pprint import pformat
import logging


#def get_batch_logs(**kwargs):
def get_batch_logs(job_id):
    batch_client = boto3.client('batch', region_name='us-east-1')

    #xcom_task_id_key = kwargs['xcom_task_id_key']
    #ti = context['task_instance']
    #job_id = ti.xcom_pull(key=xcom_task_id_key)

    response = batch_client.describe_jobs(
        jobs=[
            job_id,
        ]
    )

    if not response['jobs']:
        raise AssertionError(f'Got an empty response from AWS, is your job_id invalid?: {job_id}')

    command = pformat(response['jobs'][0]['container']['command'], indent=2)
    logging.info('*****Printing batch command that was run..******')
    logging.info(command)

    logstreamname = (response['jobs'][0]['container']['logStreamName'])

    cloudwatch_client = boto3.client('logs', region_name='us-east-1')

    response = cloudwatch_client.get_log_events(
        logGroupName='/aws/batch/job',
        logStreamName=logstreamname,
        limit=100,
        startFromHead=False
    )

    logging.info('*****Printing logs from the cloudwatch logstream..******')
    #for i, msg in enumerate(reversed(response['events'])):
    for i, msg in enumerate(response['events']):
        logging.info(str(i) + ': ' + msg['message'])
            

if __name__ == "__main__": 
    main(job_id)
