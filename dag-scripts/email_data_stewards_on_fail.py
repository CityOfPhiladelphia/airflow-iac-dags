from airflow.hooks.base_hook import BaseHook
import smtplib
from email.mime.text import MIMEText
import os, sys

data_stewards = {
    'cco': ['james.midkiff@phila.gov', 'jophy.samuel@phila.gov', 'sheyenne.delawrence@phila.gov'],
    'citygeo': ['roland.macdavid@phila.gov'],
    'cleangreen': ['michael.matela@phila.gov'],
    'dao': ['nathaniel.lownes@phila.gov'],
    'dhcd': ['noelle.vought@phila.gov'],
    'dor': ['alex.waldman@phila.gov', 'tracy.dandridge@phila.gov'],
    'dpp': ['terra.luke@phila.gov'],
    'fire': ['alexander.cottone@phila.gov'],
    'health': ['jasmine.jones@phila.gov'],
    'library': ['andrew.fleming@phila.gov'],
    'li': ['bailey.glover@phila.gov', 'daniel.whaland@phila.gov'],
    'ng911': ['christine.salvadore@phila.gov', 'kerri.smith@phila.gov'],
    'oos': ['charlotte.shade@phila.gov'],
    'opa': ['Philip.B.Daniel@phila.gov'],
    'philly_stat_360': ['henry.bernberg@phila.gov'],
    'planning': ['abigail.poses@phila.gov', 'pauline.loughlin@phila.gov', 'Anna.Cava-Grosso@phila.gov'],
    'police': ['michael.urciuoli@phila.gov'],
    'ppr': ['chris.park@phila.gov'],
    'pwd': ['larry.szarek@phila.gov', 'margo.huang@phila.gov'],
    'pwd_watersheds': ['raymond.pierdomenico@phila.gov'],
    'streets': ['jessica.gould@phila.gov']
}


def email_data_stewards_on_fail(account_name, table_name, no_email, **context):
    '''Send email alert about failed dag'''

    if no_email:
        print('"No email" option set to true for this DAG, not emailing about the failure!')
        sys.exit(0)

    recipients = []
    try:
        recipients = data_stewards[account_name]
        recipients = list(set(recipients))
    except KeyError:
        print('No data stewards listed in the recipients dictionary in this script.')
        # nvm, always email us on fail. Don't exit if there are no set data stewards
        #sys.exit(0)

    # always email us
    recipients.append('roland.macdavid@phila.gov')
    recipients.append('alex.waldman@phila.gov')
    recipients.append('matthew.jackson@phila.gov')
    recipients.append('amanda.kmetz@phila.gov')
    recipients.append('phaedra.tinder@phila.gov')

    recipients = list(set(recipients))

    our_dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID')

    dr = context["dag_run"]
    ti = context["ti"]

    # Make a dictionary of all tasks in the dag where the key is the task name, and the value is it's state
    # state being 'failed', 'success', 'running', etc.
    dag_tasks = {task.task_id: task.state for task in dr.get_task_instances() if task.task_id != ti.task_id }
        

    # Don't need to email about these, try to keep the tasks we email about minimal.
    dag_tasks.pop('email_data_stewards_on_fail', None)
    dag_tasks.pop('update_tracker_table_and_metadata', None)
    dag_tasks.pop('set_final_dag_status', None)

    # Remove success/None states, anything left means we have prior task failure
    non_success_tasks = {}
    for k,v in dag_tasks.items():
        if v == 'failed' and v != 'removed' and v is not None:
            non_success_tasks[k] = v
    
    # set_final_dag_status should be trusted to tell us if it failed or not, if skip_optional_tasks is not involved.
    # Mostly useful before I chained everything to one last task.. could still be.
    dag_success = None
    if 'set_final_dag_status' in dag_tasks.keys():
        dag_success = dag_tasks['set_final_dag_status']
        if dag_success == 'success':
            print(f'Dag completed successfully.')
            sys.exit(0)
    # For rare dags where skip_optional_tasks is set.
    elif dag_success is None and not non_success_tasks:
        print(f'Dag completed successfully.')
        sys.exit(0)
    else:
        # Pop out so we don't email about it.
        print(f'Failed tasks detected in DAG: {non_success_tasks}. Emailing.')
        relay	= 'relay.city.phila.local:25'
        sender	= 'airflow-v2-alerts@phila.gov'

        non_success_tasks_formatted = ' '.join(['<li>' + x + '</li>' for x in non_success_tasks])
        
        subject = f'ETL failure for {account_name}.{table_name}'
        text = f"""
        Your table's ETL, <strong>"{our_dag_id}"</strong> for table "<strong>{account_name}.{table_name}"</strong> has failed!<br><br>
        The ETL failed on these tasks:<br> <ul>{non_success_tasks_formatted}</ul><br>
        Please review with CityGeo to have this fixed!
        """

        msg = MIMEText(text, 'html')
        msg['To'] = ', '.join(recipients)
        msg['From'] = sender
        msg['X-Priority'] = '2'
        msg['Subject'] = subject
        server = smtplib.SMTP(relay)
        if os.environ['ENVIRONMENT'] == 'prod-v2':
            server.sendmail(sender, recipients, msg.as_string())
            server.quit()
            # If we have to email, always exit with a failed status
            # so the DAG is properly failed.
            # This task is set to 1 retry so shouldn't rerun.
            print('Email sent successfully, exiting with exit code 1.')
            sys.exit(1) 
        else:
            print('Not prod environment, not sending emails. Would have emailed:')
            print(recipients)
            sys.exit(1)
