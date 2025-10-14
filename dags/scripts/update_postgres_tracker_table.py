from airflow.hooks.base_hook import BaseHook
import psycopg2
import json
import logging
import os,sys
from time import sleep
from datetime import datetime
import pytz
import requests, datetime, io


def update_metadata_data_last_updated(user, dataset, conn, dest_schema, date):
    '''
    Rider on function that will update Knack with our change date.
    '''

    knack_conn = BaseHook.get_connection('knack')
    
    # Object and record details
    OBJECT_ID = "object_3"
    FIELD_ID = "field_202"  # The field ID for the "Data Last Updated" field in Knack

    # attempt to find the associated page in metadata.phila.gov through the data we sync from knack in citygeo.knack_metadata_phila_catalog_extract
    # Make the db1 style name as metadata might not have been updated with the db2 style name.
    # lower-case everything so case doesn't prevent a match.
    possible_names = []
    # Possible DB2 variations
    possible_names.append(f'databridge.{user.lower()}.{dataset.lower()}')
    possible_names.append(f'{user.lower()}.{dataset.lower()}')
    # Account for people using the "viewer_dept" dataset name in Metadata.
    if user.lower().startswith('viewer_'):
        possible_names.append(f'{user.split("viewer_")[1].lower()}.{dataset.lower()}')
        possible_names.append(f'databridge.{user.split("viewer_")[1].lower()}.{dataset.lower()}')
    else:
        possible_names.append(f'viewer_{user.lower()}.{dataset.lower()}')
        possible_names.append(f'databridge.viewer_{user.lower()}.{dataset.lower()}')

    # Account for force_viewer_override options.
    if dest_schema.split('viewer_')[1] != user:
        possible_names.append(f'databridge.{dest_schema.lower()}.{dataset.lower()}')
        possible_names.append(f'{dest_schema.lower()}.{dataset.lower()}')

    stmt = f"""
    SELECT name,id,dataset,summary,dataset_department,dataset_contact_text,description,development_process FROM citygeo.knack_metadata_phila_catalog_representations
    WHERE """

    for i, name in enumerate(possible_names):
        if i == 0:
            stmt += f"lower(source_path) = '{name}'"
        else:
            stmt += f" OR lower(source_path) = '{name}'"
    
    print(stmt)

    RECORD_ID = None
    with conn:
        with conn.cursor() as cur:
            cur.execute(stmt)
            RECORD_ID = cur.fetchone()

    if not RECORD_ID:
        print('Could not find metadata entry in metadata.phila.gov.')
    else:
        print(f'Found metadata.phila.gov entry! Trying to update data_last_updated field ({FIELD_ID}) in "{OBJECT_ID}" (Representations) in Knack.')

        API_URL = f"https://api.knack.com/v1/objects/{OBJECT_ID}/records/{RECORD_ID[1]}"

        if os.environ['ENVIRONMENT'] != 'prod-v2':
            print(f'Airflow is not prod, would have updated metadata against this record: {API_URL}')
        else:
            # Headers for API authentication
            headers = {
                "X-Knack-Application-Id": knack_conn.login,
                "X-Knack-REST-API-Key": knack_conn.password,
                "Content-Type": "application/json"
            }

            # field_202 is the "Data Last Updated" field for whatever reason. You can see this by browsing the dataset's fields
            # and clicking on it, the url will show up as: https://builder.knack.com/phl/inventory/schema/list/objects/object_3/fields/field_202/settings
            data = {
                FIELD_ID: date.strftime('%Y-%m-%dT%H:%M:%SZ')  # Replace with the correct field key and value
            }

            # Make PUT request to update the record
            print(f'Updating Knack, {API_URL}')
            print(f'data: {data}')
            response = requests.put(API_URL, json=data, headers=headers)

            # Check response
            if response.status_code == 200:
                print("Knack record updated successfully:", response.json())
            else:
                print("Failed to update record:", response.status_code, response.text)

def update_ago_data_last_updated(table_name, ago_user, date, alternate_upload_name=None):
    '''
    Updates the "revise date" in the item's AGO metadata. This is a "citation" field (called Updated* date in the web UI under the "citation date section in it's metadata).
    This is important to make the "Data Updated" timestamp in the details field of our opendata website properly reflect the last time the data was updated.
    '''
    from xml.etree import ElementTree as ET
    print(f'Updating AGO items metadata "reviseDate"..')

    ago_secret_name = f"ago-{ago_user.replace('.','-')}"
    ago_conn = BaseHook.get_connection(ago_secret_name)
    username = ago_conn.login
    password = ago_conn.password
    portal = 'https://phl.maps.arcgis.com'

    # First get an AGO token
    url = f"{portal}/sharing/rest/generateToken"
    data = {
        "username": username, "password": password,
        "client": "referer", "referer": portal, "expiration": 60, "f": "json"
    }
    r = requests.post(url, data=data)
    r.raise_for_status()
    token = r.json()["token"]

    if alternate_upload_name:
        table_name_to_search = alternate_upload_name
    else:
        table_name_to_search = table_name

    # First, we need to find the item ID of the dataset in AGO.
    itemid = None
    # Try to query for the itemid against it's service name API first
    fields_rest_url = f'https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/{table_name_to_search}/FeatureServer'
    print(f'Attempting to query against service URL: {fields_rest_url}')
    data = requests.get(fields_rest_url + '?f=pjson' + f'&token={token}')
    if 'error' in data.json().keys():
        if data.json()['error']['message'] == 'Invalid URL':
            print(f"Service not found with name '{table_name_to_search}'")
        else:
            print(f"Service not found with name '{table_name_to_search}'")
    else:
        print(f"Found itemid with service name query!")
        itemid = data.json()['serviceItemId']


    # if that fails, try searching by title
    if not itemid:
        url = f"{portal}/sharing/rest/search"
        params = {"q": f"title:{table_name_to_search}", "f": "json", "filter": 'type:"Feature Service"', 'max_items': 100, "token": token}
        r = requests.get(url, params=params)
        r.raise_for_status()
        results = r.json()
        if results["total"] == 0:
            raise ValueError(f"Item with title '{table_name_to_search}' not found.")
        elif results["total"] > 1:
            print(f"Found multiple items with title '{table_name_to_search}'..")
            for i in results["results"]:
                print(f"Evalutating {i['title']}")
                if i['title'].lower() == table_name_to_search.lower():
                    itemid = i['id']
                    print(f"Using ID {itemid} owned by {i['owner']}")
                    #break
        else:
            print(f"Found itemid by searching against the /sharing/rest/search endpoint!")
            itemid = results["results"][0]["id"]

    # make our ISO date naive (no timezone), because ESRI is dumb and doesn't handle timezones properly.
    naive_date = date.replace(tzinfo=None)
    iso_date = naive_date.strftime('%Y-%m-%dT%H:%M:%S')

    # now download the metadata, update it, and re-upload it.
    # Esri stores the item metadata at this path (downloadable as XML).
    url = f"{portal}/sharing/rest/content/items/{itemid}/info/metadata/metadata.xml"
    r = requests.get(url, params={"token": token})
    r.raise_for_status()
    #save a backup to a file
    with open(f"{itemid}_metadata.xml", "wb") as f:
        f.write(r.content)
    xml = r.content

    xml_obj = ET.parse(io.BytesIO(xml))
    root = xml_obj.getroot()

    # Find or create dataIdInfo
    data_id_info = root.find('dataIdInfo')
    if data_id_info is None:
        data_id_info = ET.SubElement(root, 'dataIdInfo')
    
    # Find or create idCitation
    id_citation = data_id_info.find('idCitation')
    if id_citation is None:
        id_citation = ET.SubElement(data_id_info, 'idCitation')
    
    # Find or create date element
    date_elem = id_citation.find('date')
    if date_elem is None:
        date_elem = ET.SubElement(id_citation, 'date')
    
    # Find or create reviseDate element
    revise_date_elem = date_elem.find('reviseDate')
    if revise_date_elem is None:
        revise_date_elem = ET.SubElement(date_elem, 'reviseDate')
    
    # Set the date value
    revise_date_elem.text = iso_date
    print(f"Set reviseDate to: {iso_date}")

    # Save modified XML
    modified_xml = ET.tostring(root, encoding='utf-8', xml_declaration=True)

    if os.environ['ENVIRONMENT'] != 'prod-v2':
        print(f'Airflow is not prod, would have updated AGO metadata revisedDate for this item: {itemid}')
    else:
        url = f"{portal}/sharing/rest/content/users/{ago_user}/items/{itemid}/update"
        files = {"metadata": ("metadata.xml", modified_xml, "application/xml")}
        data = {"f": "json", "token": token}
        r = requests.post(url, data=data, files=files)
        r.raise_for_status()
        print(r.json())
        print(f"Successfully updated AGO metadata reviseDate for item ID {itemid}.")


def update_postgres_tracker_table(account_name, table_name, dest_schema, conn_id, upload_to_ago, ago_user, ago_alternate_upload_name, **context):
    '''
    If the dag is successful, update the latest Postgres XMIN as well as the Metadata entry in Knack.
    '''

    # check if all past tasks successeful
    dr = context["dag_run"] 
    ti = context["ti"]
    account_name = account_name.lower()
    table_name = table_name.lower()

    # Make a dictionary of all tasks in the dag where the key is the task name, and the value is it's state
    # state being 'failed', 'success', 'running', etc.
    dag_tasks = {task.task_id: task.state for task in dr.get_task_instances() if task.task_id != ti.task_id }

    dag_tasks.pop('email_data_stewards_on_fail', None)

    # Remove success/None states, anything left means we have prior task failure
    non_success_tasks = {}
    for k,v in dag_tasks.items():
        if v != 'success' and v != 'removed' and v is not None:
            non_success_tasks[k] = v

    if non_success_tasks:
        raise Exception(f'Some task(s) havent finished or failed, not updating postgres xmin in tracker! Task(s): {non_success_tasks}')
    else:
        print("No running, failed, scheduled, or retrying tasks, updating postgres xmin in tracker table..")

        db_conn = BaseHook.get_connection(conn_id)
        db_name = json.loads(db_conn.extra)['db_name']

        conn = psycopg2.connect(host=db_conn.host, dbname=db_name, port=5432, user=db_conn.login, password=db_conn.password)

        print('Connected to %s' % conn)
        conn.autocommit = True

        # Make our very unique xcom key to get the XMIN we have stored in there from an earlier task.
        dag_run_id = context['dag_run'].run_id
        xcom_xmin_key = f'{account_name.lower()}_{table_name.lower()}__{dag_run_id}_XMIN'
        xcom_row_count_key = f'{account_name.lower()}_{table_name.lower()}__{dag_run_id}_ROW_COUNT'
        # Also grab our "tables different" xcom key.
        xcom_table_different_key = f'{account_name.lower()}_{table_name.lower()}__{dag_run_id}_TABLE_DIFFERENT'

        print(f'\nPulling xcom keys...')

        current_xmin = ti.xcom_pull(key=xcom_xmin_key)
        assert current_xmin
        print(f'{xcom_xmin_key}: {current_xmin}')
        
        current_row_count = ti.xcom_pull(key=xcom_row_count_key)
        assert current_row_count
        print(f'{xcom_row_count_key}: {current_row_count}')

        tables_different = ti.xcom_pull(key=xcom_table_different_key)
        assert tables_different
        print(f'{xcom_table_different_key}: {tables_different}')
        print()

        # Also insert the time that we actually insert the record
        eastern = pytz.timezone('America/New_York')
        timestamp = datetime.datetime.now(eastern)
       
        if tables_different == 'false':
            print('Tables detected as being the same, won\'t attempt to update Metadata.phila.gov\'s "data_last_updated" field in Knack.\n')
        if tables_different == 'true':
            update_metadata_data_last_updated(account_name.lower(), table_name.lower(), conn, dest_schema, timestamp)
            if upload_to_ago:
                update_ago_data_last_updated(table_name.lower(), ago_user, timestamp, ago_alternate_upload_name)
    
        # If the tables are different, we want to update the data_last_updated field
        if tables_different == 'true':
            upsert_stmt = f"""
            INSERT INTO citygeo.airflow_xmin_change_history 
                (table_owner, table_name, xmin_num, status, row_count, recorded, data_last_updated)
            VALUES
                ('{account_name.lower()}', '{table_name.lower()}', {current_xmin}, 'finished', {current_row_count}, '{timestamp}', '{timestamp}')
            ON CONFLICT (table_owner, table_name)
            DO UPDATE SET
                xmin_num = EXCLUDED.xmin_num,
                status = EXCLUDED.status,
                row_count = EXCLUDED.row_count,
                recorded = EXCLUDED.recorded,
                data_last_updated = EXCLUDED.data_last_updated
            """
        # Else, leave the existing value in there.
        else:
            upsert_stmt = f"""
            INSERT INTO citygeo.airflow_xmin_change_history 
                (table_owner, table_name, xmin_num, status, row_count, recorded)
            VALUES
                ('{account_name.lower()}', '{table_name.lower()}', {current_xmin}, 'finished', {current_row_count}, '{timestamp}')
            ON CONFLICT (table_owner, table_name)
            DO UPDATE SET
                xmin_num = EXCLUDED.xmin_num,
                status = EXCLUDED.status,
                row_count = EXCLUDED.row_count,
                recorded = EXCLUDED.recorded
            """

        try:
            with conn:
                with conn.cursor() as cur:
                    print('Updating xmin in citygeo.airflow_xmin_change_history table..')
                    print('Executing stmt: ' + str(upsert_stmt))
                    cur.execute(upsert_stmt)
                    assert cur.rowcount != 0
        except Exception as e:
            conn.close()
            raise e
