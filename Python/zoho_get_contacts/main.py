

# get Zoho Contacts information and load them into BQ

import json
import requests
import pandas as pd
import io
from google.cloud import bigquery

def zoho_get_contacts(event, context):
    #json_data = request.get_json(silent=True)
    #newdata = json.dumps(json_data)
    #v_data = str(json.loads(newdata))
    #response = requests.post("https://accounts.zoho.com/oauth/v2/token?refresh_token=1000.a0e829bebda1fa05c4f19eaf029b2367.f9be8dac4b2f6499310f038eb59e1d38&client_id=1000.66A85XAWMM4Z4D47ZC291ZUX0Y6O2D&client_secret=4ef78f62f8d637bdd8e63169af326f21d182501c74&grant_type=refresh_token")
    response = requests.post("https://accounts.zoho.com/oauth/v2/token?refresh_token=1000.bd2f23943488105f7796e7c8ceeefdd9.c56d8beaf6bee68d13d49a9db67deb12&client_id=1000.66A85XAWMM4Z4D47ZC291ZUX0Y6O2D&client_secret=4ef78f62f8d637bdd8e63169af326f21d182501c74&grant_type=refresh_token")
   #if response is not None:
    print(response.json())
    accessToken = response.json()['access_token']
    #accessToken = '1000.9a15b7d8a06eb38d9b705fd263aaece3.9b49e126a6a37d9f4a9dea3d5d4aabd7'
    v_access = 'Zoho-oauthtoken ' + accessToken 
    headers = {
            'Authorization': v_access
        }
    url = 'https://www.zohoapis.com/crm/v2/Brokers_Plans'

    parameters = {

         'fields': 'Name,Plan,Plan_Date,Plan_Status'
    }

    response = requests.get(url=url, headers=headers, params=parameters)
    resp = response.json()['data']

    response1 = requests.get(url=url, headers=headers, params=parameters)

    if response1.status_code == 200:
        resp = response1.json()['data']
        df = pd.DataFrame(resp)
        json_data = df.to_json(orient = 'records', lines = True)
        data_as_file = io.StringIO(json_data)
        bq_client = bigquery.Client(project='ziaway-dev')
        dataset_id = 'courtiers'
        table_id = 'tabSubscription'
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = "WRITE_TRUNCATE"
        job_config.encoding = "utf-8"
        job_config.schema = [
            bigquery.SchemaField("Name", "STRING"),
            bigquery.SchemaField("Plan", "STRING"),
            bigquery.SchemaField("Plan_Date", "STRING"),
            bigquery.SchemaField("Plan_Status", "STRING")
        ]
        #job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.autodetect = True

        job = bq_client.load_table_from_file(
            data_as_file,
            table_ref,
            location="us",  # Must match the destination dataset location.
            job_config=job_config,
        )  # API request
        job.result()
        return "Job completed successfully !!!"
    else:
        return "Job failed !!!"

