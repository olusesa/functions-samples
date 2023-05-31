# -*- coding: utf-8 -*-

# Make broker recommendation 
#import numpy as np
# import os
import json
import pandas as pd
from google.cloud import bigquery


def get_brokers_list(request):
    json_data = request.get_json(silent=True)
    json_data =json.dumps(json_data, ensure_ascii=False)
    json_data = json.loads(json_data)
    headers = {'Access-Control-Allow-Origin': '*'}
    bq_client = bigquery.Client()
    sql = """
        select nomCourtier, agence, numeroPermis
        from `ziaapp-ac0eb.unifamilialesHist.tabListeCourtiers`
        order by nomCourtier
        """
    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(sql, job_config=job_config)
    data = query_job.result()
    dforig = data.to_dataframe()
    return dforig.to_json(orient='records',force_ascii=False)
