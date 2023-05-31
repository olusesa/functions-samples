# -*- coding: utf-8 -*-

# return flag True or False if a property were on sale and has expired  
#import numpy as np
# import os
import json
import pandas as pd
from google.cloud import bigquery

def expired_flag(request):
    json_data = request.get_json(silent=True)
    json_data =json.dumps(json_data, ensure_ascii=False)
    json_data = json.loads(json_data)
    vCodePostal = json_data['postalCode']
    vCodeCivique = json_data['civicCode']
    bq_client = bigquery.Client()
    sql = """
        select adresse
        from `ziaway-dev.courtiers.vw_expiredFlag`
        where adresse like CONCAT(@vCodeCivique,'%',@vCodePostal,'%')
        """
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("vCodeCivique", "STRING", vCodeCivique),
        bigquery.ScalarQueryParameter("vCodePostal", "STRING", vCodePostal)
    ])
    query_job = bq_client.query(sql, job_config=job_config)
    result = query_job.result()
    return str((result.total_rows > 0))
