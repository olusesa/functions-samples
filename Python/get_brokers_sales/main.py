# -*- coding: utf-8 -*-

# Make broker recommendation 
#import numpy as np
# import os
import json
import pandas as pd
from google.cloud import bigquery

def get_brokers_sales(request):
    json_data = request.get_json(silent=True)
    json_data =json.dumps(json_data, ensure_ascii=False)
    json_data = json.loads(json_data)
    bq_client = bigquery.Client()
    sql = """
        select nomCourtier, categorie, municipalite, Photo, logoAgence, numeroPermis
        from `ziaway-dev.courtiers.vw_VentesCourtiers`
        """
    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(sql, job_config=job_config)
    data = query_job.result()
    df = data.to_dataframe()
    df1 = df.sample(n=20)
    result = []
    for index, row in df1.iterrows():
        dict = {'language':'en','courtier' : str(row['nomCourtier']) + ' just sold a house'  + ' in '+ str(row['municipalite']),'license':row['numeroPermis'], 'photo':row['Photo'],'logoAgence':row['logoAgence']}
        result.append(dict)
        dict1 = {'language':'fr','courtier' : str(row['nomCourtier']) + ' vient de vendre une maison' + ' dans '+ str(row['municipalite']),'license':row['numeroPermis'], 'photo':row['Photo'],'logoAgence':row['logoAgence']}
        result.append(dict1)
    return json.dumps(result, ensure_ascii=False)
