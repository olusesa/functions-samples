#  -*- coding: utf-8 -*-

# Make    broker recommendation 
# import numpy as np
# import os
import json
import pandas as pd
from google.cloud import bigquery

def get_price_range(pVente):
    if pVente >= 0 and pVente < 199999:
        return '0-199999'
    elif pVente >= 200000 and pVente < 499999:
        return '200000-499999'
    elif pVente >= 500000 and pVente < 999999:
        return '500000-999999'
    elif pVente >= 1000000:
        return 'plus de 1000000'

def recommend_broker(request):
    # from google.cloud import bigquery
    json_data = request.get_json(silent=True)
    json_data =json.dumps(json_data, ensure_ascii=False)
    json_data = json.loads(json_data)
    num_courtier = json_data["nbCourtier"]
    vMunicipal = json_data["municipalite"] 
    vGenreProp = json_data["genreProprietes"]
    vTypBat = json_data["typeBatiment"]
    vCategorie = json_data["categorie"]
   # vPriceRange = get_price_range(json_data["prixZia"])
    bq_client = bigquery.Client()
    if vCategorie == 'UNI':
        sql = """
        select A.numeroPermis,A.nomCourtier,A.agenceCourtier, A.nbVente as nbVentesComp, B.nbVente as nbVentesTotal,A.Photo, A.logoAgence from 
        (select nomCourtier,agenceCourtier, Photo, logoAgence,numeroPermis, count(adresse) nbVente
        FROM `ziaway-dev.courtiers.vw_recommendCourtiersClientsUni2021`
        WHERE municipalite = @vMunicipal AND genreProprietes = @vGenreProp AND typeBatiment = @vTypBat  
        AND categorie = @vCategorie 
        group by nomCourtier,agenceCourtier,Photo, logoAgence,numeroPermis 
        ) A
        INNER JOIN (
        select numeroPermis,count(adresse) nbVente
        FROM `ziaway-dev.courtiers.vw_recommendCourtiersClientsUni2021`
        group by numeroPermis) B ON A.numeroPermis = B.numeroPermis
        order by 3 desc
        """
    elif vCategorie == 'COP':
        sql = """
        select A.numeroPermis,A.nomCourtier,A.agenceCourtier, A.nbVente as nbVentesComp, B.nbVente as nbVentesTotal,A.Photo, A.logoAgence from 
        (select nomCourtier,agenceCourtier,Photo, logoAgence,numeroPermis,count(adresse) nbVente
        FROM `ziaway-dev.courtiers.vw_recommendCourtiersClientsCop2021`
        WHERE municipalite = @vMunicipal AND genreProprietes = @vGenreProp   
        AND categorie = @vCategorie
        group by nomCourtier,agenceCourtier,Photo, logoAgence ,numeroPermis
        ) A
        INNER JOIN (
        select numeroPermis,count(adresse) nbVente
        FROM `ziaway-dev.courtiers.vw_recommendCourtiersClientsCop2021`
        group by numeroPermis) B ON A.numeroPermis = B.numeroPermis
        order by 3 desc
        """
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("vMunicipal", "STRING", vMunicipal),
        bigquery.ScalarQueryParameter("vGenreProp", "STRING", vGenreProp),
        bigquery.ScalarQueryParameter("vTypBat", "STRING", vTypBat),
        bigquery.ScalarQueryParameter("vCategorie", "STRING", vCategorie)
    ])
    query_job = bq_client.query(sql, job_config=job_config)
    data = query_job.result()
    dforig = data.to_dataframe()
    broker = pd.DataFrame()
    for index, row in dforig.iterrows() :
        if index < num_courtier: 
            row1 = dforig[(dforig["numeroPermis"] == row[0])]
            broker = broker.append(row1)
    return broker.to_json(orient='records',force_ascii=False)
