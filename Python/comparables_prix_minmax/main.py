import numpy as np
import sklearn.preprocessing
from math import sqrt
import json
import pandas as pd
import logging
# Find the min and max values for each column
def minmax(dataset):
#	print(dataset)
	minmax = list()
	for i in range(len(dataset[0])):
		col_values = [row[i] for row in dataset]
		value_min = min(col_values)
		value_max = max(col_values)
		minmax.append([value_min, value_max])
	return minmax


# Calculate the Euclidean distance between two vectors
def euclidean_distance(row1, row2):
	distance = 0.0
	for i in range(len(row1)-1):
		distance += (row1[i] - row2[i])**2
	return sqrt(distance)

# Locate the most similar neighbors for price min & max calculation
def get_neighbors_minmax(train, test_row,nbrows):
	distances = list()
	for train_row in train:
		dist = euclidean_distance(test_row, train_row)
		distances.append((train_row, dist))
	distances.sort(key=lambda tup: tup[1])
	neighbors_minmax = list()
	for i in range(nbrows):
		if 1/(1+distances[i][1]) > 0.9:
			neighbors_minmax.append(distances[i][0])
	return neighbors_minmax

# format neighbor min max price
def return_minmax_neighbors(vneighb_minmax,scaler,dforig):
	neighbors_inverse = scaler.inverse_transform(vneighb_minmax)
	neighbors_inverse = np.array(neighbors_inverse)
	np.set_printoptions(suppress=True)
	neigh= pd.DataFrame()
	for neighbor in neighbors_inverse:
		dforig1 = dforig[(dforig["salleBains"] == int(neighbor[0])) & (dforig['taxesMunicipale'] == int(round(neighbor[1]))) & (dforig['superficieTerrain'] == round(neighbor[2],2)) & (dforig['evalMunicTot'] == int(round(neighbor[3]))) & (dforig['nbrPieces'] == int(round(neighbor[4]))) &  (dforig['stationnements'] == int(round(neighbor[5]))) &  (dforig['anneeConstruction'] == int(round(neighbor[6])))  &  (dforig['nbrChambres'] == int(round(neighbor[7])))]
		neigh = neigh.append(dforig1)
	if len(neigh) > 10 : 
		scoreConf = 5
	else : 
		scoreConf = len(neigh)/2
	neigh1 = {"MinPrixVente": [neigh["prixVente"].min()],"MaxPrixVente": [neigh["prixVente"].max()], "ScoreConf":[scoreConf]}
	neigh_result = pd.DataFrame(neigh1)
	return neigh_result.to_json(orient='records',force_ascii=False)
	
# main function	
def neigh_min_max(request):
	try:
		from google.cloud import bigquery
		from sklearn.preprocessing import MinMaxScaler
		json_data = request.get_json(silent=True)
		neighVide = {"MinPrixVente": [],"MaxPrixVente": [], "NbrComp":[]}
		neighVide = pd.DataFrame(neighVide)
		num_neighbors = json_data["nbComparable"]
		vMunicipal = json_data["municipalite"] 
		vGenreProp = json_data["genreProprietes"]
		vTypBat = json_data["typeBatiment"]
		vCategorie = json_data["categorie"]
		vCaractBien = eval('[' + str(json_data["salleBains"]) +','+str(json_data["taxesMunicipale"]) +',' + str(json_data["superficieTerrain"]) +','+str(json_data["evalMunicTot"]) 
		+',' + str(json_data["nbrPieces"]) +','+str(json_data["stationnements"]) +',' + str(json_data["anneeConstruction"]) +',' + str(json_data["nbrChambres"]) +']')
		bq_client = bigquery.Client()
		if vCategorie == 'UNI':
			sql = """
			SELECT distinct salleBains,taxesMunicipale,superficieTerrain,evalMunicTot,nbrPieces,stationnements,anneeConstruction,nbrChambres,prixVente,municipalite,genreProprietes,typeBatiment, postalCode, adresse
			FROM `ziaapp-ac0eb.unifamilialesHist.tabUnifamComparables`
			WHERE municipalite = @vMunicipal AND genreProprietes = @vGenreProp AND typeBatiment = @vTypBat
			"""
		elif vCategorie == 'COP':
			sql = """
			SELECT distinct salleBains,taxesMunicipale,superficieHabitable as superficieTerrain,evalMunicTot,nbrPieces,stationnements,anneeConstruction,nbrChambres,prixVente,municipalite,genreProprietes,typeBatiment, postalCode, adresse
			FROM `ziaapp-ac0eb.unifamilialesHist.tabCoproComparables`
			WHERE municipalite = @vMunicipal AND genreProprietes = @vGenreProp AND typeBatiment = @vTypBat
			"""		
		job_config = bigquery.QueryJobConfig(
		query_parameters=[
			bigquery.ScalarQueryParameter("vMunicipal", "STRING", vMunicipal),
			bigquery.ScalarQueryParameter("vGenreProp", "STRING", vGenreProp),
			bigquery.ScalarQueryParameter("vTypBat", "STRING", vTypBat)
		])
		query_job = bq_client.query(sql, job_config=job_config)
		result = query_job.result()
		nbrows = result.total_rows
		if result.total_rows > 0:
			dforig = query_job.to_dataframe()
			df_nc = dforig.drop(['municipalite','prixVente', 'genreProprietes','typeBatiment','adresse','postalCode'], axis=1, inplace=False)
			df_nc = df_nc.reset_index(drop=True)
			fdataset = df_nc.values.tolist()
			# define model parameter
			minmax1 = minmax(fdataset)
			scaler_model = MinMaxScaler(feature_range=(0, 1))
			fdataset = scaler_model.fit_transform(fdataset)

			# define a new record
			
			row = vCaractBien
		#    row1 = row
		#    print(row1)
			for i in range(len(row)):
				row[i] = (row[i] - minmax1[i][0]) / (minmax1[i][1] - minmax1[i][0])

			# predict the label

			neighbors1 = get_neighbors_minmax(fdataset, row, nbrows)
			if not neighbors1 :
				return neighVide.to_json(orient='records',force_ascii=False)
			else :
				return return_minmax_neighbors(neighbors1,scaler_model,dforig)
		else :
			return neighVide.to_json(orient='records',force_ascii=False)
	except Exception as e:
            logging.info(f"Uncaught exception, {e}", exc_info=e)