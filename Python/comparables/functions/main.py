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

# Locate the most similar neighbors
def get_neighbors(train, test_row, num_neighbors,vnbrows):
	distances = list()
	for train_row in train:
		dist = euclidean_distance(test_row, train_row)
		distances.append((train_row, dist))
	distances.sort(key=lambda tup: tup[1])
	neighbors = list()
	neigh_score = list()
	for i in range(num_neighbors):
		if i < vnbrows :
			neighbors.append(distances[i][0])
			neigh_score.append(1/(1+distances[i][1]))
	return neighbors,neigh_score

# format the neighbors 
def return_neighbors(vneighb,vscore,scaler,dforig):
	neighbors_inverse = scaler.inverse_transform(vneighb)
	neighbors_inverse = np.array(neighbors_inverse)
	np.set_printoptions(suppress=True)
	neigh= pd.DataFrame()
	for neighbor in neighbors_inverse:
		dforig1 = dforig[(dforig["salleBains"] == int(neighbor[0])) & (dforig['taxesMunicipale'] == int(round(neighbor[1]))) & (dforig['superficieTerrain'] == round(neighbor[2],2)) & (dforig['evalMunicTot'] == int(round(neighbor[3]))) & (dforig['nbrPieces'] == int(round(neighbor[4]))) &  (dforig['stationnements'] == int(round(neighbor[5]))) &  (dforig['anneeConstruction'] == int(round(neighbor[6])))  &  (dforig['nbrChambres'] == int(round(neighbor[7])))]
		neigh = neigh.append(dforig1)
	neigh.insert(0, "score", vscore, True)    
	return neigh.to_json(orient='records',force_ascii=False)
	
# main function	
def neighborhood(request):
	try:
		from google.cloud import bigquery
		from sklearn.preprocessing import MinMaxScaler
		json_data = request.get_json(silent=True)
		neighVide = {"salleBains": []}
		neighVide = pd.DataFrame(neighVide)
		num_neighbors = json_data["nbComparable"]
		vMunicipal = json_data["municipalite"] 
		vGenreProp = json_data["genreProprietes"]
		vCategorie = json_data["categorie"]
		vTypBat = ""
		if vCategorie == 'UNI':
			vTypBat = json_data["typeBatiment"]
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
			SELECT distinct salleBains,taxesMunicipale,superficieHabitable as superficieTerrain,evalMunicTot,nbrPieces,stationnements,anneeConstruction,nbrChambres,prixVente,municipalite,genreProprietes,'' typeBatiment, postalCode, adresse
			FROM `ziaapp-ac0eb.unifamilialesHist.tabCoproComparables`
			WHERE municipalite = @vMunicipal AND genreProprietes = @vGenreProp 
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
			
			row = [float(i) for i in vCaractBien]
			for i in range(len(row)):
				if (minmax1[i][1] - minmax1[i][0]) > 0 :
					row[i] = (row[i] - minmax1[i][0]) / (minmax1[i][1] - minmax1[i][0])
				else:
					row[i] = 1

			# predict the label

			neighbors,neighScore = get_neighbors(fdataset, row, num_neighbors, nbrows)
			if not neighbors :
				return neighVide.to_json(orient='records',force_ascii=False)
			else :
				return return_neighbors(neighbors,neighScore,scaler_model,dforig)
		else :
			return neighVide.to_json(orient='records',force_ascii=False)
	except Exception as e:
            logging.info(f"Uncaught exception, {e}", exc_info=e)