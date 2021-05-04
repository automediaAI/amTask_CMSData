##############
#### Common Library of ETL Functions ####
#################
## Feed it disease data, news data. Feed it queries. It parses and gives back just format needed ##
##############
## Ticket: https://www.notion.so/automedia/Create-data-service-for-pulling-from-disease-sh-7aded63dac7a47c9b6ca768356e4cb6d 
#############


## Declarations 
import pandas as pd
import os
import json
import ast #to covert string to list 

### DATA
### EXTRACTION using Pandas - Add more to get more out of data
#Return data by region 
def getSingleByRegion(allData, regionToGet, areaTable):
	
	print ("Entered SINGLE region --> ")
	print ("All data type BEFORE --", type(allData))
	# allData_json = json.loads(allData)
	allData_json = allData
	print ("All data type AFTER --", type(allData_json))

	for i in allData_json:
		print ("Region being checked", i['region'])
		if (i['region'].lower().strip() == regionToGet.lower().strip()) and (i['areaTable'].lower().strip() == areaTable.lower().strip()):
			return i 
			print ("value of single: ", i)
	return [] #adding to return blank in case region not found, as error check

# Get Top x by Use case
def topListByTitle(allData, sortBy, filterBy, listHowMany):
	df = pd.DataFrame(allData)
	df_filtered = df.loc[df['areaTable'].isin([filterBy])] #Filters by US state or Country data 
	sorted_df = df_filtered.sort_values(by = sortBy , ascending = False)
	if 'World' in sorted_df.values: #To skip first value if World is in list, so hard coding for country vs states list 
		rslt_df = sorted_df.head(listHowMany+1)[1:]
	else:
		rslt_df = sorted_df.head(listHowMany)
	top_list_values = rslt_df.values.tolist()
	top_list_headers = rslt_df.columns.tolist()

	# There def must be a better pandas way of doing this, but for now couldnt figure out a way to export in format I wanted
	final_list = []
	for i in top_list_values:
		temp_dict = {}
		ind_temp = 0
		for j in i:
			temp_dict[top_list_headers[ind_temp]] = j
			ind_temp += 1
		final_list.append(temp_dict)
	return final_list


### TRANSFORMATION FUNCTIONS
# Matching payload to get only limited items in mapping as wanted
def dataSingleParse(payloadToCheck, dataToFillFrom):
	outputDict = {}
	# In payload the values are keys to data dump dict
	for key, value in payloadToCheck['data_needed'].items():
		if value in dataToFillFrom: 
			if isinstance(dataToFillFrom[value], int):
				outputDict[key] = ("{:,}".format(dataToFillFrom[value])) 
			else:
				outputDict[key] = dataToFillFrom[value] 
	#Adding remaining values manually
	outputDict['type'] = payloadToCheck['type']
	outputDict['title'] = payloadToCheck['title']
	outputDict['region'] = payloadToCheck['region']
	return outputDict

# Matching payload from dataTable
def dataTableParse(payloadToCheck, dataListToFillFrom):
	outputDict = {
		"table_info": {
			"type":payloadToCheck['type'],
			"title":payloadToCheck['title'],
			"sourceName":"",
		},
		"rows" : []
	}
	dataList = []
	keyTemp = 0
	for dataToFillFrom in dataListToFillFrom: 
		tempDict = {}		
		for key, value in payloadToCheck['data_needed'].items():
			if value in dataToFillFrom: 
				if isinstance(dataToFillFrom[value], int):
					tempDict[key] = ("{:,}".format(dataToFillFrom[value])) 
				else:
					tempDict[key] = dataToFillFrom[value]
		tempDict['key'] = str(keyTemp) 
		keyTemp = keyTemp + 1 
		dataList.append(tempDict)
	outputDict["rows"] = dataList
	outputDict["table_info"]["sourceName"] = dataList[0]["sourceName"]
	return outputDict


#### NEWS 
# Gives back correct news data based on ask ie how many, and what format
def getNewsData(inputNews, inputDataFormat):
	type_asked = inputDataFormat["type"] #dataTable or dataSingle
	data_asked = inputDataFormat["data_needed"] #format of data asked
	#Different functions if List or Single data asked for
	if type_asked == 'dataSingle': #In case only 1 item asked for or available 
		tempDict = {} #since this will return a single dict
		recID_asked = inputDataFormat["recID_needed"] #which rec needed, only used if dataSingle
		if recID_asked > len(inputNews):
			return {"error":"🚫 Record asked not in dict"}
		else:
			news_index = 0
			for i in inputNews:
				if (str(news_index) == str(recID_asked)): #Only returning that recID
					for key, value in data_asked.items(): #To map it
						if value in i.keys(): 
							tempDict[key] = i[value]
				news_index+=1
			return tempDict
	
	elif type_asked == 'dataTable': #In case of newsTable
		count_asked = inputDataFormat["count_needed"] #how many needed, only used if dataTable
		range_to_check = count_asked if (count_asked <= len(inputNews)) else len(inputNews)
		outputDict = {
				"table_info": {
					"type":"dataTable",
				},
				"rows" : []
			}
		outputList = []		
		for rec in range(range_to_check): 
			tempDict = {}
			tempDict['recID'] = rec #to give it a sequence
			dataIn = inputNews[rec] #already a dict
			
			for key, value in data_asked.items():
				if value in dataIn: 
					tempDict[key] = dataIn[value] 
			outputList.append(tempDict)
		outputDict["rows"] = outputList
		return outputDict
	
	else:
		return {"error":"🚫Data input incorrect"}


#### COVID DATA 
# Gives back correct news data based on ask ie how many, and what format
def getCovidData(inputMasterDict, payload_json):
	type_asked = payload_json["type"] #Single data, or table of data
	print ('type asked: ',type_asked)
	# print ('type asked: ', type(type_asked))
	data_asked = payload_json["data_needed"] #format of data asked
	# series_workedon = i["fields"]["Series"] #to use for debug to check where failing ie Row of record

	#Different functions if List or Single data asked for
	if type_asked.strip() == "dataSingle":
		print ('........entered single..........')
		region_asked = payload_json["region"] #USA, World etc
		title_asked = payload_json["title"] #Cases, deaths etc
		areaTable = payload_json["areaTable"] #All Countries data, or US State data
		print ("Region asked", region_asked)
		data_asked = getSingleByRegion(inputMasterDict, region_asked, areaTable) #Entire dict is sent
		print ('Single output', data_asked)
		return data_asked

	elif type_asked == "dataTable":
		print ('entered table')
		filterBy = payload_json["areaTable"] #So only that data goes
		sortBy = payload_json["sortBy"] #Top by Cases, Deaths etc
		listHowMany = int(payload_json["listHowMany"]) #Give back how many records
		data_asked = topListByTitle(inputMasterDict, sortBy, filterBy, listHowMany)
		print ('Table output', data_asked)
		return data_asked

	else:
		fields = {'data_output': "ERROR - Type incorrect"}
		print ('Error output', data_asked)
		return data_asked





