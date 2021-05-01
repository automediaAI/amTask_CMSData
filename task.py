
##############
#### Airtable Data Service for CMS - ALl Services ####
#################
## Feed it news, covid data. Pulls payload from airtable service output per correct service & pulls output and uploads data in correct format as needed.  ##
##############
## Ticket:  
#############

## Declarations 
import os
from airtable import Airtable
import json
import ast #to covert string to list 
from amLibrary_ETLFunctions import getCovidData, getNewsData

# Airtable settings 
base_key = os.environ.get("PRIVATE_BASE_KEY")
table_producer = os.environ.get("PRIVATE_TABLE_NAME_PRODUCER")
api_key_airtable = os.environ.get("PRIVATE_API_KEY_AIRTABLE")
airtable_producer = Airtable(base_key, table_producer, api_key_airtable)

### DATA UPLOAD FUNCTIONS
#Uploads single json, or list to data_output of record ID as given
def uploadData(inputDictList, recToUpdate):
	recID = recToUpdate
	if isinstance(inputDictList, dict):
		fields = {'data_output': json.dumps(inputDictList)}
		# fields = {'data_output': str(inputDictList)} #Seems if I do str thats same too
	else:
		fields = {'data_output': str(inputDictList)}
	airtable_producer.update(recID, fields)



#Goes through all records and updates ones that are in the master dict
def updateLoop():
	allRecords = airtable_producer.get_all(view='nipun_test') #to test
	# allRecords_covid = airtable.get_all(view='Service - amData')
	# allRecords_news = airtable_producer.get_all(view='Service - amDataNews')
	# allRecords_images = airtable_producer.get_all(view='Service - amImagePuller')
	# allRecords = allRecords_news + allRecords_images + allRecords_covid
	# allRecords = airtable_producer.get_all()
	for i in allRecords:
		try: #In case have a prod payload or anything wrong 
			if "Prod_Ready" in i["fields"]: #Only working on prod ready ie checkboxed
				rec_ofAsked = i["id"]
				payload_service = i["fields"]["am_Service"][0] #What service is data of
				payload_native = i["fields"]["payload"] #Payload of data asked
				payload_json = json.loads(payload_native)
				type_asked = payload_json["type"] #Single data, or table of data 
				service_output = i["fields"]["service_output"][0] #Main dict to be shared with all

				print ('service: ', payload_service)
				if payload_service == "am_newspuller":
					news_output = service_output #Since output is news 
					# print ('type:', type(news_output))
					news_output_json = ast.literal_eval(news_output) #since List from airtable is in String
					# print ('type:', type(news_output_json))
					data_toUpload = getNewsData(news_output_json, payload_json)
					# print ('data to upload: ', data_toUpload)
					uploadData(data_toUpload, rec_ofAsked) #Just that bit updated 
					print ("News upload to CMS done..")

				elif payload_service == "am_CovidData":
					# print ('Entered into covid data')
					data_output = service_output #Since output is news 
					data_toUpload = getCovidData(data_output, payload_json)
					print ('Data to upload: ', data_toUpload)
					uploadData(data_toUpload, rec_ofAsked) #Just that bit updated 
					print ("Data upload to CMS done..") 

		except Exception: 
			pass
	print ("Upload to CMS done")

updateLoop()
