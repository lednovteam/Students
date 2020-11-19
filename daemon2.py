from pymongo import *
from datetime import datetime, timedelta

import sys
import daemon
import requests
import time
import logging
import logging.config
from queue import Queue
from threading import Thread
from bson.json_util import dumps, loads

__queue__ = Queue()
dictLogConfig = {
        "version":1,
        "handlers":{
            "fileHandler":{
                "class":"logging.FileHandler",
                "formatter":"myFormatter",
                "filename":"daemon.log"
            },
	"rootHandler":{
		"class":"logging.FileHandler",
		"filename":"daemon_root.log"
	}
        },
        "loggers":{
            "App":{
                "handlers":["fileHandler"],
                "level":"DEBUG",
            }
        },
        "formatters":{
            "myFormatter":{
                "format":"%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            }
        },
	"root":{
		"level": "DEBUG",
		"handlers": ["rootHandler"]
	}
    }

logging.config.dictConfig(dictLogConfig)
logger = logging.getLogger("App")
#logging.basicConfig(filename="daemon.log", level = logging.INFO)
logger.info("start program")
files = [logger.handlers[0].stream.fileno(), logging.root.handlers[0].stream.fileno()]

thread_break = False

def main():
	connect_to_mongo = 2
	rec = Thread(target = queue_into_request)
	rec.start()
	while (True):
		try:
			thread_break = False
			client = MongoClient("localhost:27002", replicaset = "My_Replica_Set")
			solr_url = "http://192.168.0.80:8983/solr"
			solr_collection = 'sbornik2'
			db = client.books2
			collection = db.users
			#print(get_last_document_by_date(solr_url, solr_collection, "q=date._date:*&rows=1&sort=date._date+desc"))
			#запрос на получение всех элементов с mongodb и даты поcледнего изменения в solr
			observer = Thread(target = observer_collection, args = (collection, solr_url, solr_collection))
			observer.start()
			force_update_baza(collection, solr_url, solr_collection)
			#функция добавления недобавленных элементов
			#queue_into_request()
			#rec = Thread(target = queue_into_request)
			#rec.start()
			observer.join()
		except Exception as e:
			print(e)
			thread_break = True
			while (observer.isAlive()):
				time.sleep(2)
			logger = logging.getLogger("App.main")
			logger.error(e)

def update_baza(collection, solr_url, solr_collection):
	headers = {'Content-type': "application/json"}
	URL = solr_url + '/' + solr_collection + '/update/json/docs?commit=true'
	response = get_documents_by_query(solr_url, solr_collection, "q=date._date:*&rows=1&sort=date._date+desc")
	responseForDelete = get_documents_by_query(solr_url, solr_collection, "q=_id._oid:*&fl=_id._oid&rows=" + str(response["response"]["numFound"]))
	mongoForDelete = list(collection.find({}, {"_id:":1}))
	print(responseForDelete)
	solrSetDelete = set([ i.get('_id._oid')[0] for i in responseForDelete['response']['docs']])
	print(mongoForDelete)
	mongoSetDelete = set([ str(i.get('_id')) for i in mongoForDelete])
	#print(mongoForDelete)
	print(solrSetDelete.difference(mongoSetDelete))
	for resDel in solrSetDelete.difference(mongoSetDelete):
		render_request({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query":"_id._oid:"+ resDel}})})	 
	isDelete = False
	if "error" in response:
		if response["error"]["code"] == 400:
			print("kek")
			a = collection.find()
	else:
		isDelete = True
		date = response["response"]["docs"][0]["date._date"][0]
		a= collection.find({"date": {"$gt": datetime.fromtimestamp(date / 1e3)-timedelta(hours = 5)} })
		print(datetime.fromtimestamp(date / 1e3))
	for coll in a:
		print(coll)
		if isDelete:
			render_request({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query":"_id._oid:"+str(coll["_id"])}})})
		render_request({"URL": URL,"headers": headers, "data": dumps(coll)})

def force_update_baza(collection, solr_url, solr_collection):
	headers = {'Content-type': "application/json"}
	URL = solr_url + '/' + solr_collection + '/update/json/docs?commit=true'
	response = get_documents_by_query(solr_url, solr_collection, "q=*:*&rows=0")
	responseForDelete = get_documents_by_query(solr_url, solr_collection, "q=date._date:*+%26%26+_id._oid:*&fl=_id._oid+date._date&rows=" + str(response["response"]["numFound"]))
	responseForMongo = collection.find({}, {"_id":1, "date":1})
	print(responseForDelete)
	print(responseForMongo)
	dictSolr = {i.get("_id._oid")[0]:i.get("date._date")[0] for i in responseForDelete["response"]["docs"]}
	dictMongo = {str(i.get("_id")):i.get("date") for i in list(responseForMongo)}
	solrSetIDs = set(dictSolr.keys())
	mongoSetIDs = set(dictMongo.keys())
	deleteSetForSolr = solrSetIDs.difference(mongoSetIDs)
	addSetForSolr = mongoSetIDs.difference(solrSetIDs)
	intersectionSetForSolr = mongoSetIDs.intersection(solrSetIDs)
	#сравнение элементов
	for elem in intersectionSetForSolr:
		if (datetime.fromtimestamp(dictSolr[elem] / 1e3)-timedelta(hours = 5) != dictMongo[elem]):
			deleteSetForSolr.add(elem)
			addSetForSolr.add(elem)
	#удаление элементов
	for elem in deleteSetForSolr:
		render_request({"URL": solr_url + '/' + solr_collection + '/update?commit=true', "headers":headers, "data":dumps({"delete":{"query":"_id._oid:"+elem}})})
	#добавление элементов
	addElements = collection.find({"_id":{"$in":list(addSetForSolr)}})
	for elem in addElements:
		render_request({"URL": URL,"headers": headers, "data": dumps(elem)})

def get_documents_by_query(solr_url, solr_collection, query):
	return loads(requests.get(solr_url + '/' + solr_collection + '/select?' + query ).content)


def queue_into_request():
	while True:
		data = __queue__.get()
		render_request(data)

def add_to_solr(solr_url, solr_collection, data):
	path = '/update/json/docs?commit=true'
	headers = {'Content-type': "application/json"}
	__queue__.put({"URL": solr_url + '/' + solr_collection + path, "headers":headers, "data":dumps(data)})

def delete_to_solr(solr_url, solr_collection, data):
	path = '/update?commit=true'
	headers = {'Content-type': "application/json"}
	__queue__.put({"URL": solr_url + '/' + solr_collection + path, "headers":headers, "data":dumps({"delete":{"query":"_id._oid:"+str(data)}})})

def get_document_by_id(id, collection):
	print(loads(dumps(collection.find({"_id": id})[0])))
	return collection.find({"_id": id})[0]
	

def observer_collection(collection, solr_url, solr_collection):
	try:	
		while(True):
			with collection.watch() as stream:
				for change in stream:
					#print(change)
					definition_methods(solr_url, solr_collection, change, collection)
					logger = logging.getLogger("App.observer")
					logger.info("get by mongo collection = " + collection.name + ", data = " + str(change))
			if thread_break:
				break
	except Exception as e:
		logger = logging.getLogger("App.observer")
		logger.error(e)

def definition_methods(solr_url, solr_collection, cursor, collection):
	if cursor["operationType"] == "insert":
		add_to_solr(solr_url, solr_collection, cursor["fullDocument"])
	if cursor["operationType"] == "replace":
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"]["_id"])
		add_to_solr(solr_url, solr_collection, cursor["fullDocument"])
	if cursor["operationType"] == "update":
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"]["_id"])
		add_to_solr(solr_url, solr_collection, get_document_by_id(cursor["documentKey"]["_id"], collection))
	if cursor["operationType"] == "delete":
		print(cursor['documentKey'].keys())
		delete_to_solr(solr_url, solr_collection, cursor["documentKey"]["_id"])

def render_request(data):
	while True:
		try:
			r = requests.post(data["URL"], data = data["data"], headers = data["headers"])
			if (r.status_code != 200):
				time.sleep(5)
				continue
			logger = logging.getLogger("App.render_request")
			logger.info("send request " + r.url + ", content: " + r.content.decode("utf-8"))
			return r
		except ConnectionError:
			pass

context = daemon.DaemonContext(stdout=sys.stdout, stderr=sys.stderr, files_preserve = files)
with context:
	main()

