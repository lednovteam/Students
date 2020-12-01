from pymongo import *
from bson.json_util import dumps, loads
import pandas as pd
from datetime import datetime


client = MongoClient("localhost:27001", replicaset = "My_Replica_Set")
db = client.olesya
collection = db.users


b = pd.read_json('mongo(1).json')
del b['_id']
del b['id']
def getDate(x):
	return datetime.strptime(x,'%d/%m/%Y %H:%M:%S')
b['creationDate'] = pd.to_datetime(b['creationDate'].apply(getDate))
b['lastModificationDate'] = pd.to_datetime(b['lastModificationDate'].apply(getDate))
for i,j in b.iterrows():
    collection.insert_one(dict(j))