import pymongo
import datetime
config = {
        "db_name": "test",
        "collection_name": "modforty",
        "splitSize": 1, #MB
        "inputURI": "mongodb://localhost/test.in",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        }

conn = pymongo.Connection()
db = conn[config.get('db_name')]
coll = db[config.get('collection_name')]

for i in range(400):
    post = {"name" : i%40, "date": datetime.datetime.utcnow()}
    coll.insert(post)
