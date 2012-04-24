import pymongo
import datetime

'''
File: fill_db.py
Description: Needs to be run before testing test_io.py, test_job.py and test_scheme.py. This module populates data being used by the test modules for deploying a job and testing the new scheme created.
Author/s: NYU ITP team
'''

'''
config - Configuration to be used by the Mongo-Disco adapter to insert data.
'''
config = {
        "db_name": "test",
        "collection_name": "modforty",
        "splitSize": 4, #MB
        "inputURI": "mongodb://localhost/test.in",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        }

'''
Connection to MongoDB is instantiated and data is inserted here.
'''
conn = pymongo.Connection()
db = conn[config.get('db_name')]
coll = db[config.get('collection_name')]

for i in range(400):
    post = {"name" : i%40, "date": datetime.datetime.utcnow()}
    coll.insert(post)
