from app import MongoSplitter as MS
import pymongo
from pymongo import Connection


'''
original_URI = "mongodb://localhost:9999/test.in"
new_URI = "127.0.0.1:3333"

original_URI_2 = "mongodb://wang:li@localhost:9999/test.in"
original_URI_3 = "mongodb://wang:li@localhost:9999/"
original_URI_4 = "mongodb://wang:li@localhost"

print original_URI,MS.get_new_URI(original_URI,new_URI,True)
print original_URI_2,MS.get_new_URI(original_URI_2,new_URI,None)
print original_URI_3,MS.get_new_URI(original_URI_3,new_URI,False)
print original_URI_4,MS.get_new_URI(original_URI_4,new_URI,False)

'''

config = {
        "inputURI":"mongodb://localhost/test.people",
        "slaveOk":"true",
        "useShards":True
        }

def test_chunks():

    uri = config['inputURI']
    useShards = config['useShards']
    slaveOk = config['slaveOk']
    database = "test"
    collection = "people"

    splits = MS.fetch_splits_via_chunks(config,uri,useShards,slaveOk);
    print splits
    print "splits count: %d" %len(splits)
    count = 0
    for split in splits:
        print split.format_uri_with_query()
        connection = Connection(split.inputURI)
        db= connection[database]
        col = db[collection]
        cursor = col.find(split.query)
        print cursor.count()
        count = count + cursor.count()
    print count

test_chunks()


