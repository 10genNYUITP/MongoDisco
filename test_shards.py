from app import MongoSplitter as MS
import time
from disco.core import Job,result_iterator
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from disco.schemes.scheme_mongodb import input_stream
from app.MongoSplitter import calculate_splits as do_split
import pymongo
from pymongo import Connection
from app.mongoUtil import getCollection


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
        "slaveOk":True,
        "useShards":True,
        "createInputSplits":True,
        "useChunks":True
        }


def map(record,params):
    age = record.get('age',0)/10
    yield age, 1

def reduce(iter,params):
    from disco.util import kvgroup
    for age,counts in kvgroup(sorted(iter)):
        yield age,sum(counts)

def test_traditional_way():
    start = time.clock()

    col = getCollection(config['inputURI'])
    count = {}
    cur = col.find()
    for row in cur:
        age = row['age']/10
        if age in count:
            count[age] += 1
        else:
            count[age] = 1

    end = time.clock()
    print "Time used: ", end-start
    for key in count:
        print key,count[key]
        

if __name__ == '__main__':


    job = Job().run(
            input = do_split(config),
            map = map,
            reduce = reduce,
            map_input_stream = mongodb_input_stream,
            )
    

    totalCount = 0;
    for age, count in result_iterator(job.wait(show=True)):
        range = str(age*10)+" -- "+str(age*10+9)
        totalCount += count
        print range, count

    print "total count : ", totalCount

    #test_traditional_way()

def test_chunks():

    uri = config['inputURI']
    useShards = config['useShards']
    slaveOk = config['slaveOk']
    database = "test"
    collection = "people"

    splits = MS.fetch_splits_via_chunks(config,uri,useShards,slaveOk);
    print splits

    '''
    print "splits count: %d" %len(splits)
    count = 0
    for split in splits:
        for row in split.cursor:
            count +=1
        
    print count
    '''

