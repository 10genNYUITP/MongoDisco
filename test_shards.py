import time
from job import DiscoJob
from disco.core import Job,result_iterator
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from splitter import calculate_splits as do_split
import pymongo
from pymongo import Connection
from mongo_util import get_collection

import logging

config = {
        "input_uri":"mongodb://localhost/test.people",
        "output_uri":"mongodb://localhost/test.out",
        "slave_ok":True,
        "use_shards":False,
        "create_input_splits":True,
        "use_chunks":False,
        "job_output_key":"age",
        "job_output_value":"number"
        }


def map(record,params):
    age = record.get('age',0)/10
    range = str(age*10)+"--"+str(age*10+9)
    yield range, 1

def reduce(iter,params):
    from disco.util import kvgroup
    for age,counts in kvgroup(sorted(iter)):
        yield age,sum(counts)

def test_traditional_way():
    start = time.clock()

    col = get_collection(config['input_uri'])
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


    DiscoJob(config=config,map=map,reduce=reduce).run()
    

