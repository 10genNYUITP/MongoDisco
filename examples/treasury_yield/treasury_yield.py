#!/usr/bin/env python
# encoding: utf-8

import datetime
from app.MongoSplitter import calculate_splits
from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream, mongodb_input_stream

"""
Description: calculate the average 10 year treasury bond yield for given data.
Note: run parse_yield_historical.py first to populate the mongodb with data.

example record:
{ "_id" : { "$date" : 633571200000 },
"dayOfWeek" : "MONDAY",
"bc3Year" : 8.390000000000001,
"bc5Year" : 8.390000000000001,
"bc10Year" : 8.5,
"bc20Year" : None,
"bc1Month" : None,
"bc2Year" : 8.300000000000001,
"bc3Month" : 8,
"bc30Year" : 8.539999999999999,
"bc1Year" : 8.08,
"bc7Year" : 8.449999999999999,
"bc6Month" : 8.09 }
"""

#this is the config file for the  mongosplitter
config = {
        "inputURI":"mongodb://localhost/yield_historical.in",
        "slaveOk":True,
        "useShards":True,
        "createInputSplits":True,
        "useChunks":True
        }

def map(record, params):
    time = record['_id']['$date']/1000
    year =  datetime.datetime.fromtimestamp(time).date().year
    yield year, record['bc10year']

def reduce(iter, params):
    from disco.util import kvgroup
    for year, bid_prices in kvgroup(sorted(iter)):
        avg = sum(bid_prices)/len(bid_prices)
        yield year, avg

if __name__ == '__main__':
    job = Job().run(input=calculate_splits(config) ,
            map = map,
            reduce = reduce,
            map_input_stream = mongodb_input_stream
            )

    for year, avg in result_iterator(job.wait(show=True)):
        print ("Average 10 Year treasury for %s was %s" % (year, avg))
