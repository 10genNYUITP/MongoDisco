#!/usr/bin/env python
# encoding: utf-8

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
        "input_uri": "mongodb://localhost/test.yield_historical.in",
        "slave_ok": True,
        "use_shards": True,
        "create_input_splits": True,
        "use_chunks": True,
        "print_to_stdout": True}


def map(record, params):
    year = record.get('_id').year
    yield year, record['bc10Year']


def reduce(iter, params):
    from disco.util import kvgroup
    for year, bid_prices in kvgroup(sorted(iter)):
        bd = [i for i in bid_prices]
        yield year, sum(bd)/ len(bd)


if __name__ == '__main__':
    from mongodisco.job import DiscoJob
    DiscoJob(config=config, map=map, reduce=reduce).run()

