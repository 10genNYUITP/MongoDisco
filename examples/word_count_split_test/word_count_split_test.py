#!/usr/bin/env python
# encoding: utf-8

from app.MongoSplitter import calculate_splits
from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream, mongodb_input_stream
import bson
from app.mongoUtil import getCollection

config = {
        "inputURI": "mongodb://localhost:30000/test.lines",
        "outputURI": "mongodb://localhost/test.out",
        "slaveOk": True,
        "useShards": True,
        "createInputSplits": True,
        "useChunks": True}


def map(line, params):
    for word in line.split():
        yield word, 1


def reduce(iter, params):
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)


def test(use_shards, use_chunks, slaveOk, useQuery):
    if useQuery:
        query = bson.son.SON()
        mod = bson.son.SON()
        mod[2] = 0
        query['$num'] = mod

    if use_chunks:
        if use_shards:
            output_table = "with_shards_and_chunks"
        else:
            output_table = "with_chunks"
    else:
        if use_shards:
            output_table = "with_shards"
        else:
            output_table = "no_splits"

    if slaveOk != None:
        output_table += "_" + slaveOk

    config['outputURI'] = "mongodb://localhost:30000/test." + output_table
    job = Job().run(input=calculate_splits(config),
            map=map,
            reduce=reduce,
            map_input_stream=mongodb_input_stream)

    for word, count in result_iterator(job.wait(show=True)):
        print word, count
    q = bson.son.SON()
    q['_id'] = "the"
    col = getCollection(config['outputURI'])
    cur = col.find_one(q)
    for data in cur:
        print("the count of \'the\' is: %d" % data)


if __name__ == '__main__':
    useQuery = False
    tf = [True, False]
    ntf = [None, True, False]
    for use_shards in tf:
        for use_chunks in tf:
            for slaveOk in ntf:
                test(use_shards, use_chunks, slaveOk, useQuery)
