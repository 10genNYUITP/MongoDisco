from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from MongoSplitter import calculate_splits as do_split
import logging

config = {
        "db_name": "test",
        "collection_name": "modforty",
        "splitSize": 1, #MB
        "inputURI": "mongodb://localhost/test.modforty",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        "outputURI":"mongodb://localhost/test.out",
        "job_output_key":"I am the key",
        "job_output_Value":"I ame the value"
        }

def map(record, params):
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':


    import os, sys, inspect
    
    job = Job().run(
            input= do_split(config),
            map=map,
            reduce=reduce,
            map_input_stream = mongodb_input_stream,
            reduce_output_stream=mongodb_output_stream)

    job.wait(show=True)

