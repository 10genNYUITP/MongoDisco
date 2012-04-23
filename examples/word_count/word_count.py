#!/usr/bin/env python
# encoding: utf-8

from app.MongoSplitter import calculate_splits
from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream, mongodb_input_stream


config = {
        "inputURI":"http://discoproject.org/media/text/chekhov.txt",
        "outputURI":"mongodb://localhost/test.out",
        "slaveOk":True,
        "useShards":True,
        "createInputSplits":True,
        "useChunks":True
        }

def map(line, params):
    for word in line.split():
        yield word, 1

def reduce(iter, params):
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':
    job = Job().run(input=calculate_splits(config) ,
            map = map,
            reduce = reduce,
            map_input_stream = mongodb_input_stream
            )

    for word, count in result_iterator(job.wait(show=True)):
        print word, count
