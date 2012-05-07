#!/usr/bin/env python
# encoding: utf-8

from job import DiscoJob

'''
File: word_count.py
Description: An example that tests the MongoHadoop adapter to read a text
file, print the distinct words in the file and their count.
Author/s: NYU ITP Team
To run: Run as python word_count.py
'''

'''
Configuration settings for Mongo-Disco adapter for this example
'''
config = {
        "input_uri": "mongodb://localhost/test.wc",
        "output_uri": "mongodb://localhost/test.out",
        "slaveOk": True,
        "useShards": True,
        "createInputSplits": True,
        "useChunks": True,
        "print_to_stdout" : True}


def map(word, params):
    yield word.get('file_text'), 1


def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)


if __name__ == '__main__':

    DiscoJob(config=config, map=map, reduce=reduce).run()

