from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from disco.schemes.scheme_mongodb import input_stream
from disco.worker.classic.func import task_output_stream
from app.MongoSplitter import calculate_splits as do_split

import logging

config = {
        "db_name": "test",
        "collection_name": "modforty",
        "splitSize": 1, #MB
        "inputURI": "mongodb://localhost/test.modforty",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        }

def map(record, params):
    logging.info("%s" % record.get('_id'))
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':

#    logging.getLogger().setLevel(logging.DEBUG)
    #mongodb_input_stream = (input_stream,)
    input_s_splits =  do_split(config)
    #print input_s_splits
    
    for i in input_s_splits:
        i.format_uri_with_query()
    #split = input_s_splits[0]
    
    #back_dump = split.format_uri_with_query()
    #input_urls = [split.format_uri_with_query() for split in input_s_splits]
	#use scheme to 

 #   job = Job().run(
  #          input=["mongodb://localhost/test.modforty"],
            #input= input_urls,
   #         map=map,
    #        reduce=reduce,
     #       map_input_stream = mongodb_input_stream,
      #      reduce_output_stream=mongodb_output_stream)

    #job.wait(show=True)

