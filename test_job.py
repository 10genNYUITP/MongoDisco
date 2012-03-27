from disco.core import Job, result_iterator
from mongoDisco_output import MongoDBoutput
from disco.worker.classic.func import task_output_stream
import logging

def map(record, params):
    logging.info("%s" % record.get('_id'))
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

def mongodb_output(stream,partition,url,params):
    return mongoDisco_output.MongoDBoutput(stream,params)

if __name__ == '__main__':
    mongodb_stream = tuple([mongodb_output])
    job = Job().run(input=["mongodb://localhost/test.modforty"],
            map=map,
            reduce=reduce,
            reduce_output_stream=mongodb_stream)
    

    job.wait(show=True)
#    for word, count in result_iterator(job.wait(show=True)):
 #       print word, count
