from disco.core import Job, result_iterator
from mongodb_io import mongodb_output_stream
from disco.worker.classic.func import task_output_stream

import logging

def map(record, params):
    logging.info("%s" % record.get('_id'))
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':
    job = Job().run(input=["mongodb://localhost/test.modforty"],
            map=map,
            reduce=reduce,
            reduce_output_stream=mongodb_output_stream)

    job.wait(show=True)

