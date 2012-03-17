from disco.core import Job, result_iterator
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
            reduce=reduce)
    for word, count in result_iterator(job.wait(show=True)):
        print word, count
