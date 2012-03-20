from disco.core import Job, result_iterator
from mongoDisco_output import mongodb_output
from disco.worker.classic.func import task_output_stream

def map(line, params):
    for word in line.split():
        yield word,1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)


if __name__ == '__main__':

    mongodb_stream = tuple([mongodb_output])
    job = Job().run(input=["raw://it hi mi"],
            map=map,
            reduce=reduce,
            reduce_output_stream=mongodb_stream)

    for word, count in result_iterator(job.wait(show=True)):
        print word, count
