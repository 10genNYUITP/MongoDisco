from disco.core import Job, result_iterator

def map(line, params):
    for word in record:
        yield word.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':
    job = Job().run(input=["mongodb://localhost/test/modforty"],
            map=map,
            reduce=reduce)
    for word, count in result_iterator(job.wait(show=True)):
        print word, count
