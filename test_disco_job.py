from job import DiscoJob


import logging

config = {
        "split_size": 1, #MB
        "input_uri": "mongodb://localhost/test.modforty",
        "create_input_splits": True,
        "split_key": {'_id' : 1},
        "output_uri":"mongodb://localhost/test.out",
        "job_output_key":"I am key name",
        "job_output_value":"I ame value name",
        "job_wait":True,
        "print_to_stdout":False
        }

def map(record, params):
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':

    DiscoJob(config = config,map = map,reduce = reduce).run()
    

