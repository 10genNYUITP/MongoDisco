from DiscoJob import DiscoJob


import logging

config = {
        "split_size": 1, #MB
        "input_uri": "mongodb://localhost/test.modforty",
        "create_input_splits": True,
        "split_key": {'_id' : 1},
        "output_uri":"mongodb://localhost/test.out",
        #"job_output_key":"I am the key",
        "job_output_value":"I ame the value",
        "job_wait":True
        }

def map(record, params):
    yield record.get('name', "NoName"), 1

def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)

if __name__ == '__main__':


    '''
    job = Job().run(
            #input=["mongodb://localhost/test.modforty"],
            input= do_split(config),
            map=map,
            reduce=reduce,
            map_input_stream = mongodb_input_stream,
            reduce_output_stream=mongodb_output_stream)

    job.wait(show=True)
    '''

    DiscoJob(config = config,map = map,reduce = reduce).run()
    

