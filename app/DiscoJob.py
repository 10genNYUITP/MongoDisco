#!/usr/bin/env python
# encoding: utf-8
'''
File: DiscoJob.py
Author: NYU ITP team
Description: Disco Job Wrapper

'''
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from MongoSplitter import calculate_splits as do_split


class DiscoJob():
    

    def __init__(self,config,map,reduce):
        from MongoConfigUtil import Configuration
        Configuration.read_config(config)
        
        '''
        for item in config:
            DiscoJob.config[item] = config[item]
        '''

        self.config = Configuration.get_config()
        print self.config
        self.map = map
        self.reduce = reduce
        self.job = Job()
        self.params = Params()
        for key in self.config:
            self.params.__dict__[key] = self.config[key]

    def run(self):
        from MongoConfigUtil import Configuration

        self.job.run(input = do_split(self.config),
                     map = self.map,
                     reduce = self.reduce,
                     params = self.params,
                     map_input_stream = mongodb_input_stream,
                     reduce_output_stream = mongodb_output_stream,
                     required_modules= [('mongodb_io','/home/changlewang/Programming/MongoDisco/app/mongodb_io.py'),
                               ('mongodb_output','/home/changlewang/Programming/MongoDisco/app/mongodb_output.py'),
                               ('scheme_mongodb','/home/changlewang/Programming/MongoDisco/app/scheme_mongodb.py'),
                               ('MongoConfigUtil','/home/changlewang/Programming/MongoDisco/app/MongoConfigUtil.py'),
                               ('mongo_util','/home/changlewang/Programming/MongoDisco/app/mongo_util.py')])

        if self.config.get("job_wait",False):
            self.job.wait(show=True)
            #for word,count in result_iterator(self.job.wait(show=True)):
            #    print word,count


