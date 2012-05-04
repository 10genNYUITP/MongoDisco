#!/usr/bin/env python
# encoding: utf-8
'''
File: DiscoJob.py
Author: NYU ITP team
Description: Disco Job Wrapper

'''
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from disco.worker.classic.modutil import locate_modules,find_modules
from mongodb_io import mongodb_output_stream,mongodb_input_stream
from MongoSplitter import calculate_splits as do_split


class DiscoJob():
    

    def __init__(self,config,map,reduce):
        from MongoConfigUtil import Configuration
        Configuration.read_config(config)
        
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
                     required_modules= ['mongodb_io',
                                        'scheme_mongodb',
                                        'MongoConfigUtil',
                                        'mongo_util',
                                        'mongodb_output'])

        if self.config.get("job_wait",False):
            self.job.wait(show=True)


