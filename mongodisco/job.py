#!/usr/bin/env python
# encoding: utf-8
'''
File: DiscoJob.py
Author: NYU ITP team
Description: Disco Job Wrapper

'''
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from mongodisco.mongodb_io import mongodb_output_stream, mongodb_input_stream
from splitter import calculate_splits as do_split


class DiscoJob():


    def __init__(self,config,map,reduce):
        import config_util

        self.config = config_util.config
        #if the user doesn't specify output, print to stdout
        if not config.get('output_uri') and not config.get('print_to_stdout'):
            config['print_to_stdout'] = True

        for item in config:
            self.config[item] = config[item]

        self.map = map
        self.reduce = reduce
        self.job = Job()
        self.params = Params()
        for key in self.config:
            self.params.__dict__[key] = self.config[key]

    def run(self):

        if self.config['print_to_stdout']:

            self.job.run(input = do_split(self.config),
                     map = self.map,
                     reduce = self.reduce,
                     params = self.params,
                     map_input_stream = mongodb_input_stream,
                     required_modules= ['mongodisco.mongodb_io',
                                        'mongodisco.mongodb_input',
                                        'mongodisco.config_util',
                                        'mongodisco.mongo_util',
                                        'mongodisco.mongodb_output'])
            for key, value in result_iterator(self.job.wait(show=True)):
                print key, value

        else:
            self.job.run(input = do_split(self.config),
                     map = self.map,
                     reduce = self.reduce,
                     params = self.params,
                     map_input_stream = mongodb_input_stream,
                     reduce_output_stream = mongodb_output_stream,
                     required_modules= ['mongodisco.mongodb_io',
                                        'mongodisco.mongodb_input',
                                        'mongodisco.config_util',
                                        'mongodisco.mongo_util',
                                        'mongodisco.mongodb_output'])

            if self.config.get("job_wait",False):
                self.job.wait(show=True)


