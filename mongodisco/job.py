# Copyright 2012 10gen, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

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

    DEFAULT_CONFIG = {
        "job_output_key" : "_id",
        "job_output_value" : "value",
        "input_uri" : "mongodb://localhost/test.in",
        "output_uri" : "mongodb://localhost/test.out",
        "print_to_stdout": False,
        "job_wait":True,
        "split_size" : 8,
        "split_key" : {"_id" : 1},
        "create_input_splits" : True,
        "splits_use_shards" : False,
        "splits_use_chunks" : True,
        "slaveok" : False,
        "limit" : 0,
        "skip" : 0,
        "inputKey" : None,
        "sort" : None,
        "timeout" : False,
        "fields" : None,
        "query" : {}
    }

    def __init__(self,config,map,reduce):
        self.config = DiscoJob.DEFAULT_CONFIG.copy()
        self.config.update(config)

        self.map = map
        self.reduce = reduce
        self.job = Job()
        self.params = Params(**self.config)

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


