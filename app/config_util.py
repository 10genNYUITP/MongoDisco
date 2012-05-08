#!/usr/bin/env python
# encoding: utf-8

'''
File: config_util.py
Author: NYU ITP Team
Description: Configuration helper tool for MongoDB related Map/Reduce jobs Instance based, more idiomatic for those who prefer it to the static methoding of ConfigUtil
'''
import logging
import bson


'''
Global variable config:
Holds the configuration options for the application.
Default config is the configuration options set in this file.
'''

config = {
        "job_output_key" : "_id",
        "job_output_value" : "value",
        "input_uri" : "",
        "output_uri" : "mongodb://localhost/test.out",
        "print_to_stdout": False,
        "job_wait":True,
#        "INPUT_SPLIT_SIZE" : Value to specify how many docs input is split into. Affects the number of mappers.,
        "split_size" : 8,
        "split_key" : {'_id' : 1},
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
