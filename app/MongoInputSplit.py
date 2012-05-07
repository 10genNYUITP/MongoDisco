#!/usr/bin/env python
# encoding: utf-8
'''
File: MongoInputSplit.py
Author: NYU ITP team
Description: Holds the specification for an individual
    split as calculated by MongoSplitter.py
'''
import sys, os, logging
import json
from bson import json_util,son
from mongo_util import getConnection,getCollection
from pymongo import Connection, uri_parser
from pymongo.uri_parser import (_partition,
                                _rpartition,
                                parse_userinfo,
                                split_hosts,
                                split_options,
                                parse_uri)

# TODO: Find out if we need to extend some disco inputSplit (02/22/12, 21:54, AFlock)
class MongoInputSplit():
    """
    Will hold spec for an individual split,
    and is able to pass that split's data to disco
    """

    def __init__(self, inputURI, keyField, query, fields=None, sort=None, limit=0, skip=0, timeout=True):
        self.inputURI = inputURI
        self.keyField = keyField
        self.query = query
        self.fields = fields
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.timeout = timeout


        logging.info("LOOK HERE")
        logging.info("%s" % query)
        logging.info("%s" % fields)


    def format_uri_with_query(self):
        """
        returns a formatted uri like scheme_mongo expects
        we put the query object in a json'd list so we can put it back
        into an ordered SON
        mongodb://local/test.in?query=<json of query>&limit=234&skip=293
        """
        base = self.inputURI
        queryObj = son.SON()
        queryObj['inputURI'] = self.inputURI
        queryObj['keyField'] = self.keyField
        queryObj['query'] = self.query
        queryObj['fields'] = self.fields
        queryObj['sort'] = self.sort
        queryObj['limit'] = self.limit
        queryObj['skip'] = self.skip
        queryObj['timeout'] = self.timeout

        str = json.dumps(queryObj,default=json_util.default)
        return str

    def __str__(self):
        return self.format_uri_with_query()
