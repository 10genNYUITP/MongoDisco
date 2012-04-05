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
from mongoUtil import getConnection
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


        #self.cursor = self.get_cursor()
        #TODO this ^^  feels weird and very un-pythonic to me... -AF 2/27/12
        #replacing for now with below code VV

        #Assign cursor
        uri_info = uri_parser.parse_uri(inputURI)
        host = uri_info['nodelist'][0][0]
        port = uri_info['nodelist'][0][1]
        database_name = uri_info['database']
        collection_name = uri_info['collection']

        connection = getConnection(inputURI)
        db = connection[database_name]
        collection = db[collection_name]
        logging.info("LOOK HERE")
        logging.info("%s" % query)
        logging.info("%s" % fields)
        self.cursor = collection.find(query,fields) #.sort(sortSpec) doesn't work?
                                               # @todo support limit/skip --CW

        '''
        if self.noTimeout:
            # TODO should be something else? blank for now
            self.cursor.add_option()
        '''


    def write(self, out):
        """@todo: Docstring for write

        :out: @todo
        :returns: void
        """
        pass

    def read_fields(self, input):
        """read each field sequentially?
        see http://bit.ly/y2TjIj for corresponding f(n)

        :in: @todo
        :returns: void
        """
        pass

    def get_cursor(self):
        """Do a find operation

        :returns: a cursor with the split's query
        """


        ''' @todo Encasuplate these stuff into MongoConfigUtil
            call like MongoConfigUtil.getCollection(URI)
        '''
        uri_info = uri_parser.parse_uri(uri)
        host = uri_info['nodelist'][0][0]
        port = uri_info['nodelist'][0][1]
        database_name = uri_info['database']
        collection_name = uri_info['collection']

        connection = getConnection(uri)
        db = connection[database_name]
        collection = db[collection_name]
        self.cursor = collection.find(query,fields) #.sort(sortSpec) doesn't work?
                                               # @todo support limit/skip --CW
        if self.noTimeout:
            self.cursor.add_option()

            # self.cursor.slaveOk() read from the slave(s) by using slaveOk
            # find how to do it in python --CW




    def get_BSON_encoder(self):
        """@todo: Docstring for get_BSON_encoder

        :returns: a BSON Encoder object
        """
        pass

    def get_BSON_decoder(self):
        """@todo: Docstring for get_BSON_decoder

        :returns: a BSON Decoder
        """
        pass

    # NOT INCLUDING: getters/setters  for all the data members (this is Python, not Java ^_^)

    def hashCode(self):
        """@todo: Docstring for hashCode
        :returns: @todo
        """
        pass

    def __str__(self):
        return self.cursor

    def format_uri_with_query(self):
        """
        returns a formatted uri like scheme_mongo expects
        we put the query object in a json'd list so we can put it back
        into an ordered SON
        mongodb://local/test.in?query=<json of query>&limit=234&skip=293
        """
        base = self.inputURI
        #query is a bson object
        #- need to convert to a list of tuples and jsonify
        #o_l = []
        #o_l = []
        #query contains: "$query", "min", "max

        #o_l.append(("$query", self.query['$query']))
        '''
        o_q = self.query['$query']
        #o_l.update({"$query": self.query['$query']})
        if self.query.get("$min"):
            oid = str(self.query['$min'])
            o_q.update({'$min': oid})
            #o_l.append(("$min", oid))
        if self.query.get("$max"):
            oid = str(self.query['$max'])
            o_q.update({'$max': oid})
            #o_l.append(("$max", oid))

        o_l.append(("$query", o_q))
        print 'o_l: ', o_l
        #need to rearrange any dict with an ObjectID to look live  VV
        # '{"_id": {"$oid": "4edebd262ae5e93b41000000"}}'



        base += '?query='
        base += json.dumps(self.query, default=json_util.default)
        #TODO add options of

        '''
        queryObj = son.SON()
        queryObj['inputURI'] = self.inputURI
        queryObj['keyField'] = self.keyField
        queryObj['query'] = self.query
        queryObj['fields'] = self.fields
        queryObj['sort'] = self.sort
        queryObj['limit'] = self.limit
        queryObj['skip'] = self.skip
        queryObj['timeout'] = self.timeout

        print
        str = json.dumps(queryObj,default=json_util.default)
        print str
        return str
