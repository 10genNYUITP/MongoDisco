import pymongo
import bson
from bson import json_util
import warnings
from cStringIO import StringIO
from pymongo import Connection, uri_parser
import bson.son as son
import json
import logging

def open(url=None, task=None):
    #parses a mongodb uri and returns the database
    #"mongodb://localhost/test.in?query='{"key": value}'"
    uri = url if url else "mongodb://localhost/test.in"

    #print 'uri: ' + uri
    params = uri.split('?', 1)
    uri = params[0]
    uri_info = uri_parser.parse_uri(uri)
    query = None
    #TODO test flow from a query
    #parse json to a dict = q_d
    # ^^ this is where we use json_util.object_hook
    #SON()['query'] = q_d['query']
    #for k,v  in q_d.iteritems:
    #   if k not "query":
    #      SON[k] = v
    options = {}
    if len(params) > 1:
        params = params[1]
        list_of_params = params.split('&', 1)
        for p in params:
            name, json_obj = params.split('=')
            if name == 'query':
                query = son.SON(json.loads(json_obj, object_hook=json_util.object_hook))
            else:
                options[name] = json_obj

        '''
        query = son.SON()
        li_q = json.loads(json_query)
        for tupl in li_q:
            if tupl[0] == "$max" or tupl[0] == "$min":
                obj_id = bson.objectid.ObjectId(tupl[1])
                query[tupl[0]] = {u'_id' : obj_id}
            else:
                query[tupl[0]] = tupl[1]
        '''
    if not query:
        query = {}

    #go around: connect to the sonnection then choose db by ['dbname']
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        connection = Connection(uri)
        database_name = uri_info['database']
        collection_name = uri_info['collection']
        db = connection[database_name]
        collection = db[collection_name]

        cursor =  collection.find(query, None)

        wrapper = MongoWrapper(cursor)
        return wrapper
        #WRAPPED!


class MongoWrapper(object):
    """Want to wrap the cursor in an object that
    supports the following operations: """

    def __init__(self, cursor):
        self.cursor = cursor
        self.offset = 0

    def __iter__(self):
        #most important method
        for rec in self.cursor:
            yield rec

    def __len__(self):
        #may need to do this more dynamically (see lib/disco/comm.py ln 163)
        return self.cursor.count()

    def close(self):
        self.cursor.close()

    @property
    def read(self, size=-1):
        list_of_records = []
        if size > 0:
            for i in range(size):
                list_of_records.append(self.cursor.__iter__())
        return list_of_records


def input_stream(stream, size, url, params):
    mon = open(url)
    return mon
