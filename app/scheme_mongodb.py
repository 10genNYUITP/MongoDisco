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
        #another easy way to parse parameters
        #from urlparse import parse_qs
        #return a dict {'key',['value']}
        #e.g {'limit':['100'],'skip':['99']}
        #dict_of_params = parse_qs(params)

        list_of_params = params.split('&', 1)
        for p in params:
            name, json_obj = params.split('=') #shouldn't be p.split('=')?
            if name == 'query':
                query = son.SON(json.loads(json_obj, object_hook=json_util.object_hook))
            elif name == 'fields':
                pass
            elif name == 'limit' or name == 'skip':
                pass
            elif name == 'timeout' or name == 'slave_okay':
                pass
            elif name == 'sort':
                pass
            else:
                options[name] = json_obj
                #@to-do get other parameters from url
                #do type convertion as needed

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

    spec = query
    fields = options['fields'] if 'fields' in options else None #list or dict
    skip = options['skip'] if 'skip' in options else 0 #int
    limit = options['limit'] if 'limit' in options else 0 #int
    timeout = options['timeout'] if 'timeout' in options else True #bool
    sort = options['sort'] if 'sort' in options else None #list of (key,direction) pair
    slave_okay = options['slave_oky'] if 'slave_okay' in options else False #bool
    read_preference = options['read_preference'] in 'read_preference' in options else ReadPreference.PRIMARY #pymongo.ReadPreference


    #go around: connect to the sonnection then choose db by ['dbname']
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        connection = Connection(uri)
        database_name = uri_info['database']
        collection_name = uri_info['collection']
        db = connection[database_name]
        collection = db[collection_name]

        #cursor =  collection.find(query, None)
        curson = collection.find(spec = spec, fileds = fields, skip = skip, limit = limit, sort = sort, slave_okay = slave_okay, read_preference = read_preference)

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
        return self.cursor

    def __len__(self):
        #may need to do this more dynamically (see lib/disco/comm.py ln 163)
        #may want to cache this
        return self.cursor.count()

    def close(self):
        self.cursor.close()

    def read(self, size=-1):
        #raise a non-implemented error to see if this ever pops up
        raise Exception("read is not implemented- investigate why this was called")


def input_stream(stream, size, url, params):
    mon = open(url)
    return mon
