import pymongo
import bson
from bson import json_util
import warnings
from cStringIO import StringIO
from pymongo import Connection, uri_parser, ReadPreference
import bson.son as son
import json
import logging

def open(url=None, task=None):
    from mongo_util import getCollection

    query = son.SON(json.loads(url, object_hook=json_util.object_hook))
    uri = query['inputURI']
    uri_info = uri_parser.parse_uri(uri)
    spec = query['query']
    fields = query['fields'] 
    skip = query['skip'] 
    limit = query['limit'] 
    timeout = query['timeout'] 
    sort = query['sort'] 


    #go around: connect to the sonnection then choose db by ['dbname']

    collection = getCollection(uri)
    cursor = collection.find(spec = spec, fields = fields, skip = skip, limit = limit, sort = sort, timeout = timeout)

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
    from scheme_mongodb import open
    mon = open(url)
    return mon


'''
def getConnection(uri):
    uri_info = uri_parser.parse_uri(uri)
    nodes = set()
    host = None
    port = None
    nodes.update(uri_info["nodelist"])

    if len(nodes) == 1: #How to handle multiple nodes?
        for node in nodes:
            host = node[0]
            port = node[1]

    connection = Connection(host=host,port=port)
    
    return connection
    

def getCollection(uri):

    uri_info = uri_parser.parse_uri(uri)
    username = None
    password = None
    db = None
    username = uri_info["username"] or username
    password = uri_info["password"] or password
    db = uri_info["database"]
    col = uri_info["collection"]

    connection = getConnection(uri)
    
    if username:
        if not connection[db].authenticate(username,password):
            raise ConfigurationError("authentication failed")

    return connection[db][col]
'''
