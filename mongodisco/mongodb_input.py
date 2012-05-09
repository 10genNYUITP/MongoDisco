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
    from mongo_util import get_collection

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

    collection = get_collection(uri)
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
    from mongodb_input import open
    mon = open(url)
    return mon

