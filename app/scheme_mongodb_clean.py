import pymongo
import bson
from bson import json_util
import warnings
from cStringIO import StringIO
from pymongo import Connection, uri_parser, ReadPreference
import bson.son as son
import json
import logging

def open(uri, task=None):
    #parses a mongodb uri and returns the database
    #"mongodb://localhost/test.in?"
    #If you need more configuration we highly recommend using the mongodb-disco adapter
    uri_info = uri_parser.parse_uri(uri)
    #go around: connect to the sonnection then choose db by ['dbname']
    #TODO ^^
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        connection = Connection(uri)
        database_name = uri_info['database']
        collection_name = uri_info['collection']
        db = connection[database_name]
        collection = db[collection_name]
        cursor = collection.find()
        wrapper = MongoWrapper(cursor)
        return wrapper


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
