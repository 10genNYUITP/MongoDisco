import pymongo
import bson
import warnings
from cStringIO import StringIO
from pymongo import Connection, uri_parser
import bson.son as son
import json

def open(url=None, task=None):
    #parses a mongodb uri and returns the database
    #"mongodb://localhost/test.in?query='{"key": value}'"
    uri = url if url else "mongodb://localhost/test.in"

    uri_info = uri_parser.parse_uri(uri)
    params = uri.split('?', 1)
    query = None
    #TODO test flow from a query
    if len(params) > 1 :
        params = params[1]
        name, json_query = params.split('=')
        #turn the query into a SON object
        query = son.SON()
        li_q = json.loads(json_query)
        for tupl in li_q:
            if tupl[0] == "$max" or tupl[0] == "$min":
                obj_id = bson.objectid.ObjectId(tupl[1])
                query[tupl[0]] = {u'_id' : obj_id}
            else:
                query[tupl[0]] = tupl[1]
    if not query:
        query = {}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        connection = Connection(uri)
        database_name = uri_info['database']
        collection_name = uri_info['collection']
        logging.warning(collection)
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
