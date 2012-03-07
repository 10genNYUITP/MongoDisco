import pymongo
from pymongo import Connection, uri_parser

def open(url=None, task=None):
    #parses a mongodb uri and returns the database??
    #say url is mongo://dbname.collectionName

    uri = url if url else "mongodb://localhost/test.in"
    #config.getInputURI()
    uri_info = uri_parser.parse_uri(uri)

    print uri_info
    return
    host = uri_info['nodelist'][0][0]
    port = uri_info['nodelist'][0][1]
    database_name = uri_info['database']
    collection_name = uri_info['collection']
    #connect to mongodb
    return mongodb

def input_stream(size, url, params):
    return open()

if __name__ == '__main__':
    open()

