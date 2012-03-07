import pymongo
from pymongo import Connection, uri_parser

def open(url=None, task=None):
    #parses a mongodb uri and returns the database
    #"mongodb://localhost/test.in?query='SON[()]'"
    uri = url if url else "mongodb://localhost/test.in"

    uri_info = uri_parser.parse_uri(uri)
    params = uri.split('?', 1)
    query = None
    #TODO flow from a query
    if len(params) > 1 :
        params = params[1]
        name, query = params.split('=')
        #turn the query into a SON object

    if not query:
        query = {}
    connection = Connection(uri)
    database_name = uri_info['database']
    collection_name = uri_info['collection']
    db = connection[database_name]
    collection = db[collection_name]

    cursor =  collection.find(query) #.sort(sortSpec) doesn't work?
    #get all
    return [entry for entry in cursor]




def input_stream(size, url, params):
    return open(url)


if __name__ == '__main__':
    #just for testing
    print open()

