import pymongo
from pymongo import Connection, uri_parser

def open(url=None, task=None):
    #parses a mongodb uri and returns the database??
    #say url is mongo://dbname.collectionName

    uri = url if url else "mongodb://localhost/test.in"
    #"mongodb://localhost/test.in?query='SON[()]'"
    #config.getInputURI()
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
    results =  [entry for entry in cursor]
    return results
    print params

    print uri_info



def input_stream(size, url, params):
    return open()

if __name__ == '__main__':
    print open()

