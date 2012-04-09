import pymongo
from pymongo import Connection,uri_parser
from pymongo.errors import ConfigurationError


def getConnection(uri):

    '''
    A better way to implement connection via uri
    mongodb://admin:admin@localhost/test.in
    '''
    db = None
    username = None
    uri_info = uri_parser.parse_uri(uri)
    db = uri_info['database'] or db
    username = uri_info['username'] or username

    if db:
        idx = uri.rfind('/')
        uri = uri[0:idx]

    if db and username:
        idx = uri.rfind('/')
        idy = uri.rfind('@')
        uri = uri[0:idx+1]+uri[idy+1:]

    print uri
    connection = Connection(uri)

    '''
    db = None
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
    '''

    return connection
    

def getDatabase(uri):
    uri_info = uri_parser.parse_uri(uri)
    username = None
    password = None
    db = None

    username = uri_info["username"] or username
    password = uri_info["password"] or password
    db = uri_info["database"]

    connection = getConnection(uri)
    if username:
        if not connection[db].authenticate(username,password):
            raise ConfigurationError("authentication failed")

    return connection[db]

def getCollection(uri):

    '''
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
    '''
    uri_info = uri_parser.parse_uri(uri)
    col = uri_info["collection"]
    database = getDatabase(uri)

    return database[col]

if __name__ == '__main__':

    uri = "mongodb://disco:disco@localhost/test.in"
    #uri = "mongodb://localhost/test.in"

    conn = getConnection(uri)
    print conn.database_names()

    db = getDatabase(uri)
    print db.collection_names()

    coll = getCollection(uri)
    print coll.count()


