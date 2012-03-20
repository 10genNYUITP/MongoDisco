import pymongo
from pymongo import Connection,uri_parser
from pymongo.errors import ConfigurationError


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

if __name__ == '__main__':

    uri = "mongodb://disco:disco@localhost/test.in"

    conn = getConnection(uri)
    print conn.database_names()

    coll = getCollection(uri)
    print coll.count()


