import pymongo
from pymongo import Connection,uri_parser
from pymongo.errors import ConfigurationError


def get_connection(uri):

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

    connection = Connection(uri)

    return connection
    

def get_database(uri):
    uri_info = uri_parser.parse_uri(uri)
    username = None
    password = None
    db = None

    username = uri_info["username"] or username
    password = uri_info["password"] or password
    db = uri_info["database"]

    connection = get_connection(uri)
    if username:
        if not connection[db].authenticate(username,password):
            raise ConfigurationError("authentication failed")

    return connection[db]

def get_collection(uri):

    uri_info = uri_parser.parse_uri(uri)
    col = uri_info["collection"]
    database = get_database(uri)

    return database[col]

if __name__ == '__main__':

    uri = "mongodb://disco:disco@localhost/test.in"
    #uri = "mongodb://localhost/test.in"

    conn = get_connection(uri)
    print conn.database_names()

    db = get_database(uri)
    print db.collection_names()

    coll = get_collection(uri)
    print coll.count()


