
class MongoDBoutput(object):
    '''Output stream for mongoDB 
    '''
    def __init__(self,stream,params):
        import pymongo
        from pymongo import Connection,uri_parser
        self.uri = "mongodb://localhost/test.out"
        #self.uri = url if url else "mongodb://localhost/test.out"

        uri_info = uri_parser.parse_uri(self.uri)
        nodes = set()
        host = None
        port = None
        username = None
        password = None
        db = uri_info['database']
        col = uri_info['collection']

        nodes.update(uri_info['nodelist'])

        if len(nodes) == 1:
            for node in nodes:
                host = node[0]
                port = node[1]

        self.conn = Connection(host=host,port=port)

        username = uri_info['username'] or username
        password = uri_info['password'] or password

        if username:
            if not self.conn[db].authenticate(username,password):
                raise ConfigurationError("authentication failed")

        self.coll = self.conn[db][col]
        #self.conn = mongoUtil.getConnection(uri)
        #self.coll = mongoUtil.getCollection(uri)
        self.stream = stream
        self.params = params

        self.key_name = "word"#Should read from config
        self.value_name = "count"


    def add(self,key,val):
        result_dict = {}
        #key, val = str(key), str(val)
        #result_dict[key] = val
        result_dict[self.key_name] = key
        result_dict[self.value_name] = val
        self.coll.insert(result_dict)

    def close(self):
        self.conn.close()

#def mongodb_output(stream,partition,url,params):
#    return MongoDBoutput(stream,params)



