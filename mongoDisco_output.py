
class MongoDBoutput(object):
    '''Output stream for mongoDB 
    '''
    def __init__(self,stream,params):
        #import pymongo
        #from pymongo import Connection,uri_parser
        import mongoUtil
        self.uri = "mongodb://localhost/test.out"
        #self.uri = url if url else "mongodb://localhost/test.out"
        self.conn = mongoUtil.getConnection(uri)
        self.coll = mongoUtil.getCollection(uri)
        self.stream = stream
        self.params = params

        self.key_name = "key"#Should read from config
        self.value_name = "value"


    def add(self,key,val):
        result_dict = {}
        k, v = str(k), str(v)
        result_dict[key] = val
        self.coll.insert(result_dict)

    def close(self):
        self.conn.close()

def mongodb_output(stream,partition,url,params):
    from mongoDisco_output import MongoDBoutput
    return MongoDBoutput(stream,params)



