
class MongoOutput(object):
    '''Output stream for mongoDB 
    '''
    def __init__(self,stream,params):
        import pymongo
        from pymongo import Connection,uri_parser
        from mongo_util import getConnection,getCollection
        
        config = {}
        for key, value in params.__dict__.iteritems():
            config[key] = value

        self.uri =  config.get('output_uri','mongodb://localhost/test.out')
        self.conn = getConnection(self.uri)
        self.coll = getCollection(self.uri)

        self.stream = stream
        self.params = params

        self.key_name = config.get('job_output_key','_id')
        self.value_name = config.get('job_output_value','value')


    def add(self,key,val):
        result_dict = {}
        result_dict[self.key_name] = key
        result_dict[self.value_name] = val
        self.coll.insert(result_dict)

    def close(self):
        self.conn.close()


def mongodb_output(stream,partition,url,params):
    from mongodb_output import MongoOutput
    return MongoOutput(stream,params)



