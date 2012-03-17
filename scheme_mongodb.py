import pymongo
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
    #TODO flow from a query
    if len(params) > 1 :
        params = params[1]
        name, json_query = params.split('=')
        #turn the query into a SON object
        query = son.SON()
        li_q = json.loads(json_query)
        for tupl in li_q:
            query[tupl[0]] = tupl[1]
    if not query:
        query = {}

    connection = Connection(uri)
    database_name = uri_info['database']
    collection_name = uri_info['collection']
    db = connection[database_name]
    collection = db[collection_name]

    cursor =  collection.find(query) #.sort(sortSpec) doesn't work?
    #get all  or just cursor? - basically we need to specify how to read this data somewhere.
    #return [entry for entry in cursor]

    wrapper = MongoWrapper(cursor)
    return wrapper
    #WRAPPED!


class MongoWrapper(object):
    """Want to wrap the cursor in an object that
    supports the following operations: """

    #for now, I am treating the quata of this object as one record (may need to change to bytes but hopefully not) -AF 3/10/12
    # Referencing lib/disco/comm.py,
    # but I think much of the internals of that can be simplified in our case since reading from mongo is much easier than reading from a url connection
    # for instance, I think the StringIO buffer may not be necessary at all -AF 3/10

    def __init__(self, cursor):
        self.cursor = cursor
        #self.cursor.batchsize(1) #so will only return one result per request
        #self.buf = None
        self.offset = 0
        #self.orig_offset = 0
        #self.eof = False #!self.cursor.alive (needs update after every read?
        #self.read(1)
        #self.i = 0 #Dont understand what self.i is really for... some intermediate offset? A count of how much has been read?

    def __iter__(self):
        #most important method
        #get
        for rec in self.cursor:
            yield rec
        '''
        chunk = self._read_chunk(1)
        while chunk:
            next_chunk = self._read_chunk(1)
            yield chunk
            chunk = next_chunk
        '''

    def __len__(self):
        #need to do this more dynamically (see lib/disco/comm.py ln 163)
        # in order for _read_chunk to work properly
        return self.cursor.count()

    def close(self):
        self.cursor.close()
'''#{{{

    def read(self, size=-1):
        buf = StringIO()
        #write a record to buf if record
        if size > 0:
            #seems a bit roundabout if buf.getValue just returns whatever we wrote to it
            records = self._read_chunk(size)
            #todo : put records in a string format?
            buf.write(records[0].__str__())
        return buf.getvalue()


    def _read_chunk(self, n):
        #not needed if we have records as quanta?
        #this should return n records in that case -AF 3/10
        if self.buf is None:
            if not self.cursor.alive:
                return ''
            self.i = 0
            self.buf = StringIO()
        #recs = self.cursor[self.offset: self.offset+n]
        #ret = [record for record in recs]
        ret = self.cursor.find(skip = self.offset, limit=n)
        self.offset += n
        self.i += n
        return ret


    def tell(self):
        #some sort of positioning info
        return self.orig_offset + self.i
        pass

    def seek(self, pos, mode=0):
        #changes internal positioning variables
        #i.e does not actually change the cursor position
        #and read must be positioned relative to these variables
        if mode == 0:
            self.offset = pos
        elif mode ==1:
            self.offset = self.tell() + pos
        else:
            self.offset = len(self) - pos
        self.eof = False
        self.buf = None
        self.orig_offset = self.offset
        self.i = 0


'''#}}}





def input_stream(stream, size, url, params):
    mon = open(url)
    return mon, len(mon), url


if __name__ == '__main__':
    #just for testing
    print open()

