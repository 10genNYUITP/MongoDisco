import unittest
import pymongo
from app import MongoInputSplit as MI
from app import MongoSplitter as MS
import datetime
import bson

config = {
        "db_name": "test",
        "collection_name": "in",
        "splitSize": 1, #MB
        "inputURI": "mongodb://localhost/test.in",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        }

class TestSplits(unittest.TestCase):
    def runTest(self):
        #put 20000 objects in a database, call for a split by hand, then a split by the class
        conn = pymongo.Connection()
        db = conn[config.get('db_name')]
        coll = db[config.get('collection_name')]
        #print db.command("collstats", coll.full_name)
        '''
        for i in range(20000):
            post = {"name" : i, "date": datetime.datetime.utcnow()}
            coll.insert(post)
        '''

        print coll.count()

        command = bson.son.SON()
        command['splitVector'] = coll.full_name
        command['maxChunkSize'] = config.get('splitSize')
        command['force'] = False
        command['keyPattern'] = {'_id' : 1}
        results = db.command(command)

        man_splits = results.get("splitKeys")
        assert results.get('ok') == 1.0, 'split command did not return with 1.0 ok'
        print results
        assert man_splits, 'no splitKeys returned'

        #now do it through MongoSplit
        splits = MS.calculate_splits(config)

        assert splits, "MongoSplitter did not return the right splits"
        assert len(man_splits) == len(splits), "MongoSplitter returned a different number of splits than manual splits"

if __name__ == '__main__':
    unittest.main()
