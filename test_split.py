import sys, os, logging
import unittest
import pymongo
from splitter import calculate_splits
from mongo_util import get_connection
import datetime
import bson

'''
File: test_split.py
Description: This module tests the creation of splits by MongoSplitter. THe test attempts to create splits on data inserted in collection "in" and verifies against the manual number of splits created by using the default splitSize.
Author/s: NYU ITP team
'''

'''
Description: Default configuration for inserting and reading the data.
'''
config = {
        "db_name": "test",
        "collection_name": "twitter",
        "split_size": 1, #MB
        "use_shards" : True,
        "use_chunks" : False,
        "input_uri": "mongodb://ec2-23-20-75-24.compute-1.amazonaws.com:27020/test.twitter",
        "create_input_splits": True,
        "split_key": {'_id' : 1},
        }


'''
Description: Configuration used to insert splits (created by MongoSplitter) into various collections and verify against original data.
config2 = {
        "db_name": "test",
        "collection_name": "tempSplit",
        "split_size": 1, #MB
        "input_uri": "mongodb://localhost/test.in",
        "create_input_splits": True,
        "split_key": {'_id' : 1},
        }

Test case to check MongoSplitter.
'''
class TestSplits(unittest.TestCase):
    def runTest(self):
        #put 20000 objects in a database, call for a split by hand, then a split by the class
        conn = get_connection("mongodb://ec2-23-20-75-24.compute-1.amazonaws.com:27020/test.twitter")
        db = conn[config.get('db_name')]
        coll = db[config.get('collection_name')]
        #print db.command("collstats", coll.full_name)

        #NOTE: need to run this code once to populate the database, after that comment it out
        '''
        for i in range(40000):
            post = {"name" : i, "date": datetime.datetime.utcnow()}
            coll.insert(post)
        '''



        #print coll.count()

        command = bson.son.SON()
        command['splitVector'] = coll.full_name
        command['maxChunkSize'] = config.get('split_size')
        command['force'] = False
        command['keyPattern'] = {'_id' : 1}
        #SON([('splitVector', u'test.twitter'), ('maxChunkSize', 1), ('keyPattern', {'_id': 1}), ('force', False)])
        results = db.command(command)

        man_splits = results.get("splitKeys")
        assert results.get('ok') == 1.0, 'split command did not return with 1.0 ok'
        print results
        print 'man_splits = ', len(man_splits)
        assert man_splits, 'no splitKeys returned'

        #now do it through MongoSplit
        splits = calculate_splits(config)

        assert splits, "MongoSplitter did not return the right splits"
        logging.info("Calculated %s MongoInputSplits" %  len(splits))
        #assert len(man_splits) + 1 == len(splits) , "MongoSplitter returned a different number of splits than manual splits"

        '''
        base_name = config2.get('collection_name')
        for j, i in enumerate(splits):
            coll_name = base_name + str(j)
            logging.info("Inserting split %s into %s" % (j, coll_name))
            coll = db[coll_name]
            coll.insert(i.cursor)
        '''


class TestSplitIntoInput(unittest.TestCase):
    #create split
    #turn splits into special URI
    #feed URI to input scheme
    #a list of wrappers will be returned, verify length of them
    pass


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
