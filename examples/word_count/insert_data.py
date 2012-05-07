import sys, os, logging
import pymongo
import bson
import datetime

config = {
        "db_name": "test",
        "collection_name": "wc",
        "input_uri": "mongodb://localhost/test.wc",
        "create_input_splits": True,
        "split_key": {'_id' : 1},
        }

if __name__ == '__main__':
    conn = pymongo.Connection()
    db = conn[config.get('db_name')]
    coll = db[config.get('collection_name')]
    logfile = open("beyond_lies_the_wub.txt","r").readlines()
    print 'opened file'
    for line in logfile:
        #print ', and line is %s'%line
        for word in line.split():
            post = {"file_text" : word, "date" : datetime.datetime.utcnow()}
            print 'post: %s '%post
            coll.insert(post)
            
