'''
File: config.py
Author: AFlock
Description: A sample configuration file for the mongo-disco adapter
Use: from config import config
'''

config = {
        #which collection to use for input to the job
        "input_uri": "mongodb://localhost/yield_historical.in",
        "slave_ok": True,
        "use_shards": True,
        #Split the data (almost always want True)
        "create_input_splits": True,
        #Use mongo chunks as splits
        "use_chunks": True}
