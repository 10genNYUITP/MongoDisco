'''
File: config.py
Author: AFlock
Description: A sample configuration file for the mongo-disco adapter
Use: from config import config
'''

config = {
        #which collection to use for input to the job
        "inputURI": "mongodb://localhost/yield_historical.in",
        "slaveOk": True,
        "useShards": True,
        #Split the data (almost always want True)
        "createInputSplits": True,
        #Use mongo chunks as splits
        "useChunks": True}
