#!/usr/bin/env python
# encoding: utf-8

'''
File: MongoSplitter.py
Author: NYU ITP Team
Description: Will calculate splits for a given collection/database
and store/return them in MongoSplit objects
'''
from pymongo import Connection, uri_parser
import logging
import bson

def calculate_splits(config):
    """reads config to find out what type of split to perform"""
    #pass
    uri = "mongodb://localhost/test.in" #config.getInputURI()
    uri_info = uri_parser.parse_uri(uri)

    host = uri_info['nodelist'][0][0]
    port = uri_info['nodelist'][0][1]
    database_name = uri_info['database']
    collection_name = uri_info['collection']

    connection = Connection(uri)
    db = connection[database_name]
    stats = db.command("collstats", collection_name)

    isSharded = False if "sharded" not in stats else stats["sharded"]
    useShards = False #config.canReadSplitsFromShards()
    useChunks = False #config.isShardChunkedSplittingEnabled()
    slaveOk = True #config.canReadSplitsFromSecondary()

    logging.info(" Calculate Splits Code ... Use Shards? " + useShards + ", Use Chunks? " + useChunks + "; Collection Sharded? " + isSharded);

    if config.createInputSplits():
        logging.info( "Creation of Input Splits is enabled." )
        if isSharded and (useShards or useChunks):
            if useShards and useChunks:
                logging.warn( "Combining 'use chunks' and 'read from shards directly' can have unexpected & erratic behavior in a live system due to chunk migrations. " );

                logging.info( "Sharding mode calculation entering." );
                return calculate_sharded_splits( config, useShards, useChunks, slaveOk, uri, mongo );

            else: # perfectly ok for sharded setups to run with a normally calculated split. May even be more efficient for some cases
                logging.info( "Using Unsharded Split mode (Calculating multiple splits though)" );
                return calculate_unsharded_splits( config, slaveOk, uri, coll );

    else:
        logging.info( "Creation of Input Splits is disabled; Non-Split mode calculation entering." );
        return calculate_single_split( config );



def calculate_unsharded_splits(config, etc):
    """@todo: Docstring for calculate_unsharded_splits

    :returns: @todo

    """
    #create command
    #command to split should look like this VV
    #SON([('splitVector', u'test.test_data'), ('maxChunkSize', 2), ('force', False), ('keyPattern', {'x': 1})])
    split_key  = config.get('splitKey')
    split_size = config.get('splitSize')
    full_name  = config.get('full_name')
    logging.info("Calculating unsharded splits on collection %s with Split Key %s" % (full_name, split_key))
    logging.info("Max split size :: %sMB" % split_size)


    cmd = bson.bson.SON()
    cmd["splitVector"]  = full_name
    cmd["maxChunkSize"] = split_size
    cmd["keyPattern"]   = split_key
    cmd["force"]        = False

    # TODO: pass these fields VV as parameters? (02/26/12, 11:30, AFlock)
    connection = Connection(uri)
    db = connection[database_name]
    split_results = db.command(cmd)

    if split_results:
        logging.info()

    pass

def _split(config, etc):
    """@todo: Docstring for _split

    :returns: an actual MongoSplit object
    """
    pass

def calculate_single_split(config):
    pass

def calculate_sharded_splits(config, etc):
    """Worry about this after unsharded splits are doen

    :returns: @todo
    """
    pass

def fetch_splits_from_shards(config):
    """@todo: Docstring for fetch_splits_from_shards

    :returns: @todo
    """
    pass



def fetch_splits_via_chunks(config):
    """@todo: Docstring for fetch_splits_via_chunks

    :returns: @todo
    """
    pass

def get_new_URI(original_URI, etc):
    """@todo: Docstring for get_new_URI

    :original_URI: @todo
    :etc: @todo
    :returns: a new Mongo_URI
    """

    pass



