#!/usr/bin/env python
# encoding: utf-8

'''
File: MongoSplitter.py
Author: NYU ITP Team
Description: Will calculate splits for a given collection/database
and store/return them in MongoSplit objects
'''
from pymongo import Connection, uri_parser
from MongoInputSplit import MongoInputSplit

import logging
import bson

from pymongo.uri_parser import (_partition,
                                _rpartition,
                                parse_userinfo,
                                split_hosts,
                                split_options,
                                parse_uri)

def calculate_splits(config):
    """reads config to find out what type of split to perform"""
    #pass
    uri = config.get("inputURI") if "inputURI" in config else "mongodb://localhost/test.in"

    #HACK -> make config align with this ^^
    config['inputURI'] = uri
    #/HACK

    #config.getInputURI()
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

    logging.info(" Calculate Splits Code ... Use Shards? " , useShards , ", Use Chunks? " , useChunks , "; Collection Sharded? " , isSharded);

    if config.get("createInputSplits"):
        logging.info( "Creation of Input Splits is enabled." )
        if isSharded and (useShards or useChunks):
            if useShards and useChunks:
                logging.warn( "Combining 'use chunks' and 'read from shards directly' can have unexpected & erratic behavior in a live system due to chunk migrations. " );

            logging.info( "Sharding mode calculation entering." );
            return calculate_sharded_splits( config, useShards, useChunks, slaveOk, uri, mongo );

        else: # perfectly ok for sharded setups to run with a normally calculated split. May even be more efficient for some cases
            logging.info( "Using Unsharded Split mode (Calculating multiple splits though)" );
            return calculate_unsharded_splits( config, slaveOk, uri, collection_name );

    else:
        logging.info( "Creation of Input Splits is disabled; Non-Split mode calculation entering." );
        return calculate_single_split( config );



def calculate_unsharded_splits(config, slaveOk, uri, collection_name):
    """@todo: Docstring for calculate_unsharded_splits

    :returns: @todo

    """
    splits = [] #will return this list
    print "calculating unsharded splits"

    # TODO: pass these fields VV as parameters? (02/26/12, 11:30, AFlock)
    connection = Connection(uri)
    db = connection[config["db_name"]]
    coll = db[config.get('collection_name')]

    q = {} if not "query" in config else config.get("query")

    print 'in unsharded splits'
    #print 'query in MongoSplitter is ' + q
    #for key in q:
        #print 'am I printing the query?'
        #print q[key]

    #create command
    #command to split should look like this VV
    #SON([('splitVector', u'test.test_data'), ('maxChunkSize', 2), ('force', False), ('keyPattern', {'x': 1})])
    split_key = config.get('splitKey')
    split_size = config.get('splitSize')
    full_name  = coll.full_name
    logging.info("Calculating unsharded splits on collection %s with Split Key %s" % (full_name, split_key))
    logging.info("Max split size :: %sMB" % split_size)


    cmd = bson.son.SON()
    cmd["splitVector"]  = full_name
    cmd["maxChunkSize"] = split_size
    cmd["keyPattern"]   = split_key
    cmd["force"]        = False

    logging.debug("Issuing Command: %s" % cmd)
    print "Issuing Command: %s" % cmd
    data = db.command(cmd)
    print data

    #results look like this VV
    #{u'ok': 1.0, u'splitKeys': [{u'_id': ObjectId('4f49775348d9846c5e582b00')}, {u'_id': ObjectId('4f49775548d9846c5e58553b')}]}

    if data.get("err"):
        raise Exception(data.get("err"))
    elif data.get("ok") != 1.0:
        print data
        raise Exception("Unable to calculate splits")


    split_data = data.get('splitKeys')
    if not split_data:
        logging.warning("WARNING: No Input Splits were calculated by the split code.  Proceeding with a *single* split. Data may be too small, try lowering 'mongo.input.split_size'  if this is undesirable.")
    else:
        logging.info("Calculated %s splits" % len(split_data))

        # TODO: what is the query q parameter? (02/26/12, 12:18, AFlock)
        last_key = None
        for bound in split_data:
            splits.append(_split(config, q, last_key, bound))
            last_key = bound
        splits.append(_split(config, q, last_key, None))

    return splits


def _split(config=None, q={}, min=None, max=None):
    """@todo: Docstring for _split
    :returns: an actual MongoSplit object
    """
    print "_split being created"
    query = bson.son.SON()
    query["$query"]  = q

    if min:
        query["$min"] = min

    if max:
        query["$max"] = max

    logging.info("Assembled Query: " , query)

    return MongoInputSplit(
            config.get("inputURI"),
            config.get("inputKey"),
            query,
            config.get("fields"),
            config.get("sort"),
            config.get("limit"),
            config.get("skip"),
            config.get("is_no_timeout")
            )

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

'''
def get_new_URI(original_URI, new_URI, slave_OK):
    """@todo: Docstring for get_new_URI

    :original_URI: @todo
    :etc: @todo
    :returns: a new Mongo_URI
    """

#    pass
    orig_URI_string = SCHEME_LEN
    server_end = -1
    server_start = 0

    """to find the last index of / in the original URI string """
    idx = orig_URI_string[::-1].find("/")
    if idx < 0:
        server_end = len(orig_URI_string)
    else:
        server_end = idx

    idx = orig_URI_string.find("@")

    if idx > 0:
        server_start = idx + 1

    sb = orig_URI_string
    sb.replace(orig_URI_string[server_start:server_end], new_URI)
    if slave_OK != null:
        if "?" in orig_URI_string:
            sb.append("&slaveok=").append(slave_OK)
        else
            sb.append("?slaveok=").append(slave_OK)

    ans = SCHEME + sb
    logging.debug("get_new_URI(): original " + original_URI + " new uri: " + ans )
    return ans
'''
