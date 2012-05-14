#!/usr/bin/env python
# encoding: utf-8

'''
File: MongoSplitter.py
Author: NYU ITP Team
Description: Will calculate splits for a given collection/database
and store/return them in MongoSplit objects
'''
from pymongo import uri_parser
#from sets import Set
from split import MongoInputSplit
from mongo_util import get_collection, get_connection,get_database

import logging
import bson


def calculate_splits(config):
    """reads config to find out what type of split to perform"""
    #if the user does not specify an inputURI we will need to construct it from
    #the db/collection name TODO

    uri = config.get("input_uri", "mongodb://localhost/test.in")
    config['input_uri'] = uri
    uri_info = uri_parser.parse_uri(uri)

    #database_name = uri_info['database']
    collection_name = uri_info['collection']

    db = get_database(uri)
    stats = db.command("collstats", collection_name)

    isSharded = False if "sharded" not in stats else stats["sharded"]
    useShards = config.get("use_shards", False)
    useChunks = config.get("use_chunks", False)
    slaveOk = config.get("slave_ok", False)

    logging.info(" Calculate Splits Code ... Use Shards? - %s\nUse Chunks? \
        - %s\nCollection Sharded? - %s" % (useShards, useChunks, isSharded))

    logging.info("WRAPP")
    logging.info(config)
    logging.info("WRAPP")
    if config.get("create_input_splits"):
        logging.info("Creation of Input Splits is enabled.")
        if isSharded and (useShards or useChunks):
            if useShards and useChunks:
                logging.warn("Combining 'use chunks' and 'read from shards \
                    directly' can have unexpected & erratic behavior in a live \
                    system due to chunk migrations. ")

            logging.info("Sharding mode calculation entering.")
            return calculate_sharded_splits(config, useShards, useChunks,
                    slaveOk, uri)
        # perfectly ok for sharded setups to run with a normally calculated split.
        #May even be more efficient for some cases
        else:
            logging.info("Using Unsharded Split mode \
                    (Calculating multiple splits though)")
            return calculate_unsharded_splits(config, slaveOk, uri)

    else:
        logging.info("Creation of Input Splits is disabled;\
                Non-Split mode calculation entering.")

        return calculate_single_split(config)


def calculate_unsharded_splits(config, slaveOk, uri):
    """@todo: Docstring for calculate_unsharded_splits

    :returns: @todo

    Note: collection_name seems unnecessary --CW

    """
    splits = []  # will return this
    logging.info("Calculating unsharded splits")

    coll = get_collection(uri)

    q = {} if not "query" in config else config.get("query")

    # create the command to do the splits
    # command to split should look like this VV
    # SON([('splitVector', u'test.test_data'), ('maxChunkSize', 2),
    #    ('force', True), ('keyPattern', {'x': 1})])

    split_key = config.get('split_key')
    split_size = config.get('split_size')
    full_name  = coll.full_name
    logging.info("Calculating unsharded splits on collection %s with Split Key %s" %
            (full_name, split_key))
    logging.info("Max split size :: %sMB" % split_size)

    cmd = bson.son.SON()
    cmd["splitVector"]  = full_name
    cmd["maxChunkSize"] = split_size
    cmd["keyPattern"]   = split_key
    cmd["force"]        = False

    logging.debug("Issuing Command: %s" % cmd)
    data = coll.database.command(cmd)
    logging.debug("%r" % data)

    # results should look like this
    # {u'ok': 1.0, u'splitKeys': [{u'_id': ObjectId('4f49775348d9846c5e582b00')},
    # {u'_id': ObjectId('4f49775548d9846c5e58553b')}]}

    if data.get("err"):
        raise Exception(data.get("err"))
    elif data.get("ok") != 1.0:
        raise Exception("Unable to calculate splits")

    split_data = data.get('splitKeys')
    if not split_data:
        logging.warning("WARNING: No Input Splits were calculated by the split code. \
                Proceeding with a *single* split. Data may be too small, try lowering \
                'mongo.input.split_size'  if this is undesirable.")
    else:
        logging.info("Calculated %s splits" % len(split_data))

        last_key = None
        for bound in split_data:
            splits.append(_split(config, q, last_key, bound))
            last_key = bound
        splits.append(_split(config, q, last_key, None))

    return [s.format_uri_with_query() for s in splits]


def _split(config=None, q={}, min=None, max=None):
    """ constructs a split object to be used later
    :returns: an actual MongoSplit object
    """
    print "_split being created"
    query = bson.son.SON()
    query["$query"] = q

    if min:
        query["$min"] = min

    if max:
        query["$max"] = max

    logging.info("Assembled Query: ", query)

    return MongoInputSplit(
            config.get("input_uri"),
            config.get("input_key"),
            query,
            config.get("fields"),
            config.get("sort"),
            config.get("limit", 0),
            config.get("skip", 0),
            config.get("timeout", True))


def calculate_single_split(config):
    splits = []
    logging.info("calculating single split")
    query = bson.son.SON()

    splits.append(MongoInputSplit(
            config.get("input_uri"),
            config.get("input_key"),
            query,
            config.get("fields"),
            config.get("sort"),
            config.get("limit", 0),
            config.get("skip", 0),
            config.get("timeout", True)))

    logging.debug("Calculated %d split objects" % len(splits))
    logging.debug("Dump of calculated splits ... ")
    for s in splits:
        logging.debug("    Split: %s" % s.__str__())
    return [s.format_uri_with_query() for s in splits]


def calculate_sharded_splits(config, useShards, useChunks, slaveOk, uri):
    """Calculates splits fetching them directly from a sharded setup
    :returns: A list of sharded splits
    """
    splits = []
    if useChunks:
        splits = fetch_splits_via_chunks(config, uri, useShards, slaveOk)
    elif useShards:
        logging.warn("Fetching Input Splits directly from shards is potentially \
                dangerous for data consistency should migrations occur during the retrieval.")
        splits = fetch_splits_from_shards(config, uri, slaveOk)
    else:
        logging.error("Neither useChunks nor useShards enabled; failed to pick a valid state.")

    if splits == None:
        logging.error("Failed to create/calculate Input Splits from Shard Chunks; final splits content is 'None'.")

    logging.debug("Calculated splits and returning them - splits: %r" % splits)
    return splits


def fetch_splits_from_shards(config, uri, slave_ok):
    """Internal method to fetch splits from shareded db

    :returns: The splits
    """
    logging.warn("WARNING getting splits that connect directly to the backend mongods is risky and might not produce correct results")
    connection = get_connection(uri)

    configDB = connection["config"]
    shardsColl = configDB["shards"]

    shardSet = []
    splits = []
    cur = shardsColl.find()

    for row in cur:
        host = row.get('host')
        slashIndex = host.find("/")
        if slashIndex > 0:
            host = host[slashIndex + 1:]
        shardSet.append(host)

    for host in shardSet:
        new_uri = get_new_URI(uri,host,slave_ok)
        config['input_uri'] = new_uri
        splits += calculate_unsharded_splits(config,slave_ok,new_uri)
        #I think this is better than commented way

    return splits

    '''
        splits.append(MongoInputSplit(new_uri,
                config.get("input_key"),
                config.get("query"),
                config.get("fields"),
                config.get("sort"),
                config.get("limit", 0),
                config.get("skip", 0),
                config.get("timeout", True)))

    return [s.format_uri_with_query() for s in splits]
    '''

def fetch_splits_via_chunks(config, uri, useShards, slaveOk):
    """Retrieves split objects based on chunks in mongo

    :returns: The splits
    """
    originalQuery = config.get("query")
    if useShards:
        logging.warn("WARNING getting splits that connect directly to the \
                backend mongods is risky and might not produce correct results")

    logging.debug("fetch_splits_via_chunks: originalQuery: %s" % originalQuery)

    connection = get_connection(uri)

    configDB = connection["config"]

    shardMap = {}

    if useShards:
        shardsColl = configDB["shards"]
        cur = shardsColl.find()

        for row in cur:
            host = row.get('host')
            slashIndex = host.find("/")
            if slashIndex > 0:
                host = host[slashIndex + 1:]
            shardMap[row.get('_id')] = host

    logging.debug("MongoInputFormat.getSplitsUsingChunks(): shard map is: %s" % shardMap)

    chunksCollection = configDB["chunks"]
    logging.info(configDB.collection_names())
    query = bson.son.SON()

    uri_info = uri_parser.parse_uri(uri)
    query["ns"] = uri_info['database'] + '.' + uri_info['collection']

    cur = chunksCollection.find(query)
    logging.info("query is ", query)
    logging.info(cur.count())
    logging.info(chunksCollection.find().count())

    numChunks = 0

    splits = []

    for row in cur:
        numChunks += 1
        minObj = row.get('min')
        shardKeyQuery = bson.son.SON()
        min = bson.son.SON()
        max = bson.son.SON()

        for key in minObj:
            tMin = minObj[key]
            tMax = (row.get('max'))[key]

            #@to-do do type comparison first?
            min[key] = tMin
            max[key] = tMax

        if originalQuery == None:
            originalQuery = bson.son.SON()

        shardKeyQuery["$query"] = originalQuery
        shardKeyQuery["$min"] = min
        shardKeyQuery["$max"] = max

        inputURI = config.get("input_uri")

        if useShards:
            shardName = row.get('shard')
            host = shardMap[shardName]
            inputURI = get_new_URI(inputURI, host, slaveOk)

        splits.append(MongoInputSplit(
            inputURI,
            config.get("input_key"),
            shardKeyQuery,
            config.get("fields"),
            config.get("sort"),
            config.get("limit", 0),
            config.get("skip", 0),
            config.get("timeout", True)))


    # return splits in uri format for disco
    return [s.format_uri_with_query() for s in splits]


def get_new_URI(original_URI, new_URI, slave_OK):
    """
    :returns: a new Mongo_URI
    """

    MONGO_URI_PREFIX = "mongodb://"
    orig_URI_string = original_URI[len(MONGO_URI_PREFIX):]

    server_end = -1
    server_start = 0

    """to find the last index of / in the original URI string """
    idx = orig_URI_string.rfind("/")
    if idx < 0:
        server_end = len(orig_URI_string)
    else:
        server_end = idx

    idx = orig_URI_string.find("@")

    server_start = idx + 1

    sb = orig_URI_string[0:server_start] + new_URI + orig_URI_string[server_end:]

    if slave_OK is not None:
        if "?" in orig_URI_string:
            sb = sb + "&slaveok=" + str(slave_OK).lower()
        else:
            sb = sb + "?slaveok=" + str(slave_OK).lower()

    ans = MONGO_URI_PREFIX + sb
    logging.debug("get_new_URI(): original " + original_URI + " new uri: " + ans)
    return ans
