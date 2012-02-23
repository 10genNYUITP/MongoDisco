#!/usr/bin/env python
# encoding: utf-8

'''
File: MongoSplitter.py
Author: NYU ITP Team
Description: Will calculate splits for a given collection/database
and store/return them in MongoSplit objects
'''


def calculate_splits(config):
    """reads config to find out what type of split to perform"""
    pass

def calculate_unsharded_splits(config, etc):
    """@todo: Docstring for calculate_unsharded_splits

    :returns: @todo
    """
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



