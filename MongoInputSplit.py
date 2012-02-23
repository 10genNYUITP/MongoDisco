#!/usr/bin/env python
# encoding: utf-8
'''
File: MongoInputSplit.py
Author: NYU ITP team
Description: Holds the specification for an individual
    split as calculated by MongoSplitter.py
'''
import sys, os, logging

# TODO: Find out if we need to extend some disco inputSplit (02/22/12, 21:54, AFlock)
class MongoInputSplit():
    """
    Will hold spec for an individual split,
    and is able to pass that split's data to disco
    """

    def __init__(self, inputURI, keyField, query, fields, sort, limit, skip, noTimeout):
        self.inputURI = inputURI
        self.keyField = keyField
        self.query = query
        self.fields = fields
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.noTimeout = noTimeout


    def write(self, out):
        """@todo: Docstring for write

        :out: @todo
        :returns: void
        """
        pass

    def read_fields(self, input):
        """read each field sequentially?
        see http://bit.ly/y2TjIj for corresponding f(n)

        :in: @todo
        :returns: void
        """
        pass

    def get_cursor(self):
        """@todo: Docstring for get_cursor

        :returns: a cursor with the split's query
        """
        pass

    def get_BSON_encoder(self):
        """@todo: Docstring for get_BSON_encoder

        :returns: a BSON Encoder object
        """
        pass

    def get_BSON_decoder(self):
        """@todo: Docstring for get_BSON_decoder

        :returns: a BSON Decoder
        """
        pass

    # NOT INCLUDING: getters/setters  for all the data members (this is Python, not Java ^_^)

    def hashCode(self):
        """@todo: Docstring for hashCode
        :returns: @todo
        """

        pass
