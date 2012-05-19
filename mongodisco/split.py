# Copyright 2012 10gen, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

'''
File: split.py
Author: NYU ITP team
Description: Holds the specification for an individual
    split as calculated by splitter.py
'''
import logging
import json
from bson import json_util,son


class MongoInputSplit():
    """
    Will hold spec for an individual split,
    and is able to pass that split's data to disco
    """

    def __init__(self, inputURI, keyField, query, fields=None, sort=None, limit=0, skip=0, timeout=True):
        self.inputURI = inputURI
        self.keyField = keyField
        self.query = query
        self.fields = fields
        self.sort = sort
        self.limit = limit
        self.skip = skip
        self.timeout = timeout


        logging.info("LOOK HERE")
        logging.info("%s" % query)
        logging.info("%s" % fields)


    def format_uri_with_query(self):
        """
        returns a formatted uri like scheme_mongo expects
        we put the query object in a json'd list so we can put it back
        into an ordered SON
        mongodb://local/test.in?query=<json of query>&limit=234&skip=293
        """
        base = self.inputURI
        queryObj = son.SON()
        queryObj['inputURI'] = self.inputURI
        queryObj['keyField'] = self.keyField
        queryObj['query'] = self.query
        queryObj['fields'] = self.fields
        queryObj['sort'] = self.sort
        queryObj['limit'] = self.limit
        queryObj['skip'] = self.skip
        queryObj['timeout'] = self.timeout

        str = json.dumps(queryObj,default=json_util.default)
        print str
        return str

    def __str__(self):
        return self.format_uri_with_query()
