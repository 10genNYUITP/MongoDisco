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

from bson.json_util import object_hook
import json

from mongodisco.mongo_util import get_collection

'''
File: mongodb_input.py
Description:
'''
def _open(input_description, task=None):
    """Return a :class:`~mongodisco.mongodb_input.MongoWrapper`
    which wraps a cursor selecting just those documents relevant
    to one particular map operation. `input_description` is
    a JSON string describing the documents to find, and looks like::

        {  "inputURI": "mongodb://discomaster.zeroclues.net:27017/test.twitter",
           "keyField": null,
           "query": {
             "$query": {},
             "$min": {"_id": {"$oid": "4fae7a97fa22c41aeb5d78f8"}},
             "$max": {"_id": {"$oid": "4fae7b27fa22c41aeb5d96b5"}}},
           "fields": null,
           "sort": null,
           "limit": 0,
           "skip": 0,
           "timeout": false  }
    """
    parsed = json.loads(input_description, object_hook=object_hook)
    collection = get_collection(parsed['inputURI'])

    return MongoWrapper(collection.find(
        spec=parsed['query'],
        fields=parsed['fields'],
        skip=parsed['skip'],
        limit=parsed['limit'],
        sort=parsed['sort'],
        timeout=parsed['timeout'],
        slave_okay=parsed['slave_ok']
    ))


class MongoWrapper(object):
    """Want to wrap the cursor in an object that
    supports the following operations: """

    def __init__(self, cursor):
        self.cursor = cursor
        self.offset = 0

    def __iter__(self):
        #most important method
        return self.cursor

    def __len__(self):
        #may need to do this more dynamically (see lib/disco/comm.py ln 163)
        #may want to cache this
        return self.cursor.count()

    def close(self):
        self.cursor.close()

    def read(self, size=-1):
        #raise a non-implemented error to see if this ever pops up
        raise Exception("read is not implemented- investigate why this was called")


def input_stream(stream, size, url, params):
    # This looks like a mistake, but it is intentional.
    # Due to the way that Disco imports and uses this
    # function, we must re-import the module here.
    from mongodisco.mongodb_input import _open
    return _open(url)

