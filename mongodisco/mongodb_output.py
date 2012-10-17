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

from mongodisco.mongo_util import get_connection,get_collection

class MongoOutput(object):
    '''Output stream for mongoDB
    '''
    def __init__(self,stream,params):

        config = {}
        for key, value in params.__dict__.iteritems():
            config[key] = value

        self.uri =  config.get('output_uri')
        self.conn = get_connection(self.uri)
        self.coll = get_collection(self.uri)
        self.key_name = config.get('job_output_key','_id')
        self.value_name = config.get('job_output_value')
        self.add_action = config.get('add_action', 'insert')
        self.add_upsert = config.get('add_upsert', False)


    def add(self,key,val):
        result_dict = {}
        result_dict[self.key_name] = key
        result_dict[self.value_name] = val
        if self.add_action == 'insert':
            self.coll.insert(result_dict)
        elif self.add_action == 'save':
            self.coll.save(result_dict)
        elif self.add_action == 'update':
            #In this case val needs to be an object containing commands like $set, $inc, $unset, etc
            self.coll.update({self.key_name: key}, val, upsert=self.add_upsert)

    def close(self):
        self.conn.close()


def mongodb_output(stream,partition,url,params):
    # This looks like a mistake, but it is intentional.
    # Due to the way that Disco imports and uses this
    # function, we must re-import the module here.
    from mongodisco.mongodb_output import MongoOutput
    return MongoOutput(stream,params)

