#!/usr/bin/env python
# encoding: utf-8

'''
File: twitter_tz.py
Author: AFlock
Description: Given a database of tweets, aggregate number of tweets per time
zone
Note: before running this job fill a mongodb with tweets by running :
curl https://stream.twitter.com/1/statuses/sample.json -u<user>:<pass> \
| mongoimport -c twitter


'''

from job import DiscoJob

config = {
        #NYU ITP twitter db VV
        #"input_uri": "mongodb://ec2-107-22-139-80.compute-1.amazonaws.com:27017/test.twitter",
        "input_uri": "mongodb://localhost/test.twitter",
        "output_uri" : "mongodb://localhost/test.outtwitter",
        "split_key": {'_id' : 1},
        "split_size": 1, #MB
        "createInputSplits": True,
        #"print_to_stdout" : True
        }


def map(tweet, params):
    if tweet.get('user'):
        yield tweet['user'].get('time_zone', "unlisted"), 1


def reduce(iter, params):
    from disco.util import kvgroup
    for zone, number in kvgroup(sorted(iter)):
        yield zone, sum(number)


if __name__ == '__main__':
    DiscoJob(config=config, map=map, reduce=reduce).run()

