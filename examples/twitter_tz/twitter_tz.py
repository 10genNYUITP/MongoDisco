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

# VV hacky way to get app available- solution: install?
# TODO: ask @Changle (05/06/12, 20:54, AFlock)
import sys
sys.path.append('../..')
from disco.core        import Job, result_iterator
from mongodb_io        import mongodb_input_stream
from app.MongoSplitter import calculate_splits

config = {
        #NYU ITP twitter db VV
        #"inputURI": "mongodb://ec2-107-22-139-80.compute-1.amazonaws.com:27017/test.twitter",
        "inputURI": "mongodb://localhost/twitter",
        "createInputSplits": True}


def map(tweet, params):
    if tweet.get('user'):
        yield tweet['user'].get('time_zone', "unlisted"), 1


def reduce(iter, params):
    from disco.util import kvgroup
    for zone, number in kvgroup(sorted(iter)):
        yield zone, sum(number)


if __name__ == '__main__':
    job = Job().run(input=calculate_splits(config),
            map=map,
            reduce=reduce,
            map_input_stream=mongodb_input_stream)

    for tz, count in result_iterator(job.wait(show=True)):
        print "TZ : %s :: Count : %s" % (tz, count)
