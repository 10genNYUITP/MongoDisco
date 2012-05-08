from app.mongoUtil import getConnection,getCollection
import pymongo
import bson
from pymongo import Connection,ReadPreference

uri = 'mongodb://localhost:10001/test.people'
query = bson.son.SON()
min = bson.son.SON()
max = bson.son.SON()

query['$query'] = {}
max['age'] = 47
min['age'] = 2
query['$min'] = min
query['$max'] = max
print query

col = getCollection(uri)
cursor = col.find( query)
print cursor.count()

count = 0
for row in cursor:
    count += 1

print count

print 'records num ',col.count()

