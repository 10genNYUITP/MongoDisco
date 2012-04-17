from app.mongoUtil import getConnection,getCollection
import pymongo
import bson
from pymongo import Connection

uri = 'mongodb://localhost:10001/test.people'
query = bson.son.SON()
min = bson.son.SON()
max = bson.son.SON()

query['$query'] = {}
max['age'] = 100
min['age'] = 47
query['$min'] = min
#query['$max'] = max
print query

col = getCollection(uri)
cursor = col.find(query)
print cursor.count()

print 'records num ',col.count()

