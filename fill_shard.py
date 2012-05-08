import pymongo
import datetime
import random
config = {
        "db_name": "test",
        "collection_name": "pp",
        "splitSize": 4, #MB
        "inputURI": "mongodb://localhost/test.pp",
        "createInputSplits": True,
        "splitKey": {'_id' : 1},
        }

conn = pymongo.Connection()
db = conn[config.get('db_name')]
coll = db[config.get('collection_name')]

def randomName():
    length = random.randint(3,8)
    name = ""
    base = ord('a')
    for i in range(length):
        shift = random.randint(0,25)
        charb = chr(base+shift)
        name += charb

    return name

def randomAge():
    age = random.randint(1,100)
    return age

#print randomName(),randomAge()


for i in range(1000):
    
    post = {"name" : randomName(), "age": randomAge()}
    coll.insert(post)

coll.create_index("age")
