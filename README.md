## MongoDB => Disco Mapreduce Adapter

#### Goal
The goal of this project is to provide a simple and easy way to use your mongodb data for the Disco map reduce jobs. It is implemented in a similar fashion as [MongoDB Hadoop Adapter](https://github.com/mongodb/mongo-hadoop), and provides read and write through disco jobs to mongo setups.

### Setup

We assume you have a working knowledge of disco map reduce framework. If not, http://www.discoproject.org .

To setup your disco cluster to use the MongoDisco adapter, you need to make sure each node on your disco machine has pymongo installed.

On the master node, you can install the adapter from source.

```bash
$ git clone git://github.com/10genNYUITP/MongoDisco.git
$ cd MongoDisco
$ python setup.py install
```

After that, you should be able to run any DiscoJob as usual but using a MongoDB input and optionally specifying the output to MongoDB.

For examples, see the examples directory. Particularly twitter.
