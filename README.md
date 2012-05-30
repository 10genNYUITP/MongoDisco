#MongoDB Disco Adapter

The MongoDB Disco Adapter is a plugin which connect MongoDB and Disco MapRedcue framework by enabling users the ability to use MongoDB as an data input and/or an output source.

##Prerequisites
For each machine in a disco cluster, it need following:

-Python

-PyMongo

-Disco

For instructions to setup disco clusters, please refer to the the guide(http://discoproject.org/doc/disco/start/install.html) in disco project website.

##Installation
1.  Check out the latest source code in github

```bash
   $ git clone https://github.com/10genNYUITP/MongoDisco MongoDisco
```

2.  Go to MongoDisco folder, and run the setup.py file to install MongoDisco package

```bash    
    $ python setup.py install 
```

    Note: it may request administrator privilege to run the script

It’s done! Start hacking!

##Example
Word Counting is a classic example for MapReduce framework. It could be done extremely easily using the MongoDB Disco Adapter.

Step 1. Users need to specify the configuration for this job.

For example, users could specify where the input data is stored and how they would like to store output data by providing a mongodb uri.

```python
config = {
        "input_uri": "mongodb://localhost/test.in",
        "output_uri": "mongodb://localhost/test.out",
        "create_input_splits": True,
        "split_key": {‘_id’:1},
        "split_size”:1, #MB
}
```


You can find more detailed configuration in the appendix.

Here, we assume we assume that input data is in database “test”, collection “in”, and we want to split data on “_id” field by setting the split_size equal to 1 Megabyte. The result would be written back to collection “out” at last.

Step 2. Write up its own map function

Here we would like to read the value under the field “word” and count it, so the map function would like following:

```python
def map(doc, params):
    yield record.get('doc', "NoWord"), 1
```

Note: doc is an ordinary document return by mongodb query. You can perform any operations on it as MongoDB allowed.

Setup 3. Write up reduce function

As we already get key-value generators from the map process, we only need perform sum operation for each word.

```python
def reduce(iter, params):
    from disco.util import kvgroup
    for word, counts in kvgroup(sorted(iter)):
        yield word, sum(counts)
```

The first parameter, iter, is an iterator over keys and values produced by the map function. We use disco.util.kvgroup() to simply pull out each word along with its counts, and sum them together.

Setup 4. Create a DiscoJob instance and run it

```python
from mongodisco.job import DiscoJob

DiscoJob(config = config,map = map,reduce = reduce).run()
```

Now you run it in a terminal like other python codes and check the result in MongoDB.


##Appendix

Configuration for DiscoJob

<table>
<tr><td>Name</td><td>Default Value</td><td>Note</td></tr>
<tr><td>input_uri</td><td>mongodb://localhost/test.in</td><td>mongodb uri for input data</td></tr>
<tr><td>output_uri</td><td>mongodb://localhost/test.out</td><td>mongodb uri for output result</td></tr>
<tr><td>print_to_stdout</td><td>False</td><td>if True, print result to stdout</td></tr>
<tr><td>job_wait</td><td>True</td><td>if False, code won’t wait for end of job</td></tr>
<tr><td>create_input_splits</td><td>True</td><td>if True, data will be splitted</td></tr>
<tr><td>split_size</td><td>8</td><td>size for one split</td></tr>
<tr><td>split_key</td><td>{“_id”:1}</td><td>field for performing splitting</td></tr>
<tr><td>use_shards</td><td>False</td><td>if True, directly connect to shards to retrieve data</td></tr>
<tr><td>use_chunks</td><td>True</td><td>if True, directly use chunks splitted by mongoDB as splits</td></tr>
<tr><td>input_key</td><td>None</td><td>Unknown!!!</td></tr>
<tr><td>slave_ok</td><td>False</td><td>same as slave_okay</td></tr>
<tr><td>query</td><td>{}</td><td>same as spec parameter of find method</td></tr>
<tr><td>fields</td><td>None</td><td>same as fields parameter of find method</td></tr>
<tr><td>sort</td><td>None</td><td>same as sort parameter of find method</td></tr>
<tr><td>limit</td><td>0</td><td>same as limit parameter of find method</td></tr>
<tr><td>skip</td><td>0</td><td>same as skip parameter of find method</td></tr>
<tr><td>job_output_key</td><td>“_id”</td><td>field name for output key</td></tr> 
<tr><td>job_output_value</td><td>“value”</td><td>field name for output value</td></tr>
</table>


