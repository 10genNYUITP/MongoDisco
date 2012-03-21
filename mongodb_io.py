from mongoDisco_output import MongoDBoutput
from disco.worker.classic.func import task_output_stream
from disco.schemes.scheme_mongodb import input_stream

def mongodb_output(stream,partition,url,params):
    return mongoDisco_output.MongoDBoutput(stream,params)

mongodb_output_stream = (task_output_stream,mongodb_output)
mongodb_input_stream = (input_stream,)
