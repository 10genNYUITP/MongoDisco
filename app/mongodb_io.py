from mongodb_output import mongodb_output
from scheme_mongodb import input_stream
from disco.worker.classic.func import task_output_stream


#from disco.schemes.scheme_mongodb import input_stream


mongodb_output_stream = (task_output_stream,mongodb_output)
mongodb_input_stream = (input_stream,)
