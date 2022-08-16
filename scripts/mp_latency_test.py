from os.path import join, isdir
from os import makedirs

import datetime as dt

from sequential import main as sequential_start
from mp_simple_pipe import main as mp_simple_pipe_start
from mp_simple_queue import main as mp_simple_queue_start
from mp_complex_pipe import main as mp_complex_pipe_start
from mp_complex_queue import main as mp_complex_queue_start
from mp_complex_2_queues import main as mp_complex_2_queues_start
from mp_complex_2_queues_to_1 import main as mp_complex_2_queues_to_1_start


def test_and_populate(func,ticker,path,memory_threshold,terminate_time, **kwargs):
    path = join('..\data',path,f'{memory_threshold:.1e}')
    # print(path)
    if not isdir(path):
        makedirs(path)
    func(ticker,path,memory_threshold=memory_threshold,terminate_time=terminate_time,**kwargs)    

# sequential_start()               # main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None)
# mp_simple_pipe_start()           # main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None)
# mp_simple_queue_start()          # main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None)
# mp_complex_pipe_start()          # main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None)
# mp_complex_queue_start()         # main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None)
# mp_complex_2_queues_start()      # main(ticker,path,end_hour=0,size_threshold=100000,terminate_time=None)
# mp_complex_2_queues_to_1_start() # main(ticker,path,end_hour=0,size_threshold=100000,memory_threshold=1e+9,terminate_time=None)

if __name__ == '__main__':

    ticker = 'RIU2'
    # end_hour = 0
    memory_thresholds = [1e+6,1e+7,1e+8]
    test_time = 600
    
    funcs = {
             'sequential':               sequential_start,
             'mp_simple_pipe':           mp_simple_pipe_start,
             'mp_simpe_queue':           mp_simple_queue_start,
             'mp_complex_pipe':          mp_complex_pipe_start,
             'mp_complex_queue':         mp_complex_queue_start,
             'mp_complex_2_queues':      mp_complex_2_queues_start,
             'mp_complex_2_queues_to_1': mp_complex_2_queues_to_1_start}
    
    for name,func in funcs.items():
        for memory_threshold in memory_thresholds:
            print(dt.datetime.now(),'-'*20)
            print(dt.datetime.now(),name,f'{memory_threshold:.1e} -- START')
            print(dt.datetime.now(),'-'*20)
            test_and_populate(func,ticker,name,memory_threshold,test_time)
            print(dt.datetime.now(),'-'*20)
            print(dt.datetime.now(),name,f'{memory_threshold:.1e} -- DONE')
            print(dt.datetime.now(),'-'*20)
    
    

