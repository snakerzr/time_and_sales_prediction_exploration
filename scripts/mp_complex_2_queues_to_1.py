from os.path import join

import multiprocessing
import time
import datetime as dt

# import MetaTrader5 as mt5
from mt5_ import trading_terminal_init, market_book_subscribe, market_book_get_df, market_book_unsubscribe
import numpy as np
import pandas as pd

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

from utils import iter_



def get_market_book_df(queue_0,
                       queue_1,
                       size_threshold,
                       queue_switch,
                       start_conveyor_switch,
                       ticker,
                       end_hour=0):
    
    counter = 0
    
    first_cycle = True
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour:
        
        while counter < size_threshold:
            data = market_book_get_df(ticker)
            if queue_switch.value == 0:
                queue_0.put(data)
            else:
                queue_1.put(data)
            counter += 1
            
        print(dt.datetime.now(),'size threshold reached')

        
        if queue_switch.value == 0:
            # queue_0.put('SWITCH')
            with queue_switch.get_lock():
                queue_switch.value = 1
            print(dt.datetime.now(),'q switch to 1')
                
            if first_cycle:
                with start_conveyor_switch.get_lock():
                    start_conveyor_switch.value = 1
                first_cycle = False
                
        else:
            # queue_1.put('SWITCH')
            with queue_switch.get_lock():
                queue_switch.value = 0
            print(dt.datetime.now(),'q switch to 0')
                
        counter = 0
            
    market_book_unsubscribe(ticker)
    queue_0.put('END')
    queue_1.put('END')
    queue_0.close()
    queue_1.close()

    print(dt.datetime.now(),'get_market_book_df - done')
    print(dt.datetime.now(),'mp_complex_2_queues_to_1 - done')    





    
def queue_2_to_1(q_0,q_1,q_out,queue_switch,start_conveyor_switch):
    
    while start_conveyor_switch.value == 0:
        continue
        
    print(dt.datetime.now(),'belt started --------------')
    while 1:
        while queue_switch.value == 1:
            q_out.put(q_0.get())
        
        while queue_switch.value == 0:
            q_out.put(q_1.get())
            
            
    
    
def make_df_save_csv(queue,
                     memory_threshold,
                     path):
    
    counter = 0
    
    df = pd.DataFrame()
    
    for data in iter_(queue.get,str):
        df = pd.concat([df,data])
        print(dt.datetime.now(),'df memory usage',df.memory_usage().sum())
        if df.memory_usage().sum() > memory_threshold:
            df.to_csv(join(path,str(counter)+'.csv'))
            counter += 1
            print(dt.datetime.now(),'df saved')
            df=pd.DataFrame()
    
    # save remainders
    df.to_csv(join(path,str(counter)+'.csv'))
    
        
   
# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,size_threshold=100000,memory_threshold=1e+9,terminate_time=None): 

    q_0 = multiprocessing.Queue()
    q_1 = multiprocessing.Queue()
    q_end = multiprocessing.Queue()
    
    queue_switch = multiprocessing.Value('i',0)
    start_conveyor_switch = multiprocessing.Value('i',0)

    p1 = multiprocessing.Process(target=get_market_book_df, args=(q_0,
                                                                  q_1,
                                                                  size_threshold,
                                                                  queue_switch,
                                                                  start_conveyor_switch,
                                                                  ticker,
                                                                  end_hour))

    p2 = multiprocessing.Process(target=queue_2_to_1,args=(q_0,
                                                           q_1,
                                                           q_end,
                                                           queue_switch,
                                                           start_conveyor_switch))
    
    
    
    p3 = multiprocessing.Process(target=make_df_save_csv, args=(q_end,
                                                                memory_threshold,
                                                                path))

    # running processes
    p1.start()
    p2.start()
    p3.start()
    
    if terminate_time != None:
        print('termination stated:',terminate_time,'seconds')
        time.sleep(terminate_time)
        p1.terminate()
        p2.terminate()
        p3.terminate()
    
    # wait until processes finish
    p1.join()
    p2.join()    
    p3.join()    

if __name__ == '__main__':
    
    ticker = 'RIU2'
    size_threshold = 10000
    memory_threshold = 1e+6
    end_hour = 0

    path = '../data/mp_complex_2_queues_to_1/'
    terminate_time = 900
    main(ticker,path,end_hour=end_hour,size_threshold=size_threshold,memory_threshold=memory_threshold,terminate_time=terminate_time)
    
    