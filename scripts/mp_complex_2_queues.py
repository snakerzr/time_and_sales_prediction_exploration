# --------------------------------------------------------------
# Save to csv is too slow. So slow that it stays on first queue.
# --------------------------------------------------------------

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
                       make_df_switch,
                       ticker,
                       end_hour=0):
    
    counter = 0
    
    first_cycle = True
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour:
        
        while counter < size_threshold:

            data = market_book_get_df(ticker)
            queue_0.put(data)
            counter +=1
            # print(dt.datetime.now(),'q_0 added')
            
            if dt.datetime.now().hour == end_hour:
                print('break')
                break
            
            
        queue_0.put('NEXT')
        print(dt.datetime.now(),'q_0 next')
        
        if first_cycle:
            with make_df_switch.get_lock():
                make_df_switch.value = 1
            first_cycle = False
        
        while counter < 2*size_threshold:
            
            data = market_book_get_df(ticker)
            queue_1.put(data)
            counter +=1
            # print(dt.datetime.now(),'q_1 added')
            
            if dt.datetime.now().hour == end_hour:
                print('break')
                break

        queue_1.put('NEXT')
        counter = 0
        print(dt.datetime.now(),'q_1 next')
        
        
    market_book_unsubscribe(ticker)
    queue_0.close()
    queue_1.close()

    print(dt.datetime.now(),'get_market_book_df - done')
    print(dt.datetime.now(),'mp_complex_2_queues - done')

            
    
    
def make_df_save_csv(queue_0,
                     queue_1,
                     make_df_switch,
                     path):
    
    counter = 0
    
    while make_df_switch.value == 0:
        continue
    
    print(dt.datetime.now(),'mk df started------------------------')
    
    while 1:
        print(dt.datetime.now(),'in loop')
        
        df = pd.DataFrame()
        
        for data in iter_(queue_0.get,str):
            df = pd.concat([df,data])
            print(dt.datetime.now(),'in q0 concat')
            

        df.to_csv(join(path,str(counter)+'.csv'))
        print(dt.datetime.now(),'df saved from 0')
        counter += 1
            
        df = pd.DataFrame()
            

        for data in iter_(queue_1.get,str):
            df = pd.concat([df,data])
            print(dt.datetime.now(),'in q1 concat')

        df.to_csv(join(path,str(counter)+'.csv'))
        print(dt.datetime.now(),'df saved from 1')

        counter += 1
            
            

    
    



# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,size_threshold=100000,memory_threshold=0,fterminate_time=None): 

    q_0 = multiprocessing.Queue()
    q_1 = multiprocessing.Queue()
    
    make_df_switch = multiprocessing.Value('i',0)

    p1 = multiprocessing.Process(target=get_market_book_df, args=(q_0,
                                                                  q_1,
                                                                  size_threshold,
                                                                  make_df_switch,
                                                                  ticker,
                                                                  end_hour))
    
    
    
    p2 = multiprocessing.Process(target=make_df_save_csv, args=(q_0,
                                                                q_1,
                                                                make_df_switch,
                                                                path))

    # running processes
    p1.start()
    p2.start()
    
    if terminate_time != None:
        print('termination stated:',terminate_time,'seconds')
        time.sleep(terminate_time)
        p1.terminate()
        p2.terminate()
    
    # wait until processes finish
    p1.join()
    p2.join()    

if __name__ == '__main__':
    
    ticker = 'RIU2'
    size_threshold = 1000
    end_hour = 0

    path = '../data/mp_complex_2_queues/'
    terminate_time = 900
    main(ticker,path,end_hour=end_hour,size_threshold=size_threshold,terminate_time=terminate_time)
    
    