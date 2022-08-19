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



def make_market_book_df(queue,ticker,end_hour=0,memory_threshold=1e+9):

    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour: # cycle works until end hour
        
        
        df_market_book = pd.DataFrame()
        
        while df_market_book.memory_usage().sum() < memory_threshold:
            data = market_book_get_df(ticker)
            df_market_book = pd.concat([df_market_book,data])
            
            if dt.datetime.now().hour == end_hour:
                break
            
        queue.put(df_market_book)
        print(dt.datetime.now(), 'df sent')

    market_book_unsubscribe(ticker)
    print(dt.datetime.now(),'mp_simple_queue - done')
    
def save_to_csv(queue,path):
    counter = 0

    while 1:
        while not queue.empty():
            df = queue.get()
            print(dt.datetime.now(),'df received')
            df.to_csv(join(path,str(counter)+'.csv'))
            print(dt.datetime.now(),'df saved')
            counter += 1    

# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):          
    
    q = multiprocessing.Queue()

    p1 = multiprocessing.Process(target=make_market_book_df, args=(q,
                                                                   ticker,
                                                                   end_hour,
                                                                   memory_threshold))
    
    p2 = multiprocessing.Process(target=save_to_csv, args=(q,
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
    end_hour = 0
    memory_threshold = 1e+6
    path = '../data/mp_simple_queue/'
    terminate_time = 900
    # main(ticker,path,end_hour=end_hour,memory_threshold=memory_threshold,terminate_time=terminate_time)
    main(ticker,path,end_hour=end_hour,memory_threshold=memory_threshold)

