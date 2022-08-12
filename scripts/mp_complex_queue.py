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



def get_market_book_df(queue_to_make_df,
                       ticker,
                       end_hour=0):
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour:

        data = market_book_get_df(ticker)

        queue_to_make_df.put(data)

    market_book_unsubscribe(ticker)
    queue_to_make_df.put('STOP')
    print(dt.datetime.now(),'get_market_book_df - done')
    print(dt.datetime.now(),'mp_complex_queue - done')
    

def market_book_make_big_df(queue_from_market_book,
                            queue_to_save_csv,
                            name,
                            memory_threshold=1e+9):
    # time.sleep(0.1)
    
    df = pd.DataFrame()
    for data in iter_(queue_from_market_book.get,str):
        df = pd.concat([df,data])
        if df.memory_usage().sum() > memory_threshold:
            queue_to_save_csv.put(df)
            df = pd.DataFrame()
     
    queue_to_save_csv.put(df)
    queue_to_save_csv.put('STOP')
    
    print(dt.datetime.now(),'trading period ended (?)')
    print(dt.datetime.now(),'market_book_make_big_df - done')

        

def save_to_csv(queue_from_make_df,
                path):
    counter = 0
    
    for df in iter_(queue_from_make_df.get,str):
        print(dt.datetime.now(),'df received')
        # print(df.tail(1))
        df.to_csv(join(path,str(counter)+'.csv'))
        print(dt.datetime.now(),'df saved')
        counter += 1    

# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None): 

    q_mb_df = multiprocessing.Queue()
    q_df_csv = multiprocessing.Queue()

    p1 = multiprocessing.Process(target=get_market_book_df, args=(q_mb_df,
                                                                  ticker,
                                                                  end_hour))
    
    p2_0 = multiprocessing.Process(target=market_book_make_big_df, args=(q_mb_df,
                                                                         q_df_csv,
                                                                         'zero',
                                                                         memory_threshold))
    
    p2_1 = multiprocessing.Process(target=market_book_make_big_df, args=(q_mb_df,
                                                                         q_df_csv,
                                                                         'one',
                                                                         memory_threshold))
    
    p3 = multiprocessing.Process(target=save_to_csv, args=(q_df_csv,path))

    # running processes
    p1.start()
    p2_0.start()
    p2_1.start()
    p3.start()
    
    if terminate_time != None:
        print('termination stated:',terminate_time,'seconds')
        time.sleep(terminate_time)
        p1.terminate()
        p2_0.terminate()
        p2_1.terminate()
        p3.terminate()
    
    # wait until processes finish
    p1.join()
    p2_0.join()
    p2_1.join()    
    p3.join()    

if __name__ == '__main__':
    
    ticker = 'RIU2'
    memory_threshold = 1e+7
    end_hour = 0

    path = '../data/mp_complex_queue/'
    terminate_time = 900
    main(ticker,path,end_hour=end_hour,memory_threshold=memory_threshold,terminate_time=terminate_time)
    
    