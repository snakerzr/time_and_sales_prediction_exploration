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



def make_market_book_df(connector,ticker,end_hour=0,memory_threshold=1e+9):
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour: # cycle works until end hour
        
        df_market_book = pd.DataFrame()
        
        while df_market_book.memory_usage().sum() < memory_threshold:
            data = market_book_get_df(ticker)
            df_market_book = pd.concat([df_market_book,data])
            
            if dt.datetime.now().hour == end_hour:
                break
            
        connector.send(df_market_book)
        print(dt.datetime.now(), 'df sent')

    market_book_unsubscribe(ticker)
    connector.send('END')
    print(dt.datetime.now(),'mp_simple_pipe - done')
    connector.close()
    
def save_to_csv(connector,path):
    counter = 0

    while 1:
        df = connector.recv()
        if type(df) == str:
            print(df)
            print('save_done')
            connector.close()
            break
        print(dt.datetime.now(),'df received')
        df.to_csv(join(path,str(counter)+'.csv'))
        print(dt.datetime.now(),'df saved')
        counter += 1    

# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):       
    
    parent_conn, child_conn = multiprocessing.Pipe()

    p1 = multiprocessing.Process(target=make_market_book_df, args=(parent_conn,
                                                                   ticker,
                                                                   end_hour,
                                                                   memory_threshold))
    
    p2 = multiprocessing.Process(target=save_to_csv, args=(child_conn,
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
    memory_threshold = 1e+7
    path = '../data/mp_simple_pipe/'
    terminate_time = 900
    main(ticker,path,end_hour=end_hour,memory_threshold=memory_threshold,terminate_time=terminate_time)

