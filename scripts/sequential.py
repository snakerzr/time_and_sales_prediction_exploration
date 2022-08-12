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



def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):
    
    if terminate_time != None:
        print('termination stated:',terminate_time,'seconds')
        terminate_stated = True
        start_time = dt.datetime.now()
    
    counter = 0
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour: # cycle works until end hour
        
        
        df_market_book = pd.DataFrame()
        print(dt.datetime.now(), 'new df')
        
        while df_market_book.memory_usage().sum() < memory_threshold:
            data = market_book_get_df(ticker)
            df_market_book = pd.concat([df_market_book,data])
            
            if terminate_stated:
                if (dt.datetime.now() - start_time).seconds == terminate_time:
                    df_market_book.to_csv(join(path,str(counter)+'.csv'))
                    print(dt.datetime.now(), 'df saved to csv')
                    print(dt.datetime.now(), 'test ended')
                    return
            
            if dt.datetime.now().hour == end_hour:
                break
                
        print(dt.datetime.now(), 'df full')
            
        df_market_book.to_csv(join(path,str(counter)+'.csv'))
        counter += 1
                              
        print(dt.datetime.now(), 'df saved to csv')

    market_book_unsubscribe(ticker)
    
    print(dt.datetime.now(),'sequential - done')
    
    

if __name__ == '__main__':

    ticker = 'RIU2'
    end_hour = 0
    memory_threshold = 1e+7
    terminate_time = 900
    path = '../data/sequential/'
    
    main(ticker,path=path,memory_threshold=memory_threshold,terminate_time=terminate_time)
