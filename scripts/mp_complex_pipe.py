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



def get_market_book_df(connector_0,
                       connector_1,
                       make_df_switch,
                       keep_working,
                       ticker,
                       end_hour=0):
    
    trading_terminal_init()
    market_book_subscribe(ticker)
    
    while dt.datetime.now().hour != end_hour:

        data = market_book_get_df(ticker)
        print(dt.datetime.now(),'made_df_switch at get mb', make_df_switch.value)
        if make_df_switch.value:
            connector_1.send(data)
            print(dt.datetime.now(),'data send to one')
        else:
            connector_0.send(data)
            print(dt.datetime.now(),'data send to zero')

        # print(dt.datetime.now(), 'df sent to',make_df_switch.value)

    market_book_unsubscribe(ticker)
    with keep_working.get_lock():
        keep_working.value = 0
    print(dt.datetime.now(),'DONE')
    

def market_book_make_big_df(connector_from_mb,
                            connetor_to_csv_save,
                            make_df_switch,
                            keep_working,
                            name,
                            memory_threshold=1e+9):
    
    while keep_working.value:
        df = pd.DataFrame()
        
        while df.memory_usage().sum() < memory_threshold:

            data = connector_from_mb.recv()
            df = pd.concat([df,data])
            
            if keep_working.value == 0:
                print(dt.datetime.now(),'trading period ended (?)')
                break
        
        if make_df_switch.value == 1:
            with make_df_switch.get_lock():
                make_df_switch.value = 0
                print(dt.datetime.now(),'made_df_switch at',name, make_df_switch.value,'-'*100)
                
        else:
            with make_df_switch.get_lock():
                make_df_switch.value = 1
                print(dt.datetime.now(),'made_df_switch at',name, make_df_switch.value,'-'*100)
                
        df['name'] = name        
        connetor_to_csv_save.send(df)
        

def save_to_csv(connector_from_make_df,
                keep_working,
                path):
    counter = 0
    while keep_working.value:
        df = connector_from_make_df.recv()
        print(dt.datetime.now(),'df received')
        df.to_csv(join(path,str(counter)+'.csv'))
        print(dt.datetime.now(),'df saved')
        counter += 1    

# def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None):        
def main(ticker,path,end_hour=0,memory_threshold=1e+9,terminate_time=None): 

    parent_conn_0, child_conn_0 = multiprocessing.Pipe()
    parent_conn_1, child_conn_1 = multiprocessing.Pipe()

    parent_conn_end, child_conn_end = multiprocessing.Pipe()
    
    make_df_switch = multiprocessing.Value('i')
    with make_df_switch.get_lock():
        make_df_switch.value = 0
    
    keep_working = multiprocessing.Value('i')
    with keep_working.get_lock():
        keep_working.value = 1

    p1 = multiprocessing.Process(target=get_market_book_df, args=(parent_conn_0,
                                                                  parent_conn_1,
                                                                  make_df_switch,
                                                                  keep_working,
                                                                  ticker,
                                                                  end_hour))
    
    p2_0 = multiprocessing.Process(target=market_book_make_big_df, args=(child_conn_0,
                                                                         parent_conn_end,
                                                                         make_df_switch,
                                                                         keep_working,
                                                                         'zero',
                                                                         memory_threshold))
    
    p2_1 = multiprocessing.Process(target=market_book_make_big_df, args=(child_conn_1,
                                                                         parent_conn_end,
                                                                         make_df_switch,
                                                                         keep_working,
                                                                         'one',
                                                                         memory_threshold))
    
    p3 = multiprocessing.Process(target=save_to_csv, args=(child_conn_end,keep_working,path))

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

    path = '../data/mp_complex_pipe/'
    terminate_time = 900
    main(ticker,path,end_hour=end_hour,memory_threshold=memory_threshold,terminate_time=terminate_time)
    
    