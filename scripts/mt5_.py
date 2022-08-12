import MetaTrader5 as mt5
import datetime as dt
import pandas as pd

MB_COLUMNS = ['type','price','volume','volume_dbl'] # columns to use in df_market_book


def trading_terminal_init():
    counter = 0
    while counter < 5:
        if not mt5.initialize():
            print("initialize() failed, error code =",mt5.last_error())
            counter += 1
        else:
            print(dt.datetime.now(),'mt5 initialized:',mt5.last_error())
            break
            
def market_book_subscribe(ticker):
    counter = 0
    while counter < 5:
        if not mt5.market_book_add(ticker): # market book subscription
            print(dt.datetime.now(),'Failed to market_book_add, trying again')
            counter += 1
        else:
            print(dt.datetime.now(),'market_book_add - successful')
            break
            
def market_book_get(ticker):
    return mt5.market_book_get(ticker)

def market_book_get_df(ticker):
    time_before = dt.datetime.now()
    data = pd.DataFrame(market_book_get(ticker),columns=MB_COLUMNS)
    data['time_after'] = dt.datetime.now()
    data['time_before'] = time_before
    return data



def market_book_unsubscribe(ticker):
    result = mt5.market_book_release(ticker)
    print(dt.datetime.now(),'market_book_release:',result)