import datetime as dt
import time

import numpy as np
import pandas as pd

import MetaTrader5 as mt5


def get_ticks(symbol,days_before=1, copy_tick_flag=mt5.COPY_TICKS_ALL):
    '''
    Берет с мт5 тики за период от (текущей даты минус 
    days_before, начало дня) до текущей даты (даже если это выходной).
    
    Столбец 'time' перевод в пандавский дейттайм
    Столбец 'time_msc' тоже самое, только с учетом мс.
    
    Если запускать в выходные то может вернуть наны, 
    нужно это учитывать определяя days_before
    
    '''
    date_to = dt.datetime.now(dt.timezone.utc)
    date_from = date_to - dt.timedelta(days = days_before)
    date_from = date_from.replace(hour=0, minute=0, second=0,microsecond=0)
    

    df_rates = pd.DataFrame(mt5.copy_ticks_range(symbol,
                                                 date_from,
                                                 date_to,
                                                 copy_tick_flag))
    
    df_rates['time'] = pd.to_datetime(df_rates['time'], unit='s',utc=True)
    df_rates['time_msc'] = pd.to_datetime(df_rates['time_msc'],unit='ms',utc=True)
    df_rates["symbol"] = symbol
#     df_rates = df_rates.set_index('time_msc',drop=True)

    return df_rates
 