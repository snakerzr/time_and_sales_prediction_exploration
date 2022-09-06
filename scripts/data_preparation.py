import pandas as pd

def load_trades_from_csv(path):
    trades = pd.read_csv(path,
                         sep=';',
                         header=None,
                         names=['datetime','id','price','volume','origin_side'])#[['date','time','price','volume','origin_side']]
    trades['datetime'] = pd.to_datetime(trades['datetime'],format='%Y:%m:%d %H:%M:%S.%f')
    # display(trades.head(1))
    # display(trades['time'].str + trades['date'].str)
    # trades['datetime'] = pd.to_datetime(trades['time'])
    # display(trades.head(1))
    # trades = trades.drop(columns=['date','time'])[['datetime','id','price','volume','origin_side']]
    return trades


def get_bar_stats(agg_trades):
    
    def weighted_average(x):
        try:
            result = np.average(x['price'],weights=x['volume'])
            return np.round(result,-1)
        except:
            return 0
    
    # vwap = agg_trades.apply(lambda x: np.average(x['price'], 
    #        weights=x['volume'])).to_frame('vwap')
    vwap = agg_trades.apply(weighted_average).to_frame('vwap')
    ohlc = agg_trades['price'].ohlc()
    vol = agg_trades['volume'].sum().to_frame('volume')
    txn = agg_trades['volume'].size().to_frame('txn')

    return pd.concat([ohlc, vwap, vol, txn], axis=1)

# resampled = trades.groupby(pd.Grouper(freq='1Min'))
# # time_bars = get_bar_stats(resampled)
# one_minute_bar = get_bar_stats(resampled)

def get_volume_bars(df,trades_per_min=None):
    temp_ = df.copy().reset_index()

    temp_['volume_cumsum'] = temp_['volume'].cumsum()

    if trades_per_min  == None:
        trades_per_min  = temp_['volume'].sum()/(60*14) # 14 hours * 60 minutes

    by_vol = temp_.groupby(temp_['volume_cumsum'].div(trades_per_min ).round().astype(int))

    vol_bars = pd.concat([by_vol['datetime'].last().to_frame('timestamp'), get_bar_stats(by_vol)], axis=1)
    
    return vol_bars

# vol_bars = get_volume_bars(trades)

def get_dollar_bars(df,value_per_min=None):

    temp_ = df.copy().reset_index()

    value_per_min = temp_['volume'].mul(temp_['price']).sum()/(60*14)

    temp_['cumul_val'] = temp_['volume'].mul(temp_['price']).cumsum()

    by_value = temp_.groupby(temp_['cumul_val'].div(value_per_min).round().astype(int))

    dollar_bars = pd.concat([by_value['datetime'].last().to_frame('timestamp'), get_bar_stats(by_value)], axis=1)

    return dollar_bars

# dollar_bars = get_dollar_bars(trades)