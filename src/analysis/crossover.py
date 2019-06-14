import pandas as pd

from common.decorators import timer
from datastructure.datacontainer import DataContainer

thresholds = [10, 15, 20, 25, 30, 35, 40]


@timer
def count_crossovers(data: DataContainer, thresholds=thresholds):
    """
    crossover: v < threshold at `t` and v > threshold at `t-1`
    for each crossover in day: ctr += 1
    """
    daily_df = data.daily_price_df.copy()
    minute_df = data.full_minute_price_df.copy()

    minute_df['High_bf'] = minute_df.High.shift(1)
    minute_df['Low_bf'] = minute_df.Low.shift(1)

    daily_df['date'] = daily_df.index.date
    minute_df['date'] = minute_df.index.date
    minute_df['datetime'] = minute_df.index
    minute_df = pd.merge(daily_df, minute_df, on='date', suffixes=('_d', ''))
    minute_df = minute_df.set_index('datetime')

    trading_above = minute_df['Open'] > minute_df['Open_d']
    trading_below = ~trading_above

    def ct_high(pip): return ((minute_df['High'] > minute_df['t_{}'.format(pip)]) &
                              (minute_df['High_bf'] < minute_df['t_{}'.format(pip)]))

    def ct_low(pip): return ((minute_df['Low'] < minute_df['t_n{}'.format(pip)]) &
                             (minute_df['Low_bf'] > minute_df['t_n{}'.format(pip)]))

    for t in thresholds:
        t_pr = t / 10000
        minute_df['t_n{}'.format(t)] = minute_df['Open_d'] - t_pr
        minute_df['t_{}'.format(t)] = minute_df['Open_d'] + t_pr
        minute_df['a_n{}'.format(t)] = False
        minute_df['a_{}'.format(t)] = False

        msk_h = ct_high(t)
        msk_l = ct_low(t)
        minute_df.at[trading_above & msk_h, 'a_{}'.format(t)] = True
        minute_df.at[trading_below & msk_l, 'a_n{}'.format(t)] = True
