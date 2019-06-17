from datetime import time
import pandas as pd
from typing import List

from common.decorators import timer
from datastructure.datacontainer import DataContainer
from datastructure.daytimerange import DayTimeRange


@timer
def include_avg(data: DataContainer, sections: List):

    def include(section):
        df = pd.DataFrame()
        start_time = section.start_time
        end_time = section.end_time
        filtered = data.full_minute_price_df.between_time(start_time, end_time)
        filtered.insert(loc=1, column='Time', value=filtered.index.time)
        filtered.insert(loc=1, column='Date', value=filtered.index.date)

        min_time_mask = (
            filtered['Close'] ==
            filtered.groupby(filtered.index.date).Close.transform(min)
        )

        max_time_mask = (
            filtered['Close'] ==
            filtered.groupby(filtered.index.date).Close.transform(max)
        )

        df['Mean'] = filtered.groupby(
            filtered.index.date).Close.mean().round(5)

        min_series = filtered[min_time_mask]
        min_series.index = min_series.index.date
        min_series = min_series[~min_series.index.duplicated(keep='last')]

        max_series = filtered[max_time_mask]
        max_series.index = max_series.index.date
        max_series = max_series[~max_series.index.duplicated(keep='last')]

        df.insert(loc=1, column='TimeForMin', value=min_series['Time'])
        df.insert(loc=2, column='TimeForMax', value=max_series['Time'])

        df.columns = pd.MultiIndex.from_product([
            ['{}_{}'.format(str(start_time), str(end_time))], df.columns
        ])

        return df

    outputs = map(include, sections)
    df_master = pd.concat(outputs, axis=1)

    return df_master


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

    raw_ans = minute_df.loc[:, minute_df.columns.str.startswith('a_')]
    days = raw_ans.groupby(minute_df.index.date)
    ans = days.agg(['sum'])
    return ans


@timer
def find_max_pips(data: DataContainer, benchmark_times: List[time]):
    df_min = data.minute_price_df   \
        .copy()                     \
        .drop(columns=['date', 'Open', 'High', 'Low'])

    sel_max = (
        df_min['Close'] ==
        df_min.Close.groupby(df_min.index.date).transform(max)
    )

    sel_min = (
        df_min['Close'] ==
        df_min.Close.groupby(df_min.index.date).transform(min)
    )

    def finder(bt: time):
        """
        For each day, find the MIN and MAX of in the time period.

        Algorithm
        ---------
        MaxPipUp = pip(MAX((MAX(TP) - BT), 0))
        MaxPipDn = pip(MAX((MIN(TP) - BT), 0))

        Required Columns
        ----------------
            BenchmarkPrice
            MaxPipUp            MaxPipDown
            PriceAtMaxPipUp     PriceAtMaxPipDown
            TimeAtMaxPipUp      TimeAtMaxPipDown
        """

        df_maxpip = df_min[sel_max]
        df_maxpip.columns = ['PriceAtMaxPipUp']
        df_maxpip.insert(loc=1, column='TimeAtMaxPipUp',
                         value=df_maxpip.index.time)
        df_maxpip.insert(loc=1, column='date', value=df_maxpip.index.date)
        df_maxpip.drop_duplicates(subset=['date'], keep='last', inplace=True)
        df_maxpip.set_index('date', inplace=True)

        df_minpip = df_min[sel_min]
        df_minpip.columns = ['PriceAtMaxPipDown']
        df_minpip.insert(loc=1, column='TimeAtMaxPipDown',
                         value=df_minpip.index.time)
        df_minpip.insert(loc=1, column='date', value=df_minpip.index.date)
        df_minpip.drop_duplicates(subset=['date'], keep='last', inplace=True)
        df_minpip.set_index('date', inplace=True)

        df = pd.concat([df_maxpip, df_minpip], axis=1)

        benchmark_prices = df_min.at_time(bt)['Close']
        benchmark_prices.index = benchmark_prices.index.date

        df.insert(loc=1, column='BenchmarkPrice', value=benchmark_prices)

        df['MaxPipUp'] = (10000 * (
            df['PriceAtMaxPipUp'] - df['BenchmarkPrice'])).round(2)
        df['MaxPipDown'] = (10000 * (
            df['PriceAtMaxPipDown'] - df['BenchmarkPrice'])).round(2)

        df = df[['BenchmarkPrice',
                 'MaxPipUp', 'PriceAtMaxPipUp', 'TimeAtMaxPipUp',
                 'MaxPipDown', 'PriceAtMaxPipDown', 'TimeAtMaxPipDown']]

        df.columns = pd.MultiIndex.from_product([[str(bt)], df.columns])
        return df

    maxpips = map(finder, benchmark_times)
    df_master = pd.concat(maxpips, axis=1)
    print(df_master.head())
    return df_master


@timer
def include_minute_data(data: DataContainer, sections: List) -> pd.DataFrame:

    def include(section):
        start_time = section['range_start']
        end_time = section['range_end']
        timerange = DayTimeRange(start_time, end_time)

        def get_min_data(t: time):
            df = data.full_minute_price_df.at_time(t).Close
            df.index = df.index.date
            return df

        outs = map(get_min_data, timerange)
        df = pd.concat(outs, axis=1)
        return df

    outputs = map(include, sections)
    df_master = pd.concat(outputs, axis=1)

    df_master.columns = pd.MultiIndex.from_product([
        ['Selected Minute Data'], df_master.columns
    ])

    return df_master
