from datetime import time
from typing import List

import pandas as pd

from common.decorators import timer
from ds.datacontainer import DataContainer
from ds.timeranges import DayTimeRange

__all__ = [
    'include_ohlc',
    'include_avgs',
    'include_crossovers',
    'include_max_pips',
    'include_minute_data',
]

pd.options.mode.chained_assignment = None


def include_ohlc(data: DataContainer):
    ohlc = data.daily_price_df.copy()
    ohlc.columns = pd.MultiIndex.from_product([['OHLC'], ohlc.columns])
    return ohlc


@timer
def include_avgs(data: DataContainer, periods: List):

    def include(period):
        df = pd.DataFrame()
        start_time = period.start_time
        end_time = period.end_time
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

    outputs = map(include, periods)
    df_master = pd.concat(outputs, axis=1)

    return df_master


thresholds = [10, 15, 20, 25, 30, 35, 40]


@timer
def include_crossovers(data: DataContainer, thresholds=thresholds):
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

    def ct_high(pip):
        return ((minute_df['High'] > minute_df['t_{}'.format(pip)]) &
                (minute_df['High_bf'] < minute_df['t_{}'.format(pip)]))

    def ct_low(pip):
        return ((minute_df['Low'] < minute_df['t_n{}'.format(pip)]) &
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
def include_max_pips(data: DataContainer,
                     benchmark_times: List[time] = None,
                     pdfx: bool = False, cp_name: str = None):
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

    def validate_args():
        if benchmark_times is None and pdfx is False:
            return False
        elif pdfx is True and cp_name is None:
            return False
        else:
            return True

    assert validate_args()

    df_min = data.minute_price_df   \
        .copy()                     \
        .drop(columns=['date', 'Open', 'High', 'Low'])

    def pip_extrema(mask, state: str):
        assert state == 'Up' or state == 'Down' or state == 'Dn'

        def inner():
            df = df_min[mask]
            df.columns = ['PriceAtMaxPip{}'.format(state)]
            df.insert(loc=1, column='TimeAtMaxPip{}'.format(state),
                      value=df.index.time)
            df.insert(loc=1, column='date', value=df.index.date)
            df.drop_duplicates(subset=['date'], keep='last', inplace=True)
            df.set_index('date', inplace=True)
            return df

        return inner

    def pip_mask(func):
        return (
            df_min['Close'] ==
            df_min.Close.groupby(df_min.index.date).transform(func)
        )

    df_maxpip = pip_extrema(pip_mask(max), 'Up')()
    df_minpip = pip_extrema(pip_mask(min), 'Down')()

    def fix_benchmark() -> pd.DataFrame:
        df = pd.DataFrame()
        f_cpname = '{}-{}'.format(cp_name[:3], cp_name[3:])

        # load data
        df['CDFX'] = data.fix_price_df[f_cpname]
        df['BenchmarkPrice'] = df.CDFX.shift(1)

        # fill NaNs and drop weekends
        df.dropna(how='all', inplace=True)
        df.BenchmarkPrice.fillna(method='ffill', inplace=True)
        df.dropna(subset=['CDFX'], inplace=True)

        return df

    def normal_benchmark(bt: time):
        benchmark_data = df_min.at_time(bt)['Close'].to_frame()
        benchmark_data.columns = ['BenchmarkPrice']
        benchmark_data.index = benchmark_data.index.date
        return benchmark_data

    def append_max_pips(df: pd.DataFrame) -> pd.DataFrame:
        mpipup = (10000 * (
            df['PriceAtMaxPipUp'] - df['BenchmarkPrice'])).round(2)
        mpipup[mpipup < 0] = 0
        mpipdn = (10000 * (
            df['PriceAtMaxPipDown'] - df['BenchmarkPrice'])).round(2)
        mpipdn[mpipdn > 0] = 0

        df.insert(loc=1, column='MaxPipUp', value=mpipup)
        df.insert(loc=4, column='MaxPipDown', value=mpipdn)
        return df

    def finder_strategy(bt=None):
        def inner(bt):
            bt_str = str(bt)
            df = pd.concat([df_maxpip, df_minpip], axis=1)
            if isinstance(bt, time):
                benchmark_data = normal_benchmark(bt)
            else:
                benchmark_data = fix_benchmark()
            df = benchmark_data.join(df)
            df = append_max_pips(df)
            df.columns = pd.MultiIndex.from_product([[bt_str], df.columns])
            return df
        return inner

    finder = finder_strategy()
    finder_pdfx = finder_strategy('PDFX')

    if pdfx:
        df_master = finder_pdfx('PDFX')
    else:
        maxpips = map(finder, benchmark_times)
        df_master = pd.concat(maxpips, axis=1)

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
