from datetime import time
from functools import reduce
from itertools import accumulate
import pandas as pd
from typing import List

from common.decorators import timer
from datastructure.datacontainer import DataContainer


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
