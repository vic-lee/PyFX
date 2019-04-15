from datetime import timedelta, time
import pandas as pd

from metrics.metric import Metric
from dataio.datareader import DataReader
from datastructure.daytimerange import TimeRangeInDay
from datastructure.daterange import DateRange


class PeriodPriceAvg(Metric):
    def __init__(self, price_dfs, cp_name: str, time_range: TimeRangeInDay,
                 date_range: DateRange, time_range_for_avg: TimeRangeInDay,
                 include_open=None):

        Metric.__init__(self, time_range, date_range, price_dfs, cp_name)
        self.time_range_for_avg = time_range_for_avg
        self.avgs = self._calc_avgs()

    def _calc_avgs(self):
        df = self._generate_period_prices_df()
        df_stats = pd.DataFrame()
        df_stats['Mean'] = df.mean(axis=1).round(5)

        for index, row in df.iterrows():
            df_stats.at[index, 'Time for Min'] = self._find_time_for_min(row)
            df_stats.at[index, 'Time for Max'] = self._find_time_for_max(row)

        return df_stats

    @staticmethod
    def _find_time_for_min(row):
        min_price = None
        min_time = None
        for time, price in row.iteritems():
            if min_price == None or price < min_price:
                min_price = price
                min_time = time
        return min_time[:-6]

    @staticmethod
    def _find_time_for_max(row):
        max_price = None
        max_time = None
        for time, price in row.iteritems():
            if max_price == None or price > max_price:
                max_price = price
                max_time = time
        return max_time[:-6]

    def _generate_period_prices_df(self):
        time_cur = self.time_range_for_avg.start_time
        df_list = {}

        while time_cur <= self.time_range_for_avg.end_time:
            single_minute_df = self.minute_price_df.between_time(time_cur, time_cur)[
                'Close']
            single_minute_df = single_minute_df.rename(
                str(time_cur) + " Close")
            single_minute_df.index = single_minute_df.index.normalize()
            df_list[time_cur] = single_minute_df
            time_cur = self.incr_one_min(time_cur)

        df = self._join_minute_dfs(df_list)

        return df

    @staticmethod
    def _join_minute_dfs(df_list):
        df_out = pd.DataFrame()
        for _, right_df in df_list.items():
            df_out = df_out.join(right_df, how="outer")
        return df_out

    @staticmethod
    def incr_one_min(time_cur):
        old_min = time_cur.minute
        if old_min == 59:
            new_min = 0
            new_hour = time_cur.hour + 1
        else:
            new_min = old_min + 1
            new_hour = time_cur.hour
        time_cur = time(hour=new_hour, minute=new_min, second=time_cur.second)
        return time_cur

    def to_df(self):
        return self.avgs
