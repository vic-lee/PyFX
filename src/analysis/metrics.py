from collections import namedtuple
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time

from common.config import Config

from datastructure.datacontainer import DataContainer
from datastructure.daterange import DateRange
from datastructure.daytimerange import DayTimeRange
from datastructure.pricetime import PriceTime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Metric:

    def __init__(self, currency_pair_name: str, config: Config):

        self.time_range = config.time_range
        self.date_range = config.date_range
        self.currency_pair_name = currency_pair_name

    def _get_prior_fix_recursive(self, d, prices: DataContainer):

        def daydelta(d, delta): return d - timedelta(days=delta)

        fx = None

        if self.date_range.is_datetime_in_range(d):

            try:
                cp_identifier = self.currency_pair_name[:3] \
                    + '-' + self.currency_pair_name[3:]
                fx = prices.fix_price_df.loc[str(d)][cp_identifier]
            except Exception as e:
                logger.error(
                    "Could not locate the previous fix price, possibly due to out of bounds." + str(e))
                return None
            if not np.isnan(fx):
                index = datetime(year=d.year, month=d.month,
                                 day=d.day, hour=0, minute=0, second=0)
                return PriceTime(price=fx, datetime=index)
            else:
                return self._get_prior_fix_recursive(daydelta(d, 1), prices)

        else:
            return None

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


class MinuteData(Metric):

    def __init__(self, prices: DataContainer, cp_name: str, config: Config):

        Metric.__init__(self, config=config, currency_pair_name=cp_name)

        self.prices = prices
        self.specs = config.minutely_data_sections
        self.df = self._generate_output_df()

        print("Appending minute data...")

    def _generate_output_df(self):
        df_list = []

        for spec in self.specs:
            df_for_spec = self._generate_period_prices_df(spec)
            df_list.append(df_for_spec)

        return self._join_minute_dfs(df_list)

    def _generate_period_prices_df(self, spec):
        df_list = []

        start_time = spec["range_start"]
        end_time = spec["range_end"]
        metric_to_include = spec["include"]
        time_cur = start_time

        while time_cur <= end_time:

            for m in metric_to_include:
                df = self.prices.full_minute_price_df.between_time(
                    time_cur, time_cur)[m]
                df = df.rename("{} {}".format(str(time_cur), m))
                df.index = df.index.normalize()
                df_list.append(df)

            time_cur = self.incr_one_min(time_cur)

        return self._join_minute_dfs(df_list)

    def _join_minute_dfs(self, df_list):
        df_out = pd.DataFrame()
        for right_df in df_list:
            df_out = df_out.join(right_df, how="outer")
        return df_out

    def to_df(self):
        return self.df


# class PeriodMinMaxTime(Metric):
#     def __init__(self, price_dfs, time_range, time_range_for_calc):
#         Metric.__init__(self, time_range, price_dfs)
#         self.time_range_for_minmax = time_range_for_calc
#         self.min_max_time = self._find_min_max_time()

#     def _find_min_max_time(self):
#         pass


class PeriodPriceAvg(Metric):

    def __init__(self, prices: DataContainer, cp_name: str, config: Config, time_range_for_avg: DayTimeRange):

        Metric.__init__(self, config=config, currency_pair_name=cp_name)

        self.prices = prices
        self.time_range_for_avg = time_range_for_avg
        self.avgs = self._calc_avgs()

        print("Appending period price average data...")

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
            single_minute_df = self.prices.minute_price_df.between_time(time_cur, time_cur)[
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

    def to_df(self):
        return self.avgs
