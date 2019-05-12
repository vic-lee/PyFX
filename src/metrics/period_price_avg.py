from datetime import timedelta, time
import logging
import pandas as pd

from metrics.metric import Metric

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader

from datastructure.datacontainer import DataContainer
from datastructure.daytimerange import DayTimeRange
from datastructure.daterange import DateRange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class PeriodPriceAvg(Metric):

    def __init__(self, prices: DataContainer, cp_name: str, config: ConfigReader, time_range_for_avg: DayTimeRange):

        Metric.__init__(self, config=config, currency_pair_name=cp_name)

        self.prices = prices
        self.time_range_for_avg = time_range_for_avg
        self.avgs = self._calc_avgs()

        logger.info("Appending period price average data...")

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
