import numpy as np
from datetime import datetime, timedelta

from dataio.datareader import DataReader
from datastructure.pricetime import PriceTime


class Metric:
    def __init__(self, time_range, price_dfs, currency_pair_name: str):
        self.time_range = time_range
        self.currency_pair_name = currency_pair_name
        self.fix_price_df = price_dfs[DataReader.FIX]
        self.daily_price_df = price_dfs[DataReader.DAILY]
        self.minute_price_df = self._filter_df_to_time_range(
            price_dfs[DataReader.MINUTELY])
        self.full_minute_price_df = price_dfs[DataReader.MINUTELY]

    def _filter_df_to_time_range(self, df):
        return df.between_time(self.time_range.start_time, self.time_range.end_time)

    def _get_prior_fix_recursive(self, d):

        def daydelta(d, delta): return d - timedelta(days=delta)

        fx = None
        try:
            cp_identifier = self.currency_pair_name[:3] + \
                '-' + self.currency_pair_name[3:]
            fx = self.fix_price_df.loc[str(d)][cp_identifier]
        except Exception as e:
            print(
                "Could not locate the previous location, possibly due to out of bounds.")
            print(e)
            return None
        if not np.isnan(fx):
            index = datetime(year=d.year, month=d.month,
                             day=d.day, hour=0, minute=0, second=0)
            return PriceTime(price=fx, datetime=index)
        else:
            return self._get_prior_fix_recursive(daydelta(d, 1))
