from datetime import time
import logging
import pandas as pd

from metrics.metric import Metric

from datastructure.datacontainer import DataContainer

from dataio.configreader import ConfigReader

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MinutelyData(Metric):

    def __init__(self, prices: DataContainer, cp_name: str, config: ConfigReader):

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
