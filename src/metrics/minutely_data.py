import pandas as pd
from datetime import time
from metrics.metric import Metric


class MinutelyData(Metric):

    def __init__(self, price_dfs, time_range, date_range, cp_name, specs):
        Metric.__init__(self, time_range, date_range, price_dfs, cp_name)
        self.specs = specs
        self.df = self._generate_output_df()

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
                df = self.full_minute_price_df.between_time(
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
        return self.df
