from datareader import DataReader

class Metric:
    def __init__(self, time_range, price_dfs):
        self.time_range = time_range
        self.fix_price_df = price_dfs[DataReader.FIX]
        self.daily_price_df = price_dfs[DataReader.DAILY]
        self.minute_price_df = self._filter_df_to_time_range(price_dfs[DataReader.MINUTELY])

    def _filter_df_to_time_range(self, df):
        return df.between_time(self.time_range.start_time, self.time_range.end_time)
