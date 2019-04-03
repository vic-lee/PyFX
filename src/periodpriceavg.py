from datetime import timedelta, time
import pandas as pd

from metric import Metric
from datareader import DataReader
from daytimerange import TimeRangeInDay

class PeriodPriceAvg(Metric): 
    def __init__(self, price_dfs, time_range: TimeRangeInDay, time_range_for_avg: TimeRangeInDay):
        Metric.__init__(self, time_range, price_dfs)
        self.time_range_for_avg = time_range_for_avg
        self.avgs = self._calc_avgs()

    
    def _calc_avgs(self):
        '''
        Generate a df for every PriceTime within the time range. 
        Lump dfs together. 
        Create a new column for avg. 
        Calculate avg for every datapoint in the row. 
        '''

        price_in_range_df = self._generate_df_for_prices_in_range()
        df = pd.DataFrame()
        return df
    
    def _generate_df_for_prices_in_range(self):
        time_cur = self.time_range_for_avg.start_time

        df_list = {}
        while time_cur <= self.time_range_for_avg.end_time:
            single_minute_df = self.minute_price_df.between_time(time_cur, time_cur)['Close']
            single_minute_df = single_minute_df.rename(time_cur)
            single_minute_df.index = single_minute_df.index.normalize()
            df_list[time_cur] = single_minute_df
            time_cur = self.incr_one_min(time_cur)

        df = self._join_minute_dfs(df_list)
        df['Mean'] = df.mean(axis=1).round(5)
        print(df)

        return df


    def _join_minute_dfs(self, df_list):
        df_out = pd.DataFrame()
        for _, right_df in df_list.items():
            df_out = df_out.join(right_df, how="outer")
        return df_out


    def incr_one_min(self, time_cur):
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
        pass
