from collections import namedtuple
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader
from datastructure.pricetime import PriceTime
from datastructure.daytimerange import DayTimeRange
from datastructure.daterange import DateRange

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

DSTConfig = namedtuple('DSTConfig', 'mask timerange')


class Metric:

    def __init__(self, price_dfs, currency_pair_name: str, config: ConfigReader):

        self.time_range = config.time_range
        self.date_range = config.date_range
        self.currency_pair_name = currency_pair_name

        self.fix_price_df = price_dfs[DataReader.FIX]
        self.daily_price_df = price_dfs[DataReader.DAILY]
        self.full_minute_price_df = price_dfs[DataReader.MINUTELY]

        self._dst_configs = self._init_dst_config(df=self.full_minute_price_df,
                                                  config=config)
        self.minute_price_df = self._filter_df_to_time_range(df=self.full_minute_price_df,
                                                             config=config)
        print(self.minute_price_df)

    def _filter_df_to_time_range(self, df: pd.DataFrame, config: ConfigReader) -> pd.DataFrame:

        if config.should_enable_daylight_saving_mode:
            return self._adjust_df_for_dst(df, config)
        else:
            return df.between_time(config.time_range.start_time, config.time_range.end_time)

    def _adjust_df_for_dst(self, df: pd.DataFrame, config: ConfigReader) -> pd.DataFrame:
        df_list = []

        for dst_config in self._dst_configs:

            df_segment = df.loc[dst_config.mask]
            df_segment = df_segment.between_time(*dst_config.timerange)

            start_time = dst_config.timerange[0]
            if self._should_normalize_time_index(start_time):

                if self._should_decr_hour(start_time, config):
                    df_segment.index = (df_segment.index +
                                        pd.DateOffset(hours=-1))

                elif self._should_incr_hour(start_time, config):
                    df_segment.index = (df_segment.index +
                                        pd.DateOffset(hours=-1))

                else:
                    logger.error("DST period identified but hr not normalized")

            df_list.append(df_segment)

        target = pd.concat(df_list)
        target.to_excel("out_norm.xlsx")
        return target

    def _init_dst_config(self, df: pd.DataFrame, config: ConfigReader) -> list:
        conf = []

        timerange_normal = [config.time_range.start_time,
                            config.time_range.end_time]

        timerange_ahead = [config.dst_hour_ahead_time_range.start_time,
                           config.dst_hour_ahead_time_range.end_time]

        timerange_delayed = [config.dst_hour_behind_time_range.start_time,
                             config.dst_hour_behind_time_range.end_time]

        if not config.should_enable_daylight_saving_mode:
            return conf

        else:
            conf = [
                # Before DST hr ahead period
                DSTConfig(mask=(df['date'] < self._to_datetime(config.dst_hour_ahead_period.start_date)),
                          timerange=timerange_normal),

                # DST hr ahead period
                DSTConfig(mask=((df['date'] >= self._to_datetime(config.dst_hour_ahead_period.start_date)) &
                                (df['date'] <= self._to_datetime(config.dst_hour_ahead_period.end_date))),
                          timerange=timerange_ahead),

                # Between DST hr ahead and DST hr delay period
                DSTConfig(mask=((df['date'] > self._to_datetime(config.dst_hour_ahead_period.end_date)) &
                                (df['date'] < self._to_datetime(config.dst_hour_behind_period.start_date))),
                          timerange=timerange_normal),

                # DST hr delay period
                DSTConfig(mask=((df['date'] >= self._to_datetime(config.dst_hour_behind_period.start_date)) &
                                (df['date'] <= self._to_datetime(config.dst_hour_behind_period.end_date))),
                          timerange=timerange_delayed),

                # After DST delay period
                DSTConfig(mask=(df['date'] > self._to_datetime(config.dst_hour_behind_period.end_date)),
                          timerange=timerange_normal)
            ]

            return conf

    def _get_prior_fix_recursive(self, d):

        def daydelta(d, delta): return d - timedelta(days=delta)

        fx = None

        if self.date_range.is_datetime_in_range(d):

            try:
                cp_identifier = self.currency_pair_name[:3] + \
                    '-' + self.currency_pair_name[3:]
                fx = self.fix_price_df.loc[str(d)][cp_identifier]
            except Exception as e:
                logger.error(
                    "Could not locate the previous fix price, possibly due to out of bounds." + str(e))
                return None
            if not np.isnan(fx):
                index = datetime(year=d.year, month=d.month,
                                 day=d.day, hour=0, minute=0, second=0)
                return PriceTime(price=fx, datetime=index)
            else:
                return self._get_prior_fix_recursive(daydelta(d, 1))

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

    @staticmethod
    def _to_datetime(date: datetime.date) -> datetime:
        return datetime.combine(date, datetime.min.time())

    def _should_normalize_time_index(self, start_time: datetime.time) -> bool:
        return start_time != self.time_range.start_time

    @staticmethod
    def _should_decr_hour(start_time, config: ConfigReader) -> bool:
        return start_time == config.dst_hour_ahead_time_range.start_time

    @staticmethod
    def _should_incr_hour(start_time, config: ConfigReader) -> bool:
        return start_time == config.dst_hour_behind_time_range.start_time
