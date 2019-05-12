from collections import namedtuple
from datetime import datetime, timedelta, time
import logging
import pandas as pd

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DSTConfig = namedtuple('DSTConfig', 'mask timerange')


class DataContainer:

    def __init__(self, price_dfs, currency_pair_name: str, config: ConfigReader):

        self._fix_price_df = price_dfs[DataReader.FIX]
        self._daily_price_df = price_dfs[DataReader.DAILY]
        self._full_minute_price_df = price_dfs[DataReader.MINUTELY]

        self._dst_configs = self._init_dst_config(df=self.full_minute_price_df,
                                                  config=config)
        self._minute_price_df = self._filter_df_to_time_range(df=self.full_minute_price_df,
                                                              config=config)

    @property
    def fix_price_df(self) -> pd.DataFrame:
        return self._fix_price_df

    @property
    def daily_price_df(self) -> pd.DataFrame:
        return self._daily_price_df

    @property
    def full_minute_price_df(self) -> pd.DataFrame:
        return self._full_minute_price_df

    @property
    def minute_price_df(self) -> pd.DataFrame:
        return self._minute_price_df

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
            if self._should_normalize_time_index(start_time, config):

                if self._should_decr_hour(start_time, config):
                    df_segment.index = (df_segment.index +
                                        pd.DateOffset(hours=-1))

                elif self._should_incr_hour(start_time, config):
                    df_segment.index = (df_segment.index +
                                        pd.DateOffset(hours=1))

                else:
                    logger.error("DST period identified but hr not normalized")

            df_list.append(df_segment)

        target = pd.concat(df_list)

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
                                (df['date'] < self._to_datetime(config.dst_hour_delay_period.start_date))),
                          timerange=timerange_normal),

                # DST hr delay period
                DSTConfig(mask=((df['date'] >= self._to_datetime(config.dst_hour_delay_period.start_date)) &
                                (df['date'] <= self._to_datetime(config.dst_hour_delay_period.end_date))),
                          timerange=timerange_delayed),

                # After DST delay period
                DSTConfig(mask=(df['date'] > self._to_datetime(config.dst_hour_delay_period.end_date)),
                          timerange=timerange_normal)
            ]

            return conf

    @staticmethod
    def _to_datetime(date: datetime.date) -> datetime:
        return datetime.combine(date, datetime.min.time())

    @staticmethod
    def _should_normalize_time_index(start_time: datetime.time, config: ConfigReader) -> bool:
        return start_time != config.time_range.start_time

    @staticmethod
    def _should_decr_hour(start_time, config: ConfigReader) -> bool:
        return start_time == config.dst_hour_ahead_time_range.start_time

    @staticmethod
    def _should_incr_hour(start_time, config: ConfigReader) -> bool:
        return start_time == config.dst_hour_behind_time_range.start_time
