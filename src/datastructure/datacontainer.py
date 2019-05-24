from collections import namedtuple
from datetime import datetime, timedelta, time
import logging
import pandas as pd

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader
from datastructure.daterange import DateRange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DSTConfig = namedtuple('DSTConfig', 'mask timerange')


class DataContainer:

    def __init__(self, price_dfs, currency_pair_name: str, config: ConfigReader):

        self._fix_price_df = price_dfs[DataReader.FIX]
        self._daily_price_df = price_dfs[DataReader.DAILY]
        self._full_minute_price_df = price_dfs[DataReader.MINUTELY]
        self._minute_price_df = self._adjust_for_dst(config=config)

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

    def _adjust_for_dst(self, config: ConfigReader) -> pd.DataFrame:

        filtered_df = self.full_minute_price_df.between_time(config.time_range.start_time,
                                                             config.time_range.end_time)

        if config.should_enable_daylight_saving_mode:

            for ahead_period in config.dst_hour_ahead_periods:
                filtered_df = self._adjust_for_ahead_period(
                    filtered_df, ahead_period, config)

        return filtered_df

    def _adjust_for_ahead_period(self, filtered_df: pd.DataFrame,
                                 ahead_period: DateRange,
                                 config: ConfigReader) -> pd.DataFrame:

        mask = ((self.full_minute_price_df['date'] >= self._to_datetime(ahead_period.start_date))
                & (self.full_minute_price_df['date'] <= self._to_datetime(ahead_period.end_date)))

        df_segment = self.full_minute_price_df.loc[mask].copy()
        df_segment = df_segment.between_time(config.dst_hour_ahead_time_range.start_time,
                                             config.dst_hour_ahead_time_range.end_time)

        df_segment.index = (df_segment.index + pd.DateOffset(hours=-1))

        filtered_df.loc[mask] = df_segment

        return filtered_df

    @staticmethod
    def _to_datetime(date: datetime.date) -> datetime:
        """Convert date to datetime."""
        return datetime.combine(date, datetime.min.time())

    @staticmethod
    def _should_normalize_time_index(start_time: datetime.time,
                                     config: ConfigReader) -> bool:
        return start_time != config.time_range.start_time

    @staticmethod
    def _should_decr_hour(start_time: datetime.time,
                          config: ConfigReader) -> bool:
        """Should decrement hour if the time is within the hour_ahead range"""
        return start_time == config.dst_hour_ahead_time_range.start_time

    @staticmethod
    def _should_incr_hour(start_time: datetime.time,
                          config: ConfigReader) -> bool:
        """Should increment hour if the time is within the hour_behind range"""
        return start_time == config.dst_hour_behind_time_range.start_time
