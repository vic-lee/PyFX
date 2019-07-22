import functools
import os
from datetime import datetime
from typing import Callable, List

import pandas as pd

from common.decorators import timer
from ds.timeranges import DateRange

__all__ = ['MINUTE', 'FIX', 'DAILY', 'read_data']


MINUTE = 0,
FIX = 1,
DAILY = 2


def read_data(fpaths: dict, cp_name: str) -> dict:
    resp = {}
    if MINUTE in fpaths:
        resp[MINUTE] = _read_and_process_minute_data(fpaths[MINUTE], cp_name)
    if FIX in fpaths:
        resp[FIX] = _read_and_process_fix_data(fpaths[FIX])
    if DAILY in fpaths:
        resp[DAILY] = _read_and_process_daily_data(fpaths[DAILY], cp_name)
    return None if resp == {} else resp


def cache(cache_fname: str):
    """Decorator for df-reading functions.
    Speeds up data-reading process by attempting to access a cached version
    before opening csv / xlsx.

    Parameter
    ---------
        cache_fname:
            Deserializes this file into a dataframe if the file exists.
            Otherwise, creates a file w/ this fname and cache the dataframe
            to this location.
    """
    def _cache(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if os.path.isfile(cache_fname):
                return pd.read_pickle(cache_fname)
            else:
                df = func(*args, **kwargs)
                df.to_pickle(cache_fname)
                return df
        return wrapper
    return _cache


@timer
def _read_and_process_minute_data(
    fpath: str, cp_name: str,
    processor: Callable[[pd.DataFrame], pd.DataFrame] = None
) -> pd.DataFrame:

    # @cache('cache/min_{}'.format(cp_name))
    def _process_minute_data(min_df: pd.DataFrame) -> pd.DataFrame:

        if 'Volume' in min_df.columns:
            min_df.drop(columns=['Volume'], inplace=True)

        min_df.rename({"Local time": "datetime"},
                        inplace=True, axis='columns')

        min_df['date'] = min_df['datetime'].str.slice(0, 10)
        min_df['date'] = pd.to_datetime(min_df['date'], format='%d.%m.%Y')
        min_df['datetime'] = min_df['datetime'].str.slice(0, 19)

        min_df['datetime'] = pd.to_datetime(
            min_df['datetime'], format="%d.%m.%Y %H:%M:%S")

        min_df.set_index('datetime', inplace=True)

        return min_df

    if not os.path.isfile(fpath):
        raise FileNotFoundError
    df = pd.read_csv(fpath)
    return _process_minute_data(df) if processor == None else processor(df)


@timer
def _read_and_process_fix_data(
    fpath: str, processor: Callable[[pd.DataFrame], pd.DataFrame] = None
) -> pd.DataFrame:

    # @cache('cache/fix')
    def _process_fix_data(fix_df: pd.DataFrame) -> pd.DataFrame:

        fix_df['datetime'] = pd.to_datetime(
            fix_df['datetime'], format="%Y-%m-%d")
        fix_df.set_index('datetime', inplace=True)

        return fix_df

    if not os.path.isfile(fpath):
        raise FileNotFoundError
    df = pd.read_csv(fpath)
    return _process_fix_data(df) if processor == None else processor(df)


@timer
def _read_and_process_daily_data(
    fpath: str, cp_name: str,
    processor: Callable[[pd.DataFrame], pd.DataFrame] = None
) -> pd.DataFrame:

    # @cache('cache/day_{}'.format(cp_name))
    def process_daily_data(day_df: pd.DataFrame,
                           cp_name: str) -> pd.DataFrame:

        f_cpname = reformat_cpname(cp_name)
        assert validate_day_df(day_df, f_cpname)
        day_df.rename(columns={'Date': 'datetime'}, inplace=True)

        day_df = drop_cols(day_df, f_cpname)
        day_df = rename_cols(day_df, f_cpname)

        day_df = day_df.loc[(day_df['datetime'] > "2018-01-01") &
                            (day_df['datetime'] <= "2019-01-01")]

        day_df['datetime'] = day_df['datetime'].apply(
            lambda dt: datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S").date())

        day_df.datetime = pd.to_datetime(day_df.datetime)
        day_df = day_df.set_index('datetime')

        return day_df

    def drop_cols(df: pd.DataFrame, f_cpname: str) -> pd.DataFrame:
        return df.drop(columns=[
            '{}(Open, Ask)'     .format(f_cpname),
            '{}(High, Ask)'     .format(f_cpname),
            '{}(Low, Ask)'      .format(f_cpname),
            '{}(Close, Ask)'    .format(f_cpname),
            'Tick Volume({})'   .format(f_cpname)
        ])

    def rename_cols(df: pd.DataFrame, f_cpname: str) -> pd.DataFrame:
        return df.rename(columns={
            '{}(Open, Bid)*'    .format(f_cpname): 'Open',
            '{}(High, Bid)*'    .format(f_cpname): 'High',
            '{}(Low, Bid)*'     .format(f_cpname): 'Low',
            '{}(Close, Bid)*'   .format(f_cpname): 'Close',
        })

    def reformat_cpname(cp_name: str) -> str:
        assert len(cp_name) == 6
        return '{}/{}'.format(cp_name[:3], cp_name[3:])

    @timer
    def validate_day_df(day_df: pd.DataFrame, f_cpname: str) -> bool:
        assert len(f_cpname) == 7 and '/' in f_cpname
        required = set([
            '{}(Open, Ask)'     .format(f_cpname),
            '{}(High, Ask)'     .format(f_cpname),
            '{}(Low, Ask)'      .format(f_cpname),
            '{}(Close, Ask)'    .format(f_cpname),
            '{}(Open, Bid)*'    .format(f_cpname),
            '{}(High, Bid)*'    .format(f_cpname),
            '{}(Low, Bid)*'     .format(f_cpname),
            '{}(Close, Bid)*'   .format(f_cpname),
            'Tick Volume({})'   .format(f_cpname),
        ])
        return (required.issubset(set(day_df.columns)))

    if not os.path.isfile(fpath):
        raise FileNotFoundError
    df = pd.read_excel(fpath)
    return process_daily_data(df, cp_name) \
        if processor == None else processor(df)
