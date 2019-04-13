from os.path import abspath
from datetime import datetime, time
import pandas as pd

from dataio.datareader import DataReader
from dataio.datawriter import DataWriter
from dataio.dfbundler import DataFrameBundler

from metrics.max_price_movements import MaxPriceMovements
from metrics.period_price_avg import PeriodPriceAvg
from metrics.minutely_data import MinutelyData

from datastructure.daytimerange import TimeRangeInDay


def main():
    start_time = datetime.now()

    currency_pairs = [
        "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD",
        "USDCHF", "USDJPY", "USDMXN", "USDTRY", "USDZAR"
    ]

    fname_suffix = generate_folder_timestamp()

    for currency_pair in currency_pairs:
        analyze_currency_pair(currency_pair, fname_suffix)

    # analyze_currency_pair("AUDUSD", fname_suffix)

    end_time = datetime.now()
    print("\nProgram runtime: {}".format((end_time - start_time)))


def analyze_currency_pair(currency_pair_name, timestamp) -> None:
    df_dict = {}

    '''Read data'''
    price_data = read_price_data(currency_pair_name)

    '''Generate data analysis'''
    '''2.0 Daily OHLC Prices'''
    df_dict["OHLC"] = price_data[DataReader.DAILY]

    '''2.1 Max Pip Movements'''
    price_movements = setup_price_movement_obj(
        data=price_data, cp_name=currency_pair_name)

    price_movements.find_max_price_movements()
    price_movement_analyses = price_movements.to_benchmarked_results()
    df_dict = {**df_dict, **price_movement_analyses}

    '''2.2 Selected Minute Data'''
    selected_minute_data = include_minutely_data(
        data=price_data, cp_name=currency_pair_name)
    df_dict["Selected Minute Data"] = selected_minute_data

    '''2.3 Price Average data from range'''
    price_avg_data, timerange = include_period_avg_data(
        price_data, currency_pair_name)
    column_str = str(timerange.start_time) + "_" + str(timerange.end_time)
    df_dict[column_str] = price_avg_data

    '''Aggregate dataframes'''
    dfbundler = DataFrameBundler(df_dict)
    master_df = dfbundler.output()

    '''Output to excel'''
    output_writer = DataWriter(
        df=master_df, currency_pair_name=currency_pair_name, timestamp=timestamp)
    output_writer.df_to_xlsx()


def setup_price_movement_obj(data, cp_name) -> MaxPriceMovements:

    if (cp_name == "GBPUSD"):
        pip_movement_config = {
            MaxPriceMovements.TIME_RANGE: TimeRangeInDay(
                start_time=time(hour=10, minute=30),
                end_time=time(hour=11, minute=2)
            ),
            MaxPriceMovements.BENCHMARK_TIMES: [
                time(hour=10, minute=30),
                time(hour=10, minute=45)
            ],
            MaxPriceMovements.CURRENCY_PAIR: cp_name
        }
    else:
        pip_movement_config = {
            MaxPriceMovements.TIME_RANGE: TimeRangeInDay(
                start_time=time(hour=17, minute=30),
                end_time=time(hour=18, minute=2)
            ),
            MaxPriceMovements.BENCHMARK_TIMES: [
                time(hour=17, minute=30),
                time(hour=17, minute=45)
            ],
            MaxPriceMovements.CURRENCY_PAIR: cp_name
        }

    return MaxPriceMovements(price_dfs=data, config=pip_movement_config)


def include_minutely_data(data, cp_name: str) -> pd.DataFrame:
    if cp_name == "GBPUSD":
        """HACK: Think of a solution to remove dual time standards."""
        minute_data = MinutelyData(
            price_dfs=data,
            time_range=TimeRangeInDay(
                start_time=time(hour=10, minute=30),
                end_time=time(hour=11, minute=2)
            ),
            cp_name=cp_name,
            specs=[
                {
                    "range_start": time(hour=10, minute=49),
                    "range_end": time(hour=11, minute=2),
                    "include": ["Close"]
                },
                {
                    "range_start": time(hour=11, minute=30),
                    "range_end": time(hour=11, minute=30),
                    "include": ['Close']
                },
                {
                    "range_start": time(hour=11, minute=45),
                    "range_end": time(hour=11, minute=45),
                    "include": ['Close']
                },
            ]).to_df()

    else:
        minute_data = MinutelyData(
            price_dfs=data,
            time_range=TimeRangeInDay(
                start_time=time(hour=10, minute=30),
                end_time=time(hour=11, minute=2)
            ),
            cp_name=cp_name,
            specs=[
                {
                    "range_start": time(hour=17, minute=49),
                    "range_end": time(hour=18, minute=2),
                    "include": ["Close"]
                },
                {
                    "range_start": time(hour=18, minute=30),
                    "range_end": time(hour=18, minute=30),
                    "include": ['Close']
                },
                {
                    "range_start": time(hour=18, minute=45),
                    "range_end": time(hour=18, minute=45),
                    "include": ['Close']
                },
            ]).to_df()

    return minute_data


def include_period_avg_data(data, cp_name: str):
    if cp_name == "GBPUSD":
        time_range_start = time(hour=10, minute=58)
        time_range_end = time(hour=11, minute=2)
        df = PeriodPriceAvg(
            price_dfs=data,
            cp_name=cp_name,
            time_range=TimeRangeInDay(
                start_time=time(hour=10, minute=30),
                end_time=time(hour=11, minute=2)
            ),
            time_range_for_avg=TimeRangeInDay(
                start_time=time_range_start,
                end_time=time_range_end
            ),
            include_open={
                time(hour=10, minute=55),
                time(hour=10, minute=56),
            }
        ).to_df()

    else:
        time_range_start = time(hour=17, minute=58)
        time_range_end = time(hour=18, minute=2)
        df = PeriodPriceAvg(
            price_dfs=data,
            cp_name=cp_name,
            time_range=TimeRangeInDay(
                start_time=time(hour=17, minute=30),
                end_time=time(hour=18, minute=2)
            ),
            time_range_for_avg=TimeRangeInDay(
                start_time=time_range_start,
                end_time=time_range_end
            ),
            include_open={
                time(hour=17, minute=55),
                time(hour=17, minute=56),
            }
        ).to_df()

    return df, TimeRangeInDay(start_time=time_range_start, end_time=time_range_end)


def read_price_data(currency_pair_name) -> dict:
    in_fpaths = {
        DataReader.FIX: abspath("../data/datasrc/fix1819.csv"),
        DataReader.MINUTELY: abspath("../data/datasrc/{}_Minute.csv".format(currency_pair_name)),
        DataReader.DAILY: abspath(
            "../data/datasrc/{}_Daily.xlsx".format(currency_pair_name))
    }

    fx_reader = DataReader(in_fpaths, currency_pair_name)
    package = fx_reader.read_data()
    return package


def generate_folder_timestamp() -> str:
    now = datetime.now()
    fname_suffix = now.strftime("_%Y%m%d_%H%M%S")
    return fname_suffix


if __name__ == '__main__':
    main()
