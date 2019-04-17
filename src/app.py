"""
This file is the starting point of the program. It provides a bird's eye view 
of the operations carried out to generate key metrics and to export to excel. 
"""

from os.path import abspath
from datetime import datetime, time, date
import pandas as pd
import logging

from dataio.datareader import DataReader
from dataio.datawriter import DataWriter
from dataio.dfbundler import DataFrameBundler

from metrics.max_price_movements import MaxPriceMovements
from metrics.period_price_avg import PeriodPriceAvg
from metrics.minutely_data import MinutelyData

from datastructure.daytimerange import TimeRangeInDay
from datastructure.daterange import DateRange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('app.log')
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def main():
    """
    This function houses the application logic. At the highest level, the 
    program performs the same analysis for each currency pair, stored in 
    the array. 
    """
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
    logger.info("\nProgram runtime: {}".format((end_time - start_time)))


def analyze_currency_pair(currency_pair_name, timestamp) -> None:
    """
    This function encapsulates the key steps to perform to generate metrics for 
    the output. Comments below detail the steps. 
    """

    logger.info("Initialize analysis for {}".format(currency_pair_name))

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
    """
    This function encapsulates the definition of a `MaxPriceMovements` class, 
    which iterates to find the max pips for each day. To do this, it defines 
    the `MaxPriceMovements` class by specifying: 

        1) the starting time, ending time of the range to search for, 
        2) the starting date, ending date to search for, and 
        3) the benchmark prices. 

    The choice to hard-code the config parameters, rather than passing in the 
    specifications, is intentional. 
    First, the config should ideally not be tampered with very often, once the 
    workflow is appropriately set up. 
    Second, passing in all the params make this function no different from the 
    class's default constructor. 

    For detailed implementation of `MaxPriceMovements`, see 
    `metrics/max_price_movements.py`.
    """

    pip_movement_config = {
        MaxPriceMovements.TIME_RANGE: TimeRangeInDay(
            start_time=time(hour=14, minute=30),
            end_time=time(hour=15, minute=2)
        ),
        MaxPriceMovements.DATE_RANGE: DateRange(
            start_date=date(year=2018, month=1, day=1),
            end_date=date(year=2018, month=12, day=31)
        ),
        MaxPriceMovements.BENCHMARK_TIMES: [
            time(hour=14, minute=30),
            time(hour=14, minute=45)
        ],
        MaxPriceMovements.CURRENCY_PAIR: cp_name
    }

    return MaxPriceMovements(price_dfs=data, config=pip_movement_config)


def include_minutely_data(data, cp_name: str) -> pd.DataFrame:
    """
    This function defines what minutely data to include in the output. 
    The minutely data to include can be specified by two dimensions: 

        1) the time range of prices to include (e.g. 3:20-3:40PM)
        2) the type of price to include (OHLC)

    Additional details, such as starting time, ending time, starting date, and
    ending date, are also defined. 

    Please see `metrics/minutely_data.py` for detailed implementation. 
    """

    minute_data = MinutelyData(
        price_dfs=data,
        time_range=TimeRangeInDay(
            start_time=time(hour=14, minute=30),
            end_time=time(hour=15, minute=2)
        ),
        date_range=DateRange(
            start_date=date(year=2018, month=1, day=1),
            end_date=date(year=2018, month=12, day=31)
        ),
        cp_name=cp_name,
        specs=[
            {
                "range_start": time(hour=14, minute=49),
                "range_end": time(hour=15, minute=2),
                "include": ["Close"]
            },
            {
                "range_start": time(hour=15, minute=30),
                "range_end": time(hour=15, minute=30),
                "include": ['Close']
            },
            {
                "range_start": time(hour=15, minute=45),
                "range_end": time(hour=15, minute=45),
                "include": ['Close']
            },
        ]).to_df()

    return minute_data


def include_period_avg_data(data, cp_name: str):
    """
    This function calculates and returns a series of average prices, given the
    starting time and ending time for the calculation period. 
    """

    avg_time_range_start = time(hour=14, minute=58)
    avg_time_range_end = time(hour=15, minute=2)
    df = PeriodPriceAvg(
        price_dfs=data,
        cp_name=cp_name,
        time_range=TimeRangeInDay(
            start_time=time(hour=14, minute=30),
            end_time=time(hour=15, minute=2)
        ),
        date_range=DateRange(
            start_date=date(year=2018, month=1, day=1),
            end_date=date(year=2018, month=12, day=31)
        ),
        time_range_for_avg=TimeRangeInDay(
            start_time=avg_time_range_start,
            end_time=avg_time_range_end
        ),
        include_open={
            time(hour=14, minute=55),
            time(hour=14, minute=56),
        }
    ).to_df()

    return df, TimeRangeInDay(start_time=avg_time_range_start, end_time=avg_time_range_end)


def read_price_data(currency_pair_name) -> dict:
    """
    This function reads in data from the data source directory. 

    The input files follow a specific naming convention, to make this function's 
    implementation easier. 

    Specifically, the convention is as follows:

        * file for fix price (in CSV):      fix1819.csv
        * file for minutely data (in CSV):  {CurrencyPairName}_Minute.csv
        * file for daily data (in xlsx):    {CurrencyPairName}_Daily.xslx
    """

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
    """
    This is a helper function that generates a timestamp that will be appended
    to an output's folder name. Appending timestamps to a folder name 
    containing outputs prevent old outputs from being overriden (thus wiping 
    out the output history). 
    """

    now = datetime.now()
    fname_suffix = now.strftime("_%Y%m%d_%H%M%S")
    return fname_suffix


if __name__ == '__main__':
    main()
