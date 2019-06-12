"""
This file is the starting point of the program. It provides a bird's eye view 
of the operations carried out to generate key metrics and to export to excel. 
"""

from datetime import datetime, time, date
import logging
from os.path import abspath
import pandas as pd

from analysis.metrics import PeriodPriceAvg, MinutelyData
from analysis.pricemvmts import MaxPriceMovements

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader
from dataio.datawriter import DataWriter
from dataio.dfbundler import DataFrameBundler

from datastructure.datacontainer import DataContainer
from datastructure.daterange import DateRange
from datastructure.daytimerange import DayTimeRange

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.FileHandler('app.log')

handler.setLevel(logging.INFO)

fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)
logger.addHandler(handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


def main():
    """
    This function houses the application logic. At the highest level, the
    program performs the same analysis for each currency pair, stored in
    the array.
    """
    start_time = datetime.now()

    config = ConfigReader('config.json')
    print(config.dst_hour_ahead_periods)

    fname_suffix = generate_folder_timestamp()

    for currency_pair in config.currency_pairs:
        analyze_currency_pair(currency_pair, fname_suffix, config)

    end_time = datetime.now()
    logger.info("\nProgram runtime: {}".format((end_time - start_time)))


def analyze_currency_pair(currency_pair_name: str,
                          timestamp, config: ConfigReader) -> None:

    master_df = perform_analysis(currency_pair_name, config)

    '''Output to excel'''
    output_writer = DataWriter(df=master_df,
                               currency_pair_name=currency_pair_name,
                               timestamp=timestamp)
    output_writer.df_to_xlsx()


def perform_analysis(currency_pair_name, config: ConfigReader) -> pd.DataFrame:
    """
    This function encapsulates the key steps to perform to generate metrics for 
    the output. Comments below detail the steps. 
    """

    logger.info("Initialize analysis for {}".format(currency_pair_name))

    df_dict = {}

    '''Read data'''
    price_data = read_price_data(currency_pair_name)
    data_container = DataContainer(price_dfs=price_data,
                                   config=config,
                                   currency_pair_name=currency_pair_name)

    '''Generate data analysis'''
    '''2.0 Daily OHLC Prices'''
    df_dict["OHLC"] = price_data[DataReader.DAILY]

    '''2.1 Max Pip Movements'''
    price_movements = MaxPriceMovements(price_data=data_container,
                                        config=config,
                                        currency_pair_name=currency_pair_name)

    price_movements.find_max_price_movements()
    price_movement_analyses = price_movements.to_benchmarked_results()
    df_dict = {**df_dict, **price_movement_analyses}

    '''2.2 Selected Minute Data'''
    if config.should_include_minutely_data:

        selected_minute_data = include_minutely_data(
            price_data=data_container, cp_name=currency_pair_name, config=config)

        df_dict["Selected Minute Data"] = selected_minute_data

    '''2.3 Price Average data from range'''
    if config.should_include_minutely_data:

        for avg_data_section in config.period_average_data_sections:

            price_avg_data, timerange = include_period_avg_data(
                data_container, currency_pair_name,
                config=config, avgrange=avg_data_section)

            column_str = (str(timerange.start_time)
                          + "_"
                          + str(timerange.end_time))

            df_dict[column_str] = price_avg_data

    '''Aggregate dataframes'''
    dfbundler = DataFrameBundler(df_dict)
    master_df = dfbundler.output()

    return master_df


def include_minutely_data(price_data: DataContainer, cp_name: str,
                          config: ConfigReader) -> pd.DataFrame:
    """
    This function defines what minutely data to include in the output. 
    The minutely data to include can be specified by two dimensions: 

        1) the time range of prices to include (e.g. 3:20-3:40PM)
        2) the type of price to include (OHLC)

    Additional details, such as starting time, ending time, starting date, and
    ending date, are also defined. 

    Please see `metrics/minutely_data.py` for detailed implementation. 
    """

    minute_data = MinutelyData(prices=price_data,
                               config=config,
                               cp_name=cp_name,
                               ).to_df()

    return minute_data


def include_period_avg_data(data: DataContainer, cp_name: str,
                            config: ConfigReader, avgrange: DayTimeRange):
    """
    This function calculates and returns a series of average prices, given the
    starting time and ending time for the calculation period. 
    """

    df = PeriodPriceAvg(prices=data,
                        cp_name=cp_name,
                        config=config,
                        time_range_for_avg=avgrange,
                        ).to_df()

    return df, DayTimeRange(start_time=avgrange.start_time,
                            end_time=avgrange.end_time)


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
        DataReader.FIX: abspath("data/datasrc/fix1819.csv"),
        # DataReader.MINUTELY: abspath("data/datasrc/{}_Minute.csv".format(currency_pair_name)),
        DataReader.MINUTELY: abspath("data/datasrc/GBPUSD_Candlestick.csv"),
        DataReader.DAILY: abspath(
            "data/datasrc/{}_Daily.xlsx".format(currency_pair_name))
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
