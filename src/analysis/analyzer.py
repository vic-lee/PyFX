from datetime import datetime
import logging
import functools
from os.path import abspath
import pandas as pd

from analysis.pricemvmts import MaxPriceMovements
from analysis.metrics import MinuteData, PeriodPriceAvg

from common.config import Config

from dataio.dfbundler import DataFrameBundler
from dataio.datareader import DataReader
from dataio.datawriter import DataWriter

from datastructure.datacontainer import DataContainer

import pyfx.write as write


logger = logging.getLogger(__name__)


class Analyzer():

    def __init__(self, config: Config):
        self.__config = config
        self.__FOLDER_TIMESTAMP = self._generate_folder_timestamp()

    def execute(self):
        pass

    def analyze_currency_pair(self, cp_name: str):
        logger.info("Initialize analysis for {}".format(cp_name))
        dataframes = {}

        # 1. Read data
        price_data = self._read_price_data(cp_name)
        data_container = DataContainer(price_dfs=price_data, config=self.__config,
                                       currency_pair_name=cp_name)

        # 2. Include OHLC data
        dataframes['OHLC'] = price_data[DataReader.DAILY]

        # 3. Include Max Pip Movements
        price_movements = MaxPriceMovements(price_data=data_container,
                                            config=self.__config,
                                            currency_pair_name=cp_name)

        price_movements.find_max_price_movements()
        price_movement_analyses = price_movements.to_benchmarked_results()
        dataframes = {**dataframes, **price_movement_analyses}

        if self.__config.should_include_minutely_data:
            # 4. Include selected minute data
            selected_minute_data = MinuteData(
                prices=data_container, config=self.__config, cp_name=cp_name
            ).to_df()
            dataframes['Selected Minute Data'] = selected_minute_data

            # 5. Include Price average data from range
            for avg_data_section in self.__config.period_average_data_sections:
                price_avg_data = PeriodPriceAvg(
                    prices=data_container, cp_name=cp_name,
                    config=self.__config, time_range_for_avg=avg_data_section
                ).to_df()
                col_name = "{}_{}".format(str(avg_data_section.start_time),
                                          str(avg_data_section.end_time))
                dataframes[col_name] = price_avg_data

        dfbundler = DataFrameBundler(dataframes)
        master_df = dfbundler.output()

        write.df_to_xlsx(df=master_df,
                         dir='data/dataout/', folder_name='dataout_',
                         fname=('dataout_{}'.format(cp_name)),
                         folder_unique_id=self.__FOLDER_TIMESTAMP,
                         sheet_name='max_pip_mvmts', col_width=20)

    def _read_price_data(self, currency_pair_name) -> dict:
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

    def _generate_folder_timestamp(self) -> str:
        now = datetime.now()
        fname_suffix = now.strftime("_%Y%m%d_%H%M%S")
        return fname_suffix
