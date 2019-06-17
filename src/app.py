"""
This file is the starting point of the program. It provides a bird's eye view 
of the operations carried out to generate key metrics and to export to excel. 
"""

from datetime import datetime
import logging
from os.path import abspath
import pandas as pd

from analysis.analyzer import Analyzer
from common.config import Config
from common.decorators import timer
from datastructure.datacontainer import DataContainer
from pyfx import read, write, analysis


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

DEFAULT_CONFIG_FPATH = 'config.json'


@timer
def main():
    """
    This function houses the application logic. At the highest level, the
    program performs the same analysis for each currency pair, stored in
    the array.
    """

    config = Config(DEFAULT_CONFIG_FPATH)
    # analyzer = Analyzer(config)
    # analyzer.analyze_currency_pair(cp_name="GBPUSD")

    fpaths = {
        read.MINUTE:    abspath("data/datasrc/GBPUSD_Candlestick.csv"),
        read.FIX:       abspath("data/datasrc/fix1819.csv"),
        read.DAILY:     abspath("data/datasrc/GBPUSD_Daily.xlsx")
    }

    dfs = read.read_and_process_data(fpaths, cp_name="GBPUSD")
    data = DataContainer(dfs, "GBPUSD", config)

    outputs = []

    ohlc = data.daily_price_df
    ohlc.columns = pd.MultiIndex.from_product([['OHLC'], ohlc.columns])
    outputs.append(ohlc)

    maxpips = analysis.find_max_pips(data, config.benchmark_times)
    outputs.append(maxpips)

    if config.should_include_minutely_data:
        minute_data = analysis.include_minute_data(
            data, config.minutely_data_sections)
        outputs.append(minute_data)

    if config.should_include_period_average_data:
        avg_data = analysis.include_avg(
            data, config.period_average_data_sections)
        assert avg_data is not None
        outputs.append(avg_data)

    df_master = pd.concat(outputs, axis=1)
    df_master.index = df_master.index.date

    write.df_to_xlsx(df=df_master,
                     dir='data/dataout/', folder_name='dataout_',
                     fname=('dataout_{}'.format("GBPUSD")),
                     folder_unique_id=datetime.now().strftime("_%Y%m%d_%H%M%S"),
                     sheet_name='max_pip_mvmts', col_width=20)


if __name__ == '__main__':
    main()
