"""
This file is the starting point of the program. It provides a bird's eye view 
of the operations carried out to generate key metrics and to export to excel. 
"""

import logging
from os.path import abspath

from analysis.analyzer import Analyzer

from common.config import Config
from common.decorators import timer

import pyfx.read as read


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
    analyzer = Analyzer(config)
    analyzer.analyze_currency_pair(cp_name="GBPUSD")

    # fpaths = {
    #     # read.MINUTE:    abspath("data/datasrc/GBPUSD_Candlestick.csv"),
    #     # read.FIX:       abspath("data/datasrc/fix1819.csv"),
    #     read.DAILY:     abspath("data/datasrc/GBPUSD_Daily.xlsx")
    # }

    # dfs = read.read_and_process_data(fpaths, cp_name="GBPUSD")
    # print(dfs[read.DAILY])

    # data_container = DataContainer(read_price_data("GBPUSD"), "GBPUSD", config)
    # count_crossovers(data_container)


if __name__ == '__main__':
    main()
