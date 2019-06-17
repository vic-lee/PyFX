"""
This file is the starting point of the program. It provides a bird's eye view 
of the operations carried out to generate key metrics and to export to excel. 
"""

from datetime import datetime
import logging
from os.path import abspath
import pandas as pd

from common.config import Config
from common.decorators import timer
from common.utils import run
from ds.datacontainer import DataContainer
from pyfx import read, write, analytics


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


def io(func):
    def wrapper(*args, **kwargs):
        cp_name = kwargs.get('cp_name')
        config = kwargs.get('config')

        fpaths = {
            read.MINUTE:
                abspath("data/datasrc/GBPUSD_Candlestick.csv"),
            read.FIX:
                abspath("data/datasrc/fix1819.csv"),
            read.DAILY:
                abspath("data/datasrc/{}_Daily.xlsx".format(cp_name))
        }

        dfs = read.read_and_process_data(fpaths, cp_name=cp_name)
        data = DataContainer(dfs, cp_name, config)

        df_master = func(*args, **kwargs, data=data)

        write.df_to_xlsx(df=df_master,
                         dir='data/dataout/', folder_name='dataout_',
                         fname=('dataout_{}'.format(cp_name)),
                         folder_unique_id=datetime.now().strftime("_%Y%m%d_%H%M%S"),
                         sheet_name='max_pip_mvmts', col_width=20)
    return wrapper


@io
def exec(cp_name: str, config: Config, **kwargs):

    data = kwargs.get('data')

    output_funcs = [
        run(analytics.include_ohlc, data),
        run(analytics.include_max_pips, data, config.benchmark_times),
        run(analytics.include_max_pips, data, pdfx=True, cp_name=cp_name),
        run(analytics.include_minute_data, data, config.minutely_data_sections),
        run(analytics.include_avgs, data, config.period_average_data_sections)
    ]

    outputs = map(lambda f: f(), output_funcs)

    df_master = pd.concat(outputs, axis=1)
    df_master.index = df_master.index.date

    return df_master


@timer
def main():
    config = Config(DEFAULT_CONFIG_FPATH)

    for cp in config.currency_pairs:
        exec(cp_name=cp, config=config)


if __name__ == '__main__':
    main()
