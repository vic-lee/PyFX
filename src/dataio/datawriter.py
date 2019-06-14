from datetime import datetime
import functools
import os
import pandas as pd

from common.utils import comp_xlsx


class XlsxOutputInconsistentError(Exception):
    pass


class DataWriter:

    class _Decorators():

        @staticmethod
        def test_output_consistency(benchmark_file_fname: str):

            if not os.path.isfile(benchmark_file_fname):
                raise FileNotFoundError

            def _test(xlsx_writing_func):
                @functools.wraps(xlsx_writing_func)
                def wrapper(self, *args, **kwwargs):
                    xlsx_writing_func(self, *args, **kwwargs)
                    is_consistent = comp_xlsx(original_fname=benchmark_file_fname,
                                              new_fname=self._default_fname_xlsx)
                    if not is_consistent:
                        print("Consistency check: False")
                        raise XlsxOutputInconsistentError
                    else:
                        print("Consistency check: True")
                return wrapper
            return _test

    def __init__(self, df, currency_pair_name: str, timestamp: str, filename="data/dataout/"):
        self.__dfout = df
        self.__default_fname = filename + "dataout_{}".format(timestamp)

        if not os.path.exists(self.__default_fname):
            os.makedirs(self.__default_fname)

        self._default_fname_xlsx = (self.__default_fname
                                    + '/dataout_'
                                    + currency_pair_name + ".xlsx")

        self._default_fname_csv = (self.__default_fname
                                   + '/dataout_'
                                   + currency_pair_name + ".csv")

    @_Decorators.test_output_consistency("data/dataout/dataout__20190606_230751/dataout_GBPUSD.xlsx")
    def df_to_xlsx(self):
        with pd.ExcelWriter(self._default_fname_xlsx, engine='xlsxwriter') as writer:
            self.__dfout.to_excel(writer, sheet_name="max_pip_mvmts")
            sheet = writer.sheets['max_pip_mvmts']
            wide_col = 20
            sheet.set_column(0, len(self.__dfout.columns), wide_col)
