import os
import pandas as pd
from datetime import datetime

from common.decorators import comp_xlsx


class DataWriter:

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

    def df_to_xlsx(self):
        with pd.ExcelWriter(self._default_fname_xlsx, engine='xlsxwriter') as writer:
            self.__dfout.to_excel(writer, sheet_name="max_pip_mvmts")
            sheet = writer.sheets['max_pip_mvmts']
            wide_col = 20
            sheet.set_column(0, len(self.__dfout.columns), wide_col)

        consistent = comp_xlsx(new_fname=self._default_fname_xlsx,
                               original_fname="data/dataout/dataout__20190606_230751/dataout_GBPUSD.xlsx")

        print("Consistency: {}".format(consistent))
