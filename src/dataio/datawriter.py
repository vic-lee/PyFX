import pandas as pd
from datetime import datetime


class DataWriter:

    def __init__(self, df, currency_pair_name, filename="../data/dataout/dataout"):
        self._df = df
        self._default_fname = filename

        fname_suffix = self._generate_time_suffix()

        self._default_fname_xlsx = self._default_fname + '_' + \
            currency_pair_name + fname_suffix + ".xlsx"

        self._default_fname_csv = currency_pair_name + '_' + \
            currency_pair_name + self._default_fname + fname_suffix + ".csv"

    def _generate_time_suffix(self):
        now = datetime.now()
        fname_suffix = now.strftime("_%Y%m%d_%H%M%S")
        return fname_suffix

    def df_to_xlsx(self):
        with pd.ExcelWriter(self._default_fname_xlsx, engine='xlsxwriter') as writer:
            self._df.to_excel(writer, sheet_name="max_pip_mvmts")
            sheet = writer.sheets['max_pip_mvmts']
            wide_col = 20
            sheet.set_column(0, len(self._df.columns), wide_col)
