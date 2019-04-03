import pandas as pd

class DataWriter: 
    
    def __init__(self, df, filename="dataout"):
        self._df = df
        self._default_fname = filename
        self._default_fname_xlsx = self._default_fname + ".xlsx"
        self._default_fname_csv = self._default_fname + ".csv"


    def df_to_xlsx(self):
        with pd.ExcelWriter(self._default_fname_xlsx, engine='xlsxwriter') as writer: 
            self._df.to_excel(writer, sheet_name="max_pip_mvmts")
            sheet = writer.sheets['max_pip_mvmts']
            wide_col = 20
            sheet.set_column(0, len(self._df.columns), wide_col)
