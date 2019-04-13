import pandas as pd
import numpy as np

class DataFrameBundler:
    
    def __init__(self, df_dict):
        self._df_dict = df_dict
        self._merged_df = self._merge_dfs()

    
    def _merge_dfs(self):
        target = pd.DataFrame()
        target.columns = pd.MultiIndex.from_product([[""], target.columns])

        for header_name, df in self._df_dict.items():
            df.columns = pd.MultiIndex.from_product([[header_name], df.columns])
            target = target.join(df, how="right")

        target.index = target.index.strftime('%Y-%m-%d')
        return target


    def output(self) -> pd.DataFrame:
        return self._merged_df
