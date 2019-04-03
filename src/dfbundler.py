import pandas as pd

class DataFrameBundler:
    
    def __init__(self, df_dict):
        self._df_dict = df_dict
        self._merged_df = self._merge_dfs()

    
    def _merge_dfs(self):
        target = pd.DataFrame()

        for header_name, df in self._df_dict.items():
            df.columns = pd.MultiIndex.from_product([[header_name], df.columns])
            target = target.join(df)

        return target


    def df_output(self):
        return self._merge_dfs
