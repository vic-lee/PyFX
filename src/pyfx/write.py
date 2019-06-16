import functools
import os
from pathlib import Path
import pandas as pd
from typing import List

from common.utils import comp_xlsx

__all__ = ['df_to_xlsx', 'merge_dfs']


def check_xlsx_consistency(benchmark_fname: str):
    """
    Func decorator that checks if the excel produced by the function is the 
    same as the benchmark. Result is printed.
    """
    if not os.path.isfile(benchmark_fname):
        raise FileNotFoundError

    def _test(xlsx_writing_func):
        @functools.wraps(xlsx_writing_func)
        def wrapper(*args, **kwargs):
            fpath = xlsx_writing_func(*args, **kwargs)
            is_consistent = comp_xlsx(
                original_fname=benchmark_fname, new_fname=fpath)
            print("Consistency check: {}".format(is_consistent))
        return wrapper
    return _test


@check_xlsx_consistency("data/dataout/dataout__20190606_230751/dataout_GBPUSD.xlsx")
def df_to_xlsx(df: pd.DataFrame, fname: str, dir: str = '',
               folder_name: str = '', folder_unique_id: str = '',
               fname_unique_id: str = '', sheet_name: str = 'sheet1',
               col_width: int = 15) -> str:
    """Output a dataframe to an excel file.

    Parameters
    ----------
        df : dataframe for output
        fname : output filename
        dir : optional, path to data output directory
        folder_name : optional, name of the folder that houses this output
        folder_unique_id : optional, appended to `folder_name` if provided
        fname_unique_id : optional, appended to `fname` if provided
        sheet_name : optional, default to `sheet1`
        col_width : optional, adjust excel output column width; default to 15

    Note
    ----
        Ensure `dir` ends with `/` in order for it to be a directory.
        `.xlsx` will be appended to fname if not provided.
    """

    path = _build_path(dir, folder_name, folder_unique_id)
    fname += fname_unique_id
    if fname[-5] != '.xlsx':
        fname += '.xlsx'
    fname = '/'.join([path, fname])

    with pd.ExcelWriter(fname, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name=sheet_name)
        sheet = writer.sheets[sheet_name]
        sheet.set_column(0, len(df.columns), col_width)

    return fname


def _build_path(dir: str, folder_name: str, folder_uid: str) -> str:
    """Builds fpath based on dir, folder_name, and folder_uid."""
    folder_name = folder_name + folder_uid
    path = dir + folder_name
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def merge_dfs(dfs: dict) -> pd.DataFrame:
    target = pd.DataFrame()
    target.columns = pd.MultiIndex.from_product([[""], target.columns])

    for header_name, df in dfs.items():
        df.columns = pd.MultiIndex.from_product([[header_name], df.columns])
        target = target.join(df, how="right")

    target.index = target.index.strftime('%Y-%m-%d')
    return target
