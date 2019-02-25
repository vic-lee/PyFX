import pandas as pd
from datetime import datetime
import os
import matplotlib.pyplot as plt
import xlsxwriter


c_mpip_1030 = '1030_MAX_PIP'
c_mpip_1045 = '1045_MAX_PIP'
c_mpip_1030_dt = "1030_DATETIME_MPIP"
c_mpip_1045_dt = "1045_DATETIME_MPIP"
c_mpip_1030_pr = "1030_PRICE_MPIP"
c_mpip_1045_pr = "1045_PRICE_MPIP"

c_ls_1030 = '1030_LS'
c_ls_1045 = '1045_LS'
c_1030_pr = '1030_PRICE'
c_1045_pr = '1045_PRICE'
c_1102_pr = '1102_CLOSE'


pipmvmt = lambda final, initial: (final - initial) * 10000

is_same_date = lambda d1, d2: (d1.year == d2.year) \
    and (d1.month == d2.month) and (d1.day == d2.day)

days_against_pip_mvmt = lambda df, pipmvmt: df.query(\
    '{} < pip & pip < 0'.format(pipmvmt))

init_mpip = lambda: { 
        c_mpip_1030: 0,
        c_mpip_1045: 0,
        c_mpip_1030_dt: 0,
        c_mpip_1045_dt: 0,
        c_mpip_1030_pr: 0,
        c_mpip_1045_pr: 0
    }

init_ls = lambda: {
        c_ls_1030: 'N/A',
        c_ls_1045: 'N/A',
        c_1030_pr: 'N/A',
        c_1045_pr: 'N/A',
        c_1102_pr: 'N/A'
    }

def csv_in(fpath):
    df = pd.read_csv(fpath)

    df.columns = ["date", "time", "val", "B", "C", "D", "E"]
    df = df.drop(columns=['B', 'C', 'D', 'E'])

    df['datetime'] = df["date"].map(str) + " " + df["time"] 
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = pd.to_datetime(df["date"]) 
    df = df.set_index('datetime')
    df = df[['date', 'time', 'val']]
    df = df.drop(columns=['time', 'date'])

    return df


def proc_df():
    cur_date = None
    p1030 = None
    p1045 = None

    mpip = {}  
    ls = {}  
    
    for index, row in mdf.iterrows():

        if index.date() != cur_date:
            cur_date = index.date()
            mpip[cur_date] = init_mpip()
            ls[cur_date] = init_ls()
            df_1030_idx = str(cur_date) + " 10:30:00"
            df_1045_idx = str(cur_date) + " 10:45:00"
            p1030 = df_1030.loc[df_1030_idx]['val']
            p1045 = df_1045.loc[df_1045_idx]['val']
            mpip[cur_date][c_1030_pr] = p1030
            mpip[cur_date][c_1045_pr] = p1045

        # Record / update daily max pip movement compared to 10:30 & 10:45 prices
        cur_pip_1030 = round((row['val'] - p1030) * 10000, 4)
        cur_pip_1045 = round((row['val'] - p1045) * 10000, 4)
        if cur_pip_1030 > mpip[cur_date][c_mpip_1030]:
            mpip[cur_date][c_mpip_1030] = cur_pip_1030
            mpip[cur_date][c_mpip_1030_dt] = index
            mpip[cur_date][c_mpip_1030_pr] = row['val']
        if cur_pip_1045 > mpip[cur_date][c_mpip_1045]:
            mpip[cur_date][c_mpip_1045] = cur_pip_1045
            mpip[cur_date][c_mpip_1045_dt] = index
            mpip[cur_date][c_mpip_1045_pr] = row['val']

        if str(index) == str(cur_date) + " 11:02:00":
            ls[cur_date][c_1030_pr] = p1030
            ls[cur_date][c_1045_pr] = p1045
            ls[cur_date][c_1102_pr] = row['val']
            if row['val'] > p1030:
                ls[cur_date][c_ls_1030] = 'LONG'
            elif row['val'] < p1030:
                ls[cur_date][c_ls_1030] = 'SHORT'
            else:
                ls[cur_date][c_ls_1030] = 'PAR'
            if row['val'] > p1045:
                ls[cur_date][c_ls_1045] = 'LONG'
            elif row['val'] < p1045:
                ls[cur_date][c_ls_1045] = 'SHORT'
            else:
                ls[cur_date][c_ls_1045] = 'PAR'
    
    df_mpip = mpip_to_df(mpip)
    print(df_mpip)
    df_ls = ls_to_df(ls)
    # df_mpip[[c_mpip_1030, c_mpip_1045]].plot(figsize=(12, 5))

    return df_mpip, df_ls

def ls_to_df(ls):
    df_ls = pd.DataFrame.from_dict(ls, orient='index')
    df_ls = df_ls[[c_ls_1030, c_1030_pr, \
        c_ls_1045, c_1045_pr, c_1102_pr]]
    return df_ls

def mpip_to_df(mpip):
    df_mpip = pd.DataFrame.from_dict(mpip, orient='index')
    df_mpip = df_mpip[[c_mpip_1030, c_1030_pr, \
        c_mpip_1030_pr, c_mpip_1030_dt, c_mpip_1045, \
        c_1045_pr,c_mpip_1045_pr, c_mpip_1045_dt]]
    return df_mpip


def daily_pip_mvmt():
    pipmvmts = {}
    for date, row in df_1102.iterrows():
        close_price = row['val']
        open_price = df_1030.loc[str(date.date()) + ' 10:30:00']['val']
        pipmvmts[date.date()] = { 
            'pip': pipmvmt(close_price, open_price), 
            'open': open_price, 
            'close': close_price
        }
    return pipmvmts


def pip_mvmt_to_excel(df_pip, mvmts):
    with pd.ExcelWriter('2018_daily_pip_mvmts.xlsx', engine='xlsxwriter') as writer: 
        df_pip.to_excel(writer, sheet_name='all_daily_pip_mvmts')
        for pip_mvmt in mvmts: 
            df = days_against_pip_mvmt(df_pip, pip_mvmt)
            df.to_excel(writer, sheet_name='{} pips'.format(pip_mvmt))


def main():
    global mdf, df_1030, df_1045, df_1102
    df = csv_in("GBPUSD_2018.csv")
    mdf = df.between_time('10:30', '11:02')
    df_1030 = df.between_time('10:30', '10:30')
    df_1045 = df.between_time('10:45', '10:45')
    df_1102 = df.between_time('11:02', '11:02')

    df_mpip, df_ls = proc_df()

    daily_pip = daily_pip_mvmt()
    df_daily_pip = pd.DataFrame.from_dict(daily_pip, orient='index')

    pip_neg3 = days_against_pip_mvmt(df_daily_pip, -3)
    pip_neg4 = days_against_pip_mvmt(df_daily_pip, -4)
    pip_neg5 = days_against_pip_mvmt(df_daily_pip, -5)
    pip_neg6 = days_against_pip_mvmt(df_daily_pip, -6)
    print("less than 3 pips: {} days".format(len(pip_neg3)))
    print("less than 4 pips: {} days".format(len(pip_neg4)))
    print("less than 5 pips: {} days".format(len(pip_neg5)))
    print("less than 6 pips: {} days".format(len(pip_neg6)))

    pip_mvmt_to_excel(df_daily_pip, [-3, -4, -5, -6])


if __name__ == '__main__':
    main()
