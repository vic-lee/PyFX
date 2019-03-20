import pandas as pd
from datetime import datetime, timedelta
import os
import numpy as np
import matplotlib.pyplot as plt
import xlsxwriter
import pickle
from keydef import *

pipmvmt = lambda final, initial: round((final - initial) * 10000, 1)

daydelta = lambda d, delta: d - timedelta(days=delta)

is_same_date = lambda d1, d2: (d1.year == d2.year) \
    and (d1.month == d2.month) and (d1.day == d2.day)

days_against_pip_mvmt = lambda df, pipmvmt: df.query(\
    '{} < pip & pip < 0'.format(pipmvmt))

init_mpip = lambda p1030, p1045, dt1030, dt1045, fx, pdfx, o, h, l, c: { 

        c_open: o, 
        c_high: h, 
        c_low: l, 
        c_close: c,

        c_1030_pr: p1030, 
        c_1045_pr: p1045, 

        c_mpip_up_1030: 0,
        c_mpip_up_1045: 0,
        c_mpip_up_1030_dt: str(dt1030)[-8:],
        c_mpip_up_1045_dt: str(dt1045)[-8:],
        c_mpip_up_1030_pr: p1030,
        c_mpip_up_1045_pr: p1045,

        c_mpip_dn_1030: 0,
        c_mpip_dn_1045: 0,
        c_mpip_dn_1030_dt: str(dt1030)[-8:],
        c_mpip_dn_1045_dt: str(dt1045)[-8:],
        c_mpip_dn_1030_pr: p1030,
        c_mpip_dn_1045_pr: p1045,

        c_fx_pr: fx,
        c_pdfx_pr: pdfx, 
        c_mpip_up_pdfx: 0, 
        c_mpip_up_pdfx_dt: None, 
        c_mpip_up_pdfx_pr: None, 

        c_mpip_dn_pdfx: 0, 
        c_mpip_dn_pdfx_dt: None, 
        c_mpip_dn_pdfx_pr: None,

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


def fix_csv_in(fpath): 
    df = pd.read_csv(fpath)
    df['datetime'] = pd.to_datetime(df['datetime'])
    df = df.set_index('datetime')
    return df


def daily_csv_in(fpath):
    df = pd.read_csv(fpath)
    df.drop(df.tail(1).index,inplace=True)
    for index, row in df.iterrows():
        s = str(row["Date Time"])
        newstr = s[-10:].strip()
        df.at[index, "Date Time"] = newstr
    df['datetime'] = pd.to_datetime(df['Date Time'], format='%Y-%m-%d')
    df = df.drop(columns=['Date Time'])
    df = df.set_index('datetime')
    return df


def daily_xls_in(fpath):
    df = pd.read_excel(fpath)
    df.rename(columns = {'Date': 'datetime'}, inplace=True)
    # df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
    df = daily_xls_filter_ohlc(df)
    df = df.set_index('datetime')
    print(df)
    return df


def daily_xls_filter_ohlc(df):
    """This function returns only bid prices and removes ask prices. 
    We do this in compliance with the requirements. Should requirements change, 
    one may update this function to choose new columns to include / not include 
    in the dataframe. 
    print df.columns to see original column names. 
    """
    df = df.drop(columns=[
        'GBP/USD(Open, Ask)', 'GBP/USD(High, Ask)', 
        'GBP/USD(Low, Ask)', 'GBP/USD(Close, Ask)'
    ])
    df = df.rename(columns={
        'GBP/USD(Open, Bid)*': 'Open',
        'GBP/USD(High, Bid)*': 'High',
        'GBP/USD(Low, Bid)*': 'Low',
        'GBP/USD(Close, Bid)*': 'Close',
        'Tick Volume(GBP/USD)': 'Volume',
    })
    return df


def get_prior_fix_recursive(d):
    fx = None
    try: 
        fx = fixdf.loc[str(d)][CP]
    except Exception as e: 
        print("Could not locate the previous location, possibly due to out of bounds.")
        print(e)
        return fx, None
    if not np.isnan(fx):
        return fx, d
    else: 
        return get_prior_fix_recursive(daydelta(d, 1))


def get_prior_fix(d):
    fx = None
    try: 
        fx = fixdf.loc[str(daydelta(d, 1))][CP]
        return fx
    except Exception as e: 
        print("Could not locate the previous location, possibly due to out of bounds.")
        print(e)
        return fx


def get_fix_pr(d):
    fx = fixdf.loc[d][CP]
    if np.isnan(fx):
        return None
    return fx


def process_data():
    cur_date = None
    p1030 = None
    p1045 = None
    pdfx = None
    fx = None
    
    mpip = {}  
    ls = {}  
    
    for date_minute, row in morning_df.iterrows():

        if date_minute.date() != cur_date:
            o = None
            h = None
            l = None
            c = None
            cur_date = date_minute.date()
            cur_date_1045 = date_minute.replace(hour=10, minute=45)
            dt1030 = str(cur_date) + " 10:30:00"
            dt1045 = str(cur_date) + " 10:45:00"
            dtpdfx_dt = daydelta(cur_date, 1)
            p1030 = df_1030.loc[dt1030]['val']
            p1045 = df_1045.loc[dt1045]['val']
            pdfx, dtpdfx_dt = get_prior_fix_recursive(dtpdfx_dt)
            fx = get_fix_pr(str(cur_date))
            if str(cur_date) in dailydf.index:
                print(str(cur_date) + "\tOpen:\t" + str(float(dailydf.loc[str(cur_date)]['Open'])))
                # print(dailydf.loc[str(cur_date)])
                o = float(dailydf.loc[str(cur_date)]['Open'])
                h = float(dailydf.loc[str(cur_date)]['High'])
                l = float(dailydf.loc[str(cur_date)]['Low'])
                c = float(dailydf.loc[str(cur_date)]['Close'])
            mpip[cur_date] = init_mpip(p1030, p1045, dt1030, dt1045, fx, pdfx, o, h, l, c)
            ls[cur_date] = init_ls()

        cur_pr = row['val']

        pip1030 = pipmvmt(cur_pr, p1030)
        pip1045 = pipmvmt(cur_pr, p1045)

        pippdfx = None
        if not pdfx == None and not np.isnan(pdfx): 
            pippdfx = pipmvmt(cur_pr, pdfx)

        td = mpip[cur_date]

        if pip1030 > td[c_mpip_up_1030]:
            td[c_mpip_up_1030] = pip1030
            td[c_mpip_up_1030_dt] = str(date_minute)[-8:]
            td[c_mpip_up_1030_pr] = cur_pr
        # if date_minute.minute >= 45:
        if date_minute > cur_date_1045:
            if pip1045 > td[c_mpip_up_1045]:
                td[c_mpip_up_1045] = pip1045
                td[c_mpip_up_1045_dt] = str(date_minute)[-8:]
                td[c_mpip_up_1045_pr] = cur_pr

        if pip1030 < td[c_mpip_dn_1030]:
            td[c_mpip_dn_1030] = pip1030
            td[c_mpip_dn_1030_dt] = str(date_minute)[-8:]
            td[c_mpip_dn_1030_pr] = cur_pr
        # if date_minute.minute >= 45:
        if date_minute > cur_date_1045:
            if pip1045 < td[c_mpip_dn_1045]:
                td[c_mpip_dn_1045] = pip1045
                td[c_mpip_dn_1045_dt] = str(date_minute)[-8:]
                td[c_mpip_dn_1045_pr] = cur_pr
        
        if pippdfx is not None: 
            if pippdfx > td[c_mpip_up_pdfx]:
                td[c_mpip_up_pdfx] = pippdfx
                td[c_mpip_up_pdfx_dt] = str(date_minute)[-8:]
                td[c_mpip_up_pdfx_pr] = cur_pr
            if pippdfx < td[c_mpip_dn_pdfx]:
                td[c_mpip_dn_pdfx] = pippdfx
                td[c_mpip_dn_pdfx_dt] = str(date_minute)[-8:]
                td[c_mpip_dn_pdfx_pr] = cur_pr

        if str(date_minute) == str(cur_date) + " 11:02:00":
            handle_ls(p1030, ls, cur_date, p1045, row)

    return mpip, ls


def handle_ls(p1030, ls, cur_date, p1045, row):
    p1102 = row['val']
    ls[cur_date][c_1030_pr] = p1030
    ls[cur_date][c_1045_pr] = p1045
    ls[cur_date][c_1102_pr] = p1102
    if p1102 > p1030:
        ls[cur_date][c_ls_1030] = 'LONG'
    elif p1102 < p1030:
        ls[cur_date][c_ls_1030] = 'SHORT'
    else:
        ls[cur_date][c_ls_1030] = 'PAR'
    if p1102 > p1045:
        ls[cur_date][c_ls_1045] = 'LONG'
    elif p1102 < p1045:
        ls[cur_date][c_ls_1045] = 'SHORT'
    else:
        ls[cur_date][c_ls_1045] = 'PAR'


def ls_to_df(ls):
    df_ls = pd.DataFrame.from_dict(ls, orient='index')
    df_ls = df_ls[[c_ls_1030, c_1030_pr, \
        c_ls_1045, c_1045_pr, c_1102_pr]]
    return df_ls


def mpip_to_df(mpip):
    df_mpip = pd.DataFrame.from_dict(mpip, orient='index')
    df_mpip = df_mpip[[
        c_open, c_high, c_low, c_close,
        c_1030_pr, 
        c_mpip_up_1030, c_mpip_up_1030_pr, c_mpip_up_1030_dt, 
        c_mpip_dn_1030, c_mpip_dn_1030_pr, c_mpip_dn_1030_dt, 
        c_1045_pr,
        c_mpip_up_1045, c_mpip_up_1045_pr, c_mpip_up_1045_dt, 
        c_mpip_dn_1045, c_mpip_dn_1045_pr, c_mpip_dn_1045_dt, 
        c_fx_pr, c_pdfx_pr,
        c_mpip_up_pdfx, c_mpip_up_pdfx_pr, c_mpip_up_pdfx_dt,
        c_mpip_dn_pdfx, c_mpip_dn_pdfx_pr, c_mpip_dn_pdfx_dt,
    ]]
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


def df_to_xls(df, fname):
    with pd.ExcelWriter(fname, engine='xlsxwriter') as writer: 
        df.to_excel(writer, sheet_name="max_pip_mvmts")
        sheet = writer.sheets['max_pip_mvmts']
        wide_col = 20
        sheet.set_column(0, len(df.columns), wide_col)
        workbook = writer.book


def pipmvmt_to_xls(df_pip, fname, mvmts):
    with pd.ExcelWriter(fname, engine='xlsxwriter') as writer: 
        df_pip.to_excel(writer, sheet_name='all_daily_pip_mvmts')
        for pip_mvmt in mvmts: 
            df = days_against_pip_mvmt(df_pip, pip_mvmt)
            df.to_excel(writer, sheet_name='{} pips'.format(pip_mvmt))


def main():
    global morning_df, df_1030, df_1045, df_1102, fixdf, dailydf

    dailydf = daily_xls_in("../data/datasrc/gbp_daily.xlsx")
    fixdf = fix_csv_in("../data/datasrc/fix1819.csv")
    datadf = csv_in("../data/datasrc/GBPUSD_2018.csv")
    morning_df = datadf.between_time('10:30', '11:02')
    df_1030 = datadf.between_time('10:30', '10:30')
    df_1045 = datadf.between_time('10:45', '10:45')
    df_1102 = datadf.between_time('11:02', '11:02')

    mpip, ls = process_data()
    # with open ("cache/data.pickle", "rb") as pin: 
        # mpip = pickle.load(pin)
        
    df_mpip = mpip_to_df(mpip)
    # df_ls = ls_to_df(ls)
    df_to_xls(df_mpip, '../data/dataout/18mpip.xlsx')

    daily_pip = daily_pip_mvmt()
    df_daily_pip = pd.DataFrame.from_dict(daily_pip, orient='index')

    # pip_neg3 = days_against_pip_mvmt(df_daily_pip, -3)
    # pip_neg4 = days_against_pip_mvmt(df_daily_pip, -4)
    # pip_neg5 = days_against_pip_mvmt(df_daily_pip, -5)
    # pip_neg6 = days_against_pip_mvmt(df_daily_pip, -6)
    # print("less than 3 pips: {} days".format(len(pip_neg3)))
    # print("less than 4 pips: {} days".format(len(pip_neg4)))
    # print("less than 5 pips: {} days".format(len(pip_neg5)))
    # print("less than 6 pips: {} days".format(len(pip_neg6)))

    pipmvmt_to_xls(df_daily_pip, '../data/dataout/18dpip.xlsx', [-3, -4, -5, -6])

    with open("cache/data.pickle", "wb") as pout: 
        pickle.dump(mpip, pout)


if __name__ == '__main__':
    main()
