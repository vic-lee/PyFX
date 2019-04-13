import pandas as pd
from datetime import datetime


class DataReader:

    FIX = "fix"
    MINUTELY = "minutely"
    DAILY = "daily"

    def __init__(self, fpaths):
        self.fpaths = fpaths

    def read_data(self):
        return {
            self.FIX: self.read_fix_data(),
            self.MINUTELY: self.read_minute_data(),
            self.DAILY: self.read_daily_data(),
        }

    def read_fix_data(self):
        if not self._does_fpath_exist(mode=self.FIX):
            self._log_read_error(mode=self.FIX)
            return None
        fpath = self.fpaths[self.FIX]
        df = pd.read_csv(fpath)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime')
        return df

    def read_daily_data(self):
        if not self._does_fpath_exist(mode=self.DAILY):
            self._log_read_error(mode=self.DAILY)
            return None
        fpath = self.fpaths[self.DAILY]
        df = pd.read_excel(fpath)
        df.rename(columns={'Date': 'datetime'}, inplace=True)
        df = self._filter_ohlc_in_daily(df)

        mask = (df['datetime'] > "2018-01-02") & (df['datetime'] <= "2019-01-01")
        df = df.loc[mask]

        df['datetime'] = df['datetime'].apply(
            lambda dt: datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S").date())

        df = df.set_index('datetime')
        return df

    def read_minute_data(self):
        if not self._does_fpath_exist(mode=self.MINUTELY):
            self._log_read_error(mode=self.MINUTELY)
            return None
        fpath = self.fpaths[self.MINUTELY]
        df = pd.read_csv(fpath)

        if len(df.columns) == 7:
            """HACK: If source data has 7 columns, then it is of an old format. 
            Combining date and time columns and converting them to datetime required. 
            """
            df.columns = ["date", "time", "Open", "High", "Low", "Close", "E"]
            df = df.drop(columns=['E'])
            df['datetime'] = df["date"].map(str) + " " + df["time"]
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.drop(columns=['time', 'date'])

        else:
            """HACK: Otherwise source data has 5 columns; it is of the new format.
            Processing the datetime column and conversion to datetime required. 

            Note that it is an intentional decision to make column==5 or 7 
            mutually exclusive. We want the code to fail should there be a new format. 
            """
            df.columns = ["datetime", "Open", "High", "Low", "Close"]

            df['datetime'] = df['datetime'].map(
                lambda s: s[25:] + s[5:14])

            df["datetime"] = pd.to_datetime(df["datetime"], format="%Y-%m-%d %H:%M:%S")

        df = df.set_index('datetime')
        return df

    def _log_read_error(self, mode):
        print("Error in reading {} data. Aborting...".format(mode))

    def _filter_ohlc_in_daily(self, df):
        """This function returns only bid prices and removes ask prices. 
        We do this in compliance with the requirements. Should requirements change, 
        one may update this function to choose new columns to include / not include 
        in the dataframe. 
        print df.columns to see original column names. 
        """
        df = df.drop(columns=[
            'GBP/USD(Open, Ask)', 'GBP/USD(High, Ask)',
            'GBP/USD(Low, Ask)', 'GBP/USD(Close, Ask)',
            'Tick Volume(GBP/USD)'
        ])
        df = df.rename(columns={
            'GBP/USD(Open, Bid)*': 'Open',
            'GBP/USD(High, Bid)*': 'High',
            'GBP/USD(Low, Bid)*': 'Low',
            'GBP/USD(Close, Bid)*': 'Close',
            # 'Tick Volume(GBP/USD)': 'Volume',
        })
        return df

    def _does_minuitely_fpath_exist(self):
        pass

    def _does_fix_fpath_exist(self):
        pass

    def _does_daily_fpath_exist(self):
        pass

    def _does_fpath_exist(self, mode):
        if mode in self.fpaths:
            return True
        return False
