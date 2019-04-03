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
        df.rename(columns = {'Date': 'datetime'}, inplace=True)
        df = self._filter_ohlc_in_daily(df)

        df['datetime'] = df['datetime'].apply(lambda dt: datetime.strptime(str(dt), "%Y-%m-%d %H:%M:%S").date())
            
        df = df.set_index('datetime')
        return df


    def read_minute_data(self):
        if not self._does_fpath_exist(mode=self.MINUTELY):
            self._log_read_error(mode=self.MINUTELY)
            return None
        fpath = self.fpaths[self.MINUTELY] 
        df = pd.read_csv(fpath)

        df.columns = ["date", "time", "Open", "High", "Low", "Close", "E"]
        df = df.drop(columns=['E'])

        df['datetime'] = df["date"].map(str) + " " + df["time"] 
        df["datetime"] = pd.to_datetime(df["datetime"])
        df["date"] = pd.to_datetime(df["date"]) 
        df = df.set_index('datetime')
        # df = df[['date', 'time', 'val']]
        df = df.drop(columns=['time', 'date'])
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

