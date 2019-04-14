import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class DataReader:

    FIX = "fix"
    MINUTELY = "minutely"
    DAILY = "daily"

    def __init__(self, fpaths, currency_pair_name: str):
        self.fpaths = fpaths
        self._currency_pair_name = currency_pair_name

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

        if (len(df.columns) == 11):     #HACK
            df = df.drop(columns=['O'])            

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

            df["datetime"] = pd.to_datetime(
                df["datetime"], format="%Y-%m-%d %H:%M:%S")

        df = df.set_index('datetime')
        return df

    def _log_read_error(self, mode):
        logger.error("Error in reading {} data. Aborting...".format(mode))

    def _filter_ohlc_in_daily(self, df):
        """This function returns only bid prices and removes ask prices. 
        We do this in compliance with the requirements. Should requirements change, 
        one may update this function to choose new columns to include / not include 
        in the dataframe. 
        print df.columns to see original column names. 
        """

        cp_identifier = self._currency_pair_name[:3] + \
            '/' + self._currency_pair_name[3:]

        df = df.drop(columns=[
            '{}(Open, Ask)'.format(cp_identifier),
            '{}(High, Ask)'.format(cp_identifier),
            '{}(Low, Ask)'.format(cp_identifier),
            '{}(Close, Ask)'.format(cp_identifier),
            'Tick Volume({})'.format(cp_identifier)
        ])

        df = df.rename(columns={
            '{}(Open, Bid)*'.format(cp_identifier): 'Open',
            '{}(High, Bid)*'.format(cp_identifier): 'High',
            '{}(Low, Bid)*'.format(cp_identifier): 'Low',
            '{}(Close, Bid)*'.format(cp_identifier): 'Close',
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
