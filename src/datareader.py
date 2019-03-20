import pandas as pd

class FXDataReader:
    def __init__(self, fpaths):
        self.fpaths = fpaths
        self.FIX = "fix"
        self.MINUTELY = "minutely"
        self.DAILY = "daily"
    
    def read_fix_csv(self):
        if not self._does_fpath_exist(mode=self.FIX):
            self._log_read_error(mode=self.FIX)
            return None  
        fpath = self.fpaths[self.FIX]
        df = pd.read_csv(fpath)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index('datetime')
        return df
    
    def read_daily_xlsx(self):
        pass

    def read_minute_csv(self):
        pass

    def _log_read_error(self, mode):
        print("Error in reading {} data. Aborting...".format(mode))

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