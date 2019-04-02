from metric import Metric
from datareader import DataReader

class PeriodPriceAvg(Metric): 
    def __init__(self, price_dfs, time_range, time_range_for_avg):
        Metric.__init__(self, time_range, price_dfs)
        self.time_range_for_avg = time_range_for_avg
        self.avgs = self._calc_avgs()

    
    def _calc_avgs(self):
        pass
    

    def to_df(self):
        pass
