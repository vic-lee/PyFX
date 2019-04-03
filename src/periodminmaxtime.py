from metric import Metric
from daytimerange import TimeRangeInDay

class PeriodMinMaxTime(Metric): 
    def __init__(self, price_dfs, time_range, time_range_for_calc):
        Metric.__init__(self, time_range, price_dfs)
        self.time_range_for_minmax = time_range_for_calc
        self.min_max_time = self._find_min_max_time()

    
    def _find_min_max_time(self):
        pass