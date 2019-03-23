from daytimerange import TimeRangeInDay
from daymvmt import DayPipMovmentToPrice
from datetime import datetime
from price import Price

class MaxPriceMovements:
    """This class finds the daily price movements within a period of time. 
    It is used in conjunction with DayPipMovmentToPrice class.
    """

    TIME_RANGE = "time range"
    BENCHMARK_TIMES = "benchmark_times"

    def __init__(self, minute_price_df, config):
        '''
        Note: the initialization is highly coupled. Later initializations may 
        depend on earlier initializations. Change the sequence of initialization
        with caution. 
        '''
        '''
        TODO: Data structure choice for max_price_movements is subject to change.
        Consider multilevel dataframe (possible difficulty in excel exporting). 
        '''
        self.time_range = config[self.TIME_RANGE]
        self.benchmark_times = config[self.BENCHMARK_TIMES]
        self.max_price_movements = \
            self._generate_price_movements_obj_from_benchmark_times()
        self.minute_price_df = self._filter_df_to_time_range(minute_price_df)
        print(self.minute_price_df)


    def _generate_price_movements_obj_from_benchmark_times(self):
        ret = {}
        for btime in self.benchmark_times:
            ret[btime] = {}
        return ret
    

    def _filter_df_to_time_range(self, df):
        return df.between_time(self.time_range.start_time, self.time_range.end_time)

    
    def to_excel(self, fname=None):
        pass


    def find_max_price_movements(self):
        """
        Algorithm: 
        For each day, perform max day price movement check. 
        """
        for btime in self.max_price_movements:
            self.max_price_movements[btime] = \
                self._find_max_price_movement_against_benchmark(benchmark_time=btime)


    def _find_max_price_movement_against_benchmark(self, benchmark_time):
        day_objs = {}
        price_movement_day_obj = None
        current_date = None
        for time_index, row in self.minute_price_df.iterrows():
            current_price = Price(price=row['val'], time=time_index)
            if self._is_row_new_day(date=current_date, index=time_index):
                day_objs = self._save_prior_day_obj(price_movement_day_obj, day_objs)

                current_date = time_index.date()

                price_movement_day_obj = DayPipMovmentToPrice(
                    date=current_date, 
                    benchmark_price=current_price,
                    time_range=self.time_range)

            price_movement_day_obj.update_max_pip(current_price)
        
        return day_objs


    def _save_prior_day_obj(self, prior_day_obj, day_objs):
        if prior_day_obj != None: 
            print(prior_day_obj.to_string())
            day_objs[prior_day_obj.date] = prior_day_obj
        return day_objs

    
    def _is_row_new_day(self, date, index):
        if date == None: 
            return True
        if index.date() != date.date():
            return True
        return False
