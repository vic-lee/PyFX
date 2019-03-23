from datetime import datetime, time

from daytimerange import TimeRangeInDay
from daymvmt import DayPipMovmentToPrice
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

        Current implementation: 
        "benchmark_time": {
            "date": {
                pip_info
            }
        }
        '''
        self.time_range = config[self.TIME_RANGE]
        self.benchmark_times = config[self.BENCHMARK_TIMES]
        self.max_price_movements = \
            self._generate_price_movements_obj_from_benchmark_times()
        self.minute_price_df = self._filter_df_to_time_range(minute_price_df)
        self.benchmark_prices_matrix = self._generate_benchmark_prices_matrix()


    def _generate_price_movements_obj_from_benchmark_times(self):
        ret = {}
        for btime in self.benchmark_times:
            ret[btime] = {}
        return ret
    

    def _filter_df_to_time_range(self, df):
        return df.between_time(self.time_range.start_time, self.time_range.end_time)
    

    def _generate_benchmark_prices_matrix(self):
        ret = {}
        for btime in self.benchmark_times:
            ret[btime] = self.minute_price_df.between_time(btime, btime)
        return ret

    
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


    def _find_max_price_movement_against_benchmark(self, benchmark_time: time):
        day_objs = {}
        daily_max_pips = None
        current_date = None
        for time_index, row in self.minute_price_df.iterrows():
            current_price = Price(price=row['val'], time=time_index)

            if self._is_row_new_day(date=current_date, index=time_index):
                day_objs = self._save_prior_day_obj(daily_max_pips, day_objs)
                current_date = self._update_current_date(newdate=time_index)
                daily_max_pips = self._init_new_day_obj(current_date, current_price)

            daily_max_pips.update_max_pip(current_price)
        
        return day_objs


    def _generate_benchmark_prices_from_benchmark_times(self):
        pass


    def _init_new_day_obj(self, current_date: datetime.date, current_price: Price) -> DayPipMovmentToPrice:
        return DayPipMovmentToPrice(
            date=current_date, 
            benchmark_price=current_price,
            time_range=self.time_range)


    def _save_prior_day_obj(self, prior_day_obj: DayPipMovmentToPrice, day_objs):
        if prior_day_obj != None: 
            day_objs[prior_day_obj.date] = prior_day_obj
        return day_objs


    def _update_current_date(self, newdate: datetime) -> datetime.date:
        return newdate.date()

    
    def _is_row_new_day(self, date: datetime.date, index: datetime) -> bool:
        if date == None: 
            return True
        if index.date() != date:
            return True
        return False


    def to_string(self):
        benchmark_time_header_template = \
            "\n/********************** Benchmark Time: {} **********************/\n"
        for btime in self.max_price_movements:
            print(benchmark_time_header_template.format(btime))
            max_pips_for_btime = self.max_price_movements[btime]
            for day in max_pips_for_btime:
                day_max_pips = max_pips_for_btime[day]
                print(day_max_pips.to_string())
