from daytimerange import TimeRangeInDay
from daymvmt import DayPipMovmentToPrice
from datetime import datetime
from price import Price

class DurationPipMovmentToPrice:
    """This class finds the daily price movements within a period of time. 
    It is used in conjunction with DayPipMovmentToPrice class.
    """

    TIME_RANGE = "time range"
    BENCHMARK_TIMES = "benchmark_times"

    def __init__(self, minute_price_df, config):
        self.minute_price_df = minute_price_df    # minutely data
        self.max_price_movements = []   # an array of DayMovements
        self.current_date = None
        self.time_range = config.time_range
        self.benchmark_times = config.benchmark_times
        
    
    def to_excel(self, fname=None):
        pass


    def find_max_price_movements(self):
        """
        Algorithm: 
        For each day, perform max day price movement check. 
        """

        for index, row in self.minute_price_df.iterrows():
            current_price = Price(price=row['val'], time=index)
            if self._is_row_new_day(date=index):
                self._update_current_date(date=index)
                benchmark_prices = self._generate_benchmark_prices()
                price_mvmt_day_obj = DayPipMovmentToPrice(
                    date=self.current_date, 
                    benchmark_price=current_price, 
                    time_range=self.time_range
                )

            price_mvmt_day_obj.update_max_pip(current_price)

    
    def _is_row_new_day(self, date):
        if date != self.current_date:
            return False
        return True
    

    def _update_current_date(self, date):
        self.current_date = date

    
    def _generate_benchmark_prices(self):
        benchmark_prices = {}
        for timestamp in self.benchmark_times:
            benchmark_price = self.minute_price_df[self.current_date]['val']
            benchmark_prices[timestamp] = benchmark_price
        return benchmark_prices