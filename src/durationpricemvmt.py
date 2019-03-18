from daytimerange import TimeRange
from daymvmt import DayPipMovmentToPrice
from datetime import datetime
from price import Price

class DurationPipMovmentToPrice:
    """This class finds the daily price movements within a period of time. 
    It is used in conjunction with DayPipMovmentToPrice class.
    """
    def __init__(self, minute_price_df, config):
        self.minute_price_df = minute_price_df    # minutely data
        self.max_price_movements = []   # an array of DayMovements
        self.current_date = None
        self.time_range = config.time_range
        self.benchmark_times = config.benchmark_times

        #config.time_range
        #config.benchmarks

        # time_start = datetime(year=1970, month=1, day=1, hour=10, minute=30)
        # time_end = datetime(year=1970, month=1, day=1, hour=11, minute=2)
        # self.time_range = TimeRange(time_start, time_end)
    

    def find_max_price_movements(self):
        """
        Algorithm: 
        For each day, perform max day price movement check. 
        """
        day_price_movements = None
        for index, row in self.minute_price_df.iterrows():
            current_price = Price(price=row['val'], time=index)
            if self.is_row_new_day(date=index):
                self.update_current_day(date=index)
                day_price_movements = DayPipMovmentToPrice(
                    date=self.current_date, 
                    benchmark_price=current_price, 
                    time_range=self.time_range
                )
            day_price_movements.update_max_pip(current_price)

    
    def is_row_new_day(self, date):
        if date != self.current_date:
            return False
        return True
    

    def update_current_day(self, date):
        self.current_date = date
