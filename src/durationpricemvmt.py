from daytimerange import TimeRange
from daymvmt import DayPipMovmentToPrice
from datetime import datetime

class DurationPipMovmentToPrice:
    """This class finds the daily price movements within a period of time. 
    It is used in conjunction with DayPipMovmentToPrice class.
    """
    def __init__(self, minute_price_df):
        self.minute_price_df = minute_price_df    # minutely data
        self.max_price_movements = []   # an array of DayMovements
        self.current_date = None

        time_start = datetime(year=1970, month=1, day=1, hour=10, minute=30)
        time_end = datetime(year=1970, month=1, day=1, hour=11, minute=2)

        self.benchmark_1030 = datetime(year=1970, month=1, day=1, hour=10, minute=30)
        self.benchmark_1045 = datetime(year=1970, month=1, day=1, hour=10, minute=45)

        self.time_range = TimeRange(time_start, time_end)
    

    def find_max_price_movements(self):
        """
        Algorithm: 
        For each day, perform max day price movement check. 
        """
       
        '''
        For each new day (method to see if it is new day), get daymvmt obj
        '''
        day_price_movements = None
        for index, row in self.minute_price_df.iterrows():
            current_price = row['val']
            if self.is_row_new_day():
                day_price_movements = DayPipMovmentToPrice(
                    date=self.current_date, 
                    benchmark_price=current_price, 
                    time_range=self.time_range
                )
            day_price_movements.update_max_pip(current_price)

    
    def is_row_new_day(self):
        pass
