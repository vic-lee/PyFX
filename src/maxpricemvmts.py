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
        self.max_price_movements = []   # an array of DayMovements
        self.current_date = None
        self.time_range = config[self.TIME_RANGE]
        self.benchmark_times = config[self.BENCHMARK_TIMES]
        self.minute_price_df = self._filter_df_to_time_range(minute_price_df)
        print(self.minute_price_df)
    

    def _filter_df_to_time_range(self, df):
        return df.between_time(self.time_range.start_time, self.time_range.end_time)

    
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
                benchmark_prices = self._generate_benchmark_prices()    #1030
                price_mvmt_day_obj = DayPipMovmentToPrice(
                    date=self.current_date, 
                    benchmark_price=current_price, 
                    time_range=self.time_range
                )

            price_mvmt_day_obj.update_max_pip(current_price)

    
    def _is_row_new_day(self, date):
        if date != self.current_date:
            return True
        return False
    

    def _update_current_date(self, date):
        self.current_date = date

    
    def _generate_benchmark_prices(self):
        benchmark_prices = {}
        for timestamp in self.benchmark_times:
            benchmark_price = self.minute_price_df.loc[self.current_date]['val']
            benchmark_prices[timestamp] = benchmark_price
        return benchmark_prices