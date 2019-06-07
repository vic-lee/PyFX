from datastructure.pricetime import PriceTime
from datastructure.daytimerange import DayTimeRange
import pandas as pd
import datetime


class DayPipMovmentToPrice:
    """For a given day, this object encapsulates the maximum 
    upward and downward pip movements to a given price. 

    The benchmark price is not directly given; rather, it should
    be passed in as a timestamp. The class then looks up the 
    benchmark price using the timestamp. 

    Foreseeably, in a for loop looping through each day, this class
    can generate each day's daily pip movements. 

    Initialized daily. 
    """

    UP = "up"
    DOWN = "down"

    def __init__(self, date, benchmark_pricetime: PriceTime, time_range_start_pricetime: PriceTime, time_range: DayTimeRange):
        self.__date = date
        self.__benchmark_pricetime = benchmark_pricetime
        self.__time_range_start_pricetime = time_range_start_pricetime
        self.__time_range = time_range

        self.__max_pip_up = 0
        self.__max_pip_up_time = self._initialize_datetime_for_max_min()
        self.__price_at_max_pip_up = self._initialize_prices_for_max_min()

        self.__max_pip_down = 0
        self.__max_pip_down_time = self._initialize_datetime_for_max_min()
        self.__price_at_max_pip_down = self._initialize_prices_for_max_min()

    def _initialize_max_pips(self):
        if (self.__time_range.is_datetime_in_range(self.__benchmark_pricetime.datetime)):
            return 0
        else:
            return None

    def _initialize_datetime_for_max_min(self):
        if (self.__time_range.is_datetime_in_range(self.__benchmark_pricetime.datetime)):
            return self.__benchmark_pricetime.datetime
        else:
            return self.__time_range_start_pricetime.datetime

    def _initialize_prices_for_max_min(self):
        if (self.__time_range.is_datetime_in_range(self.__benchmark_pricetime.datetime)):
            return self.__benchmark_pricetime.price
        else:
            return self.__time_range_start_pricetime.price

    @property
    def date(self):
        return self.__date

    def update_max_pip(self, current_price):
        self._update_max_pip_up(current_price)
        self._update_max_pip_down(current_price)

    def _update_max_pip_up(self, current_price):
        new_pip = current_price.pip_movement_from(self.__benchmark_pricetime)
        if self._is_in_benchmark_period(current_price) and self._is_new_pip_greater(new_pip):
            self.__max_pip_up = new_pip
            self.__max_pip_up_time = current_price.datetime
            self.__price_at_max_pip_up = current_price.price

    def _update_max_pip_down(self, current_price):
        new_pip = current_price.pip_movement_from(self.__benchmark_pricetime)
        if self._is_in_benchmark_period(current_price) and self._is_new_pip_lower(new_pip):
            self.__max_pip_down = new_pip
            self.__max_pip_down_time = current_price.datetime
            self.__price_at_max_pip_down = current_price.price

    def __str__(self):
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        return ("{:20} {:20} {:30} {:24} {:30}".format(
            "Date: {}".format(self.__date),
            "Max pip up: {}".format(self.__max_pip_up),
            "Max pip up time: {}".format(f_max_pip_up_time),
            "Max pip down: {}".format(self.__max_pip_down),
            "Max pip down time: {}".format(f_max_pip_down_time)))

    def _is_new_pip_greater(self, new_pip):
        if self.__max_pip_up is not None:
            return (new_pip > self.__max_pip_up)
        else:
            return (new_pip > 0)

    def _is_new_pip_lower(self, new_pip):
        if self.__max_pip_down is not None:
            return (new_pip < self.__max_pip_down)
        else:
            return (new_pip < 0)

    def _format_max_pip_up_time(self):
        return self._format_time(time=self.__max_pip_up_time)

    def _format_max_pip_down_time(self):
        return self._format_time(time=self.__max_pip_down_time)

    def _format_time(self, time):
        if time == None:
            return None
        else:
            return time.time()

    def _is_in_benchmark_period(self, current_price: PriceTime) -> bool:
        return current_price.is_later_than(self.__benchmark_pricetime)

    def to_df(self) -> pd.DataFrame:
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        df = pd.DataFrame({
            "Benchmark Price": self.__benchmark_pricetime.price,
            "Max Pip Up": self.__max_pip_up,
            "Price at Max Pip Up": self.__price_at_max_pip_up,
            "Time at Max Pip Up": f_max_pip_up_time,
            "Max Pip Down": self.__max_pip_down,
            "Price at Max Pip Down": self.__price_at_max_pip_down,
            "Time at Max Pip Down": f_max_pip_down_time,
        }, index=[self.__date])

        return df
