from datastructure.pricetime import PriceTime
import pandas as pd

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

    def __init__(self, date, benchmark_pricetime: PriceTime, time_range):
        self.date = date
        self.benchmark_pricetime = benchmark_pricetime
        self.time_range = time_range

        self.max_pip_up = None
        self.max_pip_up_time = self._initialize_datetime_for_max_min()
        self.price_at_max_pip_up = self._initialize_prices_for_max_min()

        self.max_pip_down = None
        self.max_pip_down_time = self._initialize_datetime_for_max_min()
        self.price_at_max_pip_down = self._initialize_prices_for_max_min()


    def _initialize_datetime_for_max_min(self):
        if (self.time_range.is_datetime_in_range(self.benchmark_pricetime.get_datetime())):
            return self.benchmark_pricetime.get_datetime()
        else:
            return None


    def _initialize_prices_for_max_min(self):
        if (self.time_range.is_datetime_in_range(self.benchmark_pricetime.get_datetime())):
            return self.benchmark_pricetime.get_price()
        else:
            return None
    

    def update_max_pip(self, current_price):
        self._update_max_pip_up(current_price)
        self._update_max_pip_down(current_price)

    
    def _update_max_pip_up(self, current_price):
        new_pip = current_price.pip_movement_from(self.benchmark_pricetime)
        # if self._is_in_benchmark_period(current_price) and new_pip > self.max_pip_up:
        if self._is_in_benchmark_period(current_price) and self._is_new_pip_greater(new_pip):
            self.max_pip_up = new_pip
            self.max_pip_up_time = current_price.get_datetime()
            self.price_at_max_pip_up = current_price.get_price()


    def _update_max_pip_down(self, current_price):
        new_pip = current_price.pip_movement_from(self.benchmark_pricetime)
        # if self._is_in_benchmark_period(current_price) and new_pip < self.max_pip_down:
        if self._is_in_benchmark_period(current_price) and self._is_new_pip_lower(new_pip):
            self.max_pip_down = new_pip
            self.max_pip_down_time = current_price.get_datetime()
            self.price_at_max_pip_down = current_price.get_price()


    def to_string(self):
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        return ("{:20} {:20} {:30} {:24} {:30}".format(
            "Date: {}".format(self.date),
            "Max pip up: {}".format(self.max_pip_up),
            "Max pip up time: {}".format(f_max_pip_up_time),
            "Max pip down: {}".format(self.max_pip_down),
            "Max pip down time: {}".format(f_max_pip_down_time)))


    def _is_new_pip_greater(self, new_pip):
        if self.max_pip_up is not None:
            return (new_pip > self.max_pip_up)
        else: 
            return (new_pip > 0)

    
    def _is_new_pip_lower(self, new_pip):
        if self.max_pip_down is not None:
            return (new_pip < self.max_pip_down)
        else: 
            return (new_pip < 0)
    

    def _format_max_pip_up_time(self):
        return self._format_time(time=self.max_pip_up_time)
    

    def _format_max_pip_down_time(self):
        return self._format_time(time=self.max_pip_down_time)


    def _format_time(self, time):
        if time == None:
            return None
        else:
            return time.time()


    def _is_in_benchmark_period(self, current_price: PriceTime) -> bool: 
        return current_price.is_later_than(self.benchmark_pricetime)

    
    def to_df(self) -> pd.DataFrame:
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        df = pd.DataFrame({
            "Benchmark Price": self.benchmark_pricetime.get_price(),
            "Max Pip Up": self.max_pip_up,
            "Price at Max Pip Up": self.price_at_max_pip_up,
            "Time at Max Pip Up": f_max_pip_up_time,
            "Max Pip Down": self.max_pip_down,
            "Price at Max Pip Down": self.price_at_max_pip_down,
            "Time at Max Pip Down": f_max_pip_down_time,
        }, index=[self.date])
        
        return df