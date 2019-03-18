from price import Price

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

    def __init__(self, date, benchmark_price, time_range):
        self.date = date
        self.benchmark_price = benchmark_price
        self.time_range = time_range
        self.max_pip_up = 0
        self.max_pip_up_time = None
        self.max_pip_down = 0
        self.max_pip_up_time = None

    
    def update_max_pip(self, current_price):
        self.update_max_pip_up(current_price)
        self.update_max_pip_down(current_price)

    
    def update_max_pip_up(self, current_price):
        new_pip = current_price.pip_movement_from(self.benchmark_price)
        if new_pip > self.max_pip_up:
            self.max_pip_up = new_pip
            self.max_pip_up_time = current_price.get_time()


    def update_max_pip_down(self, current_price):
        new_pip = current_price.pip_movement_from(self.benchmark_price)
        if new_pip < self.max_pip_down:
            self.max_pip_down = new_pip
            self.max_pip_down_time = current_price.get_time()

