class Price: 
    def __init__(self, price, time):
        self.price = price
        self.time = time


    def get_time(self):
        return self.time


    def get_price(self):
        return self.price


    def delta_from(self, other_price):
        return (self.price - other_price.get_price())

    
    def pip_movement_from(self, other_price):
        return round(self.delta_from(other_price) * 10000, 1)