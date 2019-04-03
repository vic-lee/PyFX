class PriceTime: 
    def __init__(self, price, datetime):
        self.price = price
        self.datetime = datetime


    def get_datetime(self):
        return self.datetime


    def get_price(self):
        return self.price


    def delta_from(self, other_price):
        return (self.price - other_price.get_price())

    
    def pip_movement_from(self, other_price):
        return round(self.delta_from(other_price) * 10000, 1)

    
    def is_earlier_than(self, other_price) -> bool: 
        return self.datetime < other_price.datetime
    

    def is_later_than(self, other_price) -> bool:
        return self.datetime > other_price.datetime