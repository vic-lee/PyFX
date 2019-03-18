class Price: 
    def __init__(self, price, time):
        self.price = price
        self.time = time

    def get_time(self):
        return self.time

    def get_price(self):
        return self.price
    
    def delta_price(self, other_price):
        return (other_price.get_price() - self.price)