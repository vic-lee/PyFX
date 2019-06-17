from datetime import datetime


class PriceTime:
    def __init__(self, price: float, datetime: datetime):
        self.__price = price
        self.__datetime = datetime

    @property
    def datetime(self):
        return self.__datetime

    @property
    def price(self):
        return self.__price

    def _delta_from(self, other_price):
        return (self.__price - other_price.price)

    def pip_movement_from(self, other_price):
        return round(self._delta_from(other_price) * 10000, 1)

    def is_earlier_than(self, other_price) -> bool:
        return self.__datetime < other_price.datetime

    def is_later_than(self, other_price) -> bool:
        return self.__datetime > other_price.datetime
