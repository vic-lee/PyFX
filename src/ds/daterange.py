from datetime import date


class DateRange:
    """
    This class denotes the range within which the algorithm performs
    its analysis. It has two attributes: starting date, ending date. 

    Args: both start_date and end_date are Date objects
    """

    def __init__(self, start_date: date, end_date: date):
        self.__start_date = start_date
        self.__end_date = end_date

    @property
    def start_date(self):
        return self.__start_date

    @property
    def end_date(self):
        return self.__end_date

    def is_datetime_in_range(self, date):
        return date >= self.__start_date and date <= self.__end_date

    def __repr__(self):
        return "start date: {} \tend date: {}\n".format(self.__start_date, self.__end_date)
