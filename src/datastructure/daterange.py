from datetime import date


class DateRange:
    """This class denotes the range within which the algorithm performs
    its analysis. It has two attributes / reasons to change: 
    starting date, ending date. 

    Args: both start_date and end_date are Date objects
    """

    def __init__(self, start_date: date, end_date: date):
        self.start_date = start_date
        self.end_date = end_date

    def is_datetime_in_range(self, date):
        return date >= self.start_date and date <= self.end_date
