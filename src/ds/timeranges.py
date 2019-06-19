from datetime import datetime, date, time, timedelta


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
    def start_date(self) -> date:
        """The starting date for this DateRange object."""
        return self.__start_date

    @property
    def end_date(self) -> date:
        """The ending date for this DateRange object."""
        return self.__end_date

    @property
    def start_date_dt(self) -> datetime:
        """The starting date, in datetime, for this DateRange object.""" 
        return datetime(
            year=self.__start_date.year, month=self.__start_date.month,
            day=self.__start_date.day, hour=0, minute=0, second=0
        )

    @property
    def end_date_dt(self) -> datetime: 
        """The ending date, in datetime, for this DateRange object.""" 
        return datetime(
            year=self.__end_date.year, month=self.__end_date.month,
            day=self.__end_date.day, hour=0, minute=0, second=0
        )


    def is_datetime_in_range(self, date):
        return date >= self.__start_date and date <= self.__end_date

    def __repr__(self):
        return "start date: {} \tend date: {}\n".format(self.__start_date, self.__end_date)


class DayTimeRange:
    """
    This class denotes the range within which the algorithm performs
    its analysis. It has two attributes / reasons to change: 
    starting time, ending time. 

    Args: both start_time and end_time are Time objects
    """

    def __init__(self, start_time: time, end_time: time):
        self.__start_time = start_time
        self.__end_time = end_time
        self.__idx_time = self.__start_time

        if start_time > end_time:
            raise ValueError("End time cannot be earlier than start time.")

    def __iter__(self):
        return self

    def __next__(self):
        if self.__idx_time <= self.__end_time:
            def conv(t): return datetime(
                1970, 1, 1, t.hour, t.minute, t.second)
            self.__idx_time = (conv(self.__idx_time) +
                               timedelta(minutes=1)).time()
            return self.__idx_time
        else:
            raise StopIteration

    @property
    def start_time(self):
        return self.__start_time

    @property
    def end_time(self):
        return self.__end_time

    def is_datetime_in_range(self, datetime: datetime):
        return datetime.time() >= self.__start_time and datetime.time() <= self.__end_time

    def to_string_simp(self):
        return "{}{}_{}{}".format(
            self.__start_time.hour,
            self.__start_time.minute,
            self.__end_time.hour,
            self.__end_time.minute)

    def __str__(self):
        return "start time: {}:{} \tend time: {}:{}".format(self.__start_time.hour,
                                                            self.__start_time.minute,
                                                            self.__end_time.hour,
                                                            self.__end_time.minute)
