from datetime import datetime, date, time, timedelta
from typing import Union


class DateRange:
    """
    This class denotes the range within which the algorithm performs
    its analysis. It has two attributes: starting date, ending date. 

    Args: both start_date and end_date are Date objects
    """

    def __init__(self, start_date: date, end_date: date):
        self._validate_args(start_date, end_date)
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
            day=self.__start_date.day, hour=0, minute=0, second=0)

    @property
    def end_date_dt(self) -> datetime:
        """The ending date, in datetime, for this DateRange object."""
        return datetime(
            year=self.__end_date.year, month=self.__end_date.month,
            day=self.__end_date.day, hour=0, minute=0, second=0)

    def is_datetime_in_range(self, date: Union[date, datetime]):
        return date >= self.__start_date and date <= self.__end_date

    @staticmethod
    def _validate_args(start_date: date, end_date: date):
        """Performs validation for start and end date. 
        
        Raises
        ------
        `DateRangeDateTypeError`
            if `start_date` and `end_date` are not of `date` type

        `DateRangeDateValueError`
            if `start_date` is later than `end_date`
        """

        def make_date_type_err_msg(date_type: str) -> str:
            return (f"{date_type} must be of type `date`. \n"
                    "Note that type `datetime` is not accepted. "
                    "To convert `datetime` to `date`, do `dt.date()`.")

        try:
            assert type(start_date) == date
        except AssertionError:
            raise DateRangeDateTypeError(make_date_type_err_msg('start_date'))
        try:
            assert type(end_date) == date
        except AssertionError:
            raise DateRangeDateTypeError(make_date_type_err_msg('end_date'))

        if start_date > end_date:
            raise DateRangeValueError(
                "End date cannot be earlier than start date.")

    def __repr__(self):
        return (f"start date: {self.__start_date} "
                f"\tend date: {self.__end_date}\n")


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
        return (datetime.time() >= self.__start_time and
                datetime.time() <= self.__end_time)

    def to_string_simp(self):
        return (f"{self.__start_time.hour}{self.__start_time.minute}"
                f"_{self.__end_time.hour}{ self.__end_time.minute}")

    def __str__(self):
        return (
            f"start time: {self.__start_time.hour}:{self.__start_time.minute}"
            f" \tend time: {self.__end_time.hour}:{self.__end_time.minute}")


class DateRangeDateTypeError(TypeError):
    """Raised when `start_date` and `end_date` for DateRange are not 
    of date type
    """
    pass


class DateRangeValueError(ValueError):
    """Raised when `start_date` is a date later than `end_date`"""
    pass
