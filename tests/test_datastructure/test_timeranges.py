import pytest

from datetime import datetime, date

from tests.context import ds
from ds import timeranges
from ds.timeranges import DateRange, DayTimeRange


def test_daterange_initialization():
    """Tests that Errors are raised when DateRange is initiated not with `date`
    type, and when DateRange's `start_date` is later than its `end_date`.
    """
    with pytest.raises(timeranges.DateRangeDateTypeError):
        start = datetime(2008, 1, 1, 12, 30)
        end = datetime(2009, 2, 3, 15, 22)
        d = DateRange(start_date=start, end_date=end)

    with pytest.raises(timeranges.DateRangeValueError):
        start = date(2010, 1, 1)
        end = date(2009, 2, 3)
        d = DateRange(start_date=start, end_date=end)


def test_daterange_properties():
    """Tests DateRange's @property methods"""
    d_to_dt = lambda d: datetime(d.year, d.month, d.day, 0, 0, 0)

    start = date(2018, 1, 1)
    end = date(2019, 2, 3)
    d_between = date(2018, 6, 6)
    d_not_between = date(2023, 6, 6)

    d = DateRange(start_date=start, end_date=end)

    assert d.start_date == start
    assert d.end_date == end
    assert d.start_date_dt == d_to_dt(start)
    assert d.end_date_dt == d_to_dt(end)


def test_datetime_in_range():
    """Tests that DateRange correctly determines if a date object is between 
    its start and end date.
    """
    start = date(2018, 1, 1)
    end = date(2019, 2, 3)
    d_between = date(2018, 6, 6)
    d_not_between = date(2023, 6, 6)
    dt_between = date(2018, 6, 6)
    dt_not_between = date(2023, 6, 6)

    d = DateRange(start_date=start, end_date=end)

    assert d.is_datetime_in_range(d_between)
    assert not d.is_datetime_in_range(d_not_between)

    assert d.is_datetime_in_range(dt_between)
    assert not d.is_datetime_in_range(dt_not_between)
