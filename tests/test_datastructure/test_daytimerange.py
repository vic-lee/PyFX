import unittest
from datetime import time, datetime
from ds.daytimerange import DayTimeRange


class TestDayTimeRange(unittest.TestCase):

    def setUp(self):
        self.timerange = DayTimeRange(
            start_time=time(hour=9, minute=11, second=0),
            end_time=time(hour=10, minute=20, second=0)
        )
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_is_datetime_in_range(self):
        result = self.timerange.is_datetime_in_range(
            datetime(year=2000, month=1, day=1, hour=9, minute=44, second=0)
        )
        self.assertTrue(result)

        result = self.timerange.is_datetime_in_range(
            datetime(year=2000, month=1, day=1, hour=17, minute=23, second=0)
        )
        self.assertFalse(result)

    def test_raise_err_on_endtime_earlier_than_starttime(self):
        with self.assertRaises(ValueError):
            DayTimeRange(
                start_time=time(hour=9, minute=11, second=0),
                end_time=time(hour=7, minute=11, second=0)
            )
