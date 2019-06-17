import unittest
from datetime import date
from ds.ranges import DateRange

class TestDateRange(unittest.TestCase):

    def setUp(self):
        self.test_date_range = DateRange(
            start_date=date(year=2018, month=3, day=2), 
            end_date=date(year=2018, month=6, day=3)
        )
        return super().setUp()

    def tearDown(self):
        return super().tearDown()
    
    def test_is_datetime_in_range(self):
        result = self.test_date_range.is_datetime_in_range(
            date(year=2018, month=4, day=1)
        )
        self.assertTrue(result)

        result = self.test_date_range.is_datetime_in_range(
            date(year=2018, month=8, day=21)
        )
        self.assertFalse(result)