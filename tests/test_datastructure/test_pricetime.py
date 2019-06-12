import unittest
from datetime import time, datetime
from datastructure.pricetime import PriceTime


class TestPriceTime(unittest.TestCase):

    def setUp(self):
        self.static_price = 1.22283
        self.pricetime = PriceTime(
            price=self.static_price,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=33, second=0)
        )
        return super().setUp()

    def tearDown(self):
        return super().tearDown()

    def test_pip_movement_from(self):
        pip_delta = 20
        test_price = 1.22083
        test_pricetime = PriceTime(
            price=test_price,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=33, second=0)
        )
        self.assertEqual(
            self.pricetime.pip_movement_from(test_pricetime), pip_delta)

        pip_delta = 20
        test_price = 1.26082
        test_pricetime = PriceTime(
            price=test_price,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=33, second=0)
        )
        self.assertNotEqual(
            self.pricetime.pip_movement_from(test_pricetime), pip_delta)

    def test_is_earlier_than(self):
        test_pricetime = PriceTime(
            price=2.3333,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=33, second=0)
        )
        self.assertFalse(self.pricetime.is_earlier_than(test_pricetime))

        test_pricetime = PriceTime(
            price=2.3333,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=13, second=0)
        )
        self.assertFalse(self.pricetime.is_earlier_than(test_pricetime))

        test_pricetime = PriceTime(
            price=2.3333,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=23, minute=13, second=0)
        )
        self.assertTrue(self.pricetime.is_earlier_than(test_pricetime))

    def test_is_later_than(self):
        test_pricetime = PriceTime(
            price=2.3333,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=40, second=0)
        )
        self.assertFalse(self.pricetime.is_later_than(test_pricetime))

        test_pricetime = PriceTime(
            price=2.3333,
            datetime=datetime(year=2013, month=3, day=12,
                              hour=19, minute=13, second=0)
        )
        self.assertTrue(self.pricetime.is_later_than(test_pricetime))
