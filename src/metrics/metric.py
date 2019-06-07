from collections import namedtuple
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, time

from dataio.configreader import ConfigReader
from dataio.datareader import DataReader

from datastructure.datacontainer import DataContainer
from datastructure.daterange import DateRange
from datastructure.daytimerange import DayTimeRange
from datastructure.pricetime import PriceTime

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Metric:

    def __init__(self, currency_pair_name: str, config: ConfigReader):

        self.__time_range = config.time_range
        self.__date_range = config.date_range
        self.__currency_pair_name = currency_pair_name

    def _get_prior_fix_recursive(self, d, prices: DataContainer):

        def daydelta(d, delta): return d - timedelta(days=delta)

        fx = None

        if self.__date_range.is_datetime_in_range(d):

            try:
                cp_identifier = self.__currency_pair_name[:3] \
                    + '-' + self.__currency_pair_name[3:]
                fx = prices.fix_price_df.loc[str(d)][cp_identifier]
            except Exception as e:
                logger.error(
                    "Could not locate the previous fix price, possibly due to out of bounds." + str(e))
                return None
            if not np.isnan(fx):
                index = datetime(year=d.year, month=d.month,
                                 day=d.day, hour=0, minute=0, second=0)
                return PriceTime(price=fx, datetime=index)
            else:
                return self._get_prior_fix_recursive(daydelta(d, 1), prices)

        else:
            return None

    @staticmethod
    def incr_one_min(time_cur):
        old_min = time_cur.minute

        if old_min == 59:
            new_min = 0
            new_hour = time_cur.hour + 1

        else:
            new_min = old_min + 1
            new_hour = time_cur.hour

        time_cur = time(hour=new_hour, minute=new_min, second=time_cur.second)

        return time_cur
