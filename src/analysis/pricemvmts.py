from datetime import datetime, date, time, timedelta
import logging
import pandas as pd
import numpy as np
from os.path import abspath
import sys
from time import sleep
from typing import List

from analysis.metrics import Metric

from datastructure.datacontainer import DataContainer
from datastructure.daytimerange import DayTimeRange
from datastructure.pricetime import PriceTime

from common.config import Config
from dataio.datawriter import DataWriter
from dataio.datareader import DataReader

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class _DayPipMovmentToPrice:
    """For a given day, this object encapsulates the maximum 
    upward and downward pip movements to a given price. 

    The benchmark price is not directly given; rather, it should
    be passed in as a timestamp. The class then looks up the 
    benchmark price using the timestamp. 

    Foreseeably, in a for loop looping through each day, this class
    can generate each day's daily pip movements. 

    Initialized daily. 
    """

    UP = "up"
    DOWN = "down"

    def __init__(self, date: datetime.date, benchmark_pricetime: PriceTime,
                 time_range_start_pricetime: PriceTime,
                 time_range: DayTimeRange):

        self.__date = date
        self.__benchmark_pricetime = benchmark_pricetime
        self.__time_range_start_pricetime = time_range_start_pricetime
        self.__time_range = time_range

        self.__max_pip_up = 0
        self.__max_pip_up_time = self._initialize_datetime_for_max_min()
        self.__price_at_max_pip_up = self._initialize_prices_for_max_min()

        self.__max_pip_down = 0
        self.__max_pip_down_time = self._initialize_datetime_for_max_min()
        self.__price_at_max_pip_down = self._initialize_prices_for_max_min()

    def __str__(self):
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        return ("{:20} {:20} {:30} {:24} {:30}".format(
            "Date: {}".format(self.__date),
            "Max pip up: {}".format(self.__max_pip_up),
            "Max pip up time: {}".format(f_max_pip_up_time),
            "Max pip down: {}".format(self.__max_pip_down),
            "Max pip down time: {}".format(f_max_pip_down_time)))

    @property
    def date(self):
        return self.__date

    def update_max_pip(self, current_price):
        self._update_max_pip_up(current_price)
        self._update_max_pip_down(current_price)

    def to_df(self) -> pd.DataFrame:
        f_max_pip_up_time = self._format_max_pip_up_time()
        f_max_pip_down_time = self._format_max_pip_down_time()

        df = pd.DataFrame({
            "Benchmark Price": self.__benchmark_pricetime.price,
            "Max Pip Up": self.__max_pip_up,
            "Price at Max Pip Up": self.__price_at_max_pip_up,
            "Time at Max Pip Up": f_max_pip_up_time,
            "Max Pip Down": self.__max_pip_down,
            "Price at Max Pip Down": self.__price_at_max_pip_down,
            "Time at Max Pip Down": f_max_pip_down_time,
        }, index=[self.__date])

        return df

    def _update_max_pip_up(self, current_price):
        new_pip = current_price.pip_movement_from(self.__benchmark_pricetime)
        if self._is_in_benchmark_period(current_price) \
                and self._is_new_pip_greater(new_pip):
            self.__max_pip_up = new_pip
            self.__max_pip_up_time = current_price.datetime
            self.__price_at_max_pip_up = current_price.price

    def _update_max_pip_down(self, current_price):
        new_pip = current_price.pip_movement_from(self.__benchmark_pricetime)
        if self._is_in_benchmark_period(current_price) \
                and self._is_new_pip_lower(new_pip):
            self.__max_pip_down = new_pip
            self.__max_pip_down_time = current_price.datetime
            self.__price_at_max_pip_down = current_price.price

    def _is_new_pip_greater(self, new_pip):
        if self.__max_pip_up is not None:
            return (new_pip > self.__max_pip_up)
        else:
            return (new_pip > 0)

    def _is_new_pip_lower(self, new_pip):
        if self.__max_pip_down is not None:
            return (new_pip < self.__max_pip_down)
        else:
            return (new_pip < 0)

    def _is_in_benchmark_period(self, current_price: PriceTime) -> bool:
        return current_price.is_later_than(self.__benchmark_pricetime)

    def _initialize_max_pips(self):
        if (self.__time_range.is_datetime_in_range(
                self.__benchmark_pricetime.datetime)):
            return 0
        else:
            return None

    def _initialize_datetime_for_max_min(self):
        if (self.__time_range.is_datetime_in_range(
                self.__benchmark_pricetime.datetime)):
            return self.__benchmark_pricetime.datetime
        else:
            return self.__time_range_start_pricetime.datetime

    def _initialize_prices_for_max_min(self):
        if (self.__time_range.is_datetime_in_range(
                self.__benchmark_pricetime.datetime)):
            return self.__benchmark_pricetime.price
        else:
            return self.__time_range_start_pricetime.price

    def _format_max_pip_up_time(self):
        return self._format_time(time=self.__max_pip_up_time)

    def _format_max_pip_down_time(self):
        return self._format_time(time=self.__max_pip_down_time)

    def _format_time(self, time):
        if time == None:
            return None
        else:
            return time.time()


class BenchmarkPricesContainer():
    """Provides read-only reference for all benchmark prices.

    This class facilitates benchmark price querying by filtering down raw price
    data to dataframes with just data at benchmark times.

    `__setitem__` and `__delitem__` are not implemented, for BenchmarkPrices 
    are read-only.

    args:
        price_data: data source, contained in `DataContainer`
        benchmark_times: specifies which minute datas to keep
    """

    def __init__(self, price_data: DataContainer,
                 benchmark_times: List[datetime.time]):
        self.__benchmark_prices = {
            bt: price_data.minute_price_df.at_time(bt)
            for bt in benchmark_times
        }

    def __getitem__(self, key) -> pd.DataFrame:
        try:
            assert isinstance(key, time)
        except TypeError:
            logger.error("The key supplied for type BenchmarkPricesContainer "
                         + "must be of type `datetime.time`. The key is of "
                         + "{} type.".format(type(key)))
        try:
            return self.__benchmark_prices[key]
        except IndexError:
            logger.error("Key is not availabe.")

    def __contains__(self, key):
        return key in self.__benchmark_prices

    def __len__(self):
        return len(self.__benchmark_prices)


class MaxPriceMovements(Metric):
    """This class finds the daily price movements within a period of time.
    It is used in conjunction with DayPipMovmentToPrice class.
    """

    def __init__(self, price_data: DataContainer,
                 config: Config, currency_pair_name: str):

        Metric.__init__(self, config=config,
                        currency_pair_name=currency_pair_name)

        self.__price_data = price_data
        self.__benchmark_times = config.benchmark_times

        self.__max_price_movements = {
            bt: None for bt in self.__benchmark_times
        }
        self.__benchmark_prices = BenchmarkPricesContainer(
            price_data=price_data, benchmark_times=self.__benchmark_times)

        print("Analyzing maximum price movements...")

    def __str__(self):
        benchmark_time_header_template = \
            "\n/******************** Benchmark Time: {} *******************/\n"

        for btime in self.__max_price_movements:

            print(benchmark_time_header_template.format(btime))

            max_pips_for_btime = self.__max_price_movements[btime]

            for day in max_pips_for_btime:
                day_max_pips = max_pips_for_btime[day]
                print(day_max_pips)

    def find_max_price_movements(self):
        """
        Algorithm:
        For each day, perform max day price movement check.
        """
        for btime in self.__max_price_movements:
            self.__max_price_movements[btime] \
                = self._find_max_price_movement(benchmark_time=btime)

        self.__max_price_movements["PDFX"] = self._find_max_price_movement(
            benchmark_time=None, pdfx_benchmark=True)

    def to_benchmarked_results(self):
        benchmarked_dfs = {}

        for benchmarked_time, data in self.__max_price_movements.items():
            df_list = []

            for _, daily_analysis in data.items():
                exported_df = daily_analysis.to_df()
                df_list.append(exported_df)

            df_at_benchmark = pd.concat(df_list, sort=False)

            if (str(benchmarked_time) == 'PDFX'):
                df_at_benchmark = self._merge_pdfx_with_cdfx(df_at_benchmark)

            benchmarked_dfs[str(benchmarked_time)] = df_at_benchmark

        return benchmarked_dfs

    def _find_max_price_movement(self, benchmark_time: time,
                                 pdfx_benchmark=False):

        bt_str = "PDFX" if benchmark_time == None else str(benchmark_time)
        print("\tAnalyzing against benchmark time " + bt_str + "...")

        day_objs = {}
        daily_max_pips_obj = None
        current_date = None

        data_size = len(self.__price_data.minute_price_df.index)
        progress_ctr = 0
        time_tracker = datetime.now()

        for time_index, row in self.__price_data.minute_price_df.iterrows():

            current_price = PriceTime(price=row['Close'], datetime=time_index)

            if self._is_row_new_day(known_date=current_date, new_date=time_index):

                day_objs, current_date = self._incr_one_day(
                    daily_max_pips_obj, day_objs, time_index)

                daily_max_pips_obj = self._init_new_day_obj(
                    pdfx_benchmark, time_index, current_date, benchmark_time)

            if daily_max_pips_obj is not None:
                daily_max_pips_obj.update_max_pip(current_price)

            progress_ctr += 1

            if datetime.now().microsecond - time_tracker.microsecond >= 10000:
                sys.stdout.write('\r')
                sys.stdout.write("\t[%-20s] %d%%" % (
                    '='*(int(20 * progress_ctr / data_size)),
                    (int(100 * progress_ctr / data_size))))
                sys.stdout.flush()
                time_tracker = datetime.now()

        sys.stdout.write('\r')
        sys.stdout.write("\t[%-20s] %d%%" % ('='*(20), (100)))
        sys.stdout.write('\n')
        sys.stdout.flush()

        day_objs = self._save_prior_day_obj(daily_max_pips_obj, day_objs)

        return day_objs

    def _incr_one_day(self, prior_day_obj: _DayPipMovmentToPrice,
                      day_objs, time_index: datetime):

        day_objs = self._save_prior_day_obj(prior_day_obj, day_objs)
        current_date = self._update_current_date(newdate=time_index)

        return day_objs, current_date

    def _init_new_day_obj(self, pdfx_benchmark: bool,
                          time_index: datetime, current_date: datetime.date,
                          benchmark_time: time) -> _DayPipMovmentToPrice:

        benchmark_pricetime, initial_pricetime = self._init_pricetimes(
            pdfx_benchmark, time_index, current_date, benchmark_time)

        if benchmark_pricetime is not None and initial_pricetime is not None:

            return _DayPipMovmentToPrice(
                date=current_date,
                benchmark_pricetime=benchmark_pricetime,
                time_range_start_pricetime=initial_pricetime,
                time_range=self.time_range)

        else:
            return None

    def _init_pricetimes(self, pdfx_benchmark: bool, time_index: datetime,
                         current_date: datetime.date, benchmark_time: time):

        if pdfx_benchmark == False:
            benchmark_pricetime = self._get_benchmark_price(
                date=time_index.date(), benchmark_time=benchmark_time)
            initial_pricetime = benchmark_pricetime

        else:
            prior_day = current_date - timedelta(days=1)
            benchmark_pricetime = self._get_prior_fix_recursive(
                prior_day, self.__price_data)
            initial_pricetime = self._get_benchmark_price(
                date=time_index.date(),
                benchmark_time=self.time_range.start_time)

        return benchmark_pricetime, initial_pricetime

    def _get_benchmark_price(self, date, benchmark_time) -> PriceTime:

        price_df = self.__benchmark_prices[benchmark_time]
        index = datetime(year=date.year, month=date.month,
                         day=date.day, hour=benchmark_time.hour,
                         minute=benchmark_time.minute, second=0)

        try:
            price = price_df.loc[index]['Close']
            return PriceTime(price=price, datetime=index)

        except:
            logger.error("Could not locate price for " + str(index))
            return None

    def _merge_pdfx_with_cdfx(self, df_for_benchmark):

        cp_identifier = \
            self.currency_pair_name[:3] \
            + '-' \
            + self.currency_pair_name[3:]

        current_day_fix_df = self.__price_data.fix_price_df[[cp_identifier]]

        current_day_fix_df = current_day_fix_df.loc['2018-1-2':'2018-12-31']
        current_day_fix_df = current_day_fix_df[
            np.isfinite(current_day_fix_df[cp_identifier])
        ]
        current_day_fix_df.columns = ['Current Day Fix']

        df_for_benchmark = pd.merge(left=current_day_fix_df,
                                    right=df_for_benchmark,
                                    left_index=True,
                                    right_index=True,
                                    how='outer')

        return df_for_benchmark

    @staticmethod
    def _save_prior_day_obj(prior_day_obj: _DayPipMovmentToPrice, day_objs):
        if prior_day_obj != None:
            day_objs[prior_day_obj.date] = prior_day_obj
        return day_objs

    @staticmethod
    def _update_current_date(newdate: datetime) -> datetime.date:
        return newdate.date()

    @staticmethod
    def _is_row_new_day(known_date: datetime.date, new_date: datetime) -> bool:
        if known_date == None:
            return True
        if new_date.date() != known_date:
            return True
        return False
