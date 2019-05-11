from datetime import datetime, date, time, timedelta
import pandas as pd
import numpy as np
from os.path import abspath
import logging

from datastructure.daytimerange import DayTimeRange
from datastructure.pricetime import PriceTime

from dataio.datawriter import DataWriter
from dataio.datareader import DataReader

from metrics.metric import Metric
from metrics.day_movement import DayPipMovmentToPrice
from metrics.period_price_avg import PeriodPriceAvg
from metrics.minutely_data import MinutelyData

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class MaxPriceMovements(Metric):
    """This class finds the daily price movements within a period of time.
    It is used in conjunction with DayPipMovmentToPrice class.
    """

    DATE_RANGE = "date range"
    TIME_RANGE = "time range"
    BENCHMARK_TIMES = "benchmark_times"
    CURRENCY_PAIR = "currency_pair"

    def __init__(self, price_dfs, config, currency_pair_name: str):

        Metric.__init__(self,
                        price_dfs=price_dfs,
                        config=config,
                        currency_pair_name=currency_pair_name)

        self.benchmark_times = config.benchmark_times
        self.max_price_movements = \
            self._generate_price_movements_obj_from_benchmark_times()

        self.benchmark_prices_matrix = self._generate_benchmark_prices_matrix()

    def _generate_price_movements_obj_from_benchmark_times(self):
        ret = {}
        for btime in self.benchmark_times:
            ret[btime] = {}
        return ret

    def _generate_benchmark_prices_matrix(self):
        ret = {}
        for btime in self.benchmark_times:
            ret[btime] = self.minute_price_df.between_time(btime, btime)
        return ret

    def find_max_price_movements(self):
        """
        Algorithm:
        For each day, perform max day price movement check.
        """
        for btime in self.max_price_movements:
            self.max_price_movements[btime] \
                = self._find_max_price_movement_against_benchmark(benchmark_time=btime)

        self.max_price_movements["PDFX"] = self._find_max_price_movement_against_benchmark(benchmark_time=None,
                                                                                           pdfx_benchmark=True)

    def _find_max_price_movement_against_benchmark(self, benchmark_time: time, pdfx_benchmark=False):

        day_objs = {}
        daily_max_pips_obj = None
        current_date = None

        for time_index, row in self.minute_price_df.iterrows():

            current_price = PriceTime(price=row['Close'], datetime=time_index)

            if self._is_row_new_day(known_date=current_date, new_date=time_index):

                day_objs, current_date = self._incr_one_day(daily_max_pips_obj,
                                                            day_objs,
                                                            time_index)

                daily_max_pips_obj = self._init_new_day_obj(pdfx_benchmark,
                                                            time_index,
                                                            current_date,
                                                            benchmark_time)

            if daily_max_pips_obj is not None:
                daily_max_pips_obj.update_max_pip(current_price)

        day_objs = self._save_prior_day_obj(daily_max_pips_obj, day_objs)

        return day_objs

    def _incr_one_day(self, prior_day_obj: DayPipMovmentToPrice, day_objs, time_index):

        day_objs = self._save_prior_day_obj(prior_day_obj, day_objs)
        current_date = self._update_current_date(newdate=time_index)

        return day_objs, current_date

    def _init_new_day_obj(self, pdfx_benchmark: bool, time_index,
                          current_date: datetime.date, benchmark_time) -> DayPipMovmentToPrice:

        benchmark_pricetime, initial_pricetime = self._init_pricetimes(pdfx_benchmark,
                                                                       time_index,
                                                                       current_date,
                                                                       benchmark_time)

        if benchmark_pricetime is not None and initial_pricetime is not None:

            return DayPipMovmentToPrice(date=current_date,
                                        benchmark_pricetime=benchmark_pricetime,
                                        time_range_start_pricetime=initial_pricetime,
                                        time_range=self.time_range)

        else:
            return None

    def _init_pricetimes(self, pdfx_benchmark: bool, time_index, current_date, benchmark_time):

        if pdfx_benchmark == False:
            benchmark_pricetime = self._get_benchmark_price(date=time_index.date(),
                                                            benchmark_time=benchmark_time)
            initial_pricetime = benchmark_pricetime

        else:
            prior_day = current_date - timedelta(days=1)
            benchmark_pricetime = self._get_prior_fix_recursive(prior_day)
            initial_pricetime = self._get_benchmark_price(date=time_index.date(),
                                                          benchmark_time=self.time_range.start_time)

        return benchmark_pricetime, initial_pricetime

    def _get_benchmark_price(self, date, benchmark_time) -> PriceTime:

        price_df = self.benchmark_prices_matrix[benchmark_time]
        index = datetime(year=date.year, month=date.month, day=date.day,
                         hour=benchmark_time.hour, minute=benchmark_time.minute, second=0)

        try:
            price = price_df.loc[index]['Close']
            return PriceTime(price=price, datetime=index)

        except:
            logger.error("Could not locate price for " + str(index))
            return None

    @staticmethod
    def _save_prior_day_obj(prior_day_obj: DayPipMovmentToPrice, day_objs):
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

    def __str__(self):
        benchmark_time_header_template = \
            "\n/********************** Benchmark Time: {} **********************/\n"

        for btime in self.max_price_movements:

            print(benchmark_time_header_template.format(btime))

            max_pips_for_btime = self.max_price_movements[btime]

            for day in max_pips_for_btime:
                day_max_pips = max_pips_for_btime[day]
                print(day_max_pips)

    def to_benchmarked_results(self):
        benchmarked_dfs = {}

        for benchmarked_time, data in self.max_price_movements.items():
            df_list = []

            for _, daily_analysis in data.items():
                exported_df = daily_analysis.to_df()
                df_list.append(exported_df)

            df_at_benchmark = pd.concat(df_list, sort=False)

            if (str(benchmarked_time) == 'PDFX'):
                df_at_benchmark = self._merge_pdfx_with_cdfx(df_at_benchmark)

            benchmarked_dfs[str(benchmarked_time)] = df_at_benchmark

        return benchmarked_dfs

    def _merge_pdfx_with_cdfx(self, df_for_benchmark):

        cp_identifier = \
            self.currency_pair_name[:3] \
            + '-' \
            + self.currency_pair_name[3:]

        current_day_fix_df = self.fix_price_df[[cp_identifier]]

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
