from datetime import datetime, time, timedelta
import json
import logging
import os.path

from datastructure.daytimerange import DayTimeRange
from datastructure.daterange import DateRange


logger = logging.getLogger(__name__)


class ConfigReader:

    class Decorators:
        @classmethod
        def catch_null_property_exception(cls, func):
            def wrapper():
                try:
                    func()
                except:
                    logger.error("Property does not exist")

            return wrapper

    def __init__(self, config_path: str):
        if os.path.isfile(config_path):

            with open(config_path) as conf:
                self._data = json.load(conf)

            self.process_dst_hour_ahead_periods()

        else:
            logger.error("Read config failure")

    def process_dst_hour_ahead_periods(self):
        hour_ahead_periods = []

        for period in self._data["daylight_saving_mode"]["hour_ahead_periods"]:
            date_range = self._read_date_range_obj(period)
            hour_ahead_periods.append(date_range)

        self._data["daylight_saving_mode"]["hour_ahead_periods"] = hour_ahead_periods

    @property
    def currency_pairs(self) -> list:
        return self._data["currency_pairs"]

    @property
    def time_range(self) -> DayTimeRange:
        start_time_str = self._data['time_range']['start_time']
        end_time_str = self._data['time_range']['end_time']

        start_time = self._str_to_time(start_time_str)
        end_time = self._str_to_time(end_time_str)

        return DayTimeRange(start_time=start_time, end_time=end_time)

    @property
    def date_range(self) -> DateRange:
        start_date_str = self._data['date_range']['start_date']
        end_date_str = self._data['date_range']['end_date']

        start_date = self._str_to_date(start_date_str)
        end_date = self._str_to_date(end_date_str)

        return DateRange(start_date=start_date, end_date=end_date)

    @property
    def benchmark_times(self) -> list:
        benchmark_times = []

        for benchmark_time_str in self._data["benchmark_times"]:
            new_benchmark = self._str_to_time(benchmark_time_str)
            benchmark_times.append(new_benchmark)

        return benchmark_times

    @property
    def should_enable_daylight_saving_mode(self) -> bool:
        return self._data["daylight_saving_mode"]["daylight_saving_time"]

    @property
    def dst_hour_ahead_period(self) -> DateRange:
        time_period_def = self._data['daylight_saving_mode']['hour_ahead_period']
        return self._read_date_range_obj(time_period_def)

    @property
    def dst_hour_ahead_periods(self) -> [DateRange]:
        return self._data["daylight_saving_mode"]["hour_ahead_periods"]

    @property
    def dst_hour_ahead_time_range(self) -> DayTimeRange:
        original_time_range = self.time_range

        new_start_time = self._timedelta_by_hour(time=original_time_range.start_time,
                                                 hourdelta=1)
        new_end_time = self._timedelta_by_hour(time=original_time_range.end_time,
                                               hourdelta=1)

        return DayTimeRange(new_start_time, new_end_time)

    @property
    def dst_hour_delay_period(self) -> DateRange:
        time_period_def = self._data['daylight_saving_mode']['hour_delay_period']
        return self._read_date_range_obj(time_period_def)

    @property
    def dst_hour_behind_time_range(self) -> DayTimeRange:
        original_time_range = self.time_range

        new_start_time = self._timedelta_by_hour(time=original_time_range.start_time,
                                                 hourdelta=1,
                                                 decr=True)
        new_end_time = self._timedelta_by_hour(time=original_time_range.end_time,
                                               hourdelta=1,
                                               decr=True)

        return DayTimeRange(new_start_time, new_end_time)

    @property
    def should_include_minutely_data(self) -> bool:
        return self._data["minutely_data"]["include_minutely_data"]

    @property
    def minutely_data_sections(self) -> list:
        minute_sections = []

        for section in self._data["minutely_data"]["included_sections"]:
            new_section = {}

            if "start_time" in section and "end_time" in section:

                start_time_str = section["start_time"]
                end_time_str = section["end_time"]
                start_time = self._str_to_time(start_time_str)
                end_time = self._str_to_time(end_time_str)

            else:

                time_str = section["time"]
                start_time = self._str_to_time(time_str)
                end_time = start_time

            new_section["range_start"] = start_time
            new_section["range_end"] = end_time
            new_section["include"] = section["metric"]

            minute_sections.append(new_section)

        return minute_sections

    @property
    def should_include_period_average_data(self) -> bool:
        return self._data["period_avg_data"]["include_period_avg_data"]

    @property
    def period_average_data_sections(self) -> list:
        avg_data_sections = []

        for avg_obj in self._data["period_avg_data"]["included_sections"]:
            new_section = self._read_time_range_obj(avg_obj)
            avg_data_sections.append(new_section)

        return avg_data_sections

    @staticmethod
    def _str_to_time(timestr: str) -> time:
        return datetime.strptime(timestr, "%H:%M").time()

    @staticmethod
    def _str_to_date(datestr: str) -> datetime.date:
        try:
            return datetime.strptime(datestr, "%Y/%m/%d").date()
        except ValueError:
            logger.error(
                "date string {} does not follow the %Y/%m/%d format.".format(datestr))

    @staticmethod
    def _timedelta_by_hour(time: time, hourdelta: int, decr=False) -> time:
        if decr:
            return (datetime.combine(datetime.today(), time)
                    - timedelta(hours=hourdelta)).time()
        else:
            return (datetime.combine(datetime.today(), time)
                    + timedelta(hours=hourdelta)).time()

    @classmethod
    def _read_date_range_obj(cls, date_range_obj: dict) -> DateRange:

        try:
            if "start_date" not in date_range_obj or "end_date" not in date_range_obj:
                raise KeyError

            else:
                start_date = cls._str_to_date(date_range_obj['start_date'])
                end_date = cls._str_to_date(date_range_obj['end_date'])

                return DateRange(start_date, end_date)

        except KeyError:
            logger.error(("Attempting to read date range",
                          "from a non-date-range object."))

    @classmethod
    def _read_time_range_obj(cls, time_range_obj: dict) -> DateRange:

        try:
            if "start_time" not in time_range_obj or "end_time" not in time_range_obj:
                raise KeyError

            else:
                start_time = cls._str_to_time(time_range_obj['start_time'])
                end_time = cls._str_to_time(time_range_obj['end_time'])

                return DayTimeRange(start_time, end_time)

        except KeyError:
            logger.error(("Attempting to read time range",
                          "from a non-date-range object."))
