from datetime import datetime, time
import json
import os.path

from datastructure.daytimerange import DayTimeRange
from datastructure.daterange import DateRange


class ConfigReader:

    class Decorators:
        @classmethod
        def catch_null_property_exception(cls, func):
            def wrapper():
                try:
                    func()
                except:
                    print("Property does not exist")

            return wrapper

    def __init__(self, config_path: str):
        if os.path.isfile(config_path):

            with open(config_path) as conf:
                self._data = json.load(conf)

        else:
            print("Read config failure")

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

        for benchmark_time_str in self._data["benchmark_time"]:
            new_benchmark = self._str_to_time(benchmark_time_str)
            benchmark_times.append(new_benchmark)

        return benchmark_times

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
            start_time_str = avg_obj["start_time"]
            end_time_str = avg_obj["end_time"]

            start_time = self._str_to_time(start_time_str)
            end_time = self._str_to_time(end_time_str)

            new_section = DayTimeRange(start_time, end_time)
            avg_data_sections.append(new_section)

        return avg_data_sections

    def _str_to_time(self, timestr: str) -> time:
        return datetime.strptime(timestr, "%H:%M").time()

    def _str_to_date(self, datestr: str) -> datetime.date:
        return datetime.strptime(datestr, "%Y/%m/%d").date()
