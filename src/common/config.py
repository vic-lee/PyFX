import json
import logging
import logging.config
import os
from pathlib import Path
import yaml
from datetime import datetime, time, timedelta

from common import const
from common.decorators import singleton
from common.utils import get_logger_config_fpath
from ds.timeranges import DateRange, DayTimeRange
from pyfx import read

try:
    logging.config.fileConfig(get_logger_config_fpath())
except FileNotFoundError as e:
    print(e)
logger = logging.getLogger(__name__)


METRIC_TYPES_ALLOWED = ['OHLC', 'Open', 'High', 'Low', 'Close']


class Config:

    class Decorators:
        @classmethod
        def catch_null_property_exception(cls, func):
            def wrapper():
                try:
                    func()
                except:
                    logger.error("Property does not exist")

            return wrapper

    def __init__(self, config_path: Path):
        if not os.path.isfile(config_path):
            logger.error("Failure reading config")
            raise ConfigFileNotFoundError(config_path)

        if config_path.suffix not in ['.yaml', '.yml']:
            logger.error("Config file is not yaml")
            raise ConfigFileTypeError(config_path)

        try:
            with open(config_path) as conf:
                self.__config = yaml.safe_load(conf)
        except yaml.YAMLError as e:
            logger.error("Failure loading config")
            raise ConfigFileTypeError(config_path)

        self._setup()

    def _setup(self):
        self._setup_benchmark_times()
        self._setup_time_range()
        self._setup_date_range()
        self._setup_dst_hour_ahead_periods()
        self._setup_minutely_sections()

    def _setup_benchmark_times(self):
        benchmark_times = []

        for benchmark_time_str in self.__config['setup']['benchmark_times']:
            new_benchmark = self._str_to_time(benchmark_time_str)
            benchmark_times.append(new_benchmark)

        self.__config['setup']['benchmark_times'] = benchmark_times

    def _setup_time_range(self):
        timerange = self.__config['setup']['time_range']
        self.__config['setup']['time_range'] = \
            self._read_time_range_obj(timerange)

    def _setup_date_range(self):
        start_date_str = self.__config['setup']['date_range']['start_date']
        end_date_str = self.__config['setup']['date_range']['end_date']

        start_date = self._str_to_date(start_date_str)
        end_date = self._str_to_date(end_date_str)

        self.__config['setup']['date_range'] = \
            DateRange(start_date=start_date, end_date=end_date)

    def _setup_dst_hour_ahead_periods(self):
        if ('data_adjustments' in self.__config and
                'daylight_saving_mode' in self.__config['data_adjustments']):

            hour_ahead_periods = []
            for period in self.__config['data_adjustments']['daylight_saving_mode']['hour_ahead_periods']:
                date_range = self._read_date_range_obj(period)
                hour_ahead_periods.append(date_range)

            self.__config['data_adjustments']['daylight_saving_mode']['hour_ahead_periods'] = hour_ahead_periods

    def _setup_minutely_sections(self):
        """Validate and initialize settings regarding minutely sections, if any.
        """
        if self._is_minutely_section_defined():
            self._validate_minutely_sections()
            minute_sections = []

            for section in self.__config['metrics']['minutely_data']['sections']:
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

                new_section["include"] = \
                    self._parse_src_metric_type(section["metric"])

                minute_sections.append(new_section)

            self.__config['metrics']['minutely_data']['sections'] = minute_sections

    def _is_minutely_section_defined(self) -> bool:
        """Returns whether minute section is defined in config.

        Minutely section should be defined at:
        ```
        metrics:
            minutely_data:
                ...
        ```
        """
        return (
            'metrics' in self.__config and
            'minutely_data' in self.__config['metrics']
        )

    def _validate_minutely_sections(self):
        """Checks minutely sections has all required fields.

        Minutely data configuration format
        ----------------------------------
        ```
        metrics:
            minutely_data:
                # time option one: specify start and end time
                start_time: 'HH:MM'
                end_time:   'HH:MM'

                # OR, time option two: specify time (i.e. start_time=end_time)
                time:       'HH:MM'

                # metric option one: single metric                
                metric:     any[
                                    'Open', 'High', 'Low', 'Close', 
                                    'OHLC'  # use all metrics
                                ]

                # OR, metric option two: a list of metrics
                metric:
                    - 'Open'
                    - 'High'
                    - ...
        ```
        """
        conf = self.__config['metrics']['minutely_data']
        if 'sections' not in conf:
            raise MinutelyDataConfigurationError("key 'sections' required")
        for section in conf['sections']:
            if not (all(k in section for k in ['start_time', 'end_time']) or
                    ('time' in section)):
                raise MinutelyDataConfigurationError(
                    "key 'start_time' and 'end_time' OR 'time' required")

            if 'metric' not in section:
                raise MinutelyDataConfigurationError(
                    "key 'metric' required")

            if (isinstance(section['metric'], list) and
                    any(k not in METRIC_TYPES_ALLOWED
                        for k in section['metric'])):
                raise ConfigSrcMetricTypeError(section['metric'])
            elif section['metric'] not in METRIC_TYPES_ALLOWED:
                raise ConfigSrcMetricTypeError(section['metric'])

    def _parse_src_metric_type(self, metrics):
        """Translate src metric string to OHLC enums.

        Raises
        ------
        ConfigSrcMetricTypeError:
            if metric does not conform to allowed metric strings.
        """

        def _parse(metric):
            if metric.lower() == 'close':
                return const.CLOSE
            elif metric.lower() == 'open':
                return const.OPEN
            elif metric.lower() == 'high':
                return const.HIGH
            elif metric.lower() == 'low':
                return const.LOW
            elif metric.lower() == 'ohlc':
                return const.OHLC
            else:
                raise ConfigSrcMetricTypeError(metric)

        if isinstance(metrics, list):
            return list(map(_parse, metrics))
        return _parse(metrics)

    @property
    def currency_pairs(self) -> list:
        return self.__config['setup']['currency_pairs']

    @property
    def time_range(self) -> DayTimeRange:
        return self.__config['setup']['time_range']

    @property
    def date_range(self) -> DateRange:
        """The start and end date for the analysis."""
        return self.__config['setup']['date_range']

    @property
    def benchmark_times(self) -> list:
        return self.__config['setup']['benchmark_times']

    @property
    def should_enable_daylight_saving_mode(self) -> bool:
        return self.__config['data_adjustments']['daylight_saving_mode']['enabled']

    @property
    def dst_hour_ahead_period(self) -> DateRange:
        time_period_def = self.__config['data_adjustments']['daylight_saving_mode']['hour_ahead_period']
        return self._read_date_range_obj(time_period_def)

    @property
    def dst_hour_ahead_periods(self) -> [DateRange]:
        return self.__config['data_adjustments']['daylight_saving_mode']['hour_ahead_periods']

    @property
    def dst_hour_ahead_time_range(self) -> DayTimeRange:
        original_time_range = self.time_range

        new_start_time = self._timedelta_by_hour(
            time=original_time_range.start_time, hourdelta=1)
        new_end_time = self._timedelta_by_hour(
            time=original_time_range.end_time, hourdelta=1)

        return DayTimeRange(new_start_time, new_end_time)

    @property
    def dst_hour_delay_period(self) -> DateRange:
        time_period_def = self.__config['data_adjustments']['daylight_saving_mode']['hour_delay_period']
        return self._read_date_range_obj(time_period_def)

    @property
    def dst_hour_behind_time_range(self) -> DayTimeRange:
        original_time_range = self.time_range

        new_start_time = self._timedelta_by_hour(
            time=original_time_range.start_time, hourdelta=1, decr=True)
        new_end_time = self._timedelta_by_hour(
            time=original_time_range.end_time, hourdelta=1, decr=True)

        return DayTimeRange(new_start_time, new_end_time)

    @property
    def should_include_minutely_data(self) -> bool:
        return self.__config["minutely_data"]["include_minutely_data"]

    @property
    def minutely_data_sections(self) -> list:
        if self._is_minutely_section_defined():
            return self.__config['metrics']['minutely_data']['sections']
        else:
            return None

    @property
    def should_time_shift(self) -> bool:
        if ('time_shift' in self.__config and
                'should_shift_time' in self.__config['time_shift']):
            return self.__config["time_shift"]["should_shift_time"]
        return False

    @property
    def time_shift(self) -> bool:
        return self.__config["time_shift"]["hour_delta"]

    @property
    def should_include_period_average_data(self) -> bool:
        return self.__config["period_avg_data"]["include_period_avg_data"]

    @property
    def period_average_data_sections(self) -> list:
        avg_data_sections = []

        for avg_obj in self.__config['metrics']['period_avg_data']['sections']:
            new_section = self._read_time_range_obj(avg_obj)
            avg_data_sections.append(new_section)

        return avg_data_sections

    def fpath(self, cp_name: str) -> dict:
        fpaths = {
            read.MINUTE:
                os.path.abspath("data/datasrc/{}_Minute.csv").format(cp_name),
            read.FIX:
                os.path.abspath("data/datasrc/fix1819.csv"),
            read.DAILY:
                os.path.abspath("data/datasrc/{}_Daily.xlsx".format(cp_name))
        }

        if self._should_override_fpath(cp_name):
            custom_fpaths = self._access_overridden_filepaths(cp_name)

            def use_custom_fpath(json_key, fpath_key):
                return custom_fpaths[json_key] \
                    if json_key in custom_fpaths else fpaths[fpath_key]

            fpaths[read.MINUTE] = use_custom_fpath('Minute', read.MINUTE)
            fpaths[read.FIX] = use_custom_fpath('Fix', read.FIX)
            fpaths[read.DAILY] = use_custom_fpath('Daily', read.DAILY)

        return fpaths

    @staticmethod
    def _str_to_time(timestr: str) -> time:
        return datetime.strptime(timestr, "%H:%M").time()

    @staticmethod
    def _str_to_date(datestr: str) -> datetime.date:
        try:
            return datetime.strptime(datestr, "%Y/%m/%d").date()
        except ValueError:
            logger.error(
                f"date string {datestr} does not follow the %Y/%m/%d format.")

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

    def _should_override_fpath(self, cp_name: str):
        if 'overridden_filepaths' in self.__config \
                and len(self.__config['overridden_filepaths']) > 0:
            if cp_name in self.__config['overridden_filepaths']:
                return True
        return False

    def _access_overridden_filepaths(self, cp_name: str):
        try:
            return self.__config['overridden_filepaths'][cp_name]
        except KeyError:
            pass


class ConfigFileNotFoundError(FileNotFoundError):
    """Raised when there is no project configuration file, or configuration
    file does not exist at the fpath being requested.
    """

    def __init__(self, config_path):
        super(ConfigFileNotFoundError, self).__init__(
            f"Configuration file cannot be found at `{config_path}`")


class ConfigFileTypeError(ValueError):
    """Raised when the configuration file is not of `yaml` type."""

    def __init__(self, config_path):
        super(ValueError, self).__init__((
            f"An error has occured processing `{config_path}`. It is "
            "possible the file is not of `yaml` type, or the yaml file "
            "is corrupted or misconfigured."))


class ConfigSrcMetricTypeError(ValueError):
    """Raised when the configuration file is not of `yaml` type."""

    def __init__(self, metrics_requested):
        super(ValueError, self).__init__((
            "An error has occured processing your metrics requested.\n\n"
            f"\tRequested: \t{metrics_requested}\n"
            f"\tAllowed: \t{METRIC_TYPES_ALLOWED}"))


class MinutelyDataConfigurationError(ValueError):
    """Raised when Minutely data is not configured properly."""
    pass
