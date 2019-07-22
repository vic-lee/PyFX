import pytest

from pathlib import Path
from typing import Iterator

import yaml

from tests.context import common
from common import config
from common.config import Config


def test_config_handles_file_not_found():
    """Tests Config correctly raises ConfigFileNotFoundError if the specified
    path does not lead to a file.
    """
    with pytest.raises(config.ConfigFileNotFoundError):
        cfg = Config("wrongfpath.yml")


def test_config_handles_file_type_error():
    """Tests Config raises ConfigFileTypeError if the specified file does not
    conform to the requried file type.
    """
    testfiles = [
        Path.cwd() / 'tests' / 'test_common' / f'wrongtype{s}'
        for s in ['', '.xml', '.ini']
    ]

    for tf in testfiles:
        tf.touch()
        with pytest.raises(config.ConfigFileTypeError):
            cfg = Config(tf)
        tf.unlink()


@pytest.fixture
def config_test_paths() -> Iterator[Path]:
    """Paths to config test files

    Returns
    -------
    An iterator that iterates through all `yml` files in the
    config testdata directory.
    """
    cfg_test_dir = Path.cwd() / 'tests' / 'testdata' / 'config'
    return cfg_test_dir.glob('**/cfg_default*.yml')


def test_config_property_benchmark_time(config_test_paths):
    """Tests Config loads benchmark time correctly"""
    for cfgpath in config_test_paths:
        with open(cfgpath) as cfg:
            expected_cfg = yaml.safe_load(cfg)
            test_cfg = Config(cfgpath)
            for i, bt in enumerate(test_cfg.benchmark_times):
                expected = expected_cfg['setup']['benchmark_times'][i]
                assert bt.strftime('%H:%M') == expected


def test_config_property_time_range(config_test_paths):
    """Tests Config loads time ranges correctly"""
    for cfgpath in config_test_paths:
        with open(cfgpath) as cfg:
            expected_cfg = yaml.safe_load(cfg)
            test_cfg = Config(cfgpath)

            expected_start = expected_cfg['setup']['time_range']['start_time']
            expected_end = expected_cfg['setup']['time_range']['end_time']
            got_start_str = test_cfg.time_range.start_time.strftime('%H:%M')
            got_end_str = test_cfg.time_range.end_time.strftime('%H:%M')

            assert got_start_str == expected_start
            assert got_end_str == expected_end


def test_config_property_date_range(config_test_paths):
    """Tests Config loads date ranges correctly"""
    for cfgpath in config_test_paths:
        with open(cfgpath) as cfg:
            expected_cfg = yaml.safe_load(cfg)
            test_cfg = Config(cfgpath)

            expected_start = expected_cfg['setup']['date_range']['start_date']
            expected_end = expected_cfg['setup']['date_range']['end_date']
            got_start = test_cfg.date_range.start_date.strftime('%Y/%m/%d')
            got_end = test_cfg.date_range.end_date.strftime('%Y/%m/%d')

            assert got_start == expected_start
            assert got_end == expected_end


def test_config_average_time_range(config_test_paths):
    """Tests Config loads average time range correctly"""
    for cfgpath in config_test_paths:
        with open(cfgpath) as cfg:
            expected_cfg = yaml.safe_load(cfg)
            test_cfg = Config(cfgpath)

            try:
                is_avg_time_range_defined = \
                    len(expected_cfg['metrics']
                                    ['period_avg_data']
                                    ['sections']) > 0
            except KeyError:
                is_avg_time_range_defined = False

            for i, d in enumerate(test_cfg.period_average_data_sections):
                if is_avg_time_range_defined:
                    expected = (expected_cfg['metrics']
                                            ['period_avg_data']
                                            ['sections']
                                            [i])
                    assert d.start_time.strftime(
                        '%H:%M') == expected['start_time']
                    assert d.end_time.strftime('%H:%M') == expected['end_time']
                else:
                    assert d == None


def test_config_src_metric_validation():
    """Tests Config raises error when source metric does not conform to the 
    types allowed (e.g. 'Close', 'Open').
    """
    testpath = (Path.cwd() / 'tests' / 'testdata'
                / 'config' / 'cfg_src_metric_err1.yml')
    with pytest.raises(config.ConfigSrcMetricTypeError):
        cfg = Config(testpath)
