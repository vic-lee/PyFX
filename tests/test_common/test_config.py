import pytest

import yaml

from pathlib import Path

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


def test_config_property_benchmark_time():
    """Tests Config loads benchmark time correctly"""
    cfgpath = Path.cwd() / 'tests' / 'testdata' / 'config' / 'cfg1.yml'
    with open(cfgpath) as cfg:
        expected_cfg = yaml.safe_load(cfg)
        test_cfg = Config(cfgpath)
        for bt in test_cfg.benchmark_times:
            assert bt.strftime('%H:%M') in \
                expected_cfg['setup']['benchmark_times']
