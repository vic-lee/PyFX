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
    return cfg_test_dir.glob('**/*.yml')


def test_config_property_benchmark_time(config_test_paths):
    """Tests Config loads benchmark time correctly"""
    for cfgpath in config_test_paths:
        with open(cfgpath) as cfg:
            expected_cfg = yaml.safe_load(cfg)
            test_cfg = Config(cfgpath)
            for i, bt in enumerate(test_cfg.benchmark_times):
                expected = expected_cfg['setup']['benchmark_times'][i]
                assert bt.strftime('%H:%M') == expected
