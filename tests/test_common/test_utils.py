import pytest

import os

from tests.context import common
from common import utils


def test_get_logger_cfg_fpath():
    """Tests that logger config fpath indeed leads to a file"""
    assert utils.get_logger_config_fpath().is_file()