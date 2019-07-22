import logging
import functools
import os
from datetime import datetime

import xlrd
from dotenv import load_dotenv

load_dotenv()

try:
    logging.config.fileConfig(os.getenv('LOGGER_CFG_PATH'))
except FileNotFoundError as e:
    print(e)
logger = logging.getLogger(__name__)

def timer(func):
    """A decorator that times and prints execution time."""
    def timer_wrapper(*args, **kwargs):
        start_time = datetime.now()
        resp = func(*args, **kwargs)
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info("{:<40} runtime: {}.{} secs".format(
            func.__name__, duration.seconds, duration.microseconds))
        return resp
    return timer_wrapper


def singleton(cls, *args, **kwargs):
    """Class decorator that ensures decorated classes only have one instance."""

    instances = {}

    def singleton_wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return singleton_wrapper
