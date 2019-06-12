from datetime import datetime
import functools
import xlrd


def timer(in_seconds=True):
    """A decorator that times and prints execution time."""
    def _timer(func):
        @functools.wraps(func)
        def wrapper():
            print("Starting execution...")
            start_time = datetime.now()
            func()
            end_time = datetime.now()
            duration = end_time - start_time
            if in_seconds:
                print("Program runtime: {}.{} secs".format(
                    duration.seconds, duration.microseconds))
            else:
                print("Program runtime: {}".format(duration))
        return wrapper
    return _timer


def singleton(cls, *args, **kwargs):
    """Class decorator that ensures decorated classes only have one instance."""

    instances = {}

    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper
