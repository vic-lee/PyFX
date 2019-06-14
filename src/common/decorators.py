from datetime import datetime
import functools
import xlrd


def timer(func):
    """A decorator that times and prints execution time."""
    def wrapper(*args, **kwargs):
        print("Starting execution...")
        start_time = datetime.now()
        resp = func(*args, **kwargs)
        end_time = datetime.now()
        duration = end_time - start_time
        print("Program runtime: {}.{} secs".format(
            duration.seconds, duration.microseconds))
        return resp
    return wrapper


def singleton(cls, *args, **kwargs):
    """Class decorator that ensures decorated classes only have one instance."""

    instances = {}

    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper
