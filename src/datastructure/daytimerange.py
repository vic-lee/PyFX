from datetime import datetime, time


class DayTimeRange:
    """
    This class denotes the range within which the algorithm performs
    its analysis. It has two attributes / reasons to change: 
    starting time, ending time. 

    Args: both start_time and end_time are Time objects
    """

    def __init__(self, start_time: time, end_time: time):
        self.__start_time = start_time
        self.__end_time = end_time

        if start_time > end_time:
            raise ValueError("End time cannot be earlier than start time.")

    @property
    def start_time(self):
        return self.__start_time
    
    @property
    def end_time(self):
        return self.__end_time

    def is_datetime_in_range(self, datetime: datetime):
        return datetime.time() >= self.__start_time and datetime.time() <= self.__end_time

    def to_string_simp(self):
        return "{}{}_{}{}".format(
            self.__start_time.hour,
            self.__start_time.minute,
            self.__end_time.hour,
            self.__end_time.minute)

    def __str__(self):
        return "start time: {}:{} \tend time: {}:{}".format(self.__start_time.hour,
                                                            self.__start_time.minute,
                                                            self.__end_time.hour,
                                                            self.__end_time.minute)
