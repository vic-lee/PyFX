class TimeRangeInDay:
    """This class denotes the range within which the algorithm performs
    its analysis. It has two attributes / reasons to change: 
    starting time, ending time. 

    Args: both start_time and end_time are Time objects
    """
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time


    def to_string_simp(self):
        return "{}{}_{}{}".format(
            self.start_time.hour, 
            self.start_time.minute, 
            self.end_time.hour, 
            self.end_time.minute)

        