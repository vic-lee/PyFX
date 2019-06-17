import pandas as pd
from typing import List

from common.decorators import timer
from datastructure.datacontainer import DataContainer
from datastructure.daytimerange import DayTimeRange


@timer
def include_minute_data(data: DataContainer, sections: List) -> pd.DataFrame:

    def include(section):
        start_time = section['range_start']
        end_time = section['range_end']
        timerange = DayTimeRange(start_time, end_time)

        def process(t):
            df = data.full_minute_price_df.at_time(t).Close
            df.index = df.index.date
            return df

        outs = map(process, timerange)
        df = pd.concat(outs, axis=1)
        return df


    outputs = map(include, sections)
    master = pd.concat(outputs, axis=1)
# print(master)
    return master
