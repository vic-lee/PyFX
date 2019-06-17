from datetime import time
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

        def get_min_data(t: time):
            df = data.full_minute_price_df.at_time(t).Close
            df.index = df.index.date
            return df

        outs = map(get_min_data, timerange)
        df = pd.concat(outs, axis=1)
        return df

    outputs = map(include, sections)
    df_master = pd.concat(outputs, axis=1)

    df_master.columns = pd.MultiIndex.from_product([
        ['Selected Minute Data'], df_master.columns
    ])

    return df_master
