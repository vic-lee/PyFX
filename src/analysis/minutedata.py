import pandas as pd
from typing import List

from common.decorators import timer
from datastructure.datacontainer import DataContainer


@timer
def include_minute_data(data: DataContainer, sections: List) -> pd.DataFrame:

    def include(section):
        start_time = section['range_start']
        end_time = section['range_end']

        df = data.full_minute_price_df              \
            .between_time(start_time, end_time)     \
            .Close                                  \
            .to_frame()

        df.insert(loc=1, column='Time', value=df.index.time)
        df.insert(loc=1, column='Date', value=df.index.date)

        df = df.groupby(df.index.date).apply(
            lambda series: series.pivot(index='Date', columns='Time',
                                        values='Close')
        )
        return df

    outputs = map(include, sections)
    master = pd.concat(outputs, axis=1)

    return master
