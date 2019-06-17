import pandas as pd
from typing import List

from common.decorators import timer
from datastructure.datacontainer import DataContainer


@timer
def include_avg(data: DataContainer, sections: List):

    def include(section):
        df = pd.DataFrame()
        start_time = section.start_time
        end_time = section.end_time
        filtered = data.full_minute_price_df.between_time(start_time, end_time)
        filtered.insert(loc=1, column='Time', value=filtered.index.time)
        filtered.insert(loc=1, column='Date', value=filtered.index.date)

        min_time_mask = (
            filtered['Close'] ==
            filtered.groupby(filtered.index.date).Close.transform(min)
        )

        max_time_mask = (
            filtered['Close'] ==
            filtered.groupby(filtered.index.date).Close.transform(max)
        )

        df['Mean'] = filtered.groupby(filtered.index.date).Close.mean().round(5)
        
        min_series = filtered[min_time_mask]
        min_series.index = min_series.index.date
        min_series = min_series[~min_series.index.duplicated(keep='last')]
        
        max_series = filtered[max_time_mask]
        max_series.index = max_series.index.date
        max_series = max_series[~max_series.index.duplicated(keep='last')]

        df.insert(loc=1, column='TimeForMin', value=min_series['Time'])
        df.insert(loc=2, column='TimeForMax', value=max_series['Time'])

        df.columns = pd.MultiIndex.from_product([
            ['{}_{}'.format(str(start_time), str(end_time))], df.columns
        ])

        return df
    
    outputs = map(include, sections)
    df_master = pd.concat(outputs, axis=1)
    
    return df_master