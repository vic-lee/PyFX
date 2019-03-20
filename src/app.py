from os.path import abspath
from datetime.datetime import time

from datareader import DataReader
from daytimerange import TimeRangeInDay
from durationpricemvmt import DurationPipMovmentToPrice

def main():

    package = setup_fpaths()
    minute_data = package[DataReader.MINUTELY]

    pipmvmts = setup_pip_movement_obj(minute_data)


def setup_pip_movement_obj(minute_data):
    pip_movement_config = {
        DurationPipMovmentToPrice.TIME_RANGE: TimeRangeInDay(
            start_time=time(hour=10, minute=30),
            end_time=time(hour=11, minute=2)
        ),
        DurationPipMovmentToPrice.BENCHMARK_TIMES: [
            time(hour=10, minute=30), time(hour=10, minute=45)
        ],
    }

    return DurationPipMovmentToPrice(
        minute_price_df=minute_data, config=pip_movement_config)


def setup_fpaths():
    in_fpaths = {
        DataReader.FIX: abspath("../data/datasrc/fix1819.csv"), 
        DataReader.MINUTELY: abspath("../data/datasrc/GBPUSD_2018.csv"),
        DataReader.DAILY: abspath("../data/datasrc/gbp_daily.xlsx")  
    }

    fx_reader = DataReader(in_fpaths)
    package = fx_reader.read_data()
    return package



if __name__ == '__main__':
    main()