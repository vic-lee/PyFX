from os.path import abspath

from datareader import DataReader
from durationpricemvmt import DurationPipMovmentToPrice

def main():

    package = setup_fpaths()
    minute_data = package[DataReader.MINUTELY]

    pip_movement_config = {
        DurationPipMovmentToPrice.TIME_RANGE: "",
        DurationPipMovmentToPrice.BENCHMARK_TIMES: "",
    }

    pip_movements = DurationPipMovmentToPrice(
        minute_price_df=minute_data, 
        config=pip_movement_config
    )


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