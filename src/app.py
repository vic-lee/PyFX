from os.path import abspath
from datetime import datetime, time

from dataio.datareader import DataReader
from dataio.datawriter import DataWriter

from metrics.max_price_movements import MaxPriceMovements
from metrics.period_price_avg import PeriodPriceAvg

from datastructure.daytimerange import TimeRangeInDay


def main():
    start_time = datetime.now()

    currency_pairs = [
        "AUDUSD", "EURUSD", "GBPUSD", "NZDUSD", "USDCAD",
        "USDCHF", "USDJPY", "USDMXN", "USDTRY", "USDZAR"
    ]

    # for currency_pair in currency_pairs:
    #     analyze_currency_pair(currency_pair)

    analyze_currency_pair("AUDUSD")

    end_time = datetime.now()
    print("Program runtime: {}".format((end_time - start_time)))


def analyze_currency_pair(currency_pair_name) -> None:
    price_data = read_price_data(currency_pair_name)

    price_movements = setup_price_movement_obj(data=price_data)
    price_movements.find_max_price_movements()
    price_movements.to_excel()


def setup_price_movement_obj(data):

    pip_movement_config = {
        MaxPriceMovements.TIME_RANGE: TimeRangeInDay(
            start_time=time(hour=10, minute=30),
            end_time=time(hour=11, minute=2)
        ),
        MaxPriceMovements.BENCHMARK_TIMES: [
            time(hour=10, minute=30), time(hour=10, minute=45)
        ],
    }

    return MaxPriceMovements(price_dfs=data, config=pip_movement_config)

    # return init_pip_movement_obj(price_data=package)


def read_price_data(currency_pair_name) -> dict:
    in_fpaths = {
        DataReader.FIX: abspath("../data/datasrc/fix1819.csv"),
        DataReader.MINUTELY: abspath("../data/datasrc/{}_Minute.csv".format(currency_pair_name)),
        DataReader.DAILY: abspath(
            "../data/datasrc/{}_Daily.xlsx".format(currency_pair_name))
    }

    fx_reader = DataReader(in_fpaths)
    package = fx_reader.read_data()
    return package


if __name__ == '__main__':
    main()
