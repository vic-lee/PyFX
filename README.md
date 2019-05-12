# FX Strategy Research

Please see [config.doc.md](doc/config.doc.md) for documentation on the [configuration file](src/config.json).

## About this project

This repository contains extensible and customizable python modules that help generate analysis output in excel. Key metrics generated include maximum pip movements against predefined benchmarks, average prices over a time period every day, and the times at which prices hit their low/high points. 

## How the project is structured

This is the bird's eye view of the project. Specifically, `dataio` handles the input and output of data; `datastructure` encapsulates commonly used concepts, such as price and date range, into self-contained classes; `metrics` generate metrics. 

```
.
├── app.py
├── dataio
│   ├── datareader.py             # read in data from data source
│   ├── datawriter.py             # write output to excel files  
│   └── dfbundler.py              # bundle multiple metrics into 1 table
├── datastructure
│   ├── __init__.py
│   ├── daterange.py              # date range (start date to end date)
│   ├── daytimerange.py           # time range in a day (start time to end time)
│   ├── pricetime.py              # a price is defined by the price and its time
│   └── test_daterange.py
├── metrics
│   ├── day_movement.py           # maximum daily movement (pips) against benchmark
│   ├── max_price_movements.py    # maximum movements in time period
│   ├── metric.py                 
│   ├── minutely_data.py          # include selected minute data to output
│   ├── period_minmax_time.py     # the time corresponding to min and max prices
│   └── period_price_avg.py       # average price within a period
└── test.py
```