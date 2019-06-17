# FX Strategy Research

Please see 
- [config.doc.md](doc/config.doc.md) for documentation on the [configuration file](config.json).
- [TODO](TODO.md) for known issues and future roadmap.

## About this project

This repository contains extensible and customizable python modules that help generate analysis output in excel. Key metrics generated include maximum pip movements against predefined benchmarks, average prices over a time period every day, and the times at which prices hit their low/high points. 

## How the project is structured

This is the bird's eye view of the project. Specifically, `dataio` handles the input and output of data; `datastructure` encapsulates commonly used concepts, such as price and date range, into self-contained classes; `metrics` generate metrics. 

```
.
├── app.py                      # execution file
├── common
│   ├── config.py               # read-only project configuration
│   ├── decorators.py           # shared decorators
│   └── utils.py                # shared utilities
├── ds
│   ├── __init__.py
│   ├── datacontainer.py        # read-only data container
│   └── timeranges.py           # intra- and inter-day ranges
└── pyfx
    ├── __init__.py
    ├── analysis.py             # analysis functions
    ├── read.py                 # import-related functionalities
    └── write.py                # export-related functionalities
```
