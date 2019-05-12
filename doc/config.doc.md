# Config Documentation

Below is the documentation on how to configure the application. `......` incidates user may add more specifications.

```javascript
{
    "currency_pairs": [                 // Names of the currency pairs to analyze
        "AUDUSD",
        "EURUSD",
        ........
        "USDZAR"
    ],
    "time_range": {                     // Time period to analyze on
        "start_time": "10:30",
        "end_time": "11:02"
    },
    "date_range": {                     // Lookback period
        "start_date": "2018/01/01",
        "end_date": "2018/12/31"
    },
    "benchmark_times": [                // Benchmark times against which the analysis is run
        "10:30",                        // Benchmark times must be within the time range above.
        "10:45",
        ......
    ],
    "daylight_saving_mode": {
        "daylight_saving_time": true,   // if `true`, provides adjustments for DST
        "hour_delay_period": {          // time period in which the correct hour is late by 1hr
            "start_date": "2018/10/28", // e.g. 9:30 is considered to be normal period's 10:30
            "end_date": "2018/11/03"
        },
        "hour_ahead_period": {          // time period in which the correct hour is early by 1hr
            "start_date": "2018/03/11", // e.g. 11:30 is considered to be normal period's 10:30
            "end_date": "2018/03/24"
        }
    },
    "minutely_data": {
        "include_minutely_data": true,  // if `true`, includes raw minute data based on spec below
        "included_sections": [          // there are 2 ways to include data:
            {                           // 1. you may include prices of a time period
                "start_time": "10:49",
                "end_time": "11:02",
                "metric": [
                    "Open",             // types of metrics to include for this time (period)
                    .......             // options include:
                    "Close"             //    "Open", "High", "Low", "Close" (cap-sensitive)
                ]
            },
            {                           // 2. you may include the price at a specific time
                "time": "11:30",
                "metric": [
                    "Close"
                ]
            },
           ......
        ]
    },
    "period_avg_data": {
        "include_period_avg_data": true,  // if `true`, includes the avg of the time periods below
        "included_sections": [
            {
                "start_time": "10:58",
                "end_time": "11:02"
            }
            ......
        ]
    }
}
```