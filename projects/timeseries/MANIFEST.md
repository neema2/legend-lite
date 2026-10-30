# timeseries

Layer 0, no dependencies. Package root `timeseries::`. No store and no mapping: functions over any table.

The timeseries library: gs-quant's functions as Pure, each over a table it is given (a window of its line, or the line
and its dates), declaring the column it writes; and the enumerations of their options. DataCube's timeseries tile calls
it, and the platform's `#TS{...}#` island imports it (docs/TIMESERIES_LANGUAGE_2026_10_29.md,
docs/TS_LANGUAGE_BUILD_2026_10_30.md). Until 2026-10-30 it was DataCube's `datacube::ts`
(`datacube/src/pure/datacube_ts.pure`).

## Exports

| element | kind | note |
| --- | --- | --- |
| timeseries::Statistic, PairStatistic, Returns, ... | enumerations | each function's options, checked when a query compiles |
| timeseries::mean, sum, min, max, std, var, ... | functions | rolling over a window of the line (`over(~line, ~date->ascending(), rows(-n+1, 0))`) |
| timeseries::lag, diff, returns, ... | functions | point to point, over observations |
| timeseries::lagPeriod, returnsPeriod, windowPeriod, ... | functions | over calendar periods, an as-of join of the line to itself |
| timeseries::volatility, zscores, rebase, index, ... | functions | gs-quant's figures |
