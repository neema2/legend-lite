# A query language for timeseries (2026-10-29)

**Status: design, not built. Version 3.1**, after two rounds of paper tests (§13) and the user's decisions of
2026-10-29; §14 lists what changed in each version. Step C of docs/TIMESERIES_ON_THE_PLATFORM_2026_10_28.md, worked out with the
user on 2026-10-28 and 2026-10-29. It replaces that note's `#Quant` sketch (its D1-D3).
The same language for quants, with worked examples and charts: docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md.

## 1. What it is for

One formula language for the timeseries tile, Python and Pure. Today the tile's formulas are parsed and turned into
Pure by DataCube's TypeScript (`timeseries-expr.ts`, `timeseries-query.ts`: about 4,100 lines), so the platform, Studio,
saved queries and services cannot use them. This language moves into the platform, and the TypeScript goes.

- **The tile's formula box holds one select item** (`volatility(close['SPX'], 63)`). The tile's settings write the rest
  of the query.
- **A query's meaning is its expansion** into plain SQL (the `#SQL{}#` island's Postgres SQL), which the existing
  translator turns into Pure. The library's functions (`datacube::ts`) are found by name as SQL user-defined
  functions. The database computes every figure.

## 2. Principles

Every choice below follows from one of these.

1. **Intent over mechanism.** Write what a person means (`avg(x, 20)`), not the machinery that computes it
   (`ROWS BETWEEN 19 PRECEDING AND CURRENT ROW`).
2. **A SQL word means what SQL means.** Where gs-quant and SQL use one name differently, SQL's meaning wins and
   gs-quant's gets another name.
3. **One word, one meaning,** everywhere in a query.
4. **Position carries meaning.** Where a filter is written says what it filters.
5. **Everything that changes a number is written in the query**: the period aggregate, the window, the fill. Nothing
   is inferred from a column's name. `FILTER` never changes a number.
6. **Mistakes are compile errors that say the fix,** never quietly different numbers. What cannot be an error is a
   warning.

## 3. A query

```sql
SELECT last(close['SPX'])                 AS wk_spx,
       last(close['QQQ'])                 AS wk_qqq,
       volatility(wk_spx, 3mo)            AS spx_vol,
       spx_vol - volatility(wk_qqq, 3mo)  AS spread
FROM   prices ON date BY symbol
SAMPLE BY 1w FILL(PREV)
FILTER date >= '2024-01-01'
```

A row per week: SPX's and QQQ's weekly closes, SPX's 3-month volatility, and its spread over QQQ's; where one of them
has no close in a week, its last one is used; shown from 2024, computed with every week before.

Clause order: `SELECT … FROM … SAMPLE BY … FILL(…) FILTER …`. Only `SELECT` and `FROM` are required.

## 4. The rules

### R1. Sources

`FROM <source> [alias] ON <date column> [BY <instrument column>]`

- A source is a table, `(table WHERE condition)`, or any SQL subquery: `(SELECT *, price * qty AS value FROM trades)`.
  `(prices WHERE venue = 'NYSE')` means exactly `(SELECT * FROM prices WHERE venue = 'NYSE')`.
- `ON` names the date column; `BY` the column naming each row's instrument (a symbol, a tenor, a series). `BY` is left
  out where the source has one instrument: its columns are then lines with no subscript (`total_volume`), a
  subquery's columns included.
- Several sources are separated by commas, each with its own `ON` and `BY` and an alias:
  `FROM prices px ON date BY symbol, rates rf ON date BY tenor`. **The comma is not a join**: each source's lines are
  picked first (R2), and lines meet on dates (R6).

### R2. Lines

A line is one instrument's column along its dates.

- `close['SPX']`: SPX's close. `close['SPX', venue = 'NYSE']`: SPX's close from the NYSE rows only (more matches, each
  `column = value`, after the instrument).
- `close`, with no subscript: one line per instrument. The result then carries the instrument column, a row per date
  per instrument.
- A single line **broadcasts** against per-instrument lines: `close / close['SPX']` is each instrument relative to SPX.
- With several sources, a line names its source: `px.close['SPX']`. With one, the alias is optional.
- A source with many rows per date per instrument (trades) has no line until `SAMPLE BY` aggregates it (R5).

### R3. Four kinds of function

| Kind | Example | Recognised by |
|---|---|---|
| per point | `ln(x)`, `x / y`, `greatest(x, 0)` | no window, no period |
| along time | `avg(x, 20)`, `volatility(x, 3mo)`, `sum(x, all)` | a window argument (R4) |
| within a period | `last(x)`, `sum(x)` under `SAMPLE BY` | an aggregate with no window (R5) |
| across instruments | `rank(x DESC)` | its own names (§6) |

Functions nest: `zscores(volatility(close['SPX'], 3mo), 1y)`.

### R4. Windows

A function along time takes its window as an argument. The argument's type says which kind it is:

| Window | Type | Means |
|---|---|---|
| `20` | a number | the last 20 observations, the current one included: on daily data 20 trading days, under `SAMPLE BY 1w` 20 weeks |
| `3mo`, `1y`, `30d` | a span (§5) | the dates **after** the one that far back, up to and including the current one: rolling |
| `all` | the keyword | everything to date: `sum(pnl['A'], all)` is a running total |
| `'2023-01-01'`, `date_trunc('year', latest)` | a date | the dates **on or after** that date, up to and including the current one: `sum(pnl['A'], '2023-01-01')` is the total since 2023 |

**A span and a date are different series.** `sum(pnl['A'], 1y)` is a rolling year: each date looks back a year from
itself, and the date exactly a year back is not in it. `sum(pnl['A'], latest - 1y)` is a running total from one fixed
date, that date included.

- A function that takes prices and uses their returns counts **returns**: `volatility(x, 20)` is over 20 returns,
  from 21 prices, as gs-quant's is.
- A rolling value first appears **once its window is full**: a 20-observation average on the 20th observation; a
  `1y` window from the line's first date a year on. A date window is full from that date.
- Every function runs **over its line's own values**: `avg(close['SPX'], 20)` averages SPX's last 20 observations,
  never dates filled in from another line (R6).
- **Warm-up adds up through nesting**: `zscores(volatility(x, 3mo), 1y)` needs 3 months and then a year. The compiler
  works out each item's warm-up and reports it (Show Query, the tile): "z: first value after 15 months of data
  (2016-04-01)". It warns where the data is shorter than an item's warm-up, and where a source's `WHERE` starts the
  data less than an item's warm-up before the first date shown: "ma has no values for its first 19 days: the data
  starts 2024-01-01 (WHERE); to show from that date with full values, FILTER the answer instead".
- `FILTER ready` keeps only the rows where every item has its value.

### R5. Periods

`SAMPLE BY <span>` gives one row per period: `1d`, `1w` (Monday to Sunday), `1mo`, `1q`, `1y` (calendar periods).

- **Under `SAMPLE BY`, a line appears only inside a period aggregate**, which says how its period's rows become one
  value: `last(close['SPX'])`, `first(open['SPX'])`, `max(high['SPX'])`, `min(low['SPX'])`, `sum(volume['SPX'])`,
  `avg(close['SPX'])`. Name it as an item, and later items use the name:

  ```sql
  SELECT last(close['SPX'])  AS wk_close,
         sum(volume['SPX'])  AS wk_volume,
         avg(wk_close, 4)    AS ma_4w,
         sum(wk_volume, 4)   AS volume_4w
  FROM   prices ON date BY symbol
  SAMPLE BY 1w
  ```

- **A line used bare under `SAMPLE BY` is refused**: "how is volume taken each week? write sum(volume['SPX']) AS
  wk_volume, then use wk_volume". Nothing is inferred from a column's name: a weekly volume, a month's P&L or a fund's
  monthly return each need a different aggregate (sum, sum, compounded), and the wrong one gives plausible numbers.
- **An aggregate with no window and no `SAMPLE BY` is refused**: "a sum over what? add SAMPLE BY 1w for each week's
  total, or write sum(volume['SPX'], 20) for the last 20, or sum(volume['SPX'], all) for the total to date."
- **A period has one date for every line**: its last date on which any line of the query has data (a week's Friday,
  or its Thursday in a holiday week). Two instruments whose last trades in a month fall on different days still share
  the month's row.
- **A period with no data for a line gives that line no value there**, not 0 (`coalesce(wk_volume, 0)` where a 0 is
  wanted). A period in which no line has data is not a row: under `SAMPLE BY 1d`, a holiday is no period.
- A window then counts periods: `avg(wk_close, 4)` is the average of the last 4 weekly closes.

### R6. Lines meeting, and the result's rows

When an expression combines lines (`close['SPX'] - close['QQQ']`, `px.close['SPX'] / fx.rate['EURUSD']`), its dates
are:

- **without `FILL`**: the dates every line in the expression has;
- **with `FILL(…)`**: the dates any line in the expression has, each line given a value where it has none:
  - `FILL(PREV)`: its last value before that date; **none before its first value** (never a later value);
  - `FILL(LINEAR)`: the value on the straight line in time between its values either side; none outside its first and
    last;
  - `FILL(NULL)`: none;
  - `FILL(0)`: 0.

`FILL` is written with or without `SAMPLE BY`: `FROM prices ON date BY symbol FILL(PREV)` fills daily lines.

`FILL` applies only where lines meet in an expression. A function of one line (R4) never reads filled values. A
function of two lines (`correlation`, `beta`, `hedge_ratio`, `residual`, every function of §6's "Two lines") reads
the dates both lines have, whatever `FILL` says, as gs-quant's econometrics do. A `FILL` that changes nothing gets a
warning that says where `FILL` acts: "FILL(LINEAR) changes nothing: no item combines lines with an operator, and
hedge_ratio and residual read the dates both lines have".

**The result has a row for each date** (and instrument, for per-instrument lines) **on which at least one item has a
value.** An item with no value on a row is empty there.

### R7. Names

- Each select item is `expression AS name`. A later item may use an earlier item's name.
- A name may not be a source column's name (`AS close` is refused: "close is a column of prices; call it wk_close").
- Every item is a column of the result; the tile draws the ones it is told to.
- The result always carries the date (named as the first source's date column) and, for per-instrument lines, the
  instrument. They need not be selected.

### R8. Filters: three, each in its own place

| It filters | Written | Example | Sees |
|---|---|---|---|
| the data | `WHERE` inside the source | `FROM (prices WHERE volume > 0) ON date BY symbol` | the source's columns |
| a line | the subscript | `qty['AAPL', side = 'buy']` | the source's columns |
| the answer | `FILTER` at the end | `FILTER date >= '2024-01-01' AND symbol LIKE 'XL%'` | the result's columns: date, instrument, item names |

**WHERE the data, FILTER the answer.**

- `WHERE` removes rows before anything is computed: it changes the numbers. Use it for data every figure must leave
  out: the universe (rank only these nine sectors), bad data, or history a figure may not see (a backtest that starts
  in 2024 with no earlier data). For "performance since a date", rebase at that date instead (R9):
  `rebase(close['AAPL'], '2024-01-01')`.
- `FILTER` removes rows after everything is computed: it only changes what is shown. Use it for the dates and the
  instruments to show: a 20-day average shown from 1 January still uses December's prices.
- **Refused, with the fix in the message:**
  - a `WHERE` after the sources (SQL's usual place): "to filter the data, put the WHERE with its source:
    FROM (prices WHERE …); to filter the answer, use FILTER";
  - `FILTER` on a column the result does not have (`FILTER venue = 'NYSE'`): "venue isn't a column of the result; to
    use only NYSE rows of the data, put WHERE venue = 'NYSE' with the source";
  - a subscript naming an instrument the source's `WHERE` removes (`close['SPX']` over
    `(prices WHERE symbol IN ('QQQ'))`): "SPX's rows are removed by the WHERE; to show only QQQ, FILTER the answer".
    Where that cannot be known until the query runs (`WHERE asset_class = 'equity'`), a subscript that matches no rows
    is an error when it runs: "close['SPX'] has no rows in prices after its WHERE";
  - SQL's aggregate filter, `sum(qty) FILTER (WHERE side = 'buy')`: "write sum(qty['AAPL', side = 'buy'])".

### R9. Dates

- Date literals are strings: `'2024-01-01'`.
- `latest` is **the data's last date**: the last date of any source, after its `WHERE` and before `FILTER`. It is one
  date for the whole query, not each instrument's, and may be used wherever a date may.
- Spans add to and subtract from dates by SQL's own rules: `'2024-03-31' - 1mo` is `'2024-02-29'`.
  `FILTER date > latest - 1y`.
- SQL's date functions take dates: `date_trunc('year', latest)` (the first of `latest`'s year), `year(date)`,
  `quarter(date)`, `month(date)`, `day(date)`, `isodow(date)` (1 Monday to 7 Sunday).
- **"As of" a date** means the last value on or before it. `value_at(x, '2024-01-01')` is x's value as of that date,
  on every date. `lag(x, 1y)` is x's value as of a year before each date. `index(x, '2024-01-01')` divides by the value
  as of 2024-01-01: 2023-12-29's close when the 1st is a holiday, as "performance since" is computed.

### R10. Options and results with several values

- A function's options come after its lines and window, by place or by name with `=>`:
  `returns(close['SPX'], 1, 'log')`, `returns(close['SPX'], type => 'log')`. Option values are strings; the first
  value §6 lists is the default. A bare word is a column, as in SQL; `type = 'log'` is a comparison, not an option.
- Functions with several outputs return a struct: `bollinger_bands(close['SPX'], 20, 2) AS bb` has `bb.lower`,
  `bb.upper` and `bb.width`; `ohlc(o, h, l, c)` has `open`, `high`, `low`, `close`, which the tile draws as candles.

### R11. Kinds of line

Each function says the kind of line it takes: **P**, a price or level (it takes the returns itself); **R**, a return;
or **any**. The compiler refuses the wrong kind: `volatility(returns(x), 3mo)` is refused with "volatility takes
prices and computes their returns: write volatility(x, 3mo)"; `correlation(returns(x), returns(y), 60)` likewise.

- A line read from the data, and a period aggregate of one, is **any** kind; `any` is accepted everywhere.
- `returns(…)` and the two-line statistics of returns give R; `prices(…)`, `index(…)`, `rebase(…)` and `basket(…)`
  give P. `hedge_ratio(…)` (a slope) and `residual(…)` (a spread in price units) regress levels, and give any.
- Arithmetic keeps a kind where every line in it has that kind, numbers aside: `returns(x) - 0.01` is R, `x / y` of two
  P lines is P. Otherwise it is any.

### R12. Units

Every figure is a ratio: a 20% volatility is `0.20`, a 5% return `0.05`, a capture of 100% `1.0`, a percentile rank
between 0 and 1 (SQL's `percent_rank`). The tile writes them as percentages; a query that wants a number in percent
writes `100 * x`. The language converts its own outputs,
never the data's: yields stored in percent stay in percent (`rf.yield['DE3M'] / 100`). RSI and stochastic keep their
conventional 0 to 100 (§12, decision 5).

## 5. Spans

| Written | Means |
|---|---|
| `30d` `2w` `3mo` `1q` `1y` | calendar days, weeks, months, quarters, years |
| `4h` `15min` `30s` | hours, minutes, seconds (intraday data) |
| `m` | **refused**: minutes or months? write `min` or `mo` |

`30d` is 30 calendar days, about 21 trading days. `30`, a bare number, is 30 observations.

## 6. Functions

`x`, `y` are lines; `w` a window (R4); the kind of each line (R11) in brackets; numbers with their defaults
(`obs = 1`); options with their values, the first the default (`type => 'simple' | 'log'`).

**Returns and levels**
- `returns(x [P], obs = 1, type => 'simple' | 'log' | 'absolute')` → R. `obs`: observations, or a span (as of that far
  back). simple: x / x[-obs] − 1; log: ln x − ln x[-obs]; absolute: x − x[-obs].
- `prices(r [R], initial = 1, type => 'simple' | 'log' | 'absolute')` → P: a level compounded from returns.
- `diff(x [any], obs = 1)`: x − x[-obs]. `lag(x [any], obs = 1)`: the value `obs` observations before, or as of a span
  before.
- `index(x [P], date, initial = 1)` → P: x divided by its value as of `date` (the data's first date if left out),
  times `initial`. `rebase(x [P], date, base = 100)`: `index(x, date, 100)`. `change(x [any], date)`: x less its
  value as of `date`. `percent_change(x [P], date)`: x as of `date` to now, a ratio.
- `annualize(x [any])`: times the square root of the periods a year (252 days, 52 weeks, 12 months, 4 quarters).
- `excess_returns(x [P], y)` → P: over a cash index `y` [P], x's first value times the product of
  (1 + x's return − y's); over a rate (a number, 0.05 for 5%), x less the rate accrued actual/360.
- `total_return(price [P], dividends)` → P: dividends a line of amounts on their dates, or a number (a yearly yield).
- `geometrically_aggregate(r [R])` → R: compounded to date, (1 + r₁)(1 + r₂)… − 1.
- `basket(x [P], wx, y [P], wy, …, rebalance => 'daily' | 'weekly' | 'monthly')` → P, from 100, on the dates all have.
- `weighted_sum(x [any], wx, y [any], wy, …)`: (wx·x + wy·y + …) / (wx + wy + …), on the dates all have.

**Along time (each with a window `w`)**
- SQL's aggregates, rolling: `avg(x, w)` (also `mean`), `sum`, `min`, `max`, `count`, `median`, `stddev` (also
  `std`, sample), `variance` (also `var`, sample) — any kind.
- `range_of(x, w)`: max − min. `product(x, w)`. `zscores(x, w)`: (x − mean) / sample deviation over the window.
- `percentiles(x, w)`: x's rank among the window's values, 0 to 1 (equals counted half).
  `percentile(x, q, w)`: the window's q-quantile, q from 0 to 1.
- `volatility(x [P], w, type => 'simple' | 'log')`: the annualized sample deviation of x's returns, a ratio.
- `drawdown(x [P], w)`: x / its peak over the window − 1 (`w = all`: the running peak). `max_drawdown(x [P], w)`: the
  deepest drawdown within the window, measured from peaks inside it (−0.2 for 20%).
- `sharpe_ratio(x [P], w)`: x's annualized mean return over its annualized volatility. For a line of returns, compound
  it first: `sharpe_ratio(prices(r), w)`.
- `vol_swap_volatility(x [P], w)`: the root mean square of log returns, annualized by 252, a ratio.
- `smoothed_moving_average(x, w)` (Wilder's), `rsi(x [P], w = 14)` (0 to 100), `macd(x, short = 12, long = 26,
  signal = 1)`.
- `bollinger_bands(x, w, k = 2)` → struct (`lower`, `upper`, `width` = upper − lower): the window's mean ± k sample
  deviations.
- `atr(high, low, close, w = 14)`, `stochastic(high, low, close, w = 14)` (0 to 100),
  `keltner_channels(high, low, close, w = 20, a = 10, k = 2)` → struct (`lower`, `upper`, `width`),
  `donchian_channels(high, low, w)` → struct (`lower`, `upper`, `width`), `vwap(price, volume, w)`: Σ price × volume /
  Σ volume over the window, none where nothing traded.
- `dickey_fuller(x, w)`, `half_life(x, w)`.

**From each line's first value (no window)**
- `exponential_moving_average(x, beta = 0.75)`: y₀ = x₀, y = beta·y[-1] + (1 − beta)·x.
  `exponential_std(x, beta = 0.75)`, `exponential_volatility(x [P], beta = 0.75)` (a ratio),
  `exponential_spread_volatility(x, beta = 0.75)` (in x's units).
- `obv(close, volume)`, `consecutive(x, direction => 'positive' | 'negative')`.
- `smooth_spikes(x, threshold = 0.5, type => 'percentage' | 'absolute')`.
- `trend(x, model => 'additive' | 'multiplicative', cycle => 'year' | 'quarter' | 'month' | 'week')` and
  `seasonally_adjusted(x, model, cycle)`: a classical decomposition over one cycle of the query's period (a year of
  months is 12).

**A whole line, on every date**
- `first_of(x)`, `last_of(x)`: the line's first and last values. `winsorize(x, limit = 2.5)`: held within `limit`
  sample deviations of the line's mean.

**Two lines (along time, each with a window `w`; both lines P; of their simple returns unless `type => 'levels'` (the
lines as they are) or `'changes'` (their differences, x − x[-1]: for yields and spreads))**
- `correlation(x, y, w)`, `cov(x, y, w)`, `beta(x, y, w)`, `r_squared(x, y, w)`, `alpha(x, y, w)` (x's mean return
  less beta times y's, annualized: times the periods a year, as gs-quant's).
- `tracking_error(x, b, w)` (annualized, a ratio), `information_ratio(x, b, w)`, `up_capture(x, b, w)`,
  `down_capture(x, b, w)` (ratios: 1.0 matched the benchmark), `lead_lag(x, y, lag = 1, w)` (x's returns against y's
  `lag` observations before).
- `hedge_ratio(x, y, w)` and `residual(x, y, w)` regress **the levels themselves**: the slope of x on y (with an
  intercept), and x less the regression's value: a pairs trade's hedge and spread.

**Combining and choosing**
- `if(condition, x, y)`, `CASE WHEN … THEN … ELSE … END`, `NOT`, `AND`, `OR`, `= <> < <= > >=`, `coalesce(x, y)`.
- `compare(x, y)`: 1, −1 or 0. `union_of(x, y)`: every date either has, x where it has a value, else y.
- `append(x, y)`: x to its last date, then y. `prepend(x, y)`: x before y's first date, then y.
- `interpolate(x, y, method => 'intersect' | 'nan' | 'zero' | 'step')`: x on y's dates.
- `filter_values(x, operator => 'empty' | 'less_than' | 'greater_than' | 'l_equals' | 'g_equals' | 'equals' |
  'not_equals', value = 0)`: x without the values the operator matches; later functions read the dates left.

**Per point:** `ln`, `exp`, `sqrt`, `abs`, `power(x, y)`, `greatest(x, v)`, `least(x, v)`, `round(x, n)`.

**Within a period (no window, under `SAMPLE BY`):** `first`, `last`, `avg`, `sum`, `min`, `max`, `count`, `median`.

**Across instruments, on each date:** `rank(x [DESC])`: 1 the first in that order; ties share a rank (SQL's `rank`);
an instrument with no value is not ranked.

**Dates:** `latest`, `value_at(x, date)`, `date_trunc(unit, date)`, `year`, `quarter`, `month`, `day`, `isodow`.

## 7. What differs from today's formula box

| Today (gs-quant) | Here | Why |
|---|---|---|
| `log(x)` (natural) | `ln(x)` | SQL's `log` is base 10 |
| `floor(x, 0)`, `ceil(x, 1)` (bounds) | `greatest(x, 0)`, `least(x, 1)` | SQL's `floor`/`ceil` round |
| `first(x)`, `last(x)` (the whole line's) | `first_of(x)`, `last_of(x)` | SQL's `first`/`last` are aggregates, and its `last_value` is the current row |
| `sum(x)` (running), `count(x)` (to date) | `sum(x, all)`, `count(x, all)` | a bare aggregate is the period's |
| a default window (`mean(x)` is 22 observations) | the window is always written | a missing window is the period |
| `w = 0` (from the start) | `all` | `0` reads as an empty window |
| `drawdown(x)` (from the running peak) | `drawdown(x, all)` | the window is written |
| `range`, `union`, `filter` | `range_of`, `union_of`, `filter_values` | SQL keywords |
| `not(x)` | `NOT x` | SQL |
| `==` | `=` | SQL |
| `close[SPX]` | `close['SPX']` | a bare word is a column |
| `close[asset_class=Equity]` (no instrument) | `(prices WHERE asset_class = 'Equity')` | a subscript picks an instrument |
| `3m` | `3mo` | `m` is minutes elsewhere |
| `type=log` | `type => 'log'` | Postgres's named arguments; options are strings |
| `volume[AAPL].sum` | `sum(volume['AAPL'])` under `SAMPLE BY` | SQL's aggregate |
| a line under a grain is its period's last value | the aggregate is written (R5) | nothing inferred |
| `$A` | the item's name | SQL's names |
| `index`, `rebase`, `change`, `percent_change` from the first date shown | from the data's first date, or a date given | `FILTER` never changes a number |
| `weekday(x)` (0 Monday) | `isodow(date)` (1 Monday) | SQL |
| `year(close[GLD])` | `year(date)` | a date part takes the date |
| `percentile(x, 50, w)` | `percentile(x, 0.5, w)` | ratios (R12) |
| volatility 20 (percent), capture 100, percentiles 0 to 100 | 0.20, 1.0, 0 to 1 | ratios (R12) |
| `cov(x, y, w)` of the lines as they are | of their returns; `type => 'levels'` as before | one rule for two lines |
| `FILL(PREV)`-like `step`: before a line's first value, its first | none before its first value | no value from the future |

The tile and Python keep reading today's formulas, and write them in the new form: under a grain, the tile wraps each
line in its column's aggregate from the grid's settings, so existing tiles and notebooks keep their numbers (§12,
decision 7). Likewise an `index`, `rebase`, `change` or `percent_change` written with no date is written with the first
date of the range shown, `rebase(x, '<the range's first date>')`, as `compare` is (§8): the base still moves with the
range, as it does today (the user, 2026-10-30). With no range, that date is the data's first, so the form is the same.

## 8. From today's tile and Python

| Today | Here |
|---|---|
| sources: alias, grid, date, key | `FROM <the grid's query> px ON date BY symbol` (DataCube passes the grid's query, its filter, a frozen tile's kept filter) |
| a series' formula | a select item |
| `name=` | `AS name` |
| `grain` | `SAMPLE BY 1d / 1w / 1mo / 1q / 1y`, each line wrapped in its column's aggregate |
| `align`: intersect, step, time, nan, zero | none, `FILL(PREV)`, `FILL(LINEAR)`, `FILL(NULL)`, `FILL(0)` |
| `range` | `FILTER date BETWEEN … AND …`, `FILTER date > latest - 1y`, `FILTER date >= date_trunc('year', latest)` |
| `split=` (the instrument) | the line without a subscript |
| `compare`: rebase, percent | `rebase(x, '<the range's first date>')`, `percent_change(x, '<…>')` |
| panes, axes, styles, legend, notes, drawings, replay, the point limit, hidden series | the drawing: stays in the tile spec |

Python's API is unchanged: `page.timeseries(...)`, `plot`, `edit`, `swap`, `table()`, `query()` keep their keywords,
each mapping as above. Optionally `page.query("SELECT …")` returns a DataFrame.

**The tile's other views** are plain SQL over a series query, which `WITH` names like any CTE:

```sql
WITH s AS (SELECT returns(close['SPX']) AS spx, returns(close['QQQ']) AS qqq
           FROM prices ON date BY symbol FILTER date > latest - 1y)
SELECT regr_slope(qqq, spx), regr_intercept(qqq, spx), regr_r2(qqq, spx), corr(qqq, spx) FROM s
```

A scatter's regression as above; a histogram with `width_bucket`; seasonality and calendar returns with `GROUP BY`
year and month; the statistics table and the measure tool with ordered aggregates; a correlation matrix with `corr`
per pair; the drawdowns table and a projection with table functions in `FROM`; marks and shading as queries over those
grids' rows.

## 9. What the compiler checks

**Refused** (with the fix in the message):
- a `WHERE` after the sources; a `FILTER` on a source-only column; a subscript naming an instrument the source's
  `WHERE` removes; SQL's aggregate `FILTER (WHERE …)` (R8);
- a line used bare under `SAMPLE BY`; an aggregate with no window and no `SAMPLE BY` (R5);
- a line of the wrong kind (R11);
- when it runs: a subscript that matches no rows (R8);
- an item named like a source column (R7);
- `m` as a unit (§5).

**Warned:**
- a `FILL` that changes nothing, saying where `FILL` acts (R6);
- an item whose warm-up is longer than the data, or a source's `WHERE` that starts the data inside an item's warm-up
  (R4);
- `index`, `rebase`, `change` or `percent_change` with no date in a query that `FILTER`s its dates: "these start from
  the data's first date, not the first date shown; give the date: index(x, '…')".

**Reported:** each item's warm-up (R4).

## 10. Examples

### Easy

```sql
-- a moving-average crossover, the last two years shown
SELECT close['SPX']           AS spx,
       avg(close['SPX'], 50)  AS ma50,
       avg(close['SPX'], 200) AS ma200,
       ma50 - ma200           AS gap,
       ma50 > ma200           AS golden_cross
FROM   prices ON date BY symbol
FILTER date > latest - 2y
```

```sql
-- AAPL's weekly bars, with the week's range and change
SELECT first(open['AAPL'])     AS wk_open,
       max(high['AAPL'])       AS wk_high,
       min(low['AAPL'])        AS wk_low,
       last(close['AAPL'])     AS wk_close,
       sum(volume['AAPL'])     AS wk_volume,
       wk_high - wk_low        AS wk_range,
       wk_close / wk_open - 1  AS wk_change
FROM   prices ON date BY symbol
SAMPLE BY 1w
```

```sql
-- every instrument's 3-month volatility, the last year shown
SELECT volatility(close, 3mo) AS vol
FROM   prices ON date BY symbol
FILTER date > latest - 1y
```

### Medium

```sql
-- the yield curve: two spreads in basis points, a month's change, and an inversion flag
SELECT 100 * (yield['10Y'] - yield['2Y']) AS s2s10_bp,
       100 * (yield['30Y'] - yield['5Y']) AS s5s30_bp,
       diff(s2s10_bp, 1mo)                AS s2s10_1m_chg,
       s2s10_bp < 0                       AS inverted
FROM   rates ON date BY tenor
FILTER date > latest - 5y
```

```sql
-- sector relative strength: each against SPX, its 3-month momentum, above its own 50-day trend?
SELECT close / close['SPX']    AS rel,
       returns(rel, 3mo)       AS rel_mom_3m,
       avg(rel, 50)            AS rel_ma50,
       rel > rel_ma50          AS outperforming
FROM   prices ON date BY symbol
FILTER symbol LIKE 'XL%' AND date > latest - 1y
```

```sql
-- order flow from individual trades: each day's buying, selling, imbalance and VWAP, dark trades left out
SELECT sum(qty['AAPL', side = 'buy'])                      AS bought,
       sum(qty['AAPL', side = 'sell'])                     AS sold,
       (bought - sold) / (bought + sold)                   AS imbalance,
       sum(price['AAPL'] * qty['AAPL']) / sum(qty['AAPL']) AS day_vwap,
       avg(imbalance, 20)                                  AS imbalance_20d
FROM   (trades WHERE venue <> 'DARK') ON trade_date BY ticker
SAMPLE BY 1d
```

```sql
-- inflation: CPI year on year, and its last 3 months annualized
SELECT returns(value['CPI'], 1y)                    AS yoy,
       power(1 + returns(value['CPI'], 3mo), 4) - 1 AS three_month_annualized
FROM   macro ON date BY series
```

### Complex

```sql
-- a pairs trade, KO against PEP, with its P&L since 2023
SELECT hedge_ratio(close['KO'], close['PEP'], 6mo)        AS hedge,
       residual(close['KO'], close['PEP'], 6mo)           AS spread,
       zscores(spread, 3mo)                               AS z,
       CASE WHEN z >  2 THEN -1
            WHEN z < -2 THEN  1
            ELSE 0 END                                    AS position,
       lag(position, 1) * (diff(close['KO'], 1) - lag(hedge, 1) * diff(close['PEP'], 1)) AS daily_pnl,
       sum(daily_pnl, '2023-01-01')                       AS pnl_since_2023
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'
```
The spread is the regression's own residual (its intercept included). Yesterday's position, hedged with yesterday's
ratio, times today's moves is the day's P&L. The compiler reports: z first after 9 months; position and daily_pnl a day
later.

```sql
-- risk-adjusted momentum: each month, the equities ranked by 12-1 momentum per unit of volatility; the top three
SELECT last(close)                               AS mo_close,
       lag(mo_close, 1) / lag(mo_close, 12) - 1  AS mom_12_1,
       volatility(mo_close, 12)                  AS vol_1y,
       mom_12_1 / vol_1y                         AS risk_adj,
       rank(risk_adj DESC)                       AS rnk
FROM   (prices WHERE asset_class = 'equity') ON date BY symbol
SAMPLE BY 1mo
FILTER rnk <= 3 AND date > latest - 2y
```

```sql
-- a 60/40 portfolio held in euros: weekly excess return over German 3-month rates, rolling Sharpe, worst drawdown
SELECT last(px.close['SPX'])                 AS spx,
       last(px.close['TLT'])                 AS tlt,
       last(fx.rate['EURUSD'])               AS eurusd,
       last(rf.yield['DE3M']) / 100          AS de3m,
       basket(spx, 0.6, tlt, 0.4, rebalance => 'monthly') / eurusd AS eur_6040,
       returns(eur_6040) - de3m / 52         AS excess,
       sharpe_ratio(prices(excess), 1y)      AS sharpe_1y,
       max_drawdown(eur_6040, 1y)            AS mdd_1y
FROM   prices px ON date BY symbol,
       fx     fx ON date BY pair,
       yields rf ON date BY tenor
SAMPLE BY 1w FILL(PREV)
FILTER date > latest - 3y
```

```sql
-- a snapshot table: every equity's 1-year beta to SPX and its idiosyncratic volatility, as of the latest date
SELECT beta(close, close['SPX'], 1y)                                          AS beta,
       volatility(close, 3mo) * sqrt(1 - r_squared(close, close['SPX'], 3mo)) AS idio_vol
FROM   (prices WHERE asset_class = 'equity' OR symbol = 'SPX') ON date BY symbol
FILTER symbol <> 'SPX' AND date = latest
```

## 11. Out of scope

How a result is drawn: panes, axes, log scale, styles, legend, notes, hand-drawn lines, levels, events, replay, the
point limit. These stay in the tile spec, referring to the select items by name.

## 12. Open decisions

1. The island's name in Pure: `#Quant{…}#` is a placeholder.
2. The comma between sources reads as SQL's cross join and is not one (R1). A different separator?
3. Aggregates across instruments (the market's total volume each date): names of their own, beside `rank`. Until
   then, a plain-SQL subquery as the source.
4. A split by a column that is not the instrument (today's `split=` on another text column).
5. RSI and stochastic at 0 to 100 (recommended: their scale is part of their definition) or as ratios like
   everything else.
6. Declaring a column's period aggregate once on the data (BI's semi-additive measures; a Pure profile on a class's
   properties), so `SAMPLE BY` needs no wrapping for such a source. Later, if wanted.
7. Today's spellings (§7): read forever, or read with a deprecation warning until a date.
8. The renames `range_of`, `union_of`, `filter_values`, `first_of`, `last_of` are provisional.
9. SQL's own window syntax (`OVER (…)`) as an escape hatch: not in the first version.
10. Vetted functions for recipes that are easy to get wrong (a pairs trade's P&L, hedged returns, a portfolio's P&L).
11. A rolling value starts one observation earlier than gs-quant's ramp (already recorded); a `ramp =>` option if
    anyone asks for partial windows.

## 13. Tested before it is built

1. **Writing.** An AI model given only this document turns 30 plain-English requests (10 easy, 10 medium, 10 hard)
   into queries. Each is judged against an answer written beforehand and held back: right, wrong, or the spec
   ambiguous.
2. **Reading.** The model explains 10 queries in plain English, judged against what each was written to mean: where
   it misreads `WHERE`, `FILTER`, `FILL`, subscripts, windows or periods.
3. **Coverage.** Every formula of the timeseries gallery (`datacube/src/examples/timeseries-gallery.ts`) written in
   this language: anything inexpressible, or expressible only awkwardly.

A rule that models get wrong more than once is changed before the spec is built, and the test is run again.

**First run (version 1, 2026-10-29):** the same model that wrote the spec, reading carefully. 29 of 30 written right
(the 30th, total volume across equities, correctly reported as open decision 3); 10 of 10 read right; 69 gallery tiles
translated, 5 not expressible. It found the traps and gaps that version 2 fixes (§14). A careful reader avoided the
traps; the second run uses smaller models, a better stand-in for a quick reader.

**Second run (version 2, 2026-10-29):** Sonnet and Haiku, the 30 requests unchanged and 13 readings (three of them
queries the spec refuses). Sonnet: 30 of 30 written right, 13 of 13 read right, every refusal found with its fix.
Haiku: 30 of 30 written right (two with a needless `SAMPLE BY 1d`, unsure whether `FILL` stands alone), 12 of 13 read
right, one called "not covered" (the same question). Request 27 was again reported as open decision 3, each writing it
with a plain-SQL source. Their findings are version 3's changes (§14). The next run waits for the quants' review.

## 14. What changed

### Version 3.1 (2026-10-30, from writing the guide for quants)

- `alpha` is annualized, as gs-quant's and the library's are (§6; the short definition had left it out).
- Today's `index`, `rebase`, `change` and `percent_change` with no date keep their numbers: the tile and Python write
  them with the first date of the range shown (§7), as `compare` is written (§8).

### Version 3 (from the second test)

- A period has one date for every line: its last date with any line's data (R5). Dated per line, two instruments
  whose last trades in a month differed never met in a `rank`.
- Window boundaries stated: a span excludes the date that far back, a date window includes its date (R4).
- `latest` is the data's last date, before `FILTER` (R9): the result's last date was circular.
- "Performance since a date" rebases at that date (R8, R9); `WHERE` is for data every figure must leave out. The two
  gave different bases on a holiday.
- `hedge_ratio` and `residual` regress levels and give any kind of line, not returns (R11).
- The pairs example's spread is `residual(…)`, the regression's own residual (it dropped the intercept).
- Stated: `FILL` stands alone, without `SAMPLE BY` (R6); a price function's window counts returns (R4); under
  `SAMPLE BY` a line appears only inside a period aggregate (R5); an empty period gives no value, not 0, and a period
  with no data is no row (R5); a percent is `100 * x` (R12); an option's first value is its default (R10); a source
  without `BY` has bare lines (R1); spans follow SQL's date arithmetic (R9); `vwap` and `max_drawdown` defined; what
  `'changes'` means (§6).
- New warning: a source's `WHERE` that starts the data inside an item's warm-up (R4). The idle-`FILL` warning says
  where `FILL` acts.

### Version 2 (from the first test and the user's decisions)

From the first test:
- `FILL(PREV)` no longer fills before a line's first value (it used a later value).
- Under `SAMPLE BY`, a line says its period aggregate; a bare line is refused (`sum(volume['SPX'], 4)` added four
  Fridays' volumes).
- The kinds of line (R11): a function given returns where it takes prices is refused (`correlation(returns(…), …)`
  correlated returns of returns). Two-line statistics are of returns unless `type =>` says otherwise, `cov` included.
- `first_value` and `last_value` renamed `first_of` and `last_of` (SQL's `last_value` is the current row).
- Defined: the result's rows and a combined expression's dates (R6); "as of" (R9); when a calendar window is full
  (R4); how periods are labelled and where a week starts (R5); `latest`'s scope (R9); `rank`'s ties and missing
  values (§6); option values and the signatures that were missing (§6); how a series query is a CTE (§8).
- New checks: a `FILL` that changes nothing; a subscript the source's `WHERE` empties; an item named like a column.
- §7 completed with every change of meaning.

From the user's decisions:
- Every figure a ratio (R12).
- A date is a window: `sum(pnl, '2023-01-01')` replaces `from =>`.
- Warm-up reported by the compiler, and `FILTER ready` (R4).
- Corrected examples: a pairs P&L that uses yesterday's hedge; a Sharpe ratio of excess returns through `prices(…)`.
