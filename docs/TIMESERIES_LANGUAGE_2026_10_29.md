# A query language for timeseries (2026-10-29)

**Status: design, not built.** Step C of docs/TIMESERIES_ON_THE_PLATFORM_2026_10_28.md, worked out with the user on
2026-10-28 and 2026-10-29. It replaces that note's `#Quant` sketch (its D1-D3). Before anything is built, it is tested
as §13 says.

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
5. **Everything that changes a number is written in the query.** The period, the window, the aggregate, the fill.
   `FILTER` never changes a number.
6. **Mistakes are compile errors that say the fix,** never quietly different numbers.

## 3. A query

```sql
SELECT volatility(close['SPX'], 63)           AS spx_vol,
       spx_vol - volatility(close['QQQ'], 63) AS spread
FROM   prices ON date BY symbol
SAMPLE BY 1w FILL(PREV)
FILTER date >= '2024-01-01'
```

SPX's 63-week volatility, and its spread over QQQ's, a row per week, a week either lacks carried from its last value,
shown from 2024 (computed with every week before).

Clause order: `SELECT … FROM … SAMPLE BY … FILL(…) FILTER …`. Only `SELECT` and `FROM` are required.

## 4. The rules

### R1. Sources

`FROM <source> [alias] ON <date column> [BY <instrument column>]`

- A source is a table, `(table WHERE condition)`, or any SQL subquery: `(SELECT *, price * qty AS value FROM trades)`.
  `(prices WHERE venue = 'NYSE')` means exactly `(SELECT * FROM prices WHERE venue = 'NYSE')`.
- `ON` names the date column; `BY` the column naming each row's instrument (a symbol, a tenor, a series). `BY` is left
  out where the source has one instrument.
- Several sources are separated by commas, each with its own `ON` and `BY` and an alias:
  `FROM prices px ON date BY symbol, rates rf ON date BY tenor`. **The comma is not a join**: each source's lines are
  picked first (R2), and lines meet on the date axis (R6).

### R2. Lines

A line is one instrument's column along its dates.

- `close['SPX']`: SPX's close. `close['SPX', venue = 'NYSE']`: SPX's close from the NYSE rows only (more matches,
  each `column = value`, after the instrument).
- `close`, with no subscript: one line per instrument. The result then carries the instrument column, a row per date
  per instrument.
- A single line **broadcasts** against per-instrument lines: `close / close['SPX']` is each instrument relative to SPX.
- With several sources, a line names its source: `px.close['SPX']`. With one, the alias is optional.

### R3. Four kinds of function

| Kind | Example | Recognised by |
|---|---|---|
| per point | `ln(x)`, `x / y`, `greatest(x, 0)` | no window, no period |
| along time | `avg(x, 20)`, `volatility(x, 3mo)`, `sum(x, all)` | a window argument (R4) |
| within a period | `sum(x)`, `last(x)` under `SAMPLE BY` | an aggregate with no window (R5) |
| across instruments | `rank(x DESC)` | its own names (§6) |

Functions nest: `zscores(volatility(close['SPX'], 3mo), 1y)`.

### R4. Windows

A function along time takes its window as an argument, counted as a person counts it:

- **a number: that many observations**, the current one included. `avg(close['SPX'], 20)` is the 20-day average on
  daily data (20 trading days), the 20-week one under `SAMPLE BY 1w`.
- **a calendar span** (§5): `3mo`, `1y`, `30d`. `max(close['SPX'], 1y)` is the highest close in the last year.
- **`all`: everything to date.** `sum(pnl['BOOK1'], all)` is a running total.

A rolling value first appears **once its window is full**: a 20-day average on the 20th day, none before.

Every function runs **over its line's own values**: `avg(close['SPX'], 20)` averages SPX's last 20 observations, never
dates filled in from another line (R6).

### R5. Periods

`SAMPLE BY <span>` gives one row per period: `SAMPLE BY 1d`, `1w`, `1mo`, `1q`, `1y`.

- A bare line under `SAMPLE BY` is **the period's last value**: a weekly close is the week's last close.
- An aggregate with no window aggregates **within the period**: `sum(volume['SPX'])` is the week's total,
  `first(open['SPX'])` its first open, `max(high['SPX'])` its highest high.
- A window then counts periods: `avg(close['SPX'], 4)` under `SAMPLE BY 1w` is the average of the last 4 weekly closes.
  An aggregate inside a rolling function says what each period's value is: `sum(sum(volume['SPX']), 4)` is the total
  of the last 4 weeks' volumes.
- **An aggregate with no window and no `SAMPLE BY` is refused**: "a sum over what? add SAMPLE BY 1w for each week's
  total, or write sum(volume['SPX'], 20) for the last 20, or sum(volume['SPX'], all) for the total to date."

### R6. Lines meeting

When an expression combines lines (`close['SPX'] - close['QQQ']`, `px.close['SPX'] / fx.rate['EURUSD']`), they meet on
the dates of the axis:

- **Without `FILL`**, a date is kept only where every line in the expression has a value.
- **`FILL(PREV)`**: a line takes its last value on a date it lacks (before its first value, its first).
- **`FILL(LINEAR)`**: a line takes the value on the straight line in time between its values either side (none
  outside its first and last).
- **`FILL(NULL)`**: every date any line has, none where a line has none.
- **`FILL(0)`**: the same, 0 where a line has none.

`FILL` applies only where lines meet in an expression. A function of one line (R4) never reads filled values, and a
function of two lines (`correlation`, `beta`, `hedge_ratio`, `residual`, …) reads the dates both have, whatever `FILL`
says (gs-quant's econometrics do the same).

### R7. Names and the result

- Each select item is `expression AS name`. A later item may use an earlier item's name: `spx_vol - …`.
- The result always carries the date (named as the first source's date column) and, for per-instrument lines, the
  instrument. They need not be selected.

### R8. Filters: three, each in its own place

| It filters | Written | Example | Sees |
|---|---|---|---|
| the data | `WHERE` inside the source | `FROM (prices WHERE volume > 0) ON date BY symbol` | the source's columns |
| a line | the subscript | `qty['AAPL', side = 'buy']` | the source's columns |
| the answer | `FILTER` at the end | `FILTER date >= '2024-01-01' AND symbol LIKE 'XL%'` | the result's columns: date, instrument, item names |

**WHERE the data, FILTER the answer.**

- `WHERE` removes rows before anything is computed: it changes the numbers. Use it for "since" questions (performance
  since 2024), for the universe (rank only these nine sectors), and for bad data.
- `FILTER` removes rows after everything is computed: it only changes what is shown. Use it for the dates and the
  instruments to show: a 20-day average shown from 1 January still uses December's prices.
- **Refused, with the fix in the message:**
  - a `WHERE` after the sources (SQL's usual place): "to filter the data, put the WHERE with its source:
    FROM (prices WHERE …); to filter the answer, use FILTER";
  - `FILTER` on a column the result does not have (`FILTER venue = 'NYSE'`): "venue isn't a column of the result; to
    use only NYSE rows of the data, put WHERE venue = 'NYSE' with the source";
  - SQL's aggregate filter, `sum(qty) FILTER (WHERE side = 'buy')`: "write sum(qty['AAPL', side = 'buy'])".

### R9. Dates

- Date literals are strings: `'2024-01-01'`.
- `latest` is the result's last date. Spans add to and subtract from dates: `FILTER date > latest - 1y`.
- Date parts take the date: `year(date)`, `quarter(date)`, `month(date)`, `day(date)`, `isodow(date)` (1 Monday to
  7 Sunday), as SQL has them.

### R10. Options and results with several values

- A function's options are string literals by place, or by name with `=>`: `returns(close['SPX'], 1, 'log')`,
  `returns(close['SPX'], type => 'log')`. A bare word is a column, as in SQL.
- Functions with several outputs return a struct: `bollinger_bands(close['SPX'], 20, 2) AS bb` has `bb.lower`,
  `bb.upper` and `bb.width`; `ohlc(o, h, l, c)` has `open`, `high`, `low`, `close`, which the tile draws as candles.

## 5. Units

| Written | Means |
|---|---|
| `20` | 20 observations |
| `30d` `2w` `3mo` `1q` `1y` | calendar days, weeks, months, quarters, years |
| `4h` `15min` `30s` | hours, minutes, seconds (intraday data) |
| `m` | **refused**: minutes or months? write `min` or `mo` |
| `all` | everything to date |

`30d` is 30 calendar days, about 21 trading days. `30` is 30 observations.

## 6. Functions

`x`, `y` are lines (or expressions of them); `w` a window (R4); options in quotes.

**Returns and levels**
- `returns(x, obs = 1, type = 'simple')`: over `obs` observations or a span; type `simple` (x / x[-obs] - 1), `log`
  (ln x - ln x[-obs]), `absolute` (x - x[-obs]).
- `prices(r, initial = 1, type = 'simple')`: a level compounded from returns.
- `diff(x, obs = 1)`, `lag(x, obs = 1)`: the change over, and the value, `obs` observations (or a span) before.
- `index(x, initial = 1, from => date)`, `rebase(x, base = 100, from => date)`, `change(x, from => date)`,
  `percent_change(x, from => date)`: against the value as of `from`; without `from`, as of the data's first date.
- `annualize(x)`: times the square root of the periods a year (252 days, 52 weeks, 12 months, 4 quarters).
- `excess_returns(x, y)`: over a cash index line, or a rate (a number: 0.05 is 5%, accrued actual/360).
- `total_return(price, dividends)`, `geometrically_aggregate(r)`, `basket(x, wx, y, wy, …, rebalance => 'daily')`,
  `weighted_sum(x, wx, y, wy, …)`.

**Along time (each with a window `w`)**
- SQL's aggregates: `avg(x, w)` (also `mean`), `sum`, `min`, `max`, `count`, `median`, `stddev` (also `std`),
  `variance` (also `var`).
- `range_of(x, w)`, `product(x, w)`, `zscores(x, w)`, `percentiles(x, w)`, `percentile(x, n, w)`,
  `winsorize(x, limit = 2.5)`.
- `volatility(x, w, type => 'simple')`: the annualized standard deviation of x's returns, in percent (x a price).
- `drawdown(x, w)`, `max_drawdown(x, w)`, `sharpe_ratio(r, w)` (r returns), `vol_swap_volatility(x, w)`.
- `exponential_moving_average(x, beta = 0.75)`, `exponential_std`, `exponential_volatility`,
  `exponential_spread_volatility`, `smoothed_moving_average(x, w)`, `macd(x, 12, 26, 1)`, `rsi(x, 14)`.
- `bollinger_bands(x, w, k = 2)`, `keltner_channels(high, low, close, w, a, k)`, `donchian_channels(high, low, w)`:
  structs.
- `atr(high, low, close, w)`, `stochastic(high, low, close, w)`, `obv(close, volume)`, `vwap(price, volume, w)`.
- `dickey_fuller(x, w)`, `half_life(x, w)`, `consecutive(x, direction => 'positive')`,
  `smooth_spikes(x, threshold, type)`, `trend(x, model, cycle)`, `seasonally_adjusted(x, model, cycle)`.

**Two lines, along time**
- `correlation(x, y, w)`, `beta(x, y, w)`, `r_squared(x, y, w)`, `alpha(x, y, w)`: of simple returns.
- `cov(x, y, w)`: of the lines as they are.
- `hedge_ratio(x, y, w)`: the slope of x's levels on y's (how many y hedge one x). `residual(x, y, w)`: x less what
  that regression gives: a pairs trade's spread, in price units.
- `tracking_error`, `information_ratio`, `up_capture`, `down_capture` (x, benchmark, w); `lead_lag(x, y, lag, w)`.

**Combining and choosing**
- `if(condition, x, y)`, `CASE WHEN … THEN … ELSE … END`, `NOT`, `AND`, `OR`, `= <> < <= > >=`.
- `compare(x, y)`: 1, -1 or 0. `coalesce(x, y)`. `union_of(x, y)`: every date either has, x where it has a value.
- `append(x, y)`, `prepend(x, y)`, `interpolate(x, y, method)`, `filter_values(x, operator, value)`.

**Per point:** `ln`, `exp`, `sqrt`, `abs`, `power(x, y)`, `greatest(x, v)`, `least(x, v)`, `round(x, n)`.

**Within a period (no window, under SAMPLE BY):** `first`, `last`, `avg`, `sum`, `min`, `max`, `count`.

**A whole line, on every date:** `first_value(x)`, `last_value(x)`.

**Across instruments, on each date:** `rank(x [DESC])` (1 the first in that order).

## 7. Names that differ from today's formula box

| Today (gs-quant) | Here | Why |
|---|---|---|
| `log(x)` (natural) | `ln(x)` | SQL's `log` is base 10 |
| `floor(x, 0)`, `ceil(x, 1)` (bounds) | `greatest(x, 0)`, `least(x, 1)` | SQL's `floor`/`ceil` round |
| `first(x)`, `last(x)` (whole line) | `first_value(x)`, `last_value(x)` | SQL's `first`/`last` are aggregates |
| `sum(x)` (running), `count(x)` (to date) | `sum(x, all)`, `count(x, all)` | bare SQL aggregates are the period's |
| `mean(x)` with a default window | `avg(x, w)`: a window is always written | a missing window is the period |
| `w = 0` (from the start) | `all` | `0` reads as an empty window |
| `range`, `union`, `filter` | `range_of`, `union_of`, `filter_values` | SQL keywords |
| `close[SPX]` | `close['SPX']` | a bare word is a column |
| `3m` | `3mo` | `m` is minutes elsewhere |
| `type=log` | `type => 'log'` | Postgres's named arguments |
| `==` | `=` | SQL |
| `volume[AAPL].sum` | `sum(volume['AAPL'])` under `SAMPLE BY` | SQL's aggregate |
| `$A` | the item's name | SQL's names |

The tile and Python keep reading the old spellings and write the new ones (§12, decision 7).

## 8. From today's tile and Python

| Today | Here |
|---|---|
| sources: alias, grid, date, key | `FROM <the grid's query> px ON date BY symbol` (DataCube passes the grid's query, its filter, a frozen tile's kept filter) |
| a series' formula | a select item |
| `name=` | `AS name` |
| `grain` | `SAMPLE BY 1d / 1w / 1mo / 1q / 1y` |
| `align`: intersect, step, time, nan, zero | none, `FILL(PREV)`, `FILL(LINEAR)`, `FILL(NULL)`, `FILL(0)` |
| `range` | `FILTER date BETWEEN … AND …`, or `FILTER date > latest - 1y` |
| `split=` (the instrument) | the line without a subscript |
| `compare`: rebase, percent | `rebase(x, from => …)`, `percent_change(x, from => …)` |
| panes, axes, styles, legend, notes, drawings, replay, the point limit | the drawing: stays in the tile spec |

Python's API is unchanged: `page.timeseries(...)`, `plot`, `edit`, `swap`, `table()`, `query()` keep their keywords,
each mapping as above. Optionally `page.query("SELECT …")` returns a DataFrame.

The tile's other views are queries over a series query (used as a CTE), in plain SQL: a scatter's regression
(`regr_slope`, `regr_intercept`, `regr_r2`, `corr`), a histogram (`width_bucket`), seasonality and calendar returns
(`GROUP BY` year and month), the statistics table and the measure tool (ordered aggregates), a correlation matrix
(`corr` per pair), the drawdowns table and a projection (table functions in `FROM`), marks and shading (queries over
those grids' rows).

## 9. What the compiler checks

- A query-level `WHERE`; a `FILTER` on a source-only column; SQL's aggregate `FILTER (WHERE …)` (R8).
- An aggregate with no window and no `SAMPLE BY` (R5).
- `m` as a unit (§5).
- A function given the wrong kind of line: `volatility` of returns, `sharpe_ratio` of prices (proposed, §12).
- `index`, `rebase`, `change`, `percent_change` without `from` in a query that `FILTER`s its dates: a warning, "these
  start from the data's first date, not the first date shown; add from => '…'".

## 10. Examples

### Easy

```sql
-- SPX's 50-day moving average
SELECT avg(close['SPX'], 50) AS ma50
FROM   prices ON date BY symbol
```

```sql
-- AAPL's weekly bars
SELECT first(open['AAPL'])  AS open,
       max(high['AAPL'])    AS high,
       min(low['AAPL'])     AS low,
       last(close['AAPL'])  AS close,
       sum(volume['AAPL'])  AS volume
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
-- the 2s10s curve: the 10-year yield less the 2-year
SELECT yield['10Y'] - yield['2Y'] AS curve_2s10s
FROM   rates ON date BY tenor
```

```sql
-- each sector ETF relative to SPX since 2024: the data starts there
SELECT index(close / close['SPX']) AS rel
FROM   (prices WHERE date >= '2024-01-01') ON date BY symbol
FILTER symbol LIKE 'XL%'
```

```sql
-- Bollinger bands over the last year shown, computed with all history
SELECT close['SPX']                         AS spx,
       bollinger_bands(close['SPX'], 20, 2) AS bb
FROM   prices ON date BY symbol
FILTER date > latest - 1y
```

```sql
-- inflation: CPI year on year, and its last 3 months annualized
SELECT 100 * returns(value['CPI'], 1y)                         AS yoy,
       100 * (power(1 + returns(value['CPI'], 3mo), 4) - 1)   AS three_month_annualized
FROM   macro ON date BY series
```

```sql
-- AAPL's bought and sold quantity each day, from individual trades
SELECT sum(qty['AAPL', side = 'buy'])  AS bought,
       sum(qty['AAPL', side = 'sell']) AS sold
FROM   trades ON trade_date BY ticker
SAMPLE BY 1d
```

### Complex

```sql
-- a pairs trade: KO against PEP
SELECT hedge_ratio(close['KO'], close['PEP'], 6mo) AS hedge,
       close['KO'] - hedge * close['PEP']          AS spread,
       zscores(spread, 3mo)                        AS z,
       CASE WHEN z > 2  THEN -1
            WHEN z < -2 THEN  1
            ELSE 0 END                             AS position
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'
```

```sql
-- the top three of these nine sectors by 3-month momentum, each month
SELECT returns(close, 3mo) AS mom,
       rank(mom DESC)      AS rnk
FROM   (prices WHERE symbol IN ('XLK', 'XLE', 'XLF', 'XLV', 'XLI', 'XLY', 'XLP', 'XLU', 'XLB')) ON date BY symbol
SAMPLE BY 1mo
FILTER rnk <= 3
```

```sql
-- a risk-off regime: SPX under its 200-day average, and VIX above 25 or high-yield spreads 2 deviations wide
SELECT px.close['SPX'] < avg(px.close['SPX'], 200)
       AND (px.close['VIX'] > 25
            OR cr.spread['HY'] > avg(cr.spread['HY'], 1y) + 2 * stddev(cr.spread['HY'], 1y)) AS risk_off
FROM   prices px ON date BY symbol,
       credit cr ON date BY index_name
FILL(PREV)
```

```sql
-- SPX in euros: its weekly excess return over German 3-month rates, a rolling Sharpe ratio and drawdown
SELECT px.close['SPX'] / fx.rate['EURUSD'] AS spx_eur,
       returns(spx_eur)                    AS ret,
       ret - rf.yield['DE3M'] / 100 / 52   AS excess,
       sharpe_ratio(excess, 1y)            AS sharpe,
       max_drawdown(spx_eur, 1y)           AS mdd_1y
FROM   prices px ON date BY symbol,
       fx     fx ON date BY pair,
       yields rf ON date BY tenor
SAMPLE BY 1w FILL(PREV)
```

```sql
-- a snapshot table: every stock's 1-year beta to SPX and its idiosyncratic volatility, as of the latest date
SELECT beta(close, close['SPX'], 1y)                                          AS beta,
       volatility(close, 3mo) * sqrt(1 - r_squared(close, close['SPX'], 3mo)) AS idio_vol
FROM   prices ON date BY symbol
FILTER symbol <> 'SPX' AND date = latest
```

## 11. Out of scope

How a result is drawn: panes, axes, log scale, styles, legend, notes, hand-drawn lines, levels, events, replay, the
point limit. These stay in the tile spec, referring to the select items by name.

## 12. Open decisions

1. The island's name in Pure: `#Quant{…}#` is a placeholder.
2. The comma between sources reads as SQL's cross join and is not one (R1). A different separator?
3. Aggregates across instruments (the market's total volume each date): names of their own, beside `rank`.
4. A split by a column that is not the instrument (today's `split=` on another text column).
5. Structs: their field names, and how the tile draws each.
6. A bare line under `SAMPLE BY` is the period's last value, or `last(…)` is required (InfluxQL's rule).
7. Today's spellings (§7): read forever, or read with a deprecation warning until a date.
8. The renames `range_of`, `union_of`, `filter_values` are provisional.
9. SQL's own window syntax (`OVER (…)`) as an escape hatch: not in the first version.
10. `latest` (R9) is proposed.
11. Each function's expected kind of line (price, return, level), checked by the compiler (§9).
12. A rolling value starts one observation earlier than gs-quant's ramp (already recorded); a `ramp =>` option if
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
