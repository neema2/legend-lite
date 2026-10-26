# DataCube's timeseries viewer: many series from many grids, every figure the database's (design, 2026-10-25)

**Status: agreed with the user, 2026-10-25** (§2.1); being built on the branch. `datacube/` is the DataCube + Python line's (`docs/IN_FLIGHT.md`); this work is on the session branch
`claude/timeseries-viewer-datacube-tsm0nb`. It builds on the charts design (`docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md`:
"the database computes, the chart draws"; the presets as calculated columns, §3.2) and the pages design
(`docs/DATACUBE_PAGES_DESIGN_2026_10_09.md`: every tile equal, bands, sheets).

## 0. The ask

The user, 2026-10-25: "design and implement the world's most expert historical timeseries 'viewer' as part of legend
lite datacube ... a new visualization but more powerful 'tile type' in Datacube that can plot anything timeseries
including comparing multiple time series at the same time with every option that people expect from financial time
series plot/viewer ... backed by datacube grid (or many grids potentially for comparison?) ... one inspiration might be
to look at the Goldman Sachs gs-quant library ... the client side API to 'plottool pro'".

Then, on the design: "I def want this to be built at the beginning to have multiple time series so it does not feel
like a bolt on later -- it can be phased if you think that's right but needs to be designed correctly upfront"; on how
series are queried: "definitely one query, can be snapped into duckdb wasm as a grid snap too if from different sources
and then later maybe as cross store join but that can come later".

## 1. What was read

**gs-quant** (github.com/goldmansachs/gs-quant, cloned 2026-10-25). It has no user interface; what it has is the
vocabulary PlotTool Pro speaks. Every function of `gs_quant.timeseries` marked `@plot_function` (`helper.py`:
"Indicates that fn should be exported to plottool as a pure function") is a word of PlotTool's expression language;
`@plot_measure` marks an asset's measures (`SPX.spot()`), `PTPDataset.plot()` writes PlotTool's own expression
(`Dataset("ID").field()`), and the analytics `DataGrid`'s processors each have a `get_plot_expression` ("used to go from
grid to plottool") -- a grid opened as a plot, which is what this tile is. What a PlotTool user expects, by module:

- `econometrics`: `returns` (simple, logarithmic, absolute over `obs`), `prices` (returns compounded back), `index`
  (divided by the first value), `change`, `annualize`, `volatility` (the annualized sample standard deviation of
  returns, in percent: 20% is 20.0), `correlation` and `beta` (of returns, rolling), `max_drawdown`, `sharpe_ratio`;
- `statistics`: rolling `min_`, `max_`, `range_`, `mean`, `median`, `sum_`, `std`, `var`, `cov`, `zscores`,
  `winsorize`, `percentiles`;
- `technicals`: `moving_average`, `bollinger_bands`, `relative_strength_index`, `exponential_moving_average`, `macd`,
  `exponential_volatility`, `seasonally_adjusted`, `trend`;
- `analysis`: `diff`, `lag`, `first`, `last`, `count`, `smooth_spikes`; `algebra`: arithmetic, `log`, `exp`, `sqrt`,
  `abs_`, `power`, `floor`, `ceil`, `filter_`, `if_`, `weighted_sum`; `datetime`: `align`, `interpolate`, `bucketize`
  (resampling: a frequency and an aggregate), `day`/`month`/`year`.

Two of its conventions matter everywhere. A WINDOW is `Window(w, r)`: `w` observations (or a calendar period, `'1m'`),
and a ramp `r` (default `w`) of first values left out, so a rolling figure is drawn once its window is full. And series
are ALIGNED before they are combined: `correlation` and `weighted_sum` take the dates every series has (`intersect`).

The rest of what a professional expects is common to Bloomberg's GP and COMP, TradingView and Highcharts Stock: range
buttons (1M ... 10Y, YTD, Max), a navigator to zoom and pan, a crosshair across stacked panes, a compare mode (every line
from 100, or its % change, at the left edge), each line's last value tagged on its axis, two value axes, a log scale,
annotations (levels, events, shaded periods), statistics over what is shown, and export.

## 2. Decisions

### 2.1 Decided (the user, 2026-10-25)

- **D1 Many series, many grids, from the start.** A tile reads any number of grids (its SOURCES, §3); the first
  landing compares series from several grids. Phasing is of what is built, never of the model: nothing in §3 is added
  later.
- **D2 One query per series.** Each series is one query, run where its grid runs. A series that COMBINES grids (a ratio,
  a spread, a correlation, a beta of two grids' series) is still one query: each grid's rows grouped to the tile's
  frequency, joined on the period, the windows over the joined rows. Never a tile-wide query joining every grid
  (series on different calendars would be forced onto one), never two queries combined in the browser (the browser
  would compute -- and a second copy of every window would live in TypeScript, out of the planner's reach).
- **D3 Grids on other stores are snapped.** A combination reads both grids in one database, the tab's DuckDB. A grid
  over another store (a warehouse, a remote engine) is snapped into it first -- the grid's own Snap, its copy then a
  table of the tab (`page-document.ts GridCopy`) -- and the combination reads the copy. A join across stores is later.
- **D4 No results cache.** The viewer keeps the rows its last answer gave; a change of the drawing alone (a colour, a
  pane, an annotation, a line hidden) redraws from them, and every other change asks the database again, as every
  DataCube chart does when a view lands on its grid. A zoom asks again only for the series that read the first date
  shown (an index, a change from the start, compare mode) and for the statistics. (A cache of answers by their query's
  text, kept a few seconds, was proposed and withdrawn: freshness by a clock is a guess, and the drawing's own changes
  never needed the database.)

- **D5 Pure for everything, snapped or not** (the user: "make sure even when we snap still uses platform/pure queries
  for everything not some new mechanism"). Every figure a tile draws is a Pure query planned by the platform's planner:
  a series of one grid through that grid's runner (live or snapped, the grid's own plane decides what its source reads,
  `cube.ts` `sourceFor`); a combination over one model of its sources' tables, a snapped grid's copy declared in that
  model as any table of the tab is (`join-open.ts`, infer.ts `tablesModel`). No SQL is written here, and nothing is
  computed in the browser.
- **D6 A tile kind of its own, with its API** (the user: "Yes new chart types with API") -- a fourth page view beside
  `grid`, `chart` and `text`: `timeseries`. A chart is, by definition, a view of ONE grid's query regrouped
  (`chart-spec.ts`'s header); a tile over several grids is not one, and making it a chart kind would be the bolt-on D1
  rules out. Its two page commands (§7) are made by Python in the same landing, as the one-API ledger requires
  (`test/one-api-ledger.test.ts` L8).

### 2.2 Proposed

- **P2 What is built first** (§8): phase 1 is the whole model, the viewer, sources from any number of grids, combinations
  across grids in the tab's DuckDB (the probe of §5.4 passed), the page commands and Python.

## 3. A tile: its spec

Plain JSON, ours, versioned with the page (a reader that meets a word it does not know refuses it by name, as a chart's
does). An example -- an equity index against a bond yield, their rolling correlation under them, a recession shaded:

```json
{
  "sources": [
    { "alias": "px", "cube": "cube-1", "date": "day", "key": "symbol" },
    { "alias": "rates", "cube": "cube-2", "date": "obs_date", "key": "tenor" }
  ],
  "grain": "day",
  "series": [
    { "id": "A", "expr": "<px:close[SPX]>", "style": { "color": 0 } },
    { "id": "B", "expr": "<rates:yield[10Y]>", "axis": "right" },
    { "id": "C", "expr": "<correlation($A, $B, 63)>", "pane": 1, "name": "SPX vs 10Y, 3m corr" }
  ],
  "range": { "last": "5y" },
  "panes": [{ "height": 3 }, { "height": 1 }],
  "annotations": [{ "kind": "period", "from": "2020-02-19", "to": "2020-04-07", "label": "COVID" }],
  "view": { "compare": "rebase", "stats": true }
}
```

(`<...>` stands for the expression's tree, which the spec keeps; §4 is its text.)

- **sources**: the grids the tile reads, each by its cube on the page, with its own date column and instrument column
  (two grids rarely name them alike) and a short alias the expressions name it by. A tile of one source leaves the
  alias off its expressions. A frozen tile's sources each keep their grid's filter as it was (`kept`, §7).
- **series**: what is drawn, in the legend's order, each an expression (§4); its id a capital letter (`$A`); a name;
  a split (one line per value of a column, `close` by `symbol`: every instrument); a style (line, area, bars, steps,
  dots; a palette slot, a width, a dash); a pane (0 the main one, up to 3 under it); an axis (left, right; else by
  unit: a second unit takes the right); hidden.
- **grain**: the tile's one frequency (day, week, month, quarter, year): each series' values taken to it, so series of
  different grids share one time axis. Absent, the rows' own dates.
- **range**: the window shown first -- a period back from the latest date (`1m` ... `10y`, `ytd`, `max`) or two dates.
- **panes**, **annotations** (a level on a pane, an event's date, a shaded period, each labelled), **view** (compare:
  `rebase` or `percent`; legend, last values, navigator, statistics, crosshair: each on or off).

## 4. The expression language

PlotTool's style over gs-quant's names (`timeseries-spec.ts parseExpr`, printed back by `exprText` -- the spec keeps the
tree, so what was typed and what is saved never drift):

| Text | Means |
|---|---|
| `close` | the grid's column along the date, each period's latest value |
| `close[AAPL]` | its rows whose instrument column is AAPL; `close[venue=NYSE, AAPL]` with another match |
| `volume[AAPL].sum` | each period's rows summed (last, first, average, sum, min, max) |
| `rates:yield[10Y]` | a column of the source aliased `rates` |
| `$A` | series A's expression |
| `returns(close[AAPL], 1, log)`, `volatility($A, 63)`, `zscores($A, w=252)` | a function: its series, then its parameters by place or by name |
| `close[AAPL] / close[MSFT]`, `($A + $B) / 2`, `100 * returns($A)` | arithmetic |

**The functions** (`TS_FUNCTIONS`, each with gs-quant's name, its own words, its unit, and where it differs):
returns, prices, diff, lag, annualize; index, rebase, change, percent_change (from the first date shown); mean
(moving_average, sma), sum, min, max, range, std, var, zscores, winsorize; volatility (vol), drawdown, max_drawdown,
sharpe_ratio; correlation (corr), beta, cov; bollinger_bands, rsi; log, exp, sqrt, abs, power, floor, ceil. A window
`w` is observations, 0 from the start (gs-quant's `Window(None, 0)`: running); a rolling figure is drawn once its
window is full (the ramp). Annualizing is by the tile's frequency: 252 days, 52 weeks, 12 months, 4 quarters; a tile
of no frequency ("As the rows are") by what its dates' spacing says, worked out by the database -- gs-quant's own rule
(`_get_annualization_factor`'s buckets: daily 252, weekly 52, monthly 12, quarterly 4, yearly 1; the owner's decision 2,
docs/TIMESERIES_REVIEW_2026_10_25.md §5).

**Where it differs from gs-quant, said in the catalog**: `volatility`'s window defaults to 22 (gs-quant: the whole
series); `sharpe_ratio` takes a risk-free rate of zero and the arithmetic mean of simple returns (gs-quant: excess
returns over a currency's rate, compounded); `rsi` averages its gains and losses simply (Cutler's; gs-quant smooths
them, Wilder's); a window is a number of observations, not a calendar period. Each default window that differs from
gs-quant's is said in the catalog (the review's Q10).

**A level, and its unit** (`timeseries-expr.ts levelOf`, `unitOf`; the review's Q2, Q4): a result is a level -- a
price, an index, a moving average of one -- or a figure of one (a return, a difference, a volatility, a z-score). Only a
level is compared from 100 or by its % change, drawn on a log scale, or given a level's statistics (volatility,
drawdown, growth, Sharpe); a pane of anything else asked for a log scale is drawn straight, and says why. A result's
unit follows its arithmetic: a ratio times 100 is a percent, a percent over 100 a ratio, a number over a series a
number; a function the rule does not know is refused, never guessed.

**Not built, refused in these words when typed** (`NOT_BUILT`): exponential averages, MACD, exponential volatility
and deviation, smoothed averages (each a recursion, every value from the one before, which a database's windows have
not); a rolling median and percentile rank (windows the planner does not write yet); a seasonal decomposition and its
trend (a model fitted to the series); excess returns (a risk-free series: divide by one of your own).

## 5. The queries

### 5.1 A series of one source (`timeseries-query.ts seriesQuery`)

One query of its grid, run by the grid's own runner:

1. the grid's source and its row-level calculated columns, its filter (as the tile reads it: following the grid, or
   frozen), and what the series matches (`close[AAPL]`);
2. grouped by the period -- and the split -- each column the series reads taken by its period's aggregate: the latest
   row's by default (`maxBy`, ordered by the day's number: `dateDiff` from 1970, since `maxBy` orders by a number); the
   period dated by its last observation (a week of daily rows by its last day, so a range from a date keeps the
   period that holds it: the review's Q6), and the rows tied at a period's latest time counted, said when there are
   more than one (Q7);
3. each operand on its own dates: a column, and each function of it, over the periods its own rows have; two operands
   (`a - b`, a correlation, a beta) joined on the period -- and the line, for a split -- where they meet, gs-quant's
   rule (a function of one series runs over that series' dates alone, never the dates another operand has too: the
   review's Q1, Q5); a split's first 20 values;
4. each function, innermost first: a window over the periods partitioned by the split (a frame of the last `w`, or
   running), or an expression over the row -- a return over `lag`, a deviation by `stdDevSample`, a correlation by
   `corr` over `rowMapper`, a band's edges and its width;
5. an index or a change from the start reads the value at the FIRST DATE SHOWN: the viewer passes where its zoom starts,
   and the database finds the first period at or after it with a value (a `min` window), and the value there;
6. the period, the split and the value -- a band's lower, upper and width -- latest first, at most 100,000 points.

Every series reads all its history: the window shown is the viewer's, so a rolling figure at the left edge has its
history.

### 5.2 A series combining sources

The same steps, each column of each source a part of its own -- its source's rows (its own date, instrument and
filter) grouped to the tile's frequency -- the parts joined on the period where an operator or a two-series function
meets them (inner: the dates both have, gs-quant's intersect), never before (the review's Q1). Planned over ONE model of every source's tables, as DataCube's Join opens a join of two grids
(`join-open.ts openSources`: `openInputs`' rule for any number of sources). Where the tables are is the host's, the rule
one: on the tab, its own DuckDB (`app/timeseries-together.ts tabSources`); on a page Python serves, which has no
database of its own there, Python's tables, opened there by DataCube's code headless (`join.json` op `together`:
headless `joinTogether`) -- and either way the series' one query is written by the tab over the relations given and
planned and run where the grids' rows are (the host's plan-and-run). A source's tables are where its rows are now: a
grid over a file reads its table; a snapped grid its copy (D3), on the tab and on Python's host alike -- its copy's
table, held by the session (the review's P1, P3); a source anywhere else is refused for the combination
in the join's words (`leafRefusal`) -- it is still plotted on its own, by its own grid's runner. The grids are opened
together once a round of asking -- a fetch, or the window moved: every series and statistic of it -- and put away when
it is done (`ui/timeseries-view.ts Opened`); a grid's view landing asks again, and opens them again (its tables may
have moved: a snap, a rebuild). Statistics are asked only while the table or the legend shows them.

### 5.3 The tile's bounds and statistics

- **Bounds** (`boundsQuery`): each source's latest and earliest date, and where each range button starts --
  `adjust(latest, -1, YEARS)`, the year's first day for YTD: the database's date arithmetic, the viewer only places its
  zoom there.
- **Statistics** (`statsQuery`), per line, over the dates shown: count, first and last date and value, the change and
  % change, the least and the most and when, the mean, the standard deviation; for a level its annualized volatility
  (of log returns), deepest drawdown, compound annual growth and Sharpe ratio -- each the database's, over what is shown.
  Annualized by the tile's frequency, or its dates' spacing (§4); a candle series' least and most its lows' and highs'
  (Q9); a % change over the start's size, so a start below zero rising is a rise (Q11); compound annual growth only
  over a calendar year or more (`adjust(from, 1, YEARS) <= to`, the database's): under a year none, its % change its
  growth, and the viewer says so (GIPS; the review's N3, 8,507% over four days).

### 5.3½ The expert functions (the user, 2026-10-26: "do all, including recursive and self-joins where the database supports it")

Each the database's, written in Pure as the rest are; where a window frame (which counts rows) cannot say it, a join
over the series' own rows does.

- **Calendar periods** (gs-quant's tenors, `helper._to_offset`: `3m`, `1y`, `2w`, `10d`) for any window `w` and any lag
  `obs`. A window of a period is a SELF-JOIN of the line's periods to every one of its own whose date is in
  `(adjust(date, -n, unit), date]` (gs-quant's `rolling_offset`), its read aggregated per period and joined back
  (`Writer #calendar`); drawn from the input's first date that period on (gs-quant's `apply_ramp` of a `DateOffset`).
  A lag of a period is an AS-OF JOIN: the value as of `adjust(date, -n, unit)`, its last at or before
  (`Writer #valueBack`) -- where gs-quant's `lag` moves each date on and keeps only the dates it lands on, so a year
  back from a Monday whose date a year before was a Sunday has none there (the catalog says so).
- **Risk statistics** (`statsQuery`, the panel's Risk tab): of a level's simple returns over the dates shown, pandas'
  definitions -- skew the adjusted Fisher-Pearson coefficient (`Series.skew`), kurtosis the bias-corrected excess one
  (`Series.kurt`), each from the returns' moments about their mean (a window over the shown rows, then aggregates);
  Sortino the mean return over the root mean square of those below zero, annualized; the share of periods up; the best
  and the worst; a historical value at risk, the 5% quantile the database interpolates (`percentile`, continuous), and
  the expected shortfall, the average at or below it (the quantile per line joined back beside each return); Calmar the
  growth over the deepest drawdown's depth (a year or more, a drawdown below zero). None of a level at or below zero.
  The panel's tab (Summary, Risk) is a viewing choice, as the zoom is: not saved.
- **A rolling regression** of the first series on the second, over the dates both have (DataCube's own; gs-quant has
  beta alone): `alpha` (of the simple returns, the mean less beta times the benchmark's, annualized: Jensen's at a
  risk-free rate of zero), `r_squared` (the returns' correlation squared), `hedge_ratio` (the least-squares slope of
  the levels) and `residual` (the level less its fitted value, the spread of a pairs trade -- with `zscores`, its
  signal). Each from the window's covariance, variance and means, as beta is.
- **Drawdowns** (`drawdownsQuery`, the panel's Drawdowns tab, asked when shown): each level line's deepest five over
  the dates shown, each a run from a peak (a date at or above every one before it) to the next, its recovery (none: not
  yet), its trough the least between -- the peaks a running maximum, the runs numbered by a running count of them,
  each run's recovery the first peak after its dates (a window over the dates after each, read before the runs are
  grouped), grouped, ranked by depth. **A rule this taught (2026-10-26):** a plain filter written right after a
  window's extend is folded into that window's WHERE -- legend-engine's own behaviour, which legend-lite keeps (core
  `lowering/Fold.java` filterSlot: PCT `testExtendFilterOutNull`) -- so the window sees the filtered rows. A query here
  never filters after a window whose input it must keep whole: it computes what reads every row before (a first try
  read each run's recovery as a `lead` after the depth filter, and got the next drawdown's peak).
- **Exponential averages, exact, in closed form** (gs-quant's `exponential_moving_average`, `exponential_std`,
  `exponential_volatility`, `exponential_spread_volatility`, `macd`, `smoothed_moving_average`, and RSI now Wilder's,
  as gs-quant's). Each is a recursion, `y = beta·y[-1] + (1 - beta)·x` (pandas `ewm(adjust=False)`). A window frame
  cannot recurse -- its reducer must be an aggregate: the planner refuses a `fold` there ("aggregate reduce must be a
  native reducer call"), so a recursive form would be a core compiler change of its own, and the databases' recursive
  queries are not reachable from a relation's Pure. So the DATABASE computes the recursion's CLOSED FORM: `y_t =
  (1 - beta)·Σ beta^(t-i)·x'_i`, `x'_0 = x_0 / (1 - beta)`, summed in blocks of W values (W the fewest with `beta^W`
  under 2^-60, so `beta^-pos` stays under 2^60 in a double): a running sum within a block, the block before carried in
  whole by a join, the blocks before it weighing under 2^-60 -- below a double's precision, so the value is the
  recursion's to its last bits on every database (`Writer.ewm`). The bias-corrected deviation is the weighted mean
  square less the squared weighted mean (of each value less the line's first) over 1 less the sum of squared weights
  (pandas' correction, its closed form). MACD's spans weigh `2 / (k + 1)`; Wilder's starts from the mean of w, as
  gs-quant's ramp has it. Pinned against the recursions themselves (and pandas' `ewmcov` loop) over 700 days.
- **By each window's rows** (`Writer.ofWindow`, a SELF-JOIN): each period joined to every period of its window (the
  last w, from the start, every one, or a calendar period's dates), aggregated per period and joined back -- what a
  window frame cannot say (the period's own value against each of its window's) or not on every database (a
  percentile, which Postgres takes in a GROUP BY but not as a window function). `percentiles` (scipy's
  `percentileofscore`, kind=mean: the share below, those equal counted half), `median` and `percentile` (pandas'
  quantile, the database's continuous percentile; a percentile of no window the whole series', gs-quant's scalar drawn
  as a line). The planner takes the percentile's fraction as a literal alone: gs-quant's `n` of 100 is spelled as one.
- **Conditions, shaded** (gs-quant's `algebra` comparisons; Bloomberg's and FRED's shaded periods): `<`, `<=`, `>`,
  `>=`, `==`, `!=` and `and`, `or` -- each 1 where it holds, 0 where not, none where a side has none, so a condition
  reads as a number too (`(returns($A) > 0) * returns($B)`). A series that IS a condition (its expression's top) is
  drawn as the periods it holds, shaded in its colour across every pane, on no axis: its runs the database's (each
  change of it begins one, numbered by a running count; each date that holds carries its run's first and last date,
  `TS_FROM`, `TS_TO`), the drawing placing each once. Its chip says whether it holds at the date shown last; it has
  no statistics, as a band has none.
- **Correlations** (`correlationsQuery`, the panel's Correlations tab, asked when shown): a matrix of the lines drawn on
  an axis (the first 12), each pair's correlation over the dates both have shown -- a level's simple returns (as
  gs-quant's correlation reads them), anything else as it is -- one query per pair of series: each side written in its
  own Shared (one name counter), cut to the dates shown, its returns, the two joined on the period across their lines
  (one split series' lines against each other, each pair once), and every pair of lines aggregated. Each cell tinted by
  its sign and size, drawn.
- **One rule for the tables, the screen's and Python's** (`tableSeries`): the statistics every series drawn on an axis
  (no band, no condition), the drawdowns every level's, the correlations each pair but candles. Python's
  `Timeseries.statistics()`, `drawdowns()` and `correlations()` ask `timeseriesQuery` with a `table` -- the same queries
  the tabs run (headless `askedOf`), planned and run where the rows are -- each a pandas DataFrame, held in the Python
  tests to pandas' own skew, kurtosis, quantile and correlation.

### 5.4 Proven so far

`datacube/test/timeseries-queries.test.ts` plans every function through legend-lite's planner and runs it on DuckDB,
held to gs-quant's definitions written out in the test as plain arithmetic (29 cases: returns of each type, rolling
statistics with their ramp, z-scores, volatility daily and monthly, drawdowns, correlation, beta, covariance, Bollinger
bands, RSI, prices from returns, a ratio of two instruments on the dates both have, an index from the first date shown
and from a date with no value, compare mode, bounds, statistics). A probe of §5.2 (2026-10-25, not kept): two tables of
the tab's DuckDB with different date and instrument columns, opened over one model by `openInputs`, each grouped by its
own date, joined on the period, a rolling correlation over the joined rows -- planned and run, the joined dates exactly
the shared ones.

Since (2026-10-25): §5.2 is tested the same way (a ratio across grids on the dates both have, one grid's series over
the joint model, a correlation of two grids' returns after the join, a split of the first grid against the second's
one series, monthly), and the Markets example end to end -- its two samples read into DuckDB as the tab reads them,
typed as the page declares them, and its every series (24, two across both grids), its statistics and its sources'
bounds planned and run.

## 6. The viewer

```
┌ 1M 3M 6M YTD 1Y 2Y 3Y 5Y 10Y Max │ from..to │ Daily ▾ │ Compare ▾ │ Log │ Stats │ + Series │ ⋯ ┐
│ ● SPX 4,783.45 +2.1%   ● US 10Y 4.02%   ● SPX vs 10Y, 3m corr -0.42                       │  the legend
│                                                                                           │
│   main pane: lines, bands, areas; the latest value tagged at each line's end              │
│ ─────────────────────────────────────────────────────────────────────────────────────── │
│   pane 1: an indicator, bars of volume                                                    │
│ [▁▂▃▅▇ navigator: every date, its window dragged to zoom and pan ▇▅▃▂▁]                   │
├ statistics: a row per line over the dates shown ─────────────────────────────────────────┤
```

- **Range buttons** set the window shown (saved with the page); a zoom or a pan of the navigator, the mouse wheel or a
  drag is the person's own look (not saved), and re-asks only what reads the first date shown (D4).
- **Compare**: every level from 100, or its % change, at the first date shown -- moving with the zoom; a return, a
  volatility or a z-score is drawn as it is.
- **The crosshair** crosses every pane at one date, a hover listing every line's value there, written as the grid
  writes its column (a level) or by the figure's unit (a return as a percentage, a volatility in percent).
- **The legend** names each line with its latest value and its change over the window (the statistics query's);
  a click hides or shows it, a hover brings it forward, a double-click edits it.
- **The series editor**: the expression, read as typed with its error said where it is; pickers for the source, the
  column, the instrument (the column's values, the database's), the split; the functions by family, applied around
  the expression; its style, pane, axis and name.
- **Settings**: each source's date and instrument column; the toggles; annotations (a double-click on the chart puts an
  event at that date); export as a picture and as CSV (the rows the database answered, long form).
- **What the database refuses is said where it lands** (the review's P4; AGENTS.md invariant 4): a series refused on
  its line; a range whose dates were refused in the banner, with what is shown instead; statistics refused on their
  row and in the banner; an instrument list refused on its picker -- never shown as something missing.
- Drawn by `timeseries-option.ts` (pure, tested in node) and `timeseries-render.ts` (ECharts: its own chunk, fetched
  with the first timeseries tile; the panes share one time axis, one crosshair, one zoom).

## 7. On the page

- **The page document**: a view `{ id, kind: 'timeseries', title?, spec }`, the spec's sources naming the cubes it reads
  -- so the page knows every cube a tile reads, as it knows a chart's one. No version step: a reader that does not know
  the kind refuses it by name, as the text tile came in (2026-10-24).
- **Commands** (`page-commands.ts`): `addTimeseries` (on a sheet, a band of its own) and `setTimeseries` (the spec
  whole, as `setChart`); `renameTile`, `moveTile`, `removeTile` and the layout's as for any tile. The rebase
  (`page-rebase.ts`) renames the cubes a tile's sources name, as it renames a chart's. **Removing a grid a tile reads**
  (the owner, 2026-10-25, the review's G4): the tile stays, DETACHED, and its grid's cube is kept off the board for it --
  read as it was -- exactly as a pinned or frozen chart's is. A chart survives its grid when it has a shape of its own
  (a dynamic chart's shape is its grid's grouping, so it goes with it); a timeseries' shape is always its own (its
  dates' periods and its instruments), so it always does. Undo brings the grid back.
- **The cubes a page holds** (the second audit's A-B1, 2026-10-26): each its views read and no other --
  `page-document.ts cubesRead`, which every tile kind answers (a `switch` with no default), and `cubesHeld`, which every
  writer and page command holds a page to; `writePageOf` refuses a page written without a cube a view reads.
- **States, a chart's words** (the user, 2026-10-26; the header's state button, Python's `Timeseries.state`):
  DYNAMIC follows each grid -- its filter, the selections clicked on the page, its calculated columns, a snap or going
  live -- and asks again only when one of those changed (the viewer's input key); FROZEN keeps each source's grid's
  filter as it was when it was frozen (`TsSource.kept`, every source or none, as a frozen chart's `kept`), read by the
  compiler for every query (`timeseries-spec.ts filterRead`), so neither host decides it; DETACHED, a grid it reads
  removed. No PINNED: a timeseries never takes its grid's grouping -- it reads the grid's source rows (its source, its
  row-level calculated columns, its filter) and groups them itself, by its own period and instrument -- so there is
  nothing of its grid's to pin. A grid regrouped, sorted or scrolled asks nothing.
- **Create ▸ Timeseries**, in every grid's Create ▸: a tile under the grid, its first series DataCube's rule over the
  grid's columns (`timeseries-expr.ts defaultTimeseries`: its date, its instrument column, its price-like value, a line
  per instrument) -- the rule Python's headless `timeseriesMade` asks too.
- **Python** (`python/legend_lite/page.py`): `page.timeseries(sources={'px': prices, 'rates': rates}, series=[...])`, a
  `Timeseries` tile whose `series`, `range`, `view` and `annotations` it sets; every word judged by DataCube's reader,
  asked headless -- Python keeps no list of its own (the charts' rule, PR #47). Every change of a tile, its viewer's
  and Python's `Timeseries` methods' alike, is made by one rule (`timeseries-edit.ts editTimeseries`, which Python asks
  headless: `timeseriesEdit`; the review's A2), and every kind of change is made on both sides or listed with what the
  other makes instead (`test/one-api-ledger.test.ts`; A8).
- **Export**: the page's HTML and PDF take the tile as drawn (its picture), as a chart's.

## 8. Phases

1. **The tile** -- everything in §3 to §7: sources from any number of grids; series of one source and combinations
   across sources, on the tab and on a page Python serves; the viewer whole; the page commands, the rebase, a removed
   grid; Python; an example page over a markets sample (daily prices and volumes of ten instruments with a currency
   among them, and a yield curve's tenors: two grids). Each step a commit, held by its tests (§9). **Built** (§10).
2. **Sources elsewhere**: a combination offering the grid's Snap when a source is on another store (D3), and reading
   the copy; per-source frozen filters.
3. **Later**: a join across stores (D3); events read from a grid's column (a calendar of releases) as
   annotations; regression of one series on another (a scatter with its fit: the database's slope and intercept from
   `covarPopulation` and `variancePopulation`); page filters reaching a tile's sources by class.

## 9. Tests

The queries planned and run (§5.4, growing with each function and with §5.2's combinations); the spec's reader, its
refusals and the expression language's round trip (parse, print, parse); the drawing (`timeseries-option.ts`, in node);
the viewer's DOM (jsdom, the renderer stood in for); the page commands, the rebase and a removed grid; Python's
`page.timeseries`; the example pages drawn in Chromium -- and the timeseries gallery (below) the catalog's own test:
every function on its family's sheet, every series of it planned and answered on the samples
(`test/examples.test.ts`, `test/timeseries-queries.test.ts`). Since the review (docs/TIMESERIES_REVIEW_2026_10_25.md):
every change of a tile made on the screen as a person makes it and held to the one-API checks -- told as commands,
read by the host as Python's keeper reads a write, replayed to the same page (`test/one-api-gestures.test.ts`, 20
gestures) -- and every kind of change made by the viewer and by Python (`test/one-api-ledger.test.ts`); the drawing
rendered by real ECharts (`test/timeseries-echarts.test.ts`); both example pages opened in Chromium, every tile drawn
(`demo/verify-charts.mjs`); a combined series on a page Python serves, read together by Python
(`test/python-engine/python-engine.mjs`).

## 10. What is built so far (on the branch)

- **The spec** (`timeseries-spec.ts`): the types of §3, the functions' signatures and the saved spec's reader -- what a
  page reads at its start, and all it needs to. The rest is fetched with the first timeseries tile (budgeted:
  `test/bundle-budget.test.ts`): the catalog's words, the expression language, the rules against a grid's columns and a
  first tile of a grid (`timeseries-expr.ts`); the queries (`timeseries-query.ts`, §5); the drawing
  (`timeseries-option.ts`); the viewer (`ui/timeseries-view.ts`, §6); its renderer (`timeseries-render.ts`, over the
  charts' own ECharts: `chart-render.ts canvasChart`, one build of it for both).
- **On the page** (§7): the view kind read and written (`page-document.ts`), its commands (`page-commands.ts`
  `addTimeseries`, `setTimeseries`), rebased and diffed (`page-rebase.ts`, `page-between.ts`); the board's tile
  (`page/cube-page.ts`: placed, synced to the page's state, restored, removed, exported as a picture); Create ▸
  Timeseries; grids read together on the tab (`app/timeseries-together.ts`) and on Python's page (`join.json`
  `together`). The one-API ledgers hold it: the commands are the screen's and Python's alike (`test/one-api-gestures`:
  a timeseries added, its range picked, each replayed by the host to the same page).
- **Python** (`python/legend_lite/page.py`): `page.timeseries`, `sheet.timeseries`, `grid.timeseries`, the
  `Timeseries` tile's `plot`, `unplot` and `options` -- every word judged by DataCube's code headless.
- **Samples and an example**: `samples.ts` markets (ten instruments, five years of business days, a crash and a bear
  market) and rates (a Treasury curve that rises and inverts); the Markets example page (`examples/markets.ts`: four
  sheets -- performance rebased with a vol pane; drawdowns, volatility, correlation and beta; Bollinger bands, moving
  averages, RSI and volume; the curve, the 10s2s spread, and a bond fund against its yield across two grids), drawn in
  Chromium on the demo site.
- **A series' values in Python** (`Timeseries.frame(series, start=, end=)`): the viewer's own query written headless
  over Python's tables (`timeseriesQuery`), planned and run by Python's plan-and-run -- a DataFrame indexed by date, a
  column per line (a split's per value; a band's lower and upper; candles' open, high, low and close); a window kept
  by the database after every value is computed over every date (`seriesQuery`'s clip), so its first values have their
  history.
- **Candles** (`ohlc(open, high, low, close)`, also `candles`): a candle per period -- its four series computed on
  their sources' own dates (each date's rows taken as a candle's are, `candled`: its first open, highest high, lowest
  low and last close), then each period of the tile's frequency taking its dates' first open, highest high, lowest low
  and last close (`candle`), so a weekly candle of daily rows is right, and so is one of a computed series
  (`ohlc(close / close[SPX], ...)`; its functions run over those dates: a 5 is five of them) -- drawn by ECharts' candlestick (registered with the rarer kinds, `chart-render-kinds.ts`,
  before the tile's chart is made: an ECharts chart takes the layouts it knows as it is made), never read, unmoved by
  compare. The series editor's Candles makes one of a close (`candleOf`: the open, high and low beside it, by name).
- **The timeseries gallery** (`examples/timeseries-gallery.ts`; the user, 2026-10-25: "a real saved page with multiple
  tabs showing all of these", as the chart gallery is): in the Open window's examples beside Markets -- a sheet per
  family of the catalog (returns, level, rolling statistics, risk, relationships, technicals, math), each using every
  function of its family, titled by what it does; a sheet of the ways to read a tile (a line per symbol, a month at a
  time, a log scale, every line from one start with its statistics, the styles, two grids in one query); and the two
  grids. 36 tiles, 88 series, each sheet fitting its window. Building it found three defects, each fixed with its test:
  a level read through a lag (`diff`, `lag`, `prices(returns(...))`) had its statistics name their own lag as the
  series named its own, and the planner refused the column twice (the statistics now go on writing with the series'
  own writer, `Built.writer`); `prices` took its returns' unit (a level of 1,139 read as 113,949.58%: it is an index's
  level now); and a log pane drew a second price, on the right, on a straight scale (a right axis of levels is log too;
  a ratio's keeps its zero). A small plain number (a covariance of daily returns) is written to three significant
  places, not as 0.000; a tile half a screen wide tightens its toolbar (a container query on `.dc-ts-host`).
- **A tile on a sheet not shown asks nothing** (`page/cube-page.ts #drawTimeseries`): its viewer is made, and its grids
  asked, when its sheet is shown (`showSheet`, a step that leaves one shown, a page reopened) -- the gallery opens
  asking its first sheet's 10 series, not its 88 (docs/TENETS.md: lazy Work; `test/cube-page-shell.test.ts`). Charts
  on a sheet not shown still ask at opening, as before.
- **The review** (docs/TIMESERIES_REVIEW_2026_10_25.md, before the PR): four independent reviewers -- the execution
  tenet, the one API, the numbers, the repository's rules -- and every blocking and should-fix finding fixed, each
  pinned by a test (its §6): each operand on its own dates, levels apart from units, periods dated by their last
  observation and annualized by their dates; snapped grids read from their copies on every path, Python's frame()
  included; one rule for every change of a tile, on the screen and in Python, every gesture proved so; a removed
  grid's cube kept while a tile reads it, its undo too; refusals said where they land. A second audit of those fixes
  (its §7), and its findings fixed the same way (its §8): a range's start the value as of it, `prices` from `initial`, a
  level below zero compared as a rise and given no returns' figures; one judge of a change for the viewer and Python,
  the frame's notes the drawing's; the cubes a page holds one rule every tile kind answers; a hidden tile asking
  nothing until shown.
- **States, a chart's words** (the user, 2026-10-26; §7): Dynamic, Frozen (each grid's filter kept), Detached -- on
  the screen and in Python (`Timeseries.state`).
- **PR #73's design** (the user, 2026-10-26): its state in its header; its right-click a chart's (Create ▸,
  Properties…, View ▸); its series editor and Settings a window beside it among the page's windows; both themes.
- **Next**: events from a grid's column; a regression of one series on another; a chart on a sheet not shown asking
  only when shown, as a timeseries tile does; the follow-ups of docs/TIMESERIES_REVIEW_2026_10_25.md §9.
