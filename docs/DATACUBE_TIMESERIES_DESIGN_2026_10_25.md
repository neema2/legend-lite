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
  (Q9); a % change gs-quant's, last / first - 1, none from a first of 0 (the user, 2026-10-28: "match gsquant", over the
  review's Q11 and the second audit's S1, which had taken it over the start's size); compound annual growth only
  over a calendar year or more (`adjust(from, 1, YEARS) <= to`, the database's): under a year none, its % change its
  growth, and the viewer says so (GIPS; the review's N3, 8,507% over four days).

### 5.3½ The expert functions (the user, 2026-10-26: "do all, including recursive and self-joins where the database supports it")

Each the database's, written in Pure as the rest are; where a window frame (which counts rows) cannot say it, a join
over the series' own rows does.

- **Calendar periods** (gs-quant's tenors, `helper._to_offset`: `3m`, `1y`, `2w`, `10d`) for any window `w` and any lag
  `obs`. A window of a period is a SELF-JOIN of the line's periods to every one of its own whose date is in
  `(adjust(date, -n, unit), date]` (gs-quant's `rolling_offset`), its read aggregated per period and joined back
  (`Writer #calendar`); drawn from the input's first date that period on (gs-quant's `apply_ramp` of a `DateOffset`)
  -- of a volatility or a Sharpe ratio, from the first PRICE's date, as gs-quant's returns keep it (none there; the E9
  review). A function of two series (a correlation, a beta, a regression) is drawn from the first date both have a value
  that period on: the join keeps only the dates both have, where gs-quant's ramp counts from the first series' own
  first date -- a deliberate difference, of at most the second series' late start.
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
  signal). Each from the window's covariance, variance and means, as beta is -- every one over the window's PAIRS, the
  dates both sides have a value (a lag's, a ramped mean's, a first return's leading empties masked on either side), its
  ramp a count of them (the E9 review: an intercept of two samples had its sign wrong; gs-quant's beta takes its
  variance over the pairs for a calendar window, and so does this for every window).
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
  whole -- its total a window over the block, read at this block's first value as the value before it (a lag over the
  line's values) and spread over the block by a window, never the query joined to itself (the E9 review: a join had
  doubled the query with each nested average, four 407k characters) -- the blocks before it weighing under 2^-60, below
  a double's precision, so the value is the recursion's to its last bits on every database (`Writer.ewm`). A deviation
  at beta 0 is refused in words: each value weighs alone, which has no deviation (pandas answers none at every date). The bias-corrected deviation is the weighted mean
  square less the squared weighted mean (of each value less the line's first) over 1 less the sum of squared weights
  (pandas' correction, its closed form). MACD's spans weigh `2 / (k + 1)`; Wilder's starts from the mean of w, as
  gs-quant's ramp has it. Pinned against the recursions themselves (and pandas' `ewmcov` loop) over 700 days.
- **By each window's rows** (`Writer.ofWindow`, a SELF-JOIN): each period joined to every period of its window (the
  last w, from the start, every one, or a calendar period's dates), aggregated per period and joined back -- what a
  window frame cannot say (the period's own value against each of its window's) or not on every database (a
  percentile, which Postgres takes in a GROUP BY but not as a window function). `percentiles` (scipy's
  `percentileofscore`, kind=mean: the share below, those equal counted half), `median` and `percentile` (pandas'
  quantile, the database's continuous percentile; a percentile of no window the whole series', gs-quant's scalar drawn
  as a line -- ONE aggregate a line, grouped and joined back (`Writer.ofLine`), never each row joined to every other:
  the E9 review timed that at 4.5 s over 4,000 rows). The planner takes the percentile's fraction as a literal alone:
  gs-quant's `n` of 100 is spelled as one.
- **Conditions, shaded** (gs-quant's `algebra` comparisons; Bloomberg's and FRED's shaded periods): `<`, `<=`, `>`,
  `>=`, `==`, `!=` and `and`, `or` -- each 1 where it holds, 0 where not, none where a side has none, so a condition
  reads as a number too (`(returns($A) > 0) * returns($B)`). A series that IS a condition (its expression's top) is
  drawn as the periods it holds, shaded in its colour across every pane, on no axis: its runs the database's (each
  change of it begins one, numbered by a running count; each date that holds carries its run's first date and where
  its shading ends, the line's next date after the run, from which it no longer holds -- its own last at the line's
  end -- `TS_FROM`, `TS_TO`: a run of one date a period wide, where to its last date it had none, the E9 review), the
  drawing placing each once. Its chip says whether it holds at the date shown last; it has
  no statistics, as a band has none.
- **Correlations** (`correlationsQuery`, the panel's Correlations tab, asked when shown): a matrix of the lines drawn on
  an axis (the first 12), each pair's correlation over the dates both have shown -- a level's simple returns (as
  gs-quant's correlation reads them), anything else as it is -- one query per pair of series: each side written in its
  own Shared (one name counter), cut to the dates shown, its returns, the two joined on the period across their lines
  (one split series' lines against each other, each pair once), and every pair of lines aggregated -- each line with
  itself too, its diagonal the database's (1, or none where it has under two values or does not vary: DuckDB's `corr`
  says NaN there and Postgres' none, so the query says none itself where a side's variance is zero), and its count of
  the dates both have. Each cell tinted by its sign and size, drawn.
- **One rule for the tables, the screen's and Python's** (`tableSeries`): the statistics every series drawn on an axis
  (no band, no condition), the drawdowns every level's, the correlations each pair but candles. Python's
  `Timeseries.statistics()`, `drawdowns()` and `correlations()` ask `timeseriesQuery` with a `table` -- the same queries
  the tabs run (headless `askedOf`), planned and run where the rows are -- each a pandas DataFrame, held in the Python
  tests to pandas' own skew, kurtosis, quantile and correlation. The screen's tabs ask by it too, a hidden series
  included, so showing a line again asks nothing (the E9 review: its rows had been left out); the screen draws the
  first 12 series' lines of a matrix. Python's tables asked of no dates read the tile's range, as the screen's window
  does (`rangeWindow`, headless `timeseriesWindow`: each source's bounds the database's, the latest of them), and say
  why a level's figures are blank in the screen's words, as warnings (`statBlanks`, headless `timeseriesStatNotes`).

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

### 5.5 Every question a tile asks (the user, 2026-10-26: "we need to somehow show all the pure queries used to create a timeseries chart")

A chart is drawn from several queries, not one: each source's bounds (the latest date, where each range button
starts), each series' points (a hidden one's too), the view's (a mode's), and the statistics panel's -- each line's
statistics, each level's drawdowns, each pair's correlation, each level's calendar. Which ones is one rule,
`timeseries-query.ts tileAsks`, by the rules each drawing reads (`tableSeries`, `viewSeries`): each ask its kind, what
it is of (a series, a pair, a source), its words and what it serves (the chart, or a tab). Each ask's query is written
by one function, `askQuery`, which every round of the viewer writes it by (`#runAsk`; a series' points from the first
date shown where it reads one, every date kept, the drawing's limit) and Python's too: headless `askedOf` writes each
table and view by it, and a series' points by it when asked `drawn`.

The **Query** window (the toolbar) lists every ask, the chart's open and the panel's folded: each its query as it was
sent (kept with the SQL the database ran, by its ask, for the tile it was asked of, and the dates it was asked over),
printed by the compiler, with its SQL -- or, not asked yet (a tab not shown), written as it would be, saying which tab
asks for it; or refused, saying why. Each copied, and all at once (Copy all: each its words, its Pure, its SQL).
Python's `Timeseries.queries()` is the same list (headless `timeseriesAsks`), each asked of `timeseriesQuery` as the
screen asks it and planned (the engine's dry run): `what`, `serves`, `kind`, its series, pair or source, `pure` and
`sql`, or `refused`. `query(series)` stays the frame's own (the dates asked alone, every point). Pinned:
`timeseries-queries.test.ts` "every question a tile asks" (the asks of a tile with a band, a return and a scatter;
askQuery the screen's; every one answered), `timeseries-view-dom.test.ts` "Query: every question" (the ten of a two-series
tile, which ran, which not yet, each printed, copied one and all; a tab shown, its question ran), and `test_page.py`
`test_a_timeseries_lists_every_query_it_asks_each_its_pure_and_its_sql`.

**And the tile as the code that builds it** (the user, 2026-10-26: "an option that shows how exactly to build that
chart programmatically from TS and python"). The Query window's tabs -- Queries, Python, TypeScript -- show the program
that builds the tile, written by one rule (`timeseries-code.ts tileCode`, a chunk of its own fetched with the first
Query window) from its spec and what the page has (its title, each source's grid: its title and its cube's source --
the host's `codeOf`, headless `codeOfPage`). Since 2026-10-28 (the user: "a generic snippet of python about how to
create your own chart that a user can copy paste to just create exactly that tile in their own datacube") it finds
nothing of the page it was written of: the **Python** is a page of its own (`ll.Page()`), a grid over the rows each
source's grid reads -- opened as that grid was (`file=`, `remote=`, `warehouse=` and `table=`, `cls=` at its project
and revision), else over a frame of the person's (a dataframe's grid, a saved query's, a join's, another grid's rows),
the columns the tile reads said beside it -- and the grid's `timeseries` (the page's, each source a mapping, for
several or a frozen one): each series its expression, or a mapping of it and how it is drawn (its id where it is not
the next letter); every other slot as told; then each formula's Pure printed (`tile.query(id)`) and the page shown
(`ll.show`). The **TypeScript** is the cubes it reads (`cubes`, the person's to set), the spec over them, and the page
command the screen's Create ▸ Timeseries sends (`addTimeseries`, on the sheet it is given, a band of its own) applied by
the one function every change goes through (`applyPageCommands`), and its questions (`tileAsks`). Under each program the
Code tab shows each formula with the Pure the tile writes for it (askQuery, printed by the compiler): a formula is
DataCube's gs-quant shorthand for that query (the user's "A"; a series written in Pure itself, "B", is a follow-up, §11). Each
expression is written to read back as itself (`timeseries-expr.ts exprSource`: a parameter by its place while every
one before it is told, else by its name), since `exprText` drops a default told and writes one before a later one, the
same figures but another spec. Writing it found a bug: `exprText` wrote a basket's rebalance by its place
(`basket(x, 0.6, y, 0.4, monthly)`), which reads as one more series; a function of many series' parameters are now
written by name. Python's `tile.code(language)` is the same text (headless `timeseriesCode`). Pinned by running the
programs: `timeseries-code.test.ts` imports every gallery tile's TypeScript and holds the spec it writes, the tile its
command adds (on its sheet, titled as it is) and its questions to the tile's; `test_page.py`
`test_a_timeseries_writes_the_code_that_builds_it_and_the_python_builds_it_again` runs the Python over the page and
holds the tile it makes to the tile's spec, title and sheet; `timeseries-view-dom.test.ts` the window's tabs, each
program and its copy.

### 5.6 DataCube's Pure library (the user, 2026-10-26: "try as pure function -- doesn't have to be built in -- can be datacube pure scripts")

Until now each function's query was written by TypeScript (`timeseries-query.ts`'s `Writer`): the database computed
every figure, but a function was a recipe in DataCube's code, never a Pure function a person could read or call. Pure
can take columns as parameters -- the relation natives are generic (`extend<T,Z>`, `select<T,Z>`, `ColSpec<Z⊆T>`) --
but legend-engine refuses type parameters in a user function. The user chose to let legend-lite take them for a
function over relations (`docs/SEMANTICS_REGISTER.md` S112): each call is expanded into the function's body before it
is typed, over the call's own tables, as a `NormalizeRequiredFunction` call is (`compiler/spec/RelationGenerics`).

The library is `pure-library.ts DATACUBE_LIBRARY`, a `###Pure` section of functions in `datacube::ts`, each over any
table, in the conventions of §5.7: the table, a window (its line, its order and its frame) or the line and the dates
themselves, the column it reads (by `read`, never renamed), its parameters, and the column it writes. Its first
functions (2026-10-26) were one step each -- Pure's relations could not drop a column, so a function of two steps would
have left its first step's column in the caller's table; since 2026-10-27 a function hides its steps: `without` drops its
working columns, each call renames them so a caller's never meet them, and its declared return is held to each call.

The tile's queries call it by name (`Writer.library` and its kin), one family at a time as each is converted and held
to today's numbers (§5.7): the rolled statistics (`mean`, `sum`, `min`, `max`, `range`, `std`, `var`, each over a count
of periods with its ramp, and `overPeriod` over a calendar one), the returns (`returns` and `returnsPeriod`, a type
word as gs-quant's), `volatility`, and the levels and lags (`lag`, `lagPeriod`, `diff`, `diffPeriod`, `annualize`,
`first`, `last`, and `index`, `change` and `percentChange` of the line's first value, or their `...From` of its value as
of the first date shown, `valueAt`'s), and the multi-step windows (`zscores`, `winsorize`, `drawdown`, `maxDrawdown`,
`sharpeRatio` and `bollingerBands` over a count of periods; `zscoresPeriod`, `drawdownPeriod` and
`bollingerBandsPeriod` over a calendar one, each nesting `overPeriod`'s statistics, whose output columns are the
body's own by hygiene), and the self-joins (`percentiles` and `quantile` -- a median its 0.5th -- over the rows back,
`percentilesPeriod` and `quantilePeriod` over a calendar period, each the line joined to itself on its window, and
`quantileOfLine`, one aggregate a line), and the exponentials (`ewmaFrom`, the recursion's closed form in blocks,
the database's; `ewma`, `ewmStd`, Wilder's `smoothedMean`, `ewmaSpan`, `macd`, `rsi`, `exponentialVolatility`,
`exponentialSpreadVolatility` over it -- every exponential average the tile draws, an ATR's and a Keltner channel's
among them), and the technicals over a row's fixed roles (`trueRange`, `atr`, `stochastic`, `keltnerChannels`,
`donchianChannels` and its `...Period` form, `obv`, `vwap`), the row maths (`log`, `exp`, `sqrt`, `abs`, `power`,
`floor`, `ceil`, `not`, `filterValues`) and the functions along the line (`windowProduct` and `product`,
`geometricallyAggregate`, `smoothSpikes`, `volSwapVolatility`, `consecutive`, `count`, and the date's parts `day`,
`month`, `year`, `quarter`, `weekday`), and two columns of one table, aligned by the caller (each series on its own
dates, then joined: the tile's align), over the window's pairs (`correlation`, `rSquared`, `covariance`, `beta` -- a
hedge ratio a beta of levels --, `alpha`, `residual`, `trackingError`, `informationRatio`, `upCapture`,
`downCapture`, `leadLag`), `prices`, `dickeyFuller` and `halfLife`, the excess returns over a rate or a cash index
(`excessReturns`, `excessReturnsOver`), and the combinations of aligned columns (`compare`, `ifThen`, `union`; a
number a column of it), and the classical decomposition (`trend`, its cycle and half of it -- a window's reach, a
literal -- the caller's; `seasonallyAdjusted`, each row's place in the cycle a column, the tile's calendar), the total return with a yield or dividends (`totalReturn`, `totalReturnOf`), and the members of
a weighted sum or a basket as rows -- a row a member and date, its value and weight beside it (`weightedSum`, `basket`;
§5.7's long table: the writer concatenates its aligned members into one) -- `->datacube::ts::returns(over(~line, [~period->ascending()]), ~close, 1,
datacube::ts::Returns.simple, ~ret_2)->datacube::ts::volatility(over(~line, [~period->ascending()], rows(-21, 0)), ~ret_2, 22, ~per_year_3,
~vol_4)`. A calendar period's statistic is `windowPeriod`'s (on every date, however few values) and its ramp
`rampPeriod`'s (none before the first date of a column, that period on), `overPeriod` the two over one column; so a
volatility, a maximum drawdown, a Sharpe ratio and a VWAP over a calendar period (`volatilityPeriod`,
`maxDrawdownPeriod`, `sharpeRatioPeriod`, `vwapPeriod`) ramp from the first date of the series they read (a price's),
not of the column a statistic is over (its returns' or its drawdowns', a period later). Two columns over a calendar
period are `pairPeriod`'s (a correlation or a covariance of the pairs whose dates are in it; `slopePeriod` over it),
and each pair function, benchmark-relative statistic, lead-lag, product, vol swap, Dickey-Fuller and half-life has
its `...Period` form; `append` and `prepend` splice series aligned by the caller (`union`'s way). Every function of the
catalog is the library's (docs/PURE_LIBRARY_SPIKE_2026_10_27.md, "What stays DataCube's": the tile's shaping alone
stays). So are the tile's tables and views since the focused Pure audit (2026-10-28: what the tile's TypeScript still
wrote as maths, moved, each held to today's numbers): the periods a year of a tile of no frequency (`periodsPerYear`,
gs-quant's annualization factor), compare's rebase and % change (`index`, `percentChange` and their `...From`, a
seasonality's years each a line of them -- gs-quant's x / x0 - 1, the user's "match gsquant"), the statistics' figures
of a whole line (`performanceStatistics`, pyfolio's perf_stats: volatility, Sharpe ratio, deepest drawdown, compound
annual growth, skew, kurtosis, Sortino ratio, hit rate, value at risk, expected shortfall, best and worst, one row a
line; `calmarRatio` of two of them), the scatter's and the projection's least-squares line (`regression`, `fitAt`), the correlations
table (`correlationOf`), the drawdowns table (`drawdownsOfLine`), a condition's shaded runs (`runs`), the calendar of
returns (`calendarReturns`), the histogram (`histogramOfLine`) and the seasonality's range (`seasonalRange`). Since
2026-10-28's second pass (the user: "all the typescript math ... so that these functions are useable from platform
without TS"), so is every figure the tile's TypeScript still wrote as a sum, and each view is one call a person's own
Pure makes as the tile does (`timeseries-queries.test.ts` "a person's own Pure ..."): the statistics' line figures
(`summaryStatistics`: count, dates, first and last, least and most and their dates, mean, deviation, days, change and %
change, one row a line, beside `performanceStatistics`, which says the periods a year it annualized by and the
level's least as well), the seasonality whole (`seasonality`: each date's year and place by the tile's frequency, a
daily one's weeks each their last value, each year from its first under Compare, its last years, each place's range
and its span), the trend projected whole (`projection`: the line over its last values, its value at their ends and
ahead, its slope a day), VWAP's price times volume (`tradedValue`), the time fill's value between two in time
(`linearAt`, gs-quant's align and interpolate `time`) and a decomposition's place in its cycle (`periodIndex`: a
month's, a quarter's or a week's number; at days the row's own, a place read modulo the cycle). What stays the tile's
is shaping: its periods and their ties, its operands' merge and each value's neighbours under an alignment (the windows
that find them stay inline: a library call a value would nest them as many deep, and DuckDB's filter pushdown doubles
at each), a view's cut to the dates shown, a curve's dates, the columns it answers with and their order, and the
parameters it spells (a percentile's n of 100 as a fraction, which the planner takes as a literal alone; a cycle's
half, a window frame's literal). One
number moved with it: a correlation where a side does not vary over its pairs is none in every form of it -- over a count
of rows or a calendar period, squared, lagged, a table's -- where DuckDB had said NaN and Postgres none (the
conversion's review, review doc §18).

Planning a chain of calls cost 2^k for k calls until 2026-10-28 (core): a call of a generic function types its
arguments to choose its overload, then expands its body over them and types that, so each call typed the chain under
it twice -- sixteen of the library 13 s. A table an expansion's call typed is now typed once: the body's static fold
keeps it as it is (a `.columns` it reads is its typing's) and the body's typing finds it by its node
(`Overloads.inlineNormalized`, `Typer.withPretyped`;
`UserFunctionIntegrationTest` "a chain of calls ... plans once each", sixteen calls in under 5 s, 36 ms measured). And
DuckDB's optimizer (its filter pushdown) doubles its time with each window nested under another -- twenty windows each
over the last, 9 s -- so a figure of a whole line is one aggregate where it can be, never a window of its own: the
statistics' figures written as a window each, nested eighteen deep, ran 1.3 s where the writer's had run 130 ms;
`performanceStatistics` takes three windows (the mean, the running peak, the returns' ranks) and one `groupBy`, 25 KB
of SQL and 90 to 150 ms. An expected shortfall reads its values' ranks, never its line joined to its quantile (a join
writes the query under it twice). A model takes the library only where a query
calls it: the planner plans such a query over its model with the library after it (`lite-planner.ts` and
`planner.ts #over`, a planner of its own over that model), every other query over its model as it is, byte for byte.
A host that plans and runs is sent the library BESIDE the model (`planRequest`'s `library`), never inside it: Python's
engine admits a model by its own rules (a frame's, a warehouse table's, a project's, or one reading only the tables it
serves -- which it reads through the protocol's grammar, and the protocol has no type parameters), then plans over that
model with the library after it, taking only DataCube's own library, by its text (headless `pureLibrary`; engine.py
`_library_refused`, `_plan_with`), which declares no table. Python's frames, statistics and queries send it so: headless
`timeseriesQuery` and `gridTimeseries` say `library` beside a query that calls it, and `Timeseries._query` carries it
to the plan request. On a legend-engine server the library's functions are refused in the engine's own words. The Query window's **Library** tab shows it; a query of
a person's own may call it (`timeseries-queries.test.ts` "a person's own query calls the library").

Two lowerings it needed (core): a window aggregate's reduce may be a condition over several reducers
(`y|if($y->count() < $w, |[], |$y->average())`: `Lowerer.overReducers`, one CASE whose reducers are windowed one by
one), and an `if`'s empty arm in a window column is NULL, never an empty array (DuckDB refused the CASE). Pinned: the
numbers of `timeseries-queries.test.ts` (mean, simple and log volatility, weekly volatility, every function reading
returns) held to gs-quant's as before; "mean, returns and volatility call DataCube's Pure library by name" (and a query
calling none planned over its model as it is); `ExtendWindowCheckerTest$ConditionOverReducers` (windowed, grouped, an
empty arm); `UserFunctionIntegrationTest$RelationGeneric` (columns by name, a mean of a mean).

### 5.7 The library's conventions (the user, 2026-10-27: "yes take all your suggestions")

A spike wrote one function of each family of the tile's functions as a Pure function over any table and held it to
today's writer (docs/PURE_LIBRARY_SPIKE_2026_10_27.md): every family says itself so, and three primitives were missing.
They are built, each a register row: `meta::legend::lite::without` (S113: a relation less the named columns, typed as
the select of the rest; upstream has no column drop); hygiene (S112: each expansion renames the columns its body makes
and its declared return does not name, to names none of the call's tables has); and the declared return held to each
call (S112: every declared column present and of a conforming type, no other). A fourth came of the user's question
whether a function should take its columns as parameters or require a schema its callers rename into (2026-10-27:
parameters -- a contract's names become reserved words of every caller's table, cost renames at every call, take one
column a call and collide on their outputs, and cannot be declared, where `ColSpec<V⊆T>` is): `meta::legend::lite::read`
(S114: `$r->read(~close)` is `$r.close`), so a body reads the column its caller names without renaming it, and takes
the caller's window whole (a renamed table is not the window's). Every function of the library follows
these conventions, and every function of the tile's catalog is converted to one (the user, 2026-10-27: "yes convert
every function"), each held to today's numbers by `timeseries-queries.test.ts`:

- **Over any table, its return declared.** A function is generic over the table it reads (`Relation<T>`), names the
  columns it reads and writes by parameters (`ColSpec<V⊆T>`, `ColSpec<O>`), and declares its honest return
  (`Relation<T+O>`, `Relation<T+O1+O2>` for several outputs, the group's columns for a figure of each line). Its
  working columns are its own (hygiene) and gone before it returns (`without`).
- **Many series: a long table keyed by the series** (kdb+'s, panel pandas', Polars' and SQL's model). A series is a
  value of a key column, never a column: one row per series and date, an observation's fields (open, high, low, close,
  volume) its columns. Three kinds of function follow, by what they partition or group by:
  - *per series, over time* -- partitioned by the key: one call serves one series or a thousand
    (`$px->datacube::ts::zscores(over(~symbol, ~day->ascending(), rows(-19, 0)), ~close, 20, ~z)`);
  - *across series, at each date* -- grouped or partitioned by the date: a basket (the prices joined to a table of
    weights, `basket | symbol | weight`, then summed per basket and date; rebalanced, the weights in force as-of
    joined), `weighted_sum` (each series concatenated as rows tagged with a member key, then the same), a rank or a
    z-score across the members (`over(~day)` where a series' is `over(~symbol)`);
  - *pairwise* -- a self-join on the date: a beta or a correlation of every member against a benchmark in one call (a
    key and the benchmark's value), a correlation matrix as `s1 | s2 | corr` (the members joined with themselves on the
    date, `s1 < s2`, grouped by the pair).
  No function takes a list of columns. A fixed number of series only where the roles are fixed (a series and its
  benchmark; a true range's high, low and close, which are one row's fields).
- **An option is an enumeration's value, checked when the query compiles** (the user, 2026-10-28: "are you actually
  trying to bake the errors into sql or write the query in a way that compile time checks the parameters? I hope it is
  the second"). A function's choices -- a return's type, a period statistic, a filter's operator, a spike's threshold,
  a run's direction, a decomposition's model, a basket's rebalance, a seasonality's grain and compare -- are each typed
  by an enumeration of the library's, gs-quant's own way (`datacube::ts::Returns.log`, its `Returns.LOGARITHMIC`;
  `SeasonalModel`, `Rebalance`, `Grain`, `Compare`, `Statistic`, `PairStatistic`, `FilterOperator`, `ThresholdType`,
  `Direction`): a value the enumeration has not (`Returns.Log`) is refused by the type checker before any SQL is
  written, in its words (`enumeration datacube::ts::Returns has no value 'Log'`), and a word where an option is
  (`'log'`) by its type. An `if` on an option folds where the call is expanded to the branch its value names
  (`StaticFold.EnumToken`, comparing spellings, computing nothing -- as a spelled string's comparison already folded),
  so the other branches never reach the SQL: a period's mean is its `AVG` alone. A closed enumeration leaves no default
  branch: each function's last branch is its one remaining value. Where an option was a string (to 2026-10-28) a
  misspelled one ran as the last branch -- `returns(..., 'Log')` as simple returns -- since only the tile, checking its
  words first, called them; a projection's time unit is a `DurationUnit` (its day's slope the database's, by that
  unit), where a number of 1 or 86400 left every other number meaningless.
- **Windows: what, over which rows, and when ready, apart** (gs-quant's `Window(w, r)`; pandas' `rolling`,
  `expanding`, `min_periods`). A window statistic takes the window as a value -- `win: _Window<T>[1]`, the line, the
  order and the frame -- and a ramp, the observations before a figure is given, and reads its column with `read`:
  `$px->datacube::ts::mean(over(~symbol, ~day->ascending(), rows(-19, 0)), ~close, 20, ~ma20)`. One `mean` is a
  count's (`rows(-19, 0)`, ramp 20), the expanding one (`rows(unbounded(), 0)`), the whole line's (`over(~symbol)`)
  or a centred one (`rows(-2, 2)`). Not frames: an exponential average takes an order and its decay (an α from a span or a
  half-life); a lag takes an order and a count, or a calendar period (an as-of join). A function that needs the line or
  the dates themselves (a base over the whole line, a self-join, an as-of lag) takes them as column specs.
- **Calendar windows exact.** gs-quant's window of a period is `(date − 3m, date]`, open at its start; SQL's
  `RANGE BETWEEN INTERVAL 3 MONTH PRECEDING AND CURRENT ROW`, which lite lowers, is closed at it. They differ only where
  a row falls exactly a period back, and the library keeps gs-quant's: a calendar statistic is its own function (the
  line joined to itself on the dates in the period, the spike's `meanOver`), never the SQL frame.
- **The tile writes calls.** `timeseries-query.ts`'s writer turns a tile's expression into the library's calls
  (`mean(x, 20)` the count's window with ramp 20, `mean(x, 0)` the expanding one, `mean(x, 3m)` the calendar
  function), and the planners and hosts carry the library as they do today (§5.6).

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

## 11. The next phases (the user, 2026-10-26: "lets do all the ones we can in phases")

From a survey of the tools a person would otherwise open -- Bloomberg (HRA, GF, seasonality), Macrobond (scatter,
frequency harmonization, forecasts, X-13), FRED (frequency aggregation, formulas, recession bars), TradingView (drawing
and measuring, studies, replay), Koyfin -- and gs-quant 2.2.1's own source for the functions this catalog follows. Each
phase lands with its tests against an independent reference, its gallery tile, Python's side by the one rule, and its
dated budget notes; every figure still the database's.

- **Phase 1, data: mixed frequencies and gs-quant's remaining functions.** A tile's `align` -- how two series' dates are
  combined at an operator or a two-series function: `intersect` (the dates both have, as now), `step` (every date
  either has, each side its last value at or before it -- before its first, its first: gs-quant's STEP, its arithmetic's
  default), `time` (linear in time between each side's observations, inside them: gs-quant's TIME), `nan` (every date,
  none where a side has none), `zero` (none as 0); `interpolate(x, y, method)` (x onto y's dates). The date parts
  (`day`, `month`, `year`, `quarter`, `weekday`), `if`, `not`, `filter`, `compare`, `consecutive`, `first`, `last`,
  `count`, `product`, `geometrically_aggregate`, `excess_returns` (over a benchmark series, or a rate accrued ACT/360),
  `smooth_spikes`, `vol_swap_volatility`, `union`, `append`, `prepend`, `weighted_sum`, and `trend` and
  `seasonally_adjusted` -- gs-quant's classical decomposition (a centred moving average, each position's mean of the
  detrended values), which the database computes; over the series' own observations, a cycle counted in them (a year of
  months 12, of weeks 52, of business days 252), where gs-quant first fills business days to calendar days (a
  deliberate difference: no calendar of days the data does not have).
  *Built (2026-10-26).* Two parts merged by one FULL join on their line and period (`merged`: each part read once, so
  a nesting of them grows linearly -- the Phase 1 review's 1, docs/TIMESERIES_REVIEW_2026_10_25.md §12), a one-line
  part beside a split's under the source's lines; each part's values put onto the dates by its own windows over that
  relation (`fill`: none for nan, 0 for zero; for step and time, its last and next period with a value, and the value
  there), keeping their names, so every function after it reads them as before. `if` is on its flags' dates, `append`
  and `prepend` on gs-quant's (x's to its last row, y's after; x's before y's first, y's), `interpolate` as gs-quant's
  (intersect unless told; its step walking the dates asked). A correlation, a beta, a regression and a candle read the dates both
  have whatever the tile's align (gs-quant's econometrics do). `filter` drops its rows -- a filter of a window's figure
  is the database's QUALIFY, so a window before it read every date and a window after it reads the dates left (both
  pinned). `excess_returns` over a number is a rate accrued actual/360; over a series, a cash index compounded on the
  dates both have. The macro sample (a price index, retail sales with a December peak, the unemployment rate at each
  month's last calendar day) is the gallery's monthly grid. Each function is held in `timeseries-queries.test.ts` to
  its gs-quant definition written out, the decomposition to statsmodels' (an even cycle of four weeks, an odd one of
  three months).
- **Phase 2, everyday use.** Swap an instrument across every series of the tile (Bloomberg's change of security,
  TradingView's symbol change), screen and Python by one edit; the data as a table in the tile, copied as Excel pastes
  it; export with a title, a subtitle and a source note, as SVG and at high resolution; each pane's axis inverted or
  ranged by hand; a series' window and parameters on a slider; the query shown (its Pure and its SQL).
  *Built (2026-10-26).* **Swap** (`timeseries-edit.ts` swap; Python's `swap(instrument, to, source=)`): every match of
  an instrument on its source's instrument column (`close[SPX]`, not `close[venue=SPX, ...]`'s other column), of one
  source when told; a series that read it renamed where its name says the word; refused where none reads it, and for a
  word in a column of numbers. The window lists the instruments the series read and the source's own (its instrument
  column's values, the database's). **Data** (the statistics panel's tab; `timeseries-option.ts timeseriesTable`;
  Python's `table()`): every line's figures side by side by date over the dates shown -- a band's two edges, a candle's
  four prices; no line hidden, no condition; each named as the screen names it, Python's by headless
  timeseriesLineNames -- the latest 500 listed latest first in each line's format, Copy taking every date oldest first,
  tab-separated as a spreadsheet pastes it; arranged, nothing computed; a line of two values on a date refused, never
  one dropped, and a day written as a date and as a midnight date-time one row. **A picture** (PNG and SVG on the toolbar): the
  lines as drawn, over the dates shown, drawn again off the screen with the tile's title, its subtitle and its note (the
  view's `subtitle` and `note`, drawn on the screen too, each one line) and its lines' legend above the panes -- as tall
  as on the screen (`tsPictureHeight`), no navigator under them -- on the page's surface -- a PNG at three times
  its pixels, SVG by ECharts' SVG painter, fetched with the first SVG alone (a chunk of its own, budgeted; the canvas
  painter now imported by its own module, as the charts are, so the `echarts/renderers` index brings neither). **A
  pane** upside down (`invert`: a yield's, a spread's) and its axis' least and most by hand (`min`, `max`, the
  reader refusing them the wrong way round). **Query** (the toolbar; Python's `query(series)`): each series' query as
  DataCube wrote it, printed by the compiler, and the SQL the database ran for the answer drawn -- carried with every
  answer (`RunOutcome.sql`), never planned again; Python plans it (the engine's dry run, `plan.json`), nothing run.
  **Sliders**: each number of a series' outermost function (a window's length, a beta) a slider in its editor -- a
  number of no bound either side of 0 -- the expression moved as it is dragged, the series drawn again when it stops
  (400 ms), by the tile's one rule, as a try the page is not told of: OK keeps it, one change (one step to undo);
  Cancel, Escape, Remove or another editor take it back, and a change told meanwhile (Log) is made to the tile as the
  page has it. Python's `frame()` and `query()` read a tile as the screen draws it, an index's base its first date
  shown.
- **Phase 3, new views of a tile.** A scatter of one series against another over the dates shown, its regression line
  and statistics the database's, coloured by time, the latest point marked (Bloomberg HRA, Macrobond's scatter);
  seasonality -- each year a line over its calendar, the range and average of the years before (the commodities
  standard) -- and periods overlaid from their start; a histogram of a series' values, its normal and its value at risk
  marked; a curve -- a split's values across its key (tenors) at chosen dates; a Calendar tab, each month's and each
  year's return.
  *Built (2026-10-26).* A tile's view `mode` (`scatter`, `seasonality`, `histogram`, `curve`; absent, along time) and
  its settings -- `of` (the series it reads, where it fits the mode; else the tile's first it can draw,
  `timeseries-query.ts viewSeries`, the one rule the screen and Python read), a seasonality's `years`, a histogram's
  `bins`, a curve's `dates` (`latest`,
  a period back from it, or a day) -- each judged by the tile's reader and changed by its one edit (a series taken off
  leaves the `of` that read it). Each view is one query of its series' relations (`scatterQuery`, `seasonalityQuery`,
  `histogramQuery`, `curveQuery`, `calendarQuery`), every figure the database's: a scatter's points on the dates both
  have shown and y's least squares on x over them (its slope the sample covariance over x's variance, the intercept
  through the means, R² the correlation squared, the fit at the least and the most x); a seasonality's last years, each
  date's place in its year by the tile's frequency -- a daily series' the week of the year, each year its week's last
  value, so every year before has every place -- and at each place the least, the most and the mean of the years before
  the latest, compare taking each year from its own start, refused at a yearly frequency; a histogram's every bin of
  one width (the most in the last; an empty one counted 0, numbered from the first ten values' rows each with each), a
  normal of the values' mean and sample deviation as the counts it would give each bin, and the 5th percentile
  (continuous, as pandas'); a curve's lines as they are (the tile's compare the time view's), each its last value at or
  before each date asked (a day the whole of it), and that value's own day -- an earlier one drawn hollow and said
  (`curveNotes`) -- one aggregate a line (a `maxBy` keyed by the period's number); a calendar's month-end levels, each
  month's simple return from the month before's by its number (none after a missing month, as pandas' resample) and
  each year's, read at least monthly (refused for a quarterly or yearly tile). Drawn by `timeseries-views.ts` (placed
  and worded, nothing computed; a curve's keys ordered by `curveOrder` -- tenors by their span, 3M before 2Y, a money
  market's ON, TN and SN first, then any other key by its text -- which Python asks too, headless
  `timeseriesCurveOrder`); the view's query asked alone when only the view's settings move, its SQL in the
  Query window, its picture made as the time view's is. The Calendar tab asks for the line picked, once for the rows
  drawn. Python's `scatter()`, `seasonality()`, `histogram()`, `curve()` and `calendar()` are the same queries
  (headless timeseriesQuery's `table`), held in `test_page.py` to numpy's and pandas' figures; the screen's in
  `timeseries-view-dom.test.ts` to the sample's rows; the gallery's Other views sheet has one of each.
- **Phase 4, analysts' packs.** Against a benchmark: tracking error, information ratio, up and down capture; a total
  return from a dividend; a basket of weights, rebalanced; lead and lag correlations; a pair's cointegration (its
  residual's Dickey-Fuller statistic) and its half-life; ATR, the stochastic oscillator, VWAP, OBV, Keltner and Donchian
  channels.
  *Built (2026-10-26).* Fifteen functions of the catalog, each one query of its series' relations, every figure the
  database's (`timeseries-query.ts analystCalls`, `basket`, and the main switch's Dickey-Fuller regression). A
  signature may name what each of its series is (`TsSignature.reads`: a true range's high, low and close), which its
  refusals and the reference say ("atr reads three series (high, low and close)"). *Technicals* of a candle's columns,
  each series on its own dates, then joined: `atr`, Wilder's average of true ranges (TA-Lib's: from the w-th, the mean
  of the w after the first, then `((w - 1)·y + tr) / w`, the tile's `smoothed`); `stochastic`, Lane's %K over w (none
  where the high and the low are one); `keltner_channels`, k average true ranges (over a) either side of the close's
  exponential average of span w (`ewm`, its weight of the one before (w − 1) / (w + 1), pandas' adjust=False);
  `donchian_channels`, the lowest low to the highest high of w; `obv`, the running total of each volume signed by its
  close's move, 0 at the first (StockCharts'; TA-Lib starts at the first volume); `vwap`, Σ price × volume / Σ volume
  over w (0: to date). At a frequency, or of several rows a date, each series is taken by its role (`rolled`, a
  candle's way; the Phase 4 review's 1: a week's last high and last volume had been read): a high its most, a low its
  least, a close its last, a volume and a dividend their total, a price times its volume on its own dates, totalled;
  Keltner's band is drawn once its average holds w closes. *Against a benchmark*, each series' simple returns on its own dates, then the dates both have:
  `tracking_error` (their difference's annualized deviation, in percent), `information_ratio` (its mean over that
  deviation, annualized), `up_capture` and `down_capture` (the first's mean return on the periods the benchmark rose,
  or fell, over the benchmark's then -- arithmetic means, a ratio). `total_return`: each period's (price + the dividends
  since the price before) / the price before, compounded from the first price, on the price's dates -- a dividend of a
  date with no price paid at the next (their running total over every date either has, kept by a filter reading that
  total: a filter of a plain column folds under its select's windows, the rule §5's drawdowns taught) -- or, of a number, a
  yield a year accrued day by day. `basket`: gs-quant's `backtesting.basket_series` with no costs -- each series held in
  its weight from 100, on the dates all have, units fixed between rebalances: each date's period its own (daily), or
  the weeks or months from the first date (weekly: the days since over 7; monthly: the months since, less one before
  the first date that many months on, the database's `adjust`, which keeps its day or takes its month's last as
  relativedelta does), so a rebalance is the first date on or after each, as gs-quant's `min(cal[cal >= date])`; the
  basket 100 times each period's growth into it compounded, times its growth within (1 + Σ w (price / its price at the
  period's first date − 1)), none from a price of none on (gs-quant's NaN carries), any weights (a long and a short,
  1 and −1: a basket divides by nothing). `lead_lag`: the rolling correlation of the first's returns with the second's `lag`
  observations before (below 0, after), each lagged on its own dates. `dickey_fuller` and `half_life`: b of
  Δx = a + b·x[-1] by least squares over w differences (w + 1 values; their covariance over x[-1]'s variance), its
  t-statistic from the residual variance (`√((var Δx − b² var x[-1]) / ((n − 2) var x[-1]))`, statsmodels' adfuller of
  no lags over those w + 1 values; a w of 1 or 2 refused), its half-life −ln 2 / ln(1 + b), none where b ≥ 0 or ≤ −1;
  a spread written with its ratio is tested as it is -- a running `residual(y, x, 0)` is each date's own fit, not Engle
  and Granger's one fit of the whole, so the words claim no cointegration test. Held in `timeseries-queries.test.ts` to each textbook's
  arithmetic over a sample of its own (TA-Lib's ATR and %K, the channels, OBV, VWAP, the active returns' deviation,
  capture, Pearson's correlation of lagged returns, an OLS fit written out) and the basket to gs-quant's own loop,
  daily, weekly and monthly (a 31st's month its last day, a rebalance on a date one series lacks taken at the next);
  Python's to pandas in `test_page.py`; every one on its family's gallery sheet.
- **Phase 5, on the chart itself.** A measure dragged between two dates (the change, the % and the days -- a statistics
  query over that span, the database's); drawn lines and notes; events marked from a grid's rows (earnings, meetings:
  built with Phase 6's D4 as marks from a grid's rows, below);
  shading from a 0/1 column in one step (recession bars); a replay stepping the window's end through time; a period
  brushed on the chart filtering the page's other grids; a trend's projection forward.
  Built so far (2026-10-27), each by the tile's one rule, the screen's and Python's alike:
  the **measure** -- a Measure button; a drag across the chart is a span (no pan while measuring), each line's first and
  last value, change, % and days over it asked of the database by the statistics' own query over those dates
  (`statsQuery`, a stat `days` added: `dateDiff` of the span's ends), Python's `statistics(start, end)` the same rule;
  **shade from a column** -- the change `shade` (a condition series `<column>[where] != 0`, named after the column, its
  periods the database's as every condition's are), added in Settings or by Python's `Timeseries.shade`;
  **drawings and notes** -- two annotation kinds, `line` (`{from, to, pane?, label?, color?}`) and `note` (`{at, text,
  pane?}`), each point a date and a value as its pane draws it (a person's own: no figure of the database's), drawn per
  pane on its first line's axis, left out under a compare and in a pane that draws no line (each said); the screen's
  Draw button (while drawing every pane carries its drawings' series, and the renderer reads a click in its axes: the
  pane under it, the nearest day, the value; two clicks a line, one and its text a note), Python's `annotate`;
  **a trend projected** -- an annotation `projection` (`{series, window?, ahead?, label?}`, 63 observations and `3m` by
  default): the series' line by ordinary least squares over its last `window` observations on their time (each date's
  day number; a date-time's seconds), the slope their sample covariance over time's variance, and its fit at the first
  of them, at the last and at the date `ahead` past the last (`adjust`) -- one row, every figure and date the
  database's (`projectionQuery`, an ask of its own, `projection`); drawn dashed through its line's last observations and
  on past them, in its line's colour on its axis; refused under a compare (a line's own values; none asked, the drawing
  says so); taken off with its series. Settings' Project adds one; Python's `Timeseries.project` adds one and
  `Timeseries.projections()` reads them -- held to numpy's `polyfit` there and to the fit written out in
  `timeseries-queries.test.ts`; **the replay** -- a Replay button stepping the zoom's end through the dates the lines
  show, from a quarter in to the last (about 60 steps, a tenth of a second each; Stop leaves it where it is): the chart's
  zoom alone, each step its lines as they stood that day, the figures of the dates shown asked once it settles -- the
  screen's alone, Python's by nature a loop over `frame(end=...)`. Events from a grid are Phase 6's D4 below.
  Reviewed independently (2026-10-28) and fixed, each pinned by a test: a projection of a series read from the first
  date shown (an index, a change) was fit on its values from its first date of all -- it is asked over the window's
  first date as its line is, and asked again when the window moves; a zoom inside a projection's window dropped its
  points (`filter` mode) -- it is drawn by mark lines, its points carried unseen so the time axis reaches its end; a
  pane's drawings followed whichever line was shown first -- they are on the pane's first line by the tile's order
  (`drawingsLine`), left out and said where it is hidden, read from the first date shown, or none; a chart made again
  forgot Measure and Draw; a span measured outlived a change of the tile (measured again) and a view (put away), and a
  drag in a view measured its x as dates; the replay outlived a change; a tile read by the time of day snaps a point to
  its day, so Draw says so there; a projection on a log scale (no straight line there) is left out and said; a measure
  dragged in a lower pane is measured, each end its nearest day; a double-click while drawing or measuring is no event;
  Shade with nothing to shade by says so; a shade's name is trimmed by the rule, the screen's and Python's alike;
  Python's `projections()` reads over the tile's range, as the screen does.
  Left, out of this PR's scope (the user, 2026-10-28: "put the brush work out of scope for this PR and revisit next"):
  a period brushed to focus the page. A tile reads its grids through their filters, so a span brushed onto its own
  source's filter would cut the tile itself, and every other timeseries tile over that grid would lose the history its
  rolling figures need. The options put to the user: (A) the charts' rule on the tile's sources (each grid filtered by
  its own date column, the tile reading past its own brush, a class's grids by a page filter); (B) a page filter for
  class grids alone; (C) every grid of a same-named date column (refused: a guess); (D) a page period -- every
  timeseries tile shows it as its window, its data whole, and a grid or chart filters by it through a date column it is
  given -- recommended, built in two steps (the linked window first, then the grids opting in); (E) a linked window
  alone. The gesture proposed: the Measure box's "Show this period everywhere". Open with it: whether the brushed tile
  zooms too, or keeps its whole view with the period shaded.
- **The next PR's follow-ups, beside the brush** (the user, 2026-10-28, each "so we don't forget"):
  - *A series written in Pure* (the user's "B"): a tile's series given as a Pure query of its own (a `pure` beside or
    instead of `expr`), which the tile runs as written but for its dates, values and point cut -- the formula then one
    shorthand for that same Pure, never a second meaning. Until then the code a tile writes shows, beside each formula,
    the Pure it becomes ("A", this PR).
  - *The formula language as a Pure island, `#Quant{...}#`*: the tile's gs-quant formulas (`mean(close[SPX], 50)`) are
    a DSL over Pure that DataCube's TypeScript reads today (timeseries-expr.ts, timeseries-query.ts); read instead by
    the compiler, as `#SQL{...}#` reads Postgres SQL and `#IQ{...}#` its own (docs/LEGEND_SQL_STUDY_2026_10_11.md,
    docs/CYPHER_STUDY_2026_10_15.md: one translator per language into one target), so a formula is Pure wherever Pure
    is written -- Studio, Python, a saved query -- and the tile's writer shapes alone.
  - *The SQL's size* (proposed 2026-10-28; built 2026-10-30): the lowering pastes a column's definition at each use when
    it folds consecutive extends into one SELECT, and a `sqrt` guard spells its argument some 27 times, so one
    volatility series was 25 KB of SQL, three quarters of it one CASE copied 59 times. The fold budget charged a read
    once and never saw a rule's copies; it now counts the copies a pass writes, a rule's included (`Fold.overCopied`),
    and a projection whose copies pass the budget stays in the select below, read by name. And an integer division by
    a written literal other than 0 carries no zero guard (`DecimalKindRules.divide`: the token compared, nothing
    computed). Measured by `//datacube:timeseries_sql_measure` (twelve of the tile's queries over six years of three
    instruments, on DuckDB-WASM, the median of five runs, measured on three runs before and two after): a 63-day
    volatility 25,188 bytes to 7,821 and 84-132 ms to 27-28; its z-score 25,876 to 8,539 and 83-122 ms to 28-31; RSI
    22,430 to 12,267 and 76-92 ms to 58; the other nine byte for byte as before. Left: the
    statistics' 23 KB is three sqrt guards of one column, 1.9 KB each -- the guard's message spelling a float as Pure
    prints it -- and a volatility's window writes its guarded value twice (the ramp's count and the deviation): the
    library's shape, not the fold's. Pinned in `ScaleCostsDuckDbTest`.
- **Phase 6, charts from a model** (the user, 2026-10-27: "start all the charts from Class models projected to Relation
  instead of starting with Relation tables -- we should use the same Stress data model ... this also will give us richer
  joins to reference data and be able to plot trades"). A tile's grid reads a class of the stress model
  (`core/src/test/resources/stress`) -- `MarketQuote.all()->project(~[symbol: x|$x.instrument.ticker, ...])`, the grid's
  class source (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §16.1) -- so its joins are the model's associations: an
  instrument's sector, exchange and currency to split and filter lines by, a quote's trades through its instrument. The
  library does not change (each function takes any relation, §5.7); what is new is the data and the source.
  D1: a charting dataset of the stress model's own tables -- years of daily quotes of about ten instruments, those
  instruments, their sectors, and trades on them -- beside the corpus's seed, which stays as it is (its five quotes are
  an oracle's: a quote with no time, one of an instrument that is not there). D2: core plans and runs a library call
  over a projected class through the stress mapping, on DuckDB, and the cost of planning through a mapping is measured
  (measured 2026-10-27 over today's tables: `//python:engine_test` ran its 320 tests in 310 s in a cloud container, the timeseries' about 16 s of it, the library's planning a part of those; main's CI ran it in about 137 s -- the keeper's convergence test the longest, 36 s here). D3: the stress model a sample project a page opens,
  the tile over a class-source grid, the samples, the gallery and the tests from classes; a table stays a source a person
  may use, no longer the one shown. D4: trades as buy and sell marks on the price line, from `Trade.all()->project(...)`
  -- Phase 5's events from a grid, made concrete. D5: lines split and filtered by reference data through the
  associations. D6: Python's side by the one rule. After the library's conversion (each function held to today's
  numbers over today's tables first), before the library's review, which covers both.
- **Phase 6's plan, as built (2026-10-27).** The Deephaven line decided the same (its DH34, the user, 2026-10-25: "one
  trading model for everything"): the stress corpus's classes through `stress::AllMapping`, what a screen needs and the
  corpus lacks added to the corpus, never beside it. So the charts take its pieces rather than a second model:
  (a) **the one trading model as one text**, `//scripts/corpus:trading_model` (the Deephaven line's generator, taken as
  it is: the linked projects and the stress files without legend-lite's exclusions, the services or the test data --
  2.27 MB, held by the WebAssembly planner in 4.4 s, a class query then planned in 5 ms, its measure); (b) **a history
  of the corpus's own tables for charting** -- EXCHANGE, SECTOR, INSTRUMENT, MARKET_QUOTE (a quote a day: its open,
  high, low and close, its volume, its time the close) and TRADE (the bank's buys and sells on them) -- written by one
  deterministic generator for the tab's DuckDB and the tests, under the store's own table names, beside the corpus's
  seed; (c) **core first**: a library call over a projected class planned through the mapping and run on DuckDB, held
  to the same figures over the same rows as a table, and the planning cost measured; then the tile over a class grid
  (D3), the trades as marks (D4), lines by reference data (D5), Python (D6).
  Built (2026-10-27): (a) and (b) (`stress-markets.ts stressMarketCsvs`: the markets sample's days, `samples.ts
  marketBars`, written into EXCHANGE, SECTOR, CURRENCY, INSTRUMENT, MARKET_QUOTE and TRADE, every column the store
  declares in its order -- held to the store's text by `stress-markets.test.ts`); (c) a level, a volatility, a
  correlation, returns and a calendar beta over `MarketQuote.all()->project(~[ticker: x|$x.instrument.ticker, ...])`
  through `stress::AllMapping` equal to the same series over the sample's one table, date for date. Measured there (the
  WebAssembly planner in node): the model held and a first query planned in 5.8 s, a query after it in 7 ms -- and a
  query calling the library holds the model AGAIN with the library after it (the planner's `#over`), 5.4 s more: a page
  over the trading model with a timeseries holds it twice, which D3 makes once.
  The rows as a page carries them (2026-10-27): the markets sample's tables as the project's own Data
  (`stressMarketsData`, 1.87 MB of CSV in one relational Data element), made by the store's own declaration and filled
  by the planner's `projectData` (C5) -- each column the store's type, never a reader's guess -- the same series again
  equal to the table's. The trading model leaves out the corpus's eleven `###Data` seeds for it (two contend for one
  table). Measured: the model held and its Data filled in 10.4 s, a first query then planned in 5.2 s (the same text
  compiled again by the planner that plans it), a query after in 15 ms -- with a library call's hold, three compiles of
  about 4 MB, some 15 s, before a page draws.
  D5 (2026-10-27): a quote's instrument's sector and exchange projected beside it (`$x.instrument.sector.name`,
  `$x.instrument.exchange.name`), and each series over them held to the instruments they name on the sample's table:
  `close[sector="Energy"]` is XOM's close, a sector's volume summed its instruments' added, a split by ticker within a
  sector a line per instrument of it, a split by exchange a line per listing venue.
  D3 (2026-10-27): the site serves a markets project (`BUILD.bazel :markets_project`, config.json `projects[]`
  demo:markets:0.0.0: the trading model and the markets Data), and an example page over it ("Markets on the trading
  model", `examples/markets-classes.ts`): two grids, each a saved query of the project (`MarketQuote.all()->project(...)`
  with the reference data, `Trade.all()->project(...)` with its side as its enumeration's name), and three sheets -- by
  sector, by listing venue, the bank's trades against the close in one tile over both grids. The Open window offers it
  where the page compiles against that project. Opening it was slow for a reason of its own: a saved query is typed by
  two `lambdaRelationType` requests, each carrying the model's text, and the module compiled the text again for each
  (5-6 s apiece), then once more for the project's database. The Deephaven line's two core fixes, ported unchanged
  (`HeldModels.withText`: a pure/v1 request whose model text the host holds answered over the held model;
  `ModelBuilder.legacySetsWithId`), with the planner's typing holding its model before it asks, the project's database
  said over the held model (`Boundary.projectData`) after the query is typed, and a project's elements read once: the
  first saved query opened in 7.8 s on a cold module (13.5 s before), the second in 2.6 s (12.9 s). In the browser
  (verify-charts) the example's first sheet drew 11.9 s after the click, 30.7 s before; the markets example over the
  sample's table, 2.4 s. What remains: the model held (about 4.5 s), its elements read (2 s, once a project), and a
  tile's first library call, which holds the model again with the library after it (3.7 s) -- the planner's `#over`
  compiling the model and the library as one text. Holding the library over a held model is a core question (a model
  context extended by a few functions); the D4 and Python steps do not wait on it.
  D4 (2026-10-27), Phase 5's events from a grid with it: **marks from a grid's rows**, an annotation of its own
  (`TsMarks`: `{kind: 'marks', source, where?, at?, text?, shape?, pane?, label?, color?}`) -- each row of a source's
  grid (its filter as the tile reads it) that its matches pick, at its date as a series' points are (a date-time its day
  under a frequency), on its `at` column's value in its pane (a trade at its price: a triangle up for a buy, down for a
  sell, a dot), else a line at its date across every pane (an earnings day), its `text` column beside it. Its rows are the
  database's (`marksQuery`: filtered, matched, latest first, at most `TS_MARK_LIMIT` = 2,000 and one more, so a cut is
  said -- `marksCut`, the one sentence the banner and Python's warning say); a refusal is said in the banner, never drawn
  as no marks; marks in a pane that draws no line are said (`marksUnplaced`). It is a question the tile asks like any
  other (`tileAsks`' `marks`, listed in the Query window and by Python's `queries`), added in Settings (the source, the
  rows matched as a column's brackets are typed, the column drawn at, what each says, its shape, pane and label) or by
  Python's `annotate`, its rows read by Python's `Timeseries.marks()`. The example's trades tile draws the bank's Apple
  trades so, each held to its TRADE row through the mapping (`stress-markets.test.ts`).
  D6 (2026-10-27), Python by the one rule: the example page, written by the screen's own builder (`BUILD.bazel
  :markets_page`), loaded by Python over the markets project as its host's depot gives it (`//python:markets_test`, in
  the warehouse lane beside `engine_test`, the 4 MB model compiled once for it): each grid a saved query read through
  the mapping, its rows the project's Data (13,040 quotes: ten instruments on the sample's 1,304 business days; each
  instrument's sector and exchange through the associations; the trades' side by its enumeration's name); a line per
  instrument of a sector, every day of each; Apple's close as the quotes' grid holds it; the trades as marks
  (`Timeseries.marks()`), each a row of the trades grid; the marks among the tile's `queries()`. Phase 6 is built.
- **Not built, and why:** X-13 seasonal adjustment and `smooth_outliers` (iterative procedures no query states; gs-quant's
  own seasonal adjustment is the decomposition above); PowerPoint (a document writer of its own in the bundle; the page
  already exports HTML and PDF); alerts (a scheduler and a channel outside a page); `repeat`, `date_range`,
  `day_countdown`, `align_calendar` (dates the data does not have, or business-day calendars, which no source here
  holds); `generate_series` (random data); `corr_swap_correlation` (an index's constituents and weights).
