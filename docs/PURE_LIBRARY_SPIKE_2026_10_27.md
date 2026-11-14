# The Pure library spike (2026-10-27)

The user, 2026-10-27: "Should we try to implement all the chart functions and see if everything holds? Then do the
without/except function?" -- and, of the order proposed: "Start it". Before building the primitive that drops a
function's working columns, one function of each family of the timeseries tile's functions was written as a Pure
function generic over any table (docs/SEMANTICS_REGISTER.md S112) and held to the numbers today's writer
(`datacube/src/timeseries-query.ts`) gives for the same expression, over the prices of `timeseries-queries.test.ts`, on
DuckDB, through lite's planner. The test is `datacube/test/pure-library-spike.test.ts` (`//datacube:pure_library_spike_test`).

## What holds

Every family the tile's ~100 functions fall into says itself as a Pure function, and every one matched today's writer
on every date (to 1e-9 relative):

| Family | Its functions | The representative | Held |
|---|---|---|---|
| One window, ramped | mean, sum, min, max, std, var, range, count | `datacube::ts::mean` (shipped) | yes |
| A row of a lag | returns, diff, lag, annualize, prices; row arithmetic: log, exp, sqrt, abs, power, floor, ceil, not, filter, compare, if, date parts | `datacube::ts::*Returns` (shipped) | yes |
| Multi-step windows | zscores, drawdown, max_drawdown, sharpe, winsorize, vol_swap, smooth_spikes, product, geometrically_aggregate, volatility | `spike::zscores` | yes |
| Several outputs | bollinger_bands, keltner, donchian, ohlc | `spike::bollinger` (lower, upper, width) | yes |
| A base over the whole line | index, rebase, change, percent_change, first, last | `spike::index` | yes |
| Two columns of an aligned table | correlation, beta, cov, alpha, r_squared, hedge_ratio, residual, tracking_error, information_ratio, up/down capture, excess_returns, union, append, prepend, interpolate, lead_lag; atr, stochastic, keltner (three); obv, vwap, total_return | `spike::correlation` | yes |
| Two tables of different columns, aligned | a tile's sources aligned on their dates (intersect; step, time, nan and zero fill by windows) | `spike::pairOf`, generic over two schemas | yes |
| A self-join over each window | percentiles, percentile, median, rolling percentile | `spike::percentiles` (join, group, join back) | yes |
| A calendar period | any window of `3m`; returns, lag over a tenor | `spike::meanOver` (self-join on `adjust` dates), `spike::lagBack` (as-of join) | yes |
| A group by the line | the statistics, drawdowns, calendar, histogram, seasonality and curve tables | `spike::maxDrawdownOf` (its working columns go with the group) | yes |
| A partition by a column of its own | consecutive, shading runs, a decomposition's cycle places | `spike::consecutive`, over a library call | yes |
| The deepest closed forms | exponential_moving_average, exponential_std/volatility, macd, rsi, smoothed_moving_average | `spike::ema`: fifteen steps, partitions by its own columns | yes |

Also found: a word told as a parameter (`'simple'`, `'log'`) is compared when the call is expanded, so one function of a
type keeps one branch and its SQL carries no other (`spike::returnsOf`); and the SQL a library function plans to is of
the writer's size (the exponential average: 5,682 characters, the writer's 6,733). A function body of several statements
(`let`) ends each statement after the first with `;`, the last too (legend's grammar).

## What is missing

1. **Dropping a function's working columns** (GAP 1, **closed 2026-10-27** by `without`, register S113).
   `select`/`project` keep columns by name, and a function generic over the table cannot name the caller's: so every
   multi-step function's answer carried its `ts_*` columns. `meta::legend::lite::without(~[a, b]): Relation<T-Z>` (the
   schema algebra `rename` already declares; upstream has no column drop) is typed at each call as the `select` of
   every other column -- an explicit column list in SQL, every dialect's, no new SQL record. `spike::zscores` now drops
   its three and declares `Relation<T+O>`, and a `zscores` over its own answer runs.
2. **Hygienic working names** (GAP 2, **closed 2026-10-27**, the user's choice: rename). With `without`, a function
   over its own answer no longer collided; a caller's column of a working name did (`extend(~ts_m:...)` before
   `zscores`: refused, naming `ts_m`, which the caller never wrote). Now each expansion renames the columns its body
   makes and its declared return does not name, to names none of the call's tables has, as it renames the body's
   lambda binders (`ColumnHygiene`, register S112); messages say the names the body wrote. `zscores` beside a caller's
   `ts_m` runs, the caller's column untouched.
3. **The declared return type** (GAP 3, **closed 2026-10-27**). An expanded function's declared return
   (`Relation<T+O>`) was not held to its body, which leaked a column unnoticed. Now each call's typed body is held to
   it under the call's bindings (`Overloads#holdToDeclared`, register S112): every declared column present and of a
   conforming type, and no other; `spike::leaks` is refused, naming its `ts_a`. `Relation<Any>` holds nothing.
4. **Many series** (weighted_sum, basket). No Pure function iterates over a list of columns; a function generic over N
   series is not one function. The natural Pure shape is a long table -- a row per member and date, its weight beside
   it -- which a basket groups by the date; or a function of fixed arity. A design choice for the conversion.
5. **A column passed as a parameter, read in a row** (no primitive needed now). A body renames the column it reads to
   a name of its own and back; so the column read may be neither the line nor the date (refused by name if it is).
6. **Frames** (a design choice). `rows(1 - $w, 0)` is a count's frame; from the start (`w=0`) and a calendar period are
   other functions, or the frame a parameter (`_Window<T>` parameters work: `UserFunctionIntegrationTest$RelationGeneric`).

Not missing: self-joins, as-of joins, group-bys, functions over two tables, partitions by a function's own columns, a
library function over another's answer, and static words.

## What stays DataCube's

The tile's shaping, not gs-quant's functions: a source's periods at the tile's frequency (a week's last observation),
its split into lines, the view's anchor and window, a candle's period bucketing, which queries a tile asks
(`tileAsks`), the align of several series (each on its own dates, then joined by the tile's `align`) and the compare
view's rebase.

Every function of the tile's catalog was converted (2026-10-27, ten families, each held to today's numbers by
`timeseries-queries.test.ts` and checked to call the library by name). The last of them moved the same day (the user:
"is moving the rest of the charts / functions the right thing?" -- yes): over a calendar period, a pair's statistics,
the benchmark-relative ones and a lead-lag (`pairPeriod`, `windowPeriod`'s of two columns: a correlation or a
covariance of the pairs whose dates are in the period; `slopePeriod` over it), a product (`windowPeriod` counts too:
the zeros, the negatives, the logs' sum), a vol swap's volatility, and Dickey-Fuller and a half-life, each a
`...Period` function held to a definition written out in the test; and `append` and `prepend`, one-table functions of
series aligned by the caller, as `union` is (the align says which side has a row on each date; the function keeps the
splice's rows).

## The next step

The conversion is done (2026-10-27, above). 1, 2 and 3 are built (`without`, hygiene and the return check, 2026-10-27: the catalog regenerated against the
legend-engine 4.145.0 and legend-pure 5.99.0 trees, now in the session). 4 and 6 are decided (the user, 2026-10-27:
"yes take all your suggestions"): many series as a long table keyed by the series, a window as a value with a ramp,
gs-quant's calendar windows exact (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.6). Next: convert every function,
each held to today's numbers by `timeseries-queries.test.ts` as the shipped five were, declaring its honest return.
After that, the charts start from classes of the stress model projected to relations (the user, 2026-10-27;
docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §11, Phase 6), then the library's review.

For that review, found while converting (2026-10-27): an `if` whose branches are relations of different columns is not
refused by the typer -- it reached DataCube's plan reader as "column ... appears with conflicting types" (a smoothed
average's `if($w <= 1, ...)`); and the static fold that takes a branch when a call is expanded evaluates `==` but not
`<` or `<=` over literals, so the library branches on `==` alone (`smoothedMean`, `ewmaSpan`).
