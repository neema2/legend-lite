# Timeseries, one reader: gs-quant's words everywhere (2026-10-31)

**Status: PROPOSED, version 2, for the user to ratify (§9 lists the rulings asked). Nothing here is built yet.**
Version 1 was reviewed the same day by an agent that wrote none of it, against the code and gs-quant's source; its
findings were checked and §10 says what changed. The plan replaces the direction of
`docs/TS_LANGUAGE_BUILD_2026_10_30.md` from its P6 on, and revises `docs/TIMESERIES_LANGUAGE_2026_10_29.md` (the
`#TS` spec) where §4 says. The conversation that produced it: the user, 2026-10-31, "step back ... what should we do
completely clean sheet if we wanted expert timeseries DSL with a great python DSL and datacube experience, that stayed
familiar to gs-quant", and the rulings since (§2).

## 0. What it is for

A quant writes a timeseries the way gs-quant writes one -- functions over series, `volatility(x, 63)` (gs-quant's
`volatility` takes prices and computes their returns itself: `econometrics.py`, `returns_type=SIMPLE`) -- in a Python
notebook, in DataCube's timeseries tile, or in a Pure query, and gets the same numbers everywhere, computed by the
database. One meaning per word within a statement. Nothing is written twice.

Nobody uses the tile yet (the user, 2026-10-31): there are no saved pages or notebooks to keep working. Every choice
below is made on its merits, and the examples are regenerated, not migrated.

## 1. What is wrong today

| Today | The problem |
|---|---|
| Three notations: the tile's formula box (`px:close[SPX]`, `$A`, `.sum`), `#TS` text (`px.close['SPX']`), Python's tile keywords | three things to learn for one idea |
| `datacube/src/timeseries-ts.ts` translates the tile's notation into `#TS` text | a translator is a second meaning that can drift |
| The catalog is written three times: the tile's `TS_SIGNATURES` and `timeseries-expr.ts`'s words, core's `TsFunctions` `Fn` and `SIGNATURES`, and their tables (cycles, periods a year, the renames) | knowledge duplicated |
| The spec and the code disagree, unrecorded: `TsFunctions` was built to the tile's signatures (P4), so `#TS` gives volatility in percent and defaults windows to the tile's counts (`volatility` 22, `median` 22, `sharpe_ratio` 252, the pair functions 63: `TsFunctions.java:190-222`), where the spec says ratios and written windows | the spec is not the language |
| Where `#TS` really differs from gs-quant (§4): `log` is SQL's base 10 (`PostgresFunctions.java:99`), `floor`/`ceil` round, `first`/`last` are the period's, `m` is refused, missing windows take the tile's counts, a ramp starts one point earlier | a quant's gs-quant habits give other numbers |
| Python cannot write a timeseries itself: it asks DataCube's JavaScript bundle (V8) | the platform's language reached through a product's code |
| A query crossed between programs through a foreign grammar (the bundle printed a grid's series query; Python parsed it back through the engine-exact route, which refuses `#TS{`) | the 19 Python reds of 2026-10-31, fixed (step 0) |

## 2. The decisions

**D1. One reader** (revised after the review, §10; ruling R6). Pure travels between programs as the protocol's tree;
a `#TS` query inside it travels as its own text, and **core's parser is its only reader** -- never another grammar,
never a second parser. A person's text is kept as written (its comments, its layout, `TsIslandRoundTripTest`'s "printed
as written"); a program writes the canonical text the printer gives. Inside core the parser builds the tree
(`TsAst`); every surface reaches it through text and the one reader: the formula box through the planner the tab holds
(WASM), Python through the native library. This is version 1's "one tree" without a second wire format: a
position-free, versioned tree schema with codecs in Java, TypeScript and Python is not needed, because the grammar is
the schema and the spec versions it -- as SQL is between programs everywhere.

**D2. gs-quant's names, gs-quant's meanings where they are causal, one meaning per word in a statement** (narrowed
after the review). Where gs-quant defines a word, `#TS` means what gs-quant means, read from gs-quant's source (§7),
unless that meaning reads the future (look-ahead) or clashes with SQL inside the same statement:

- **Look-ahead is never copied.** gs-quant's `zscores(x)` with no window is a two-sided z-score over the whole series
  (`statistics.py:777-783`), `winsorize` uses the whole series' mean and deviation whatever its window
  (`statistics.py:840-842`): `#TS` computes each date from the data up to it, as `FILL(PREV)` was designed not to look
  ahead. Each such departure is a row the oracle names (D9).
- **A word whose gs-quant meaning and SQL meaning differ is refused in `#TS` text, with both fixes.** A `#TS`
  statement holds SQL -- `WITH` bodies, `WHERE`, `FILTER` -- which keep Postgres's meaning (the `#SQL` island's, by
  upstream fidelity), so `log` cannot be natural in an item and base 10 in a `WHERE`. `log(x)` is refused: "write
  ln(x) (natural, gs-quant's log) or log10(x)"; `floor(x)` and `ceil(x)` likewise ("greatest(x, 0) for gs-quant's
  floor; round_down(x) to round"). The Python DSL, which holds no SQL, keeps gs-quant's `log`, `floor`, `ceil`, and
  its printer writes the unambiguous word.
- Everything else -- volatility in percent, percentiles 0 to 100, `weekday` 0 for Monday, `cov` of the series as
  given, `correlation` and `beta` of returns of prices -- is gs-quant's, and is mostly built already (§4).

**D3. A period's aggregate keeps the short words** (the user, 2026-10-31; the set widened after the review): under
`SAMPLE BY`, `first`, `last`, `min`, `max`, `sum`, `mean`, `median`, `std`, `var`, `count`, `prod` are the period's,
as pandas' `resample` names them (and kdb+'s `select last price by date`, QuestDB's `SAMPLE BY` with `last`,
TimescaleDB's `last(price, time)`; gs-quant's own `bucketize` takes max, min, mean, sum, first, last). gs-quant's
whole-line `first(x)` and `last(x)` ("series with last value of X for all dates", `analysis.py`) keep a name of their
own: `latest` is taken -- it is already `#TS`'s keyword for the data's last date, `FILTER date > latest - 1y`
(`TsToPure.java:314`) -- so the names are today's `first_of(x)` and `last_of(x)` unless the user rules another (R8).

**D4. Nothing is read from context, and a window is always written in text** (the user, 2026-10-31, "there is no way
to tell from context if last() should be latest()?"; extended after the review). A word never changes meaning because
a far clause is there or not, nor because of where in a statement it stands. In `#TS` text a windowed function's
window is written: `volatility(x)` is refused -- "volatility over what? volatility(x, 22) for the last 22, or
volatility(x, all) from the start, gs-quant's default" -- and a period word with no window and no `SAMPLE BY` likewise
("the last of what? SAMPLE BY 1w for each week's last; last_of(x) for the line's last value on every date"). This
removes the tile's own window counts, which are neither gs-quant's nor written. The Python DSL keeps gs-quant's
defaults in its signatures (visible in the stub), and its printer writes them out, so the text always says the window.

**D5. Subscripts take bare words** (the user's question, 2026-10-31; its edges set after the review): `close[SPX]` is
the value `SPX`, as written (no case folding); `close[SPX, venue = NYSE]` likewise on the right of `=`. Quoted, single
quotes as SQL's strings: anything that is not a word (`close['BRK.B']`), a reserved word (`close['ALL']`,
`close['ON']`, `close['OR']`: `SqlParser.java:30-35`), a token that starts with a digit (`close['10Y']`, which is
otherwise read as a span) and an identifier made of digits whose leading zeros count (`close['037833100']`, a CUSIP:
unquoted it is a number). A number is a number (`close[1234]`); an enumeration's value by its name (`close[TECH]`).

**D6. One catalog, as data, in core**, beside the library (`projects/timeseries/`): each function's name, gs-quant's
module, name and Python signature (lists where gs-quant takes lists: `weighted_sum([x, y], [wx, wy])`,
`union([x, y])`), the text signature where it differs (series, then parameters, gs-quant's parameter names:
`returns_type`, `type_`, `assume_zero_mean`, `annualization_factor`), kinds and defaults, units, whether its figures
are a level, its words, an example, and its oracle mapping (D9: gs-quant's counterpart, or none and why). Generated
from it: core's bindings, the formula box's completions and help, the Python DSL's stubs, the guide's function pages.
Each generated file diff-tested, as `datacube/src/generated/library-enums.ts` is.

**D7. One parser, in core**, for the expression grammar and the query clauses. The formula box calls it through the
planner the tab already holds; Python through the native library. The tile keeps no grammar of its own, and asks
core for what it needs to draw (a series' unit, whether it is a level or a candle, its problems): the analyses
`timeseries-expr.ts` holds today (`unitOf`, `candleOf`, `timeseriesProblems`, `inlined`, about 25) move to core,
read from the catalog. A saved page stores each series as its `#TS` item text.

**D8. The Python DSL is gs-quant's signatures over lazy series**, in the platform package (`pip install legend-lite`,
no V8): gs-quant's Python names (`sum_`, `range_`, `filter_`, `abs_`), its parameters and enums (`Window(w, r)`,
`Returns.LOGARITHMIC` -- gs-quant refuses a string, `econometrics.py:341-346`), its tenor strings (`'3m'` is three
months, gs-quant's letters h, d, w, m, y: `helper.py:101-122`; the text prints `3mo`). A source is a class, a table, a
grid or a DataFrame (loaded into the database first). A series is one instrument's, as gs-quant's is; `px.close`
without a subscript is every instrument's line, a frame with a column each. Nothing is computed in Python: `frame(...)`
prints the `#TS` and runs it where the rows are.

**D9. gs-quant is the test oracle, where it has an offline counterpart.** A test lane installs gs-quant at a pinned
version with pinned pandas, numpy, scipy and statsmodels, and compares the database's numbers with gs-quant's on fixed
data, for every catalog function with an offline counterpart (about 50 of about 80). Its comparison rules are written
down with the lane: one instrument at a time (gs-quant's series are single); a ramp's dropped points against missing
rows; NaN against no row; a tolerance; `bucketize`'s end-of-period labels against `SAMPLE BY ... AT END`. Every
departure is a row of the oracle's register, named, with gs-quant's answer and lite's. The rest -- functions gs-quant
has not (`drawdown`, `atr`, `stochastic`, `keltner_channels`, `donchian_channels`, `obv`, `vwap`, `hedge_ratio`,
`residual`, `up_capture`, `down_capture`, `lead_lag`, `dickey_fuller`, `half_life`, `total_return`, `rebase`,
`percent_change`), and those it has only on Marquee (`sharpe_ratio` fetches a risk-free curve; `tracking_error`,
`information_ratio`, `alpha`, `r_squared` take report ids) -- are held by frozen answers (§6's gate) and a reference
each, written down. gs-quant is test input only, never a runtime component.

**D10. The tile decides what it draws; the platform decides what a series means.** The Python `Timeseries` tile on a
page still asks DataCube what the tile draws (panes, axes, views, its tables); a series' meaning is the platform's,
the same through the formula box, `#TS` text and the Python DSL.

**D11. Every way of naming a source keeps reading, at every step** (the user, 2026-10-31: "make sure the WITH and the
inline model notation still work"): the `WITH` that flattens the model (R1.2, `WITH quotes AS (SELECT
instrument.ticker AS ticker, ... FROM MarketQuote WHERE ...)`), the class read inline with its paths where they are
used (R1.1, `FROM MarketQuote ON quoteTime BY instrument.ticker`), a table and a table's `WHERE` (R1.3), a relation of
the enclosing Pure (R1.4, `FROM $px`) and a query of the language's own (R1.5, `AS TS`) -- the five sources `TsAst`
holds (`TsAst.java:66-97`), each pinned by `TsIslandTest`'s source tests, which are in every step's gate (§6).

**D12. Show #TS is one statement** (the user, 2026-10-31: "the Show #TS of the grid to actually show that single
statement view"): the tile's Query window shows each series as one self-contained `#TS` statement, each grid it reads
printed as a `WITH` over the model above the series -- the form a person would write (§3) -- instead of `let px =
...; #TS{... FROM $px ...}`. A grid's own Show #TS shows its `WITH` body alone. A narrow printer, for display only: a
grid's relation as `#TS`'s flat source (`SELECT paths and expressions FROM class-or-table WHERE condition`). Its
subset grows a construct at a time, each with its own test: first a class's to-one paths projected and renamed, a
table's columns, and the page's and the grid's filters as `WHERE` (comparisons, `IN`, ranges, `AND`/`OR`/`NOT`,
`IS NULL`); then calculated columns, one native at a time, each printed as the Postgres the flat form reads back to the
same meaning (integer division, empty against null, dates, enumerations: the `#SQL` reader's rules). Outside it -- a
group-by, a join, a pivot, a calculated column not yet printable -- the window shows the `let` form and says why,
never an approximation. **Exact, defined** (after the review: the reader normalises a flat source, projecting then
filtering, so a grid's `filter()->project()` cannot come back as the same tree): the printed statement, parsed and
printed again, is the same text; and it answers the same rows as the tile's own query, on every example page's data.
The grids not printed are counted by reason, the count allowed only to shrink.

**D13. Show Python is the gs-quant program** (the user, 2026-10-31: "Show Python should show our gs-quant
equivalent"): the tile's Code window shows the Python DSL program (D8) that computes the same series --
`px = source(MarketQuote, date='quoteTime', by='instrument.ticker', columns=..., where=...)`, then
`volatility(px.close['AAPL'], 63)` -- gs-quant's Python names and signatures. Today's Code (a program that rebuilds the
tile on a page, `timeseries-code.ts tileCode`) stays beside it as "Build this tile". **Exact, defined:** the test runs
the printed program and compares its answer with the tile's, on every example page's data. `source(...)`'s `columns`
and `where` are the first piece of the Python platform DSL (§8); a grid they cannot say (a group-by, a join, a pivot)
is shown as `source(pure='...')` over the grid's Pure, said so.

## 3. Why `#TS` needs no SQL to name a grid (the user asked to be taught)

**Where the idea came from.** A `#TS` query is text. Text reaches a thing by naming it or by spelling it out. A grid
is a Pure relation -- a class's paths projected, its filters, its calculated columns -- so a single self-contained
`#TS` string containing a grid has to spell the grid in its own language, `WITH px AS (SELECT ... FROM MarketQuote
WHERE ...)`, and a program wanting that needs a printer from Pure to that SQL.

**A program never needs it.** A program hands a grid over by name: the grid travels as Pure, in the protocol's tree
around the `#TS` text, and the text names it.

```
let px = <the grid's relation, as Pure: the protocol's tree>;
#TS{ SELECT volatility(close[AAPL], 63) AS vol FROM $px ON quoteTime BY ticker }#
```

`FROM $px` is spec R1.4, "a relation of the enclosing Pure". Nothing about the grid is spelled in SQL, and nothing
is lost: the grid's relation is the same tree the grid itself plans.

**A person may still spell it** (the user asked, 2026-10-31, "how will that work without SQL?"). Writing SQL to
flatten the model stays, and works today (spec R1.2; pinned by `TsIslandTest.aWithSourceIsAClasssPathsNamedOnce`):

```sql
WITH quotes AS (
  SELECT instrument.ticker AS ticker, quoteTime AS day, closePrice AS close
  FROM   MarketQuote
  WHERE  instrument.sector = 'TECH'
)
SELECT volatility(close[AAPL], 63)  AS aapl_vol,
       close[AAPL] / close[MSFT]     AS ratio
FROM   quotes ON day BY ticker
```

The `WITH` body is SQL-shaped text over the model -- a class, its dotted paths, a condition -- and core's parser reads
it: `MarketQuote.all()->project(~[ticker: x|$x.instrument.ticker, day: x|$x.quoteTime, close: x|$x.closePrice,
...])->filter(...)`, the library's calls over it. The text is read, never run; the database gets the SQL the compiler
writes through the mapping. The class read directly, its paths where they are used (`FROM MarketQuote ON quoteTime BY
instrument.ticker`, R1.1), says the same.

**Show #TS goes the other way, for people only** (D12): it prints a grid as such a `WITH`, so the window shows one
statement a person could have written.

## 4. The words: gs-quant's meaning, read from its source and checked against the code

gs-quant 2.2.3 (`pip download gs-quant==2.2.3`, `gs_quant/timeseries/*.py`), read, not recalled; `#TS` today read
from `core/src/main/java/com/legend/protocol/ts/` and `projects/timeseries/model.pure`, not from the spec. Each row is
a ruling (§9, R1).

### 4.1 Windows

| | gs-quant 2.2.3 | `#TS` today (the code) | Proposed |
|---|---|---|---|
| No window | `Window(None, 0)`: the series' length, no ramp (`helper.normalize_window`) -- each date from the series' start for most functions; `zscores` and `winsorize` over the whole series, the future included; `percentile` a single number | the tile's counts: `volatility` 22, `median` 22, `sharpe_ratio` 252, the pair functions 63; `zscores`, `drawdown`, `percentiles` from the start (`TsFunctions.java:190-222`); the period words refused | always written in text (D4), refused with gs-quant's default as the fix (`all`); Python's signatures keep gs-quant's default and print it |
| An integer `w` | `Window(w, w)`: a ramp of `w`, the first `w` points dropped by position (`apply_ramp`, `x[window.r:]`) | the library's ramp is a minimum count (`model.pure:62-65`): the first value one point earlier (spec §12 item 11) | gs-quant's: a change to the library's ramp, not a new parameter |
| `Window(w, r)` | size and ramp apart | `w` only | gs-quant's: `Window(63, 10)` in Python, `w => 63, r => 10` in text |
| A calendar window | `'1m'`: `m` is months (letters h, d, w, m, y) | `3mo`; `m` refused (in QuestDB `1m` is a minute) | text keeps `3mo` and refuses `m`; Python's tenor strings are gs-quant's (`'3m'`), printed `3mo` |
| From the start | `w=None` | `all` | `all` |

### 4.2 Words

| Word | gs-quant 2.2.3 | `#TS` today (the code) | Proposed |
|---|---|---|---|
| `log(x)` | natural | SQL's base 10 (`PostgresFunctions.java:99`); `ln` natural | refused in text with both fixes (D2); Python's `log` natural, printed `ln` |
| `floor(x, value=0)`, `ceil(x, value=0)` | a bound, default 0 | SQL's rounding | refused in text with both fixes (D2); Python's are gs-quant's, printed `greatest`/`least` |
| `first(x)`, `last(x)` | the whole line's value on every date | the period's under `SAMPLE BY`; `first_of`, `last_of` the whole line's | as today (D3), the names ruled (R8) |
| `volatility` | annualized, in percent | in percent (`model.pure:281-286`) | the same: no change |
| `percentile(x, n, w)` | `n` 0 to 100 | `n` 0 to 100, default 50 (`TsFunctions.java:205,492`) | the same; its window written (D4) |
| `weekday(x)` | 0 is Monday | `weekday_of`, 0 is Monday (`model.pure:884-886`) | the same meaning; the name ruled (R3) |
| `cov(x, y, w)` | of the series as given | of the series as given (`TsFunctions.java:573`) | the same: no change |
| `correlation`, `beta` | of returns of prices (`type_=PRICES`, `prices=True`); `beta` blanks its first three values (`econometrics.py:1054`) | of returns | of returns; `beta`'s first three a departure to rule (R1) |
| `zscores(x)`, `winsorize` with no window | whole series, the future included | from the start | causal (D2), a named departure |
| `index(x, initial=1)`, `change(x)` | from the series' first value | from the data's first date, or a date given | unchanged (the user, 2026-10-30: the tile passes its range's first date) |
| `returns(x, obs=1, type)` | `Returns.SIMPLE`, `LOGARITHMIC`, `ABSOLUTE`; a string refused | `type => 'log'` | text takes gs-quant's values (`type => logarithmic`, `log` an alias); Python takes `Returns` |
| `sharpe_ratio` | `(series, currency, w)`, a 365.25-day CAGR over a risk-free curve from Marquee | mean over deviation, no risk-free rate | a named departure: no offline counterpart (D9) |
| `count(x)` | expanding, no window | a period word | the period's (D3) |
| `x / y` (two lines) | pandas: the union of dates, NaN where one is missing; gs-quant's `divide` and its siblings: `Interpolate.STEP` | the dates both lines have | ruled (R9) |

### 4.3 Spellings only (the same meaning: aliases, one printed)

| Spellings | Printed as | Note |
|---|---|---|
| `==`, `=` | `=` | |
| `not(x)`, `NOT x`; `and`, `AND`; `or`, `OR` | `NOT`, `AND`, `OR` | |
| `range`, `range_of`; `filter`, `filter_values`; `union`, `union_of` | ruled (R3) | the parser can read a keyword followed by `(` as a call; Python's are gs-quant's `range_`, `filter_`, `union` |
| `px:close`, `px.close` | `px.close` | SQL's qualifier |
| `volume[AAPL].sum` | `sum(volume[AAPL])` under `SAMPLE BY` | D3 |
| `$A` (the tile) | `A` | in `#TS`, `$name` is the enclosing Pure's variable (R1.4) |
| `type=log` | `type => logarithmic` | `=` in an argument list is a comparison in SQL |

## 5. The layers

| Layer | What it is | From today |
|---|---|---|
| The numbers | the `timeseries::` Pure library (`projects/timeseries/model.pure`) | kept; the ramp changed (§4.1) |
| Lines, periods, fills, alignment | `#TS`'s line model (`TsLines`) | kept |
| The catalog | one data file beside the library | new; replaces three copies |
| The reader | one parser in core, its tree inside core (`TsAst`) | `TsParser`; the tile's `parseExpr` and the translator deleted |
| Facts for a drawing | a series' unit, level, candle, problems, answered by core from the catalog | moved from `timeseries-expr.ts` |
| Printers | canonical `#TS` text; a grid as a flat source (D12); the Python DSL program (D13) | new |
| Python DSL | `legend_lite.timeseries` | new |
| DataCube tile | settings and gestures; each series kept as its `#TS` item text; drawing | kept; its notation deleted |

## 6. The order, each step green

Every step starts and ends green on: **frozen answers** (new in step 1: every gallery and example-page series' rows on
the sample data, frozen; a step that changes a number on purpose changes its frozen rows with a dated reason, because
`#TS` compared with `#TS` cannot see a change of meaning), `//core:ts_island_test` (its source tests among them: every
notation of D11), the Python suites, and from step 2 the oracle's register.

| # | Step | Size | Riskiest unknown | Proved by |
|---|---|---|---|---|
| 0 | **The Python fix**: `gridTimeseries` hands Python the query's tree (the island inside it its text, read by core) | done | -- | the Python suites (334 and 3, 2026-10-31) |
| 1 | **The catalog** (D6) and **the frozen answers**: the data file; core's bindings read it (the windowed basics in `TsToPure`'s `WINDOWED` too); the tile's catalog generated from it | medium | one schema for the text and the Python signatures, units, kinds, aliases | diff tests; the frozen answers unchanged |
| 2 | **The oracle** (D9), report only: the pinned lane, the comparison rules, the register of departures | large | pinning gs-quant's 24 dependencies; the comparison rules | the register, each row named |
| 3 | **One reader** (D7): the formula box parses through core (an entry point that parses one item, recovers for completions, inside the bundle and cold-start budgets); the facts move to core; the tile keeps item text; `timeseries-expr.ts`'s grammar and `timeseries-ts.ts` deleted, `timeseries-query.ts`'s use of it and the parity test rewritten; "Build this tile" prints `#TS` items | large | about 25 analyses to move; the entry point's latency | the frozen answers; the tile's suites; the Python suites |
| 4 | **The words** (D2-D5, §4 as ruled), now in one place: the parser, the library's ramp, the catalog | medium | the ramp change touching every rolled function | the oracle's rows turning to passes; the frozen answers changed only where ruled |
| 5 | **Show #TS, one statement** (D12): the flat-source printer, paths and filters first, calculated columns a native at a time | medium, growing | printing calculated columns as the Postgres the reader reads back to the same meaning | text fixpoint and row equality on every example page; the count of grids not printed |
| 6 | **The Python DSL** (D8): `legend_lite.timeseries`, stubs from the catalog, sources (a DataFrame loaded first), `frame()` | large | lines of several instruments in gs-quant's single-series terms | gs-quant's own examples ported; the oracle over the DSL |
| 7 | **Show Python** (D13) | medium | grids `source(...)` cannot say | the printed program run, its rows compared |
| 8 | **The documents**: the spec (version 4) and the guide for quants, from the catalog | small | -- | -- |

Out of the order, and said: **intraday** (`SAMPLE BY` takes `1d` and longer, `TsParser.java:65`; gs-quant's
annualization refuses several points a day, `econometrics.py:497`; time zones) is a later program.

## 7. Spec by verification, applied to gs-quant

As lite treats legend-pure and legend-engine (AGENTS.md: "spec by VERIFICATION, never by LOADING"): gs-quant's
source is read for every meaning, and its released package runs in the oracle lane; neither is a runtime component.
Every departure (D2's causal and SQL rules, D3's period words, D4's written windows, §4's rows as ruled) is posted to
the user and written down, with gs-quant's answer beside lite's: the oracle's register.

## 8. Not in this plan

- **The general printer** (any Pure relation to Postgres SQL over the model, the spike of 2026-10-31): parked. D12's
  narrow printer is in the plan (step 5); the general one (group-bys, joins, pivots as SQL) is not.
- **`grid.to_pandas()`** reads a grid's text back through the engine-exact grammar (Change 2 of 2026-10-31): latent
  for a grid over `#SQL{...}#`. It goes with the Python platform DSL below.
- **The Python platform DSL beyond timeseries** (the user, 2026-10-31: classes, mappings, stores, queries, the
  relation API, services, "things that are not DataCube specific"): its own design note. Prerequisite noted there:
  compiling the model's protocol JSON directly (`ModelReader` is not wired into compile;
  `docs/LEGEND_SQL_STUDY_2026_10_11.md`).

## 9. The rulings asked

- **R1.** §4.1 and §4.2, row by row.
- **R2.** D4: a window always written in text (the tile's counts removed; Python's gs-quant defaults printed out).
- **R3.** The names: `weekday` or `weekday_of`; `range`/`filter`/`union` or `range_of`/`filter_values`/`union_of`;
  §4.3's printed forms.
- **R4.** The order and sizes of §6.
- **R5.** The oracle lane: gs-quant and its dependencies pinned in one test lane; the frozen answers for the rest.
- **R6.** D1 as revised: one reader, text inside the protocol's tree; or version 1's tree on the wire, with its schema
  and three codecs (a large step of its own, before the reader).
- **R7.** D2 as narrowed: gs-quant's meaning unless it looks ahead or clashes with SQL in the statement; the clashing
  words refused in text.
- **R8.** The whole-line first and last: `first_of`/`last_of` (today's, no clash), or new names (`latest` is taken).
- **R9.** Two lines meeting: the dates both have (today), gs-quant's step, or pandas' union.
- **R10.** The rulings this plan changes, listed so none changes unsaid: build ruling 7 (the tile's notation stays the
  surface) is replaced -- the user, 2026-10-31: `#TS` is the tile's language, and nobody uses the tile yet; the spec's
  principle 2 ("SQL for timeseries") holds for the clauses and `WITH` bodies, while the functions take gs-quant's
  names; the spec's R12 (ratios) is withdrawn -- never built, the code already gives gs-quant's percent; build ruling 2
  (a `WITH` body is SQL with Postgres's meaning) stands and is why D2 refuses the clashing words.

## 10. What the review of version 1 changed (2026-10-31)

Every finding below was checked against the code or gs-quant's source before it was taken.

- **§4's "#TS today" column described the spec, not the code**: volatility is already in percent, percentiles 0 to
  100, `weekday_of` 0 for Monday, `cov` of the series; missing windows take the tile's counts. Rewritten from the code.
- **gs-quant facts**: `zscores` and `winsorize` look ahead with no window; `percentile`'s default is a single number;
  `sharpe_ratio` needs Marquee; `count` has no window; tenor letters include `h`; `Returns` refuses strings; the ramp
  differs by one point and needs a library change. Corrected.
- **The headline example was gs-quant's classic mistake**: `volatility(returns(x), 63)` is the volatility of the
  returns of returns. Now `volatility(x, 63)`.
- **D1**: `TsAst` cannot be put on the wire as it is (its expressions are `SqlAst`'s, with internal calls and side
  tables by text position), and authored `#TS` must keep its text. Revised to one reader (R6).
- **D2**: `#TS` statements hold SQL, so "gs-quant's meaning everywhere" would make `log` mean two things in one
  statement. Narrowed (R7).
- **D3**: `latest` is already a keyword; the period words are pandas' full set.
- **D5**: reserved words, digit-led tokens and leading zeros need quotes.
- **D9**: about 50 of about 80 functions have an offline gs-quant counterpart; the rest are named, held by frozen
  answers.
- **D12, D13**: the reader normalises a flat source, so "the same tree" cannot hold; exactness is text fixpoint plus
  equal rows.
- **The order**: the words now come after one reader, so they change in one place; the frozen answers are a gate from
  step 1, because `#TS` compared with `#TS` cannot see a change of meaning; the tree-on-the-wire step is gone if R6 is
  ruled as proposed.
- **Rulings reversed unsaid** in version 1: listed (R10).
