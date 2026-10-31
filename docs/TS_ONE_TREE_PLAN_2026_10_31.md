# Timeseries, one tree: gs-quant's words everywhere (2026-10-31)

**Status: PROPOSED, for the user to ratify (§9 lists the rulings asked). Nothing here is built yet.** It replaces
the direction of `docs/TS_LANGUAGE_BUILD_2026_10_30.md` from its P6 on, and revises
`docs/TIMESERIES_LANGUAGE_2026_10_29.md` (the `#TS` spec) where §4 says. The conversation that produced it: the user,
2026-10-31, "step back ... what should we do completely clean sheet if we wanted expert timeseries DSL with a great
python DSL and datacube experience, that stayed familiar to gs-quant", and the rulings since (§2).

## 0. What it is for

A quant writes a timeseries the way gs-quant writes one -- functions over series, `volatility(returns(x), 63)` --
in a Python notebook, in DataCube's timeseries tile, or in a Pure query, and gets the same numbers everywhere, computed
by the database. One meaning per word. Nothing is written twice.

Nobody uses the tile yet (the user, 2026-10-31): there are no saved pages or notebooks to keep working. Every choice
below is made on its merits, and the examples are regenerated, not migrated.

## 1. What is wrong today

| Today | The problem |
|---|---|
| Three notations: the tile's formula box (`px:close[SPX]`, `$A`, `.sum`), `#TS` text (`px.close['SPX']`), Python's tile keywords | three things to learn for one idea |
| `datacube/src/timeseries-ts.ts` translates the tile's notation into `#TS` text | a translator is a second meaning that can drift |
| The catalog is written three times: the tile's `TS_SIGNATURES` and `timeseries-expr.ts`'s words, core's `TsFunctions` `Fn` and `SIGNATURES`, and their tables (cycles, periods a year, the renames) | knowledge duplicated |
| Queries cross between programs as text (the bundle printed the tile's query; Python parsed it back through the engine-exact grammar, which refuses `#TS{`) | the 19 Python reds of 2026-10-31; fixed by handing over the tree (`datacube/src/headless/grid.ts` `gridTimeseries`) |
| `#TS` differs from gs-quant in a dozen words (spec §7): `ln`, ratios for volatility, `isodow`, `cov` of returns, `3mo` | a quant's gs-quant habits give other numbers |
| The tile's defaults differ from gs-quant's too: its windows default to fixed counts (`timeseries-spec.ts` `W(22)`), gs-quant's to the whole series (§4.1) | the tile is not gs-quant either |
| Python cannot write a timeseries itself: it asks DataCube's JavaScript bundle (V8) | the platform's language reached through a product's code |

## 2. The decisions

**D1. One tree.** Every surface builds the same query tree: the formula box, `#TS` text, the Python DSL. The
compiler plans the tree. Text is printed from the tree for people (show query, saved Pure, docs) and parsed into the
tree from people; it never travels between programs (the user's question of 2026-10-31, "why the passing back and
forth": it did nothing, and it broke on `#TS`).

**D2. One meaning per word, platform-wide, and it is gs-quant's** where gs-quant defines the word, read from
gs-quant's source (spec by verification, as lite reads legend-pure: §7). A spelling that means the same thing may be
accepted as an alias (`ln` for `log`); a spelling whose meaning elsewhere differs is never an alias (SQL's `log` is
base 10, so `log` cannot be "SQL's log" anywhere). Every printer prints the canonical word.

**D3. A period's aggregate keeps the short words** (the user, 2026-10-31): under `SAMPLE BY`, `last(close)`,
`first`, `max`, `min`, `sum`, `mean`, `count` are the period's, as pandas (`resample('W').last()`), kdb+
(`select last price by date`), QuestDB (`SAMPLE BY` with `last`) and TimescaleDB (`last(price, time)`) have them --
and as gs-quant's own `bucketize` names them (`AggregateFunction`: max, min, mean, sum, first, last). gs-quant's
whole-line `first(x)` and `last(x)` (`analysis.py`: "series with last value of X for all dates") are `earliest(x)`
and `latest(x)`.

**D4. Nothing is read from context** (the user, 2026-10-31, "there is no way to tell from context if last() should
be latest()?"): a word never changes meaning because a far clause is there or not. `last(close)` with no `SAMPLE BY`
is refused with the fix -- "the last of what? SAMPLE BY 1w for each week's last; latest(close) for the line's latest
value on every date" -- so a frequency switched in the tile never silently changes a formula's numbers.

**D5. Subscripts take bare words** (the user's question, 2026-10-31): `close[SPX]` is the value `SPX`, as written
(no case folding); `close[SPX, venue = NYSE]` likewise on the right of `=`; anything that is not a word keeps its
quotes, `close['BRK.B']`, single quotes as SQL's strings; a number is a number, `close[1234]`; an enumeration's value
by its name, `close[TECH]`.

**D6. One catalog, as data, in core**, beside the library (`projects/timeseries/`): each function's name, gs-quant
module and name, signature (series, then parameters with kinds and defaults), units, whether its figures are a level,
its words, an example. Generated from it: core's bindings, the formula box's completions and help, the Python DSL's
stubs (typed, IDE completion), the guide's function pages. Each generated file diff-tested, as
`datacube/src/generated/library-enums.ts` is.

**D7. One parser, in core**, for the expression grammar and the query clauses. The formula box calls it through the
planner the tab already holds (WASM); Python through the native library. The tile keeps no grammar of its own.

**D8. The Python DSL is gs-quant's signatures over lazy series**, in the platform package (`pip install legend-lite`,
no V8): gs-quant's Python names (`sum_`, `range_`, `filter_`, `abs_`: Python's own builtins taken, as gs-quant takes
them), its parameters (`w`, `obs`, `type`, `returns_type`), `Window(w, r)` as gs-quant's. A source is a class, a
table, a grid or a DataFrame. Nothing is computed in Python: `frame(...)` plans the tree and runs it where the rows are.

**D9. gs-quant is the test oracle.** CI runs `gs_quant.timeseries` on pandas over the same data and compares every
catalog function's numbers with the database's, as PCT compares lite with legend-pure. gs-quant is test input only,
never a runtime component (AGENTS.md's reference-checkout tenet, applied to gs-quant).

**D10. The tile decides what it draws; the platform decides what a series means.** The Python `Timeseries` tile on a
page still asks DataCube what the tile draws (panes, axes, views, its tables); a series' meaning is the platform's
tree, which DataCube, Python and `#TS` text all build the same way.

## 3. Why `#TS` needs no SQL to name a grid in a `WITH` (the user asked to be taught)

**Where the idea came from.** `#TS` was a piece of text. Text can only reach a thing by naming it or by spelling it
out. A grid is a Pure relation -- a class's paths projected, its filters, its calculated columns -- so to make one
self-contained `#TS` string that contains a grid, the grid had to be spelled in the string's own language:
`WITH px AS (SELECT instrument.ticker AS ticker, quoteTime, closePrice AS close FROM MarketQuote WHERE ...)`. That
needed a printer from Pure to SQL over the model (the spike of 2026-10-31, parked).

**It was never needed to run.** The tile already hands a grid over by name, not by spelling:

```
let px = <the grid's relation, as Pure>;        // a variable of the enclosing Pure query
#TS{ SELECT volatility(close[AAPL], 63) AS vol FROM $px ON quoteTime BY ticker }#
```

`FROM $px` is spec R1.4, "a relation of the enclosing Pure". The SQL `WITH` would only have bought a single string
with no `let` around it: a nicety for reading and copying, not a way to run anything.

**With the tree (D1) the question disappears.** In a tree, a source is a node that holds the relation itself:

```
Query
├─ source px:   relation = <the grid's Pure tree>   on quoteTime   by ticker
├─ item vol:    volatility(px.close[AAPL], 63)
└─ sample:      none
```

Nothing is named and nothing is spelled: the grid's tree sits inside the query's tree, the same object the grid
itself plans. Printing is only for people, and the printer chooses how to show a source: `WITH px AS (SELECT ...)`
where the grid reads clearly as SQL, `let px = ...` Pure where it does not. Either way it is presentation.

**SQL in a `WITH` stays one way a person writes a source by hand** in `#TS` text (R1.2, R1.3): typing
`WITH quotes AS (SELECT ... FROM MarketQuote)` in a notebook is still read, into the same node.

## 4. The words: gs-quant's meaning, read from its source

Read from gs-quant 2.2.3 (`pip download gs-quant==2.2.3`, `gs_quant/timeseries/*.py`), not recalled. Each row asks a
ruling (§9, R1): the proposal is gs-quant's meaning under D2-D4.

### 4.1 Windows

| | gs-quant 2.2.3 | `#TS` today | Proposed |
|---|---|---|---|
| No window | `Window(None, 0)`: the series' length, no ramp -- each date over the series from its start (`helper.normalize_window`) | refused | gs-quant's, for every function but a period aggregate's word (D3): `volatility(x)`, `zscores(x)`, `std(x)` are from the start; `mean(x)`, `sum(x)` are refused without `SAMPLE BY` or a window (D4), with `mean(x, all)` the fix |
| An integer `w` | `Window(w, w)`: the window and a ramp of `w` -- its first `w` points dropped (`apply_ramp`: `x[window.r:]`) | the first value one observation earlier than gs-quant's (the spec's §12 item 11, recorded) | gs-quant's exactly (the library already takes a ramp argument: the tile's decision A2) |
| `Window(w, r)` | size and ramp apart | `w` only (a `ramp =>` option "if anyone asks", §12 item 11) | gs-quant's: `Window(63, 10)` in Python, `w => 63, r => 10` in text |
| A calendar window | `'1m'`, `'3m'`, `'1y'`: `m` is months (`helper._to_offset`: d, w, m, y) | `3mo` (`m` is minutes elsewhere) | gs-quant's: `3m` is three months. Minutes, when intraday comes, are `min` |
| From the start | `w=None` / no window | `all` | `all` stays as the explicit word for gs-quant's default |

### 4.2 Words whose meaning differs

| Word | gs-quant 2.2.3 | `#TS` today | Proposed |
|---|---|---|---|
| `log(x)` | natural (`algebra.log`: "Natural logarithm") | `ln(x)`; SQL's `log` base 10 | `log` natural; `ln` an alias; `log10(x)` for base 10 |
| `floor(x, value=0)`, `ceil(x, value=0)` | a bound: "Floor series at minimum value", default 0 | `greatest`/`least`; SQL's `floor` rounds | gs-quant's bound; rounding is `round(x, digits)`, `round_down(x)`, `round_up(x)` |
| `first(x)`, `last(x)` | the whole line's value on every date | `first_of`, `last_of` | `earliest(x)`, `latest(x)` (D3); `first`, `last` the period's |
| `volatility` | annualized, in percent: `* sqrt(252) * 100` (its docstring) | a ratio (0.20) | percent (20.0), as gs-quant; the tile's axis formats it |
| `percentile(x, n, w)` | `n` in [0, 100] ("percentile must be in range [0, 100]") | a ratio (0.5) | 0 to 100 |
| `weekday(x)` | 0 is Monday (`dt.weekday`) | `isodow` (1 is Monday) | `weekday` 0 Monday; `isodow` kept, its own word, 1 Monday |
| `cov(x, y, w)` | of the series as given (`statistics.cov`) | of returns; `type => 'levels'` for the series | gs-quant's: of the series as given |
| `correlation`, `beta` | of returns of prices by default (`type_=SeriesType.PRICES`, `prices=True`) | of returns | unchanged: the same |
| `index(x, initial=1)`, `change(x)` | from the series' first value | from the data's first date, or a date given | unchanged: the user ruled (2026-10-30) that the tile passes its range's first date, which is gs-quant's "first value" of the series a person fetched |
| `returns(x, obs=1, type)` | `Returns`: simple, logarithmic, absolute | the same, `type => 'log'` | the same; bare option words (`type => log`) as D5's |

### 4.3 Spellings only (the same meaning: aliases, one printed)

| Today's spellings | Printed as | Note |
|---|---|---|
| `==`, `=` | `=` | the same comparison |
| `not(x)`, `NOT x`; `and`, `AND`; `or`, `OR` | `NOT`, `AND`, `OR` | |
| `range`, `range_of`; `filter`, `filter_values`; `union`, `union_of` | `range`, `filter`, `union` | the parser reads a keyword followed by `(` in an expression as a call; Python's are gs-quant's `range_`, `filter_`, `union` |
| `px:close`, `px.close` | `px.close` | SQL's qualifier |
| `volume[AAPL].sum` | `sum(volume[AAPL])` under `SAMPLE BY` | D3 |
| `$A` (the tile) | `A` | in `#TS`, `$name` is the enclosing Pure's variable (R1.4) |
| `type=log` | `type => log` | `=` in an argument list is a comparison in SQL; Python takes `type='log'` |

## 5. The layers

| Layer | What it is | From today |
|---|---|---|
| The numbers | the `timeseries::` Pure library (`projects/timeseries/model.pure`) | kept; meanings changed per §4 |
| Lines, periods, fills, alignment | `#TS`'s line model (`TsLines`) | kept |
| The catalog | one data file beside the library | new; replaces three copies |
| The tree | `#TS`'s query record (`TsAst`), carried as the protocol's JSON; a source may hold a relation's tree (§3) | `TsAst` exists; carried as text today |
| The grammar | one parser in core: expressions and clauses | `TsParser`; the tile's `parseExpr` and the translator deleted |
| Printers | tree to `#TS` text, to Python DSL code, to Pure | new (the tile's Code button prints from the tree) |
| Python DSL | `legend_lite.timeseries` | new |
| DataCube tile | settings and gestures over the tree; drawing | kept; its notation deleted |

## 6. The order, each step green

Every step starts and ends green: the gallery's every series answers (`//datacube:timeseries_ts_parity_test`),
`//core:ts_island_test`, the Python suites, and from step 2 the gs-quant oracle.

0. **The Python fix** (2026-10-31): `gridTimeseries` hands Python the tree. Done with this plan.
1. **The catalog** (D6): the data file; core's bindings read it; the tile's catalog generated from it; the generated
   files diff-tested. No meaning changes.
2. **The oracle** (D9): a test lane with gs-quant installed, every catalog function's example computed by gs-quant on
   pandas and by the database, compared. It reports the differences §4 lists, as expected failures, each named.
3. **The words** (D2-D5, §4 as ruled): the parser and the library changed row by row; each row's expected failure in
   step 2's lane turns to a pass and its name is struck.
4. **The tree on the wire** (D1): the tree as the protocol's JSON (reader and emitter in core, its mirror in
   `pure-protocol`), a source holding a relation; the printer; a round trip pinned (every gallery query printed,
   parsed, the same tree).
5. **One parser** (D7): the formula box parses through core; the tile stores the tree; `timeseries-expr.ts`'s grammar
   and `timeseries-ts.ts` deleted.
6. **The Python DSL** (D8): `legend_lite.timeseries`, its stubs generated from the catalog; `page.timeseries(...)`
   takes its series; gs-quant's own examples ported as tests.
7. **The documents**: the spec (version 4) and the guide for quants regenerated from the catalog.

## 7. Spec by verification, applied to gs-quant

As lite treats legend-pure and legend-engine (AGENTS.md: "spec by VERIFICATION, never by LOADING"): gs-quant's
source is read for every meaning and its released package runs in the oracle lane; neither is a runtime component.
Where lite departs from gs-quant (D3's period words, D4's refusals), the departure is posted to the user and written
down here, with gs-quant's answer beside lite's.

## 8. Not in this plan

- **The printer** (Pure to SQL over the model, the spike of 2026-10-31): parked. With §3 it serves only how a source
  is shown, never how it travels.
- **`grid.to_pandas()`** reads the grid's text back the same way the timeseries did (Change 2 of 2026-10-31): latent
  for a grid over `#SQL{...}#`. It goes with the Python platform DSL below.
- **The Python platform DSL beyond timeseries** (the user, 2026-10-31: classes, mappings, stores, queries, the
  relation API, services, "things that are not DataCube specific"): its own design note. Prerequisite noted there:
  compiling the model's protocol JSON directly (`ModelReader` is not wired into compile;
  `docs/LEGEND_SQL_STUDY_2026_10_11.md`).

## 9. The rulings asked

- **R1.** §4.1 and §4.2, row by row: gs-quant's meaning as proposed, or `#TS`'s.
- **R2.** D4's refusal for a period aggregate's word with no `SAMPLE BY` and no window (`mean(x)` refused, while
  gs-quant answers from the start), or gs-quant's default there too, accepting that the word then changes meaning
  when a frequency is set.
- **R3.** §4.3's printed forms (`=`, `AND`, `px.close`, `type => log`).
- **R4.** The order of §6.
- **R5.** The oracle lane's cost: gs-quant and its dependencies (pandas, numpy, scipy among them) installed in one
  test lane.
