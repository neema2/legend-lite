# The library defines the language (2026-10-31)

**Status: a proposal, from a spike. L2 is ruled and C1 is being built; §7 lists the rulings still open.** It
replaces decision D6 of `docs/TS_ONE_TREE_PLAN_2026_10_31.md` (the catalog as a JSON data file) and that plan's step
1b, if ruled. The conversation: the user asked why the catalog was a new JSON format ("We literally have a full
language to define things (pure) and you invented a json format instead?"), saw a mismatch between the language and
the library, and asked whether this should all be modelled in Pure. The answer here: yes, once one gap in core is
fixed. The evidence is `//core:ts_line_spike_test` (`core/src/test/java/com/legend/integration/TsLineSpikeTest.java`).
It is a manual target, so no gate runs it. Run it with `bazel test //core:ts_line_spike_test --test_output=all`.

## 1. The mismatch

Today a word of `#TS`, say `volatility(close[AAA], 63)`, has its meaning in two places:

- **The maths is Pure**, in the table-level library (`projects/timeseries/model.pure`):
  `timeseries::volatility(t, win, returns, ramp, perYear, out)`. It works on any relation, but its caller has to say
  everything: which columns, which window, which ramp, which column holds the periods per year, and the name of the
  output.
- **The plumbing is Java**, in `core/.../protocol/ts/TsFunctions.java` (1,149 lines). It has an enum of 71 words, a
  signature table and one Java case per word. Each case adds the ordering column and the periods per year, works out
  the returns, builds the window `over(~[line], [~ord->ascending()], rows(-(n-1), 0))`, picks the ramp and names the
  output.

So a reader can't learn what `volatility(x, 63)` means from the library alone. Half of it is in Java. A catalog file
(D6) would have been a third place, saying again in JSON what the Java cases and the Pure signatures already say.

## 2. The idea

Give the library one more layer, written in Pure: **a function of lines for each word of the language.**

- **A line** is `Relation<(date:D, key:String, value:Number)>`: one value per date per instrument, with `key` naming
  the instrument. `close[AAA]` is the line of AAA's closes; `close` with no subscript is every instrument's line, told
  apart by `key`.
- **A word** is a Pure function from lines and its parameters to a line. All of its plumbing is written once, in Pure,
  next to the maths. This is the spike's volatility:

  ```pure
  function timeseries::line::volatility(x: Relation<(date:StrictDate, key:String, value:Number)>[1], w: Integer[1])
      : Relation<(date:StrictDate, key:String, value:Number)>[1]
  {
    $x->extend(~ord:r|%1970-01-01->dateDiff($r.date, DurationUnit.DAYS))
      ->timeseries::periodsPerYear(~key, ~ord, DurationUnit.DAYS, ~py)
      ->timeseries::returns(over(~[key], [~date->ascending()]), ~value, 1, timeseries::Returns.simple, ~r)
      ->timeseries::volatility(over(~[key], [~date->ascending()], rows(-62, 0)), ~r, if($w < 2, |2, |$w), ~py, ~v)
      ->select(~[date, key, v])->rename(~v, ~value)
  }
  ```

  (`rows(-62, 0)` is written as a literal only because of gap G1 in §3. The finished function writes `rows(1 - $w, 0)`
  or something like it.)
- **Defaults are overloads.** `volatility(x)`, `volatility(x, w)` and `volatility(x, n, unit)` (a calendar window,
  such as `3mo`) are three Pure functions. The spike's calendar overload calls `timeseries::volatilityPeriod`.
- **Documentation is `doc.doc`.** Drawing facts, such as whether a word's figures are a level or what unit they are in,
  become tags of a Pure profile on the function.
- **Core keeps one generic rule** in place of the 71 Java cases. It reads the text; makes each `close[AAA]` into a line
  (a filter, a select and three renames); finds the function by its name and the kinds of its arguments, through
  `ModelContext` like any other overload; and calls it. Everything that is about the language rather than about any one
  word stays in core's Java: the parser, lines meeting on dates, `FILL`, `SAMPLE BY` and the scope rule (D3, D4),
  arithmetic between lines, and naming the outputs.
- **The catalog is the library itself.** The formula box's completions and help, the Python DSL's stubs and the guide's
  pages for each function are generated from these functions' signatures, docs and tags, read through the model. That
  is how `datacube/src/generated/library-enums.ts` is already generated from the library's enumerations.

## 3. What the spike measured

The setup: DuckDB, three instruments with 1,500 business days each, and six queries. Each query was written twice: as
today's `#TS` (the Java cases), and as the line-level Pure call the generic rule would write by hand. The spike did not
build the generic rule. The Pure side's calls are written in the exact shape the rule would emit, and that shape is
mechanical. Each side ran six times; the times below are the median of the last five.

| Query | Points | #TS SQL | Line SQL | Joins | #TS ms | Line ms |
|---|---|---|---|---|---|---|
| `volatility(CLOSE['AAA'], 63)` | 1,437 | 6,478 chars, 6 selects | 5,561, 6 | 0 / 0 | 19.3 | 20.4 |
| `volatility(CLOSE, 63)`, every line | 4,311 | 6,456, 6 | 5,527, 6 | 0 / 0 | 27.7 | 25.0 |
| `volatility(CLOSE['AAA'], 3mo)` | 1,436 | 7,724, 17 | 8,246, 20 | 2 / 2 | 24.6 | 25.4 |
| `beta(CLOSE['AAA'], CLOSE['BBB'], 63)` | 1,437 | 2,537, 7 | 1,881, 9 | 1 / 1 | 8.4 | 9.4 |
| `vwap(CLOSE['AAA'], VOLUME['AAA'], 20)` | 1,481 | 1,875, 6 | 1,233, 7 | 1 / 1 | 5.2 | 5.7 |
| `vwap(CLOSE, VOLUME, 20)`, every line | 4,443 | 1,870, 6 | 1,169, 7 | 1 / 1 | 13.8 | 12.1 |

- **The answers are the same.** Every date (and line) of all six queries matches, each figure within 1e-9 relative.
- **The SQL is smaller in five of the six**, by 14% to 38%. The lines carry three columns rather than the builder's
  working columns. The calendar case is 7% larger, with 3 more selects, because a line's own select and renames add
  layers.
- **The joins are the same.** A function of two lines joins them once (on the date, or on the date and the key), just
  as today's builder joins two operands.
- **The run times are the same, within noise**, in both directions.

What the spike found core can't do yet:

- **G1. A window can't be sized by a parameter.** `rows($back, 0)` and `rows(1 - $w, 0)` inside a function are both
  refused: "window frame bound must be a numeric literal or unbounded(), got TypedVariable". The cause: lite types a
  function's body once, with its parameters as variables (`UserCallInliner`: `specs.compile(callee).body()`), and
  `Frames.bound` classifies the frame at that point, when it needs a literal. **Upstream accepts both.** Its relational
  translation reads the window as a value it has already evaluated during planning, with the call's variables in scope
  (`pureToSQLQuery.pure`: the `_Window` from an `InstanceValue`, then `processWindowFrame` reading `offsetFrom` and
  `offsetTo` as numbers). So this is a gap between lite and upstream, not a design choice. It is the one change the move
  depends on (§4, C1).
- **G2. A window can't be passed in instead.** `_Window<T>` is invariant. A window over the line's three columns can't
  be applied to the wider relation inside the function: "type variable T bound to relation [date, key, value, ord, py,
  r] cannot also bind relation [date, key, value]". A generic `_Window<W>` is refused too ("unbound type variable W").
  Fixing G1 makes this moot.
- **G3. The library's results are typed `Number`.** So a line's value is `Number`. A line declared `value:Float` is
  refused when the function returns.
- **G4. Dates and date-times.** A line declared `date:StrictDate` refuses a date-time-dated line. A line generic in its
  date's type, `Relation<(date:D, key:String, value:Number)>`, works for both (the spike checked this with
  `timeseries::line::mean<D>`). A word whose plumbing does date arithmetic, such as the periods per year, needs that
  arithmetic made generic too. That is checked word by word as each family moves.

## 3a. What each database does with a frame offset that is not a literal (C1's probe, 2026-10-31)

`//core:frame_offset_probe_test` (manual) runs each frame over the same five rows on DuckDB, H2, SQLite and the
embedded Postgres. MariaDB, MySQL, ClickHouse and Trino were run from the same probe as a temporary `//pct` target,
not committed: their drivers are `//pct`'s alone (`tools/deps/jars_table.bzl`). Snowflake, BigQuery and Databricks
can't be reached from here.

| Database | `(3 - 1) PRECEDING` | `? PRECEDING` | A negative offset | `1 PRECEDING AND 2 PRECEDING` (start after end) |
|---|---|---|---|---|
| DuckDB | yes | yes | **quietly empty windows** (every sum null) | quietly empty |
| H2 | yes | yes | refused | quietly empty |
| SQLite | yes | yes | refused | quietly empty |
| Postgres | yes | yes | refused | quietly empty |
| Trino | yes | yes | refused | quietly empty |
| ClickHouse | yes | yes | refused | refused |
| MariaDB, MySQL | **no** (syntax error) | yes | refused | quietly empty |

Every one of them reads `RANGE (3 - 1) PRECEDING` and `RANGE ? PRECEDING` as it reads `ROWS`, except MariaDB and
MySQL, which take only the parameter. And every one refuses `CURRENT ROW AND 1 PRECEDING` as written.

What this means for C1:

- **The database can compute an offset** on six of the eight. MariaDB and MySQL take only a literal or a bare `?`. So
  on those two a computed offset is refused, by their dialects, by name. A literal frame is unchanged everywhere.
- **The two quiet cases need a guard.** Upstream refuses both: a start after the end with "Invalid window frame
  boundary - lower bound of window frame cannot be greater than the upper bound!", and a negative bound has no meaning
  in a frame. So where a bound is not a literal, the lowering wraps each windowed value in a check the database works
  out, with the raise every dialect already renders (`SqlFn.ERROR`). It checks that each offset is not negative and
  that the start does not come after the end. The guard needs no expression inside the frame itself, so it works for
  MariaDB's and MySQL's bare `?` too.
- **Calendar frames** (`_range(n, DurationUnit, ...)`) stay literal-only in C1. The library's calendar windows are
  self-joins, not frames.

## 4. The work, in order (each step green on the frozen answers)

| # | Step | Size | Riskiest unknown |
|---|---|---|---|
| C1 | **Window frames from parameters** (G1; L2 ruled): the typed `over` keeps a bound that is not a literal as a typed expression, so β-inlining substitutes the call's arguments into it. At the lowering, a bound that is a literal by then is classified by its sign, as today. Any other bound takes its direction from its shape: `-(e)` is `e PRECEDING`, anything else `e FOLLOWING`. Its value goes into the SQL for the database to compute, guarded as §3a says | medium (core) | the guard in every dialect; MariaDB and MySQL refusing a computed offset |
| M1 | **The line type and the generic rule, one family**: the rolling statistics (mean, std, volatility, zscores, ...) as `timeseries::line::` functions; their Java cases deleted; the generic rule finds functions through the model | medium | compile time when many line functions are typed in one query (measured here) |
| M2 | **Family by family**: returns and levels; pairs and regressions (two lines); several columns of one instrument (ATR, OHLC, stochastic: N lines joined on date and key); calendar forms; first and last | large | joining N lines, measured against today's builder |
| M3 | **The catalog from the functions**: completions, help, Python stubs and the guide generated from the signatures, `doc.doc` and the profile's tags; the JSON catalog (`git stash@{0}`) dropped | medium | the generator reads `model.pure` through core, inside the tile's bundle budget |

After M3, the one-tree plan carries on from its step 2. Its step 3 (one reader) gets smaller, because the tile's
function list is generated rather than moved.

The spike did not cover these, and each is checked when its family moves:

- **`SAMPLE BY` aggregates.** A period's aggregate is a GROUP BY over the period, not a window. I recommend the
  generic rule keeps these (ruling L5): they are SQL's aggregates, not words of the library.
- **Lines meeting inside a function.** `beta` joins its two lines inner, on the date. Today the builder aligns operands
  (and `FILL`s them) before a function sees them. The generic rule should do the same, so that the default alignment
  is decided in one place.
- **Keys that are not strings.** A subscript can be a number or an enumeration's value (`close[1234]`, `close[TECH]`).
  The key may need to be generic, like the date (ruling L3).
- **Working-column hygiene.** The fresh renaming of a generic body's working columns at each call (task A1, already
  built) applies unchanged.

## 5. What it changes in the one-tree plan

- **D6** becomes "the catalog is the library's own functions: their signatures, `doc.doc` and a profile's tags".
- **Step 1** is done (the frozen answers). Its catalog half is replaced by C1, M1, M2 and M3 above.
- **§5, the layers**: "The catalog: one data file beside the library" becomes "the library's line-level functions", and
  `TsFunctions`' per-word Java is deleted.

## 6. What stays as it is

The table-level library and every number it gives; `TsParser` and `TsAst`; `TsLines` (lines, periods, fills and
alignment); the island's wire form (D1, R6); the frozen answers; and the scope rule as ruled.

## 7. The rulings asked

- **L1. Move the language's words into Pure, as line-level functions?** Recommended, on this evidence: the answers are
  the same, the SQL is mostly smaller, the joins and run times are the same, and the words then live in one place. The
  alternative is to declare the words in Pure (signatures and docs only) and keep the Java cases. That still leaves
  each word's plumbing in two places.
- **L2. A frame bound computed from a parameter, such as `rows(1 - $w, 0)`.** Ruled 2026-10-31 (the user: "Okay", to
  the database computing the value, whatever it may be, its direction read from its shape). §3a has what each database
  does; on MariaDB and MySQL a computed offset is refused. The options as they were put:
  - **(a) The database computes it (recommended).** A bound written as a minus around an expression is read as
    PRECEDING, and the expression goes into the SQL for the database to work out (`ROWS BETWEEN (63 - 1) PRECEDING`).
    The library would write `rows(-($w - 1), 0)`. This is valid upstream too, where upstream computes it. A bound whose
    direction can't be read from its shape, such as `rows(1 - $w, 0)`, is refused loudly. Upstream accepts that one, so
    the refusal would be a row of `docs/SEMANTICS_REGISTER.md`. Unknown: whether every dialect takes an expression as a
    frame offset. Postgres's documentation allows one without variables. DuckDB, H2 and the rest are checked in C1,
    before it is built.
  - **(b) Planning computes it, exactly as upstream does.** This is Java working out a value from the query, which the
    execution tenet forbids. It would be a named residue in `JavaEvalLedgerTest`.
  - **(c) Only literal arguments.** The generic rule passes the bound already signed (-62), as today's builder does, so
    every function's signature gains a parameter that users should never see.
- **L3. The line's type.** `value:Number` (forced by G3). The date generic (G4, proved). The key: `String`, or generic
  so that number and enumeration keys work as they do today?
- **L4. Names.** The functions under `timeseries::line::`, with the language's own names (`timeseries::line::volatility`),
  or the language's names taking over the table-level names, with those renamed (for example, to
  `timeseries::table::`)?
- **L5. `SAMPLE BY` aggregates stay in the generic rule** (recommended), rather than becoming one line-level function
  per aggregate.

## Appendix: the spike's report, verbatim

```
volatility, 63 observations, one line      points  1437 | #TS sql   6478 chars  0 joins   6 selects    19.3 ms | line sql   5561 chars  0 joins   6 selects    20.4 ms
volatility, 63 observations, every line    points  4311 | #TS sql   6456 chars  0 joins   6 selects    27.7 ms | line sql   5527 chars  0 joins   6 selects    25.0 ms
volatility, 3 months (calendar)            points  1436 | #TS sql   7724 chars  2 joins  17 selects    24.6 ms | line sql   8246 chars  2 joins  20 selects    25.4 ms
beta of two instruments, 63                points  1437 | #TS sql   2537 chars  1 joins   7 selects     8.4 ms | line sql   1881 chars  1 joins   9 selects     9.4 ms
vwap, one instrument's two columns, 20     points  1481 | #TS sql   1875 chars  1 joins   6 selects     5.2 ms | line sql   1233 chars  1 joins   7 selects     5.7 ms
vwap, every line's two columns, 20         points  4443 | #TS sql   1870 chars  1 joins   6 selects    13.8 ms | line sql   1169 chars  1 joins   7 selects    12.1 ms
date-time line: refused -- in call to 'timeseries::line::volatility', argument 1: column 'date': expected StrictDate, got DateTime
generic-date line (date): ACCEPTED
generic-date line (date-time): ACCEPTED
```

The line-level vwap's SQL (one instrument): the two lines, joined on date and key, then the windowed sums.

```sql
SELECT t7.date, t7.key, CASE WHEN t7.n_4 < 20 OR t7.vs_3 = 0 THEN NULL ELSE (CAST(t7.pv_2 AS DOUBLE) / CAST(t7.vs_3 AS DOUBLE)) END AS value
FROM (
  SELECT t6.date, t6.key, t6.value, t6.vdate, t6.vkey, t6.vol, t6.o_0 AS traded,
         SUM(t6.o_0) OVER (PARTITION BY t6.key ORDER BY t6.date NULLS LAST ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS pv_2,
         SUM(t6.vol) OVER (PARTITION BY t6.key ORDER BY t6.date NULLS LAST ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS vs_3,
         COUNT(t6.value) OVER (PARTITION BY t6.key ORDER BY t6.date NULLS LAST ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS n_4
  FROM (
    SELECT *, CASE WHEN t2.value IS NULL OR t5.vol IS NULL THEN NULL ELSE t2.value * t5.vol END AS o_0
    FROM (SELECT t1.DAY AS date, t1.SYMBOL AS key, t1.CLOSE AS value
          FROM (SELECT t0.DAY, t0.SYMBOL, t0.CLOSE FROM PRICES AS t0 WHERE t0.SYMBOL = 'AAA') AS t1) AS t2
    JOIN (SELECT t4.DAY AS vdate, t4.SYMBOL AS vkey, t4.VOLUME AS vol
          FROM (SELECT t3.DAY, t3.SYMBOL, t3.VOLUME FROM PRICES AS t3 WHERE t3.SYMBOL = 'AAA') AS t4) AS t5
      ON (t2.date IS NOT DISTINCT FROM t5.vdate) AND (t2.key IS NOT DISTINCT FROM t5.vkey)
  ) AS t6
) AS t7
```
