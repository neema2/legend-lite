# The #TS SQL work: a handoff for a fresh session (2026-11-06)

Written 2026-11-06 because the user asked: "write up all the details and homework and context for a new fresh session
to be able to continue this work with no starting context ... anything else only known to this session". It holds
what the session of 2026-11-03 to 2026-11-06 knew that the repository did not. Its tools, query sets and measurements
are in `docs/ts-sql-homework/`, beside this file.

This document covers one track of the timeseries program: **making the SQL lite writes for a `#TS` query close to
what an expert writes by hand, with the same answers.** It also covers the work next to that track: planning time,
R7 part 3 (words as columns of one table), the open decision, and the rules. The program as a whole is
`docs/TS_V1_PROGRAM_2026_11_01.md`; its §6 status table is updated in the same commit as any work. The time model, whose
rulings constrain everything here, is `docs/TS_TIME_MODEL_2026_11_02.md`.

## 0. Start here

1. `git fetch origin claude/timeseries-language && git checkout claude/timeseries-language`. The work in this
   document ends at `8b948df5d`, plus the commit that adds this document.
2. Read §2, the user's rules. They have been broken before, and each break cost the user something.
3. Read §3. One decision waits on the user. **Ask it before building anything it touches.**
4. Build the harness (§9). Every claim below can be re-measured with it in a few minutes.
5. Pick the next item from §8. Each has a brief: the goal, the design so far, the files, the traps, and how to prove
   it.
6. Before a commit, run the checks in §11, one target at a time, and put their times in the commit message.

Read these too, in this order. Everything here assumes them.

- `AGENTS.md`: the invariants. The Lowerer emits typed MIR and the dialect owns all SQL; no fallbacks; a render switch
  has no `default`; guard files move only with a dated reason.
- `docs/TENETS.md`, including **"Java parses and binds; Pure decides"** (the user, 2026-11-03). The translator builds
  no relation operation of its own beyond a guarded list, and a word's facts are tags in the library.
- `docs/TS_V1_PROGRAM_2026_11_01.md` §1, §6, §7 and §8.
- `docs/TS_TIME_MODEL_2026_11_02.md`: rulings D1 to D7.
- `docs/TENET_CHARTER.md` C6.2b: a frame bound spelled in integers is folded.
- `docs/SEMANTICS_REGISTER.md` rows S129, S130, S131, S132 and S133.

## 1. Where things stand

**The benchmark** is spec-13 in `docs/timeseries/examples.sql`, the pairs trade:

```sql
SELECT hedge_ratio(close['KO'], close['PEP'], 6mo)        AS hedge,
       residual(close['KO'], close['PEP'], 6mo)           AS spread,
       zscores(spread, 3mo)                               AS z,
       CASE WHEN z >  2 THEN -1 WHEN z < -2 THEN 1 ELSE 0 END AS position,
       lag(position, 1) * (diff(close['KO'], 1) - lag(hedge, 1) * diff(close['PEP'], 1)) AS daily_pnl,
       sum(daily_pnl, '2023-01-01')                       AS pnl_since_2023
FROM   prices ON date BY symbol
FILTER date >= '2023-01-01'
```

The same query written by an expert is `docs/ts-sql-homework/data/spec13_by_hand.sql` (2,185 characters). Measured on
DuckDB over the documents' seeded data, lite's answer equals the hand-written one to 2.8e-14 on all 522 dates, and has
at every step below.

| Step | Commit | spec-13 SQL | DuckDB run | Joins |
|---|---|---|---|---|
| Line forms, a calendar window as a self-join | (before) | 23,941,589 chars | does not finish | |
| A calendar window as one numeric frame | `b7d69d98b` | 194,197 | ~1,050 ms | |
| Frame bounds folded (C6.2b) | `5b7803675` | 148,223 | ~930 ms | |
| Unread computed columns dropped | `5b7803675` | 118,190 | ~550 ms | |
| A select merged into its one subselect | `5b7803675` | 88,701 | ~540 ms | 31 |
| Reused relations bound by lets, written once as WITHs | `4c3a2cfbe` | 22,089 | ~70 ms | 10 |
| A count a declared key makes one folded to 1; windowed levels merged | `c4afec56d` | 20,137 | ~35 ms | 10 |
| Two lines met once; hedge and spread columns of that table | `8da2de306` | 19,784 | ~41 ms | 8 |
| Calendar spans, diff and sum-since-a-date as rows forms | `a6483bf2f` | 23,133 | ~40 ms | 5 |
| A chain of windowed levels laid out again, identical windows once | `8b948df5d` | 21,007 | **~26–37 ms** | **5** |
| **By hand** | | **2,185** | **~8–10 ms** | **1** |

On the last row, DuckDB spends 6–9 ms preparing the query and 12–19 ms running it.

Every example's current figures are in `docs/ts-sql-homework/data/examples_2026-11-04.tsv`: characters, SELECTs,
windows, joins, rows, planning ms and run ms. Over all 27 runnable examples the run time is flat-to-better: 573 ms
before the R7 part 3 work, 584 ms with the rows forms, 560 ms with SelectLayers (best of three each, summed).

**What still separates spec-13 from the hand-written SQL:**

1. **Joins: 5 against 1.**
   - **One is the same as by hand:** KO met with PEP.
   - **Two are semantics, not waste.** `diff(close['KO'], 1)` is KO's change over KO's own dates. The hand-written query
     takes `ko - lag(ko)` over the pair table's rows, which equals it only where KO and PEP trade on the same days.
     The compiler cannot know that they do. So `daily_pnl` meets the pair table with KO's line and with PEP's line.
   - **Two are the answer's spine over two tables:** the pair table, and the `daily_pnl` table. §8 W3 removes one.
2. **Windows: 15 against about 9.**
   - Each ramp has its own first-date `MIN(...) OVER (PARTITION BY ...)`.
   - The calendar z-score computes mean and std through `overPeriod` twice.
   - The by-hand query uses `regr_slope`/`regr_intercept`. Lite writes the covariance over the variance, which is
     gs-quant's formula.
3. **Bookkeeping columns.**
   - A value that is not a point of every row carries a points column (`rows_q_*`).
   - Masks: `CASE WHEN q IS NULL THEN NULL ELSE v END`.
   - Every level re-projects every column. That is most of the 21K characters.
4. **The returns-based pair words** (correlation, beta, r_squared, alpha and the rest) still compute their own tables.
   They are not in spec-13; they are in spec-16 and guide-12. See §8 W2.

**Planning time** is no longer a problem:

| Example | Before (2026-11-04 morning) | Now |
|---|---|---|
| spec-15 | 7.1 s | ~0.5 s warm, 1.5 s first |
| spec-13 | 0.86 s | ~0.2–0.3 s |
| spec-01 | 1.5 s | ~0.9 s |

**The documents' examples**: 27 of 29 run. `//core:ts_documents_test` lists the two that do not, in `NOT_YET`:

- **guide-06**, `rebase(close, latest - 3y)`: a base by a date, with `latest` in an argument. Not built.
- **spec-05**, `WITH s AS TS (...) SELECT regr_slope(...) FROM s`: plain SQL over a series query. Not built.

## 2. The user's rules

Each was learned the hard way. They are the program's §7, repeated here because a fresh session must not miss them.

- **Nothing outward without the user's explicit word in this session.** No new PR, no PR comment, no CI run, no push
  to `main` or to any other branch, no merge. An answer to a question about a PR is not permission to open one (the
  user, 2026-11-02: "Who asked you to open a PR? You're taking slots from other sessions").
- **Never merge #81.** It waits on #83.
- **Work on `claude/timeseries-language`.** Commit and push there. It has no PR of its own, so pushing starts no CI.
- **Report CI problems in chat.** Do not act on them unasked.
- **Run heavy suites one at a time** (`--local_test_jobs=1`), and put their times in the commit message. Run side by
  side, they slowed each other and one timed out (2026-11-02).
- **Never edit the workspace while a gate or test runs.** Draft in the scratchpad and copy in afterwards.
- **Commit trailers.** Every commit ends with the session's `Co-Authored-By` and `Claude-Session` lines. No model
  identifier goes in any repository artifact.
- **Upstream fidelity.** Every difference from gs-quant, legend-pure or legend-engine is first posted to the user: the
  expression, upstream's answer, lite's. It is then written as a row of `docs/SEMANTICS_REGISTER.md`, quoting the
  user's ruling. A row the user did not rule is a defect. Upstream's answer is read from source or asked of the engine
  runner, never recalled.
- **Guard ceilings, pins and allowlists** move only with a dated reason and a measurement.
  - `TsTranslatorGuardrailTest.R7_BRANCHES` may **only shrink**.
  - `CodeShapeGuardrailTest` allows at most **8** raw `SqlExpr.Column` construction sites.
  - Express a design so it needs no new entry, or ask the user.
- **Layout.** Never change the UI layout without asking. Show #TS and Show Python go in the existing boxes.
- **The stop hook** demands that uncommitted work be committed and pushed. Keep the tree clean between steps.
- **Never `git stash` or swap files** while a test or gate is running (see the equivalence method in §9).
- **When the user asks for an expert answer,** the bar is the hand-written SQL, the answers held equal, measured.

## 3. Decision waiting on the user: ask first

On 2026-11-04 the user was asked the following and has not answered:

> **Should the words that count observations be refused on a line with several rows on one date?** (Several rows on
> one date happen, for example, with `FROM prices ON date` and no BY.)

**The finding.** A line with several rows on a date (ruled allowed: D1) has no order among that date's rows. So a word
over its rows by count has no defined answer:

- `lag(x, n)`, `diff(x, n)` and `returns(x, n)`;
- any window of n observations: `mean(x, 3)`, `std(x, 20)`, `zscores(x, 3)` and the rest.

**The proof.** `docs/ts-sql-homework/data/equivalence_queries.sql`, query q13
(`SELECT close AS c, diff(close, 1) AS d, zscores(d, 1mo) AS z, max(d, 2mo) AS mx FROM prices ON date`). Its SQL run
three times, unchanged, gave three different answers.

**Why it matters now.** This was already true of the existing `lag` rows form. Since `a6483bf2f`, `diff` of
observations is a rows form too, and words of words such as `zscores(diff(close, 1), 1mo)` over such a line now answer.
They used to be refused, because their separate lines met (D1).

Calendar windows (`RANGE` frames) are deterministic under ties. Only the windows and offsets that count observations
are not.

**The recommendation given to the user:** refuse, naming the fix ("add BY, or SAMPLE BY 1d with last(x)").

**If ruled yes, build it like this:**

- The refusal belongs to the library ("Pure decides"), as D1's does. `lines::single` refuses while the query runs.
  Add the same kind of check in the rows forms and line forms of the observation-counting words: lag, diff, returns,
  and every `rows(-(w - 1), 0)` window. KeyedCounts (D2) already folds such a check to nothing where a declared key
  makes rows unique, so the cost lands only where the data could be ambiguous.
- Record it as a ruling in `docs/TS_TIME_MODEL_2026_11_02.md`. It amends D1 and D5.
- Test it beside `TsIslandTest.linesOfSeveralRowsOnADateMeetAsALookupAndBothSoAreRefused`.

**Also not decided:** whether Snowflake gets SelectMerge and SelectLayers. Snowflake is one of the program's three
databases (P4), but it is rendered and never run here, so a change to its SQL cannot be checked against an answer.
Today only DuckDB, H2, SQLite and Postgres have them. Ask before enabling them on a dialect this container cannot run.

## 4. What was built, 2026-11-03 to 2026-11-04

Newest first. Each commit message has the full measurements and test times.

| Commit | What | Why / effect |
|---|---|---|
| `8b948df5d` | `sql/dialect/SelectLayers` (after SelectMerge, on DuckDB, H2, SQLite and Postgres) and its `SelectLayersTest` | A chain of selects that keep their rows, read as one set of values. Each value is defined once: identical expressions and identical windows are one. Each is computed at the lowest level its reads allow. spec-13: SELECTs 47 → 29, windows 18 → 15, run 40 → 26 ms. |
| `a6483bf2f` | Rows forms in the library: the calendar spans of mean, sum, min, max, std, var, range, zscores, drawdown, max_drawdown, lag and diff; diff of observations; sum since a date. Plus `TsIslandTest.theWordsOfTwoLinesAreColumnsOfTheirOneMeeting`. | The words become columns of their value's own table, not lines joined back. spec-13 10 → 5 joins. |
| `8da2de306` | The translator's met tables (`TsFunctions.Ctx.met`, `TsToPure.Lines.met`), pair rows forms (`TsWords.pairForm`), library `rows::hedge_ratio/residual/cov` (each window) | Two lines met once for the same values; every word and figure of them a column of that one table. spec-13 10 → 8. |
| `1922c26d5` | `compiler/spec/TsIslands.TypedOnce`; the Typer reaches the island seam (`SqlIslands.desugar(island, typer, …)`, `TsIslands.lifted(statement, typer, …)`) | spec-15 planned in 7.1 s, 3.0 s of it typing one relation of 1,102 nodes that the translator asks the columns of: name resolution turned its shared graph into a tree. Now its reused relations are bound as lets and typed once. 7.1 s → 0.6 s. |
| `36d65acc7` | `timeseries::bounded` clamps every correlation into [-1, 1] (option A); register S133; C6.2b marked ruled | spec-16: r² of SPX against itself was 1 + 4e-16, and `sqrt(1 - r²)` refused. Exact decimals were weighed and ruled out. |
| `ad5c1b92c` | S132 ruled: a count a declared key makes one is 1, for every query | The user: "yes can make it for all queries that is fine". |
| `c4afec56d` | `lowering/KeyedCounts` (D2 in the compiler); SelectMerge merges a windowed-only inner select under outer windows | spec-13's 15 duplicate-check counts → 0. Windowed levels share a select. |
| `4c3a2cfbe` | `TsToPure.bindShared` and `TsIslands.lifted` (lets `__ts_l<n>`); let-sharing ported from `claude/cte-let-sharing` (`lowering/SharedRelations`, `dialect/SharedRelationsInPlace`, `SqlRewriter.with`) | A relation read twice is bound once and written once, as `WITH __let<n>`. spec-13 88,701 → 22,089 chars. spec-15 2.9M → 54K and runs. |
| `5b9d72714` | native claims regenerated; IQ showcase DuckDB SQL re-pinned | The fallout of the merge pass. |
| `5b7803675` | `WindowFrame.spelled` (C6.2b), `SubselectPrune` drops unread computed columns, `dialect/SelectMerge` | 194K → 89K chars. |
| `b7d69d98b` | `timeseries::periodKey`/`periodSpan`/`windowPeriod`: a calendar window of dates as one `_range` frame over a period key | No more self-join. Measured equal to the join on 9 databases. |

Earlier R7 work, on 2026-11-03: `a00fc91b2` (one table per base), `5eb1ba925` (rows forms, mean's slice),
`485ba5933` (the simple statistics), `38f635a7a` (zscores, drawdown, max_drawdown, lag).

## 5. How a #TS query becomes SQL: the map

Line numbers drift. Find the code by class and method.

**1. Parse.** `protocol/ts/TsParser` → `TsAst`.

**2. Translate** (`protocol/ts`). The translator writes Pure: library calls, per-point lambdas, and the sources.

- **`TsToPure.Builder.query()`:**
  - opens the sources (`Opening`, `Resolved`);
  - evaluates each item through `Lines`;
  - assembles the answer: each item is read from its base's newest table, and the parts are put together by
    `TsLines.answer` / `spread`, then `someValued`, FILTER, latest, `ordered`.
- **`TsToPure.Builder.Lines`** is the translator's state:
  - `extended` and `newest`: each base's growing table;
  - `grown(line, add)`: adds a column to the base's newest table;
  - `presence`, `present` and `points`: a value's points column, and its own line (`rows::points`);
  - `met(parts, pos)`: added 2026-11-04, memoized by `(base identity, value, presence)` per part;
  - `computed(e)`: a per-point expression. Its parts are on one table (a column, plus `rows::both`/`masked` for
    points), or they are met through `met`;
  - `read(key, make)`: memoizes the reads of source columns.
- **`TsWords.call`:**
  - fits a word's overloads (`bind`, `argument`, `each`, `timed`);
  - tries `rowsForm`: one line, `rows::<word>` with `(t, key, date, value, present, <own params>, out, points)`;
  - or `pairForm`: two lines, `rows::<word>` with `(t, key, date, x, y, <own params but Sample>, out)`, on `c.met`;
  - otherwise uses the line form, `TsLines.call(fqn, …)` plus `back` (rename to the line's columns, then
    `lines::timed`).
- **`TsLines`:**
  - `Line(rel, part, byInstrument, at)`, plus the columns `PERIOD` and `ORD`;
  - `column` and `sampled`: a source's line;
  - `joined`: lines met on PERIOD and key through `lines::meet`, dated at the later date;
  - `spread`: the answer's spine, from `periodsOf`, `union`, `spine`, then `onto` for each distinct line.
- **`TsToPure.bindShared`** binds each call reached from two or more parents as a let `__ts_l<n>`, when its free names
  are the query's own.
- **The library**, `projects/timeseries/model.pure`. Search for these section headings:
  - "A WORD'S ROWS FORM" (the rows forms);
  - "The rows forms of the words over a calendar span";
  - "The rows forms of the words of two lines' levels";
  - `lines::meet`, `perPeriod`, `single` (D1's check);
  - `spine`, `onto`, `someValued`;
  - `periodKey`, `periodSpan`, `windowPeriod`, `rampPeriod`, `overPeriod`;
  - `timeseries::bounded`.
- After **any** edit to `model.pure`, run `bazel run //datacube:update_generated`. It regenerates
  `datacube/src/pure-library-text.ts`, and a check fails without it.

**3. Type** (`compiler/spec`).

- `SpecCompiler.typeQueryBody` lifts the islands (`TsIslands.lifted`) and types the statements and lets in order.
- `TsIslands.translated` gives the translator its facts, `Facts` over `TypedOnce` over `SqlIslands.sources`.
- `TypedOnce.columns` types a relation with shared reads as lets, typed once.
- `Overloads.inlineNormalized` inlines a generic library call. It uses `Typer.withPretyped`, so a chain of calls is
  typed linearly.
- `StaticFold` runs at this stage.
- `typed/WindowFrame.Bound.of` folds spelled frame bounds (`spelled`), per C6.2b.

**4. G½.** `UserCallInliner` splices query lets by reference.

**5. Lower** (`lowering`).

- `Lowerer.lower(List)` returns `SubselectPrune.prune(engineText ? q : KeyedCounts.fold(q, tableKeys), !engineText)`.
- `shareRelations`, `sharing` and `sharedRead`: a let read twice becomes `WITH __let<n>`.

**6. Render** (`sql/dialect`).

- `DuckDb`'s pass list, in order: …, `OuterJoinSubqueryFilters`, **`SelectMerge`**, **`SelectLayers`**,
  [`StableScanOrder`], `VariantEgress`, then StoredReads.
- H2: `ProjectionWrapsFold`, `SelectMerge`, `SelectLayers`, `SharedRelationsInPlace.bindingAValue()`.
- SQLite: `QualifyToSubselect`, `SelectMerge`, `SelectLayers`, `ConstantKeysAsExpressions`, ….
- Postgres: …, `SelectMerge`, `SelectLayers`, `ConstantKeysAsExpressions`, `RootNumericTypes`.
- MongoDB writes a shared relation in place at each read.

**SelectLayers, in short** (`sql/dialect/SelectLayers.java`; read its javadoc first):

1. It reads a chain top-down. Every level must keep its rows: no DISTINCT, GROUP BY, HAVING, QUALIFY, ORDER BY,
   LIMIT, OFFSET, aggregate or UNNEST, and no WHERE except at the bottom. Every read must be a qualified column.
2. It interns definitions bottom-up:
   - columns of the level below become reads (`Resolve`);
   - the bottom's source columns become values of their own (`Bases`);
   - every window call becomes a value of its own (`Windows`);
   - identical canonical expressions are one value (`byExpr`).
3. A value is computed once at its own level (`own()`) if it is a window, or is read more than once with
   (reads − 1) × size > 24 nodes (`once`). Otherwise it is written into its readers.
4. A value that may raise (`ADD_INTERVAL` in calendar ramps is in `SqlFn.RAISING`) is always a column of its level,
   read or not, so it is still evaluated on every row.
5. Levels: a value sits one level above the own values it reads, and at the level of the inlined ones it reads.
6. It refuses, keeping the original, when:
   - the chain has no window (pure row-value chains belong to the lowering's fold, pinned by `ScaleCostsDuckDbTest`);
   - the result has no fewer levels;
   - the result has more than twice the nodes.

## 6. Measurements: what costs what, today

These are spec-13 on DuckDB over the documents' data. The by-hand query has 1 join, 9 window calls, 4 levels, about
8 ms.

- **Run:** 26–37 ms, of which prepare is 6–9 ms. The operators total about 15 ms; `WINDOW` is about 11 ms of that.
- **The met CTE** has 11 window calls, down from 14. The P&L CTE has 2 joins and 4 windows. The main query has 2
  joins (the spine).

Methods:

- `docs/ts-sql-homework/scripts/harness.sh.txt` `profile` writes the operator profile; summarize it with
  `duckdb_profile_ops.py.txt`.
- `ctes.py.txt` summarizes the CTEs: joins, reads and windows each.
- **Planning time:** `harness.sh plan <id>` gives the total, typing included. `harness.sh jfr <id> out.jfr`, then
  `jfr_aggregate.py.txt`, shows where it goes; full-depth stacks matter, because JFR keeps 64 frames by default.

## 7. How correctness was held

Use the same methods.

1. **Every documents example, old against new, row for row.**
   - Run `harness.sh answers <dir>` on both builds (§9's swap method).
   - Then run `compare_answers.py.txt <old> <new>`.
   - Every commit in §4 kept all 27 examples value-identical.
2. **The equivalence set**, `data/equivalence_queries.sql`: 13 queries the documents do not cover.
   - Each calendar span; words of words with ramps (points that are a prefix); subscripts; SAMPLE BY.
   - The pair words by observations, to date (`all`) and by a span; a broadcast against one line; two sources; the
     spec-13 shape.
   - Run it against the code before the change: `harness.sh answers <dir> <file>`.
   - q01–q12 matched within 5e-14; q13 is the tie case of §3.
3. **The hand-written spec-13**, compared with `compare_spec13.py.txt`.
4. **The frozen answers**, through `//datacube:timeseries_ts_parity_test`, `stress_markets_test` and
   `timeseries_queries_test`.
   - These cover the gallery: beta A–C, correlation A–B, cov A, regression A–D, pair A–B, vol-corr A–E, zscores A,
     diff-lag A–C.
   - **They are almost all integer windows**: the gallery's `'1y'` and `'6m'` are view ranges, not windows. Calendar
     windows need method 2.
5. **Tests with values worked in Java.**
   - `TsIslandTest.theWordsOfTwoLinesAreColumnsOfTheirOneMeeting`: hedge_ratio, residual and cov against least
     squares, and one join.
   - `TsIslandTest.aSpanBackIsAsOfThatDateAndADateWindowRunsFromIt`: sum since a date, lag over 2d.
   - `TimeseriesLibraryTest.aCalendarWindowOfDatesIsTheWindowAsAFrame`.
   - `SelectLayersTest`: each case checked on DuckDB against a hand-written query.
6. **The gs-quant oracle** (`tools/ts-oracle/oracle.py`) was not run in this session; gs-quant is not installed here.
   Its install is in `docs/TS_V1_PROGRAM_2026_11_01.md` §8.

## 8. Next work, in order, each with a brief

The user's agreed order of 2026-11-04 was:

1. spec-16 option A;
2. spec-15's planning time;
3. the joins (R7 part 3);
4. shared windows.

All four are done. These are the natural next items.

### W1. The §3 ruling, once the user gives it

The brief is in §3.

### W2. The returns-based pair words as rows forms

**Words:** correlation, beta, r_squared; then alpha, tracking_error, information_ratio, up_capture, down_capture,
lead_lag.

**The goal.** Like hedge_ratio and residual, each becomes a column of its two lines' met table, not a table joined
back. Then spec-16 (beta, r_squared) and guide-12 (alpha, r_squared, hedge_ratio, residual) lose their joins.

**What the line forms do.** For example, `timeseries::line::correlation(x, y, w)` is
`h::meet($x->line::returns(1, []), $y->line::returns(1, []), $sample)->timeseries::correlation(...)`. That is:

1. each line's **returns over its own dates** first;
2. then the two met;
3. then the table function.

alpha, tracking_error and information_ratio also take `h::annualizedPair` (a `py` column: periods a year, from SAMPLE
BY or the dates). lead_lag shifts y (`h::shifted`).

**Why not compute returns inside the rows form over the met table.** That would be a lag over the **pair's** rows, a
different answer wherever the two lines' dates differ. Do not do it.

**Design, proposed, not yet ruled:**

1. **A rows form for `returns`** (`rows::returns(t, key, date, value, present, obs, type, out, points)`). It is the
   table function `timeseries::returns` masked by points, with the points the value's. Check the line form's points:
   a return's first row is a point with no value.
2. **A library tag on each such word** saying its two lines are taken as their returns first, for example
   `timeseries::Words.pairs = 'returns'`. Word facts are tags already: `Words.label`, `family`, `unit`, read by
   `TsCatalog` (A3 step 1). This keeps the decision in Pure.
3. **`TsWords.pairForm`** reads the tag. It calls the `returns` word on each argument through the same `call`, which
   gives each line's returns as a column of its own base, then meets those two values with `c.met`. Then it calls
   `rows::correlation(t, key, date, x, y, w…, out)` and so on: the line form's table function on the met table.
4. **annualizedPair:** the rows form computes `py` the way `h::annualizedPair` does. Do these words last.

**Traps:**

- **The guards.** `TsTranslatorGuardrailTest` allows the translator only library calls plus `select`, `rename`,
  `concatenate`, `extend`, `filter`, `project`, `getAll` and `letFunction`. `R7_BRANCHES` may not grow: no new `if`
  that picks a table's form. `aRowsFormIsTheRowsPackages` requires `rowsFqn` to be assigned once.
- **pairForm's refusals.**
  - Under FILL it falls back to the line form: the line form meets unfilled.
  - Where both values are on one table it also falls back. Met with itself, a table with several rows on a date would
    be refused (D1), and that is the line form's to say.
  - It drops Sample-typed parameters from the matching: the translator's meet handles SAMPLE BY periods.
- **The default-window overloads** (`correlation(x, y)` meaning 63) have no rows form; see W5.

**Proof.** Run method 2 of §7 with these words added to the equivalence set: each window kind, SAMPLE BY, a broadcast
against one line. Then the frozen answers (correlation A–B, beta A–C, regression A–D), spec-16 and guide-12
value-identical, and a Java-computed test like `theWordsOfTwoLinesAreColumnsOfTheirOneMeeting` (it has `pearson` and
`covariance` helpers).

### W3. The answer's spine when one table's rows contain the other's

In spec-13 the answer spreads two tables (the pair table, and daily_pnl's) through `TsLines.spread`. That is
`periodsOf` of each, `lines::union`, `spine` (a GROUP BY), then `onto` (LEFT JOIN) for each: two joins plus a union.

daily_pnl's table was met *from* the pair table, so its periods are a subset of the pair table's. Where one distinct
line's periods contain every other's, that line can be the spine: one LEFT JOIN, no union, no group.

**The work:**

- `met` must remember where each met table came from (its parts' bases).
- `spread` (or `TsToPure`'s assembly before `TsLines.answer`) must find such a line.

**Keep:**

- the D1 seen-count checks (`sp_seen` columns): a line with several rows on a period;
- `someValued` (R6): a row where at least one item has a value.

**Prove** it with method 1 of §7 and the D1 tests in `TsIslandTest`.

### W4. SelectLayers follow-ups

- **Duplicate windows inside one select.** SelectMerge runs first, and can write one window twice into a single
  select, for example `AVG(..) * 2` beside `AVG(..)`. That is the first case of `SelectLayersTest`: rendered straight
  through DuckDB, two `AVG(` survive. SelectLayers only works on chains of two or more levels.
  - **Measure DuckDB first.** Check with `EXPLAIN` whether DuckDB already computes identical window expressions once;
    then nothing is needed.
  - If it does not, either run SelectLayers before SelectMerge, or let it share within one level by extracting
    repeated windows one level down.
- **guide-12's windows stayed 28** while spec-13's dropped. Find why sharing did not reach them: look at its CTEs with
  `ctes.py.txt`.
- **The heuristics:** `INLINE = 24` and the 2× size guard. Measure them on the examples; there may be better values.
- **Planning cost.** SelectLayers runs at every select of the post-order walk, and re-reads a chain from each level as
  the walk climbs. Planning stayed flat (sum 2,709 → 2,819 ms, noise), but an O(n²) walk on a 300-level chain is
  possible. If it ever shows, process only at chain tops.

### W5. Rows forms for the remaining one-line and default overloads

- **To date** (`w: Scope`, written `all` in #TS) has no rows form for mean, sum and the rest.
- **The default-window overloads** have none either: `hedge_ratio(x, y)` means 63; `correlation(x, y)` and others are
  the same. A rows form for the default would write 63 a second time. A better design keeps the default in one place,
  for example a tag naming the overload it delegates to, so the translator calls that one's rows form.
- Of the line forms that wrap a table function, these still have no rows form:
  - **calendar spans:** count, product, vol_swap_volatility, dickey_fuller, half_life, percentiles, median,
    percentile, returns;
  - **`percentilesPeriod` and `quantilePeriod`** keep a self-join; no frame expresses them.
- **Before adding each one,** check that its table function's ramp and windows read the value's points, not the
  table's rows. `rampPeriod` dates from the value's first non-empty date, which is correct. A function that counts
  rows, or takes `MIN(date)` over all rows, would be wrong on a table whose first rows are not the value's points.

### W6. Reconcile with `claude/cte-let-sharing` when #83 lands

That branch (`364c3e473`, `564f98829`, `cffdfcb34`) is stacked on #83 and holds the same let-sharing this branch
ported in `4c3a2cfbe`.

- Expect conflicts in `Lowerer` (shareRelations, sharing, sharedRead, lower(List) with a Frame), `SqlRewriter.with`,
  `SharedRelationsInPlace`, `BodyDriver.sharingRelations`, `SqlWith.SHARED_PREFIX`, `FreeVars`, and the two tests.
- Keep one implementation. The port left out #83's grid-report case of `SharedRelationsInPlace.binds`, because this
  stack has no BodyPlanner, ProductStatements or GridReport.
- The parser-equivalence ratchet moved +2 for the ported tests on both branches.

### W7. Main moved

`main` has #88, which pins the CI lanes to ubuntu-24.04. It is in neither this branch nor #81. Merge it only when the
user says to.

### W8. The program's own items

These are in `docs/TS_V1_PROGRAM_2026_11_01.md` §6:

- **A2:** the review, the full gate, CI on a PR. CI needs the user's word.
- **A3:** steps 3–5.
- **A4:** the words' remainder.
- **A5:** the library's text off the wire.
- **A9:**
  - spec-05: plain SQL over a series query (task: `WITH s AS TS (...) SELECT regr_slope(...) FROM s`);
  - guide-06: rebase by a date, with latest in an argument;
  - spec v4;
  - the guide brought up to the rulings.
- **A10**, and **B1–B8** after Part A.

## 9. The toolkit

All of it is in `docs/ts-sql-homework/`:

- **`harness/*.java.txt`:** the harnesses, in package `com.legend.integration`, run on `TsDocumentsTest`'s private
  `model`, `examples` and `seed` methods by reflection:
  - `SqlDump`: an example's SQL;
  - `SeedDump`: SQL timed on the seeded data, its rows as CSV;
  - `Profile`: prepare and run times, plus DuckDB's detailed JSON profile;
  - `AllAnswers`: every example's rows to CSVs;
  - `AllJoins`: per-example size, SELECTs, windows, joins, rows, planning and run;
  - `AllExamples`: the older table, with `COUNT(*) OVER` counts in place of joins;
  - `PlanOne`: one example's planning, n times.
- **`scripts/harness.sh.txt`:** builds and runs them. See its header.
- **`scripts/compare_answers.py.txt`:** two answer directories, file by file: rows, worst relative difference, null and
  text mismatches.
- **`scripts/compare_spec13.py.txt`:** lite's spec-13 against the hand-written one.
- **`scripts/ctes.py.txt`:** a statement's CTEs, each with its joins, the CTEs it reads, and its windows.
- **`scripts/jfr_aggregate.py.txt`:** self and inclusive time per method from `jfr print` output.
- **`scripts/duckdb_profile_ops.py.txt`:** operator time from DuckDB's JSON profile.
- **`data/spec13_by_hand.sql`:** the expert's spec-13.
- **`data/equivalence_queries.sql`:** the 13 queries of §7.
- **`data/examples_2026-11-04.tsv`:** every example's figures at `8b948df5d`.

Setup:

```bash
export SCRATCH=/tmp/ts-sql            # or the session's scratchpad
mkdir -p "$SCRATCH" && cp docs/ts-sql-homework/scripts/harness.sh.txt "$SCRATCH/harness.sh"
bazel build //core:ts_documents_test && bash "$SCRATCH/harness.sh" build
bash "$SCRATCH/harness.sh" sql spec-13 "$SCRATCH/s13.sql"
bash "$SCRATCH/harness.sh" time "$SCRATCH/s13.sql"
cp docs/ts-sql-homework/data/spec13_by_hand.sql "$SCRATCH/hand.sql" && bash "$SCRATCH/harness.sh" time "$SCRATCH/hand.sql"
python3 docs/ts-sql-homework/scripts/compare_spec13.py.txt "$SCRATCH/s13.sql.csv" "$SCRATCH/hand.sql.csv"
```

The harness runs on the classes Bazel built for `//core:ts_documents_test`. It takes their classpath from the
target's launcher script, and the JDK from Bazel's `remotejdk25`. **Rebuild the target and rerun `harness.sh build`
after every code change**, or you are measuring stale classes.

**Comparing old and new, safely.** The swap leaves the tree dirty, so never do it during a gate and never commit while
swapped.

```bash
mkdir -p $SCRATCH/keep
for f in <the files the change touched>; do cp $f $SCRATCH/keep/$(basename $f); git show <old-ref>:$f > $f; done
bazel build //core:ts_documents_test && bash $SCRATCH/harness.sh build && bash $SCRATCH/harness.sh answers $SCRATCH/old
for f in <the same files>; do cp $SCRATCH/keep/$(basename $f) $f; done
bazel build //core:ts_documents_test && bash $SCRATCH/harness.sh build && bash $SCRATCH/harness.sh answers $SCRATCH/new
python3 docs/ts-sql-homework/scripts/compare_answers.py.txt $SCRATCH/old $SCRATCH/new
git status --short    # must show only your intended changes
```

`git stash`, build, run, `git stash pop` works too. **A cherry-pick abort once reset HEAD**; recovering took
`git reset --hard <ref>` and `rm -rf .git/sequencer`. Prefer the copy method.

**Reading gs-quant's source.** gs-quant is not installed, and installing it pulls heavy dependencies. To read its code
without installing it:

```bash
pip download gs-quant --no-deps -d <dir>
python3 -I -m zipfile -e <dir>/gs_quant-*.whl <out>
```

The rolling words are in `timeseries/econometrics.py` and `timeseries/statistics.py`.

**Postgres as root.** The embedded Postgres refuses to run as root, so the 8 `*_postgres*` targets fail in under a
second in this container. Run them as `ubuntu`:

```bash
mkdir -p /tmp/pgrun && cp tools/postgres/as_unprivileged.sh /tmp/pgrun/ && chmod 755 /tmp/pgrun /tmp/pgrun/as_unprivileged.sh
bazel test //core:cypher_parity_postgres_test //core:fixture_aside_postgres_test //core:host_operations_postgres_test \
  //core:iq_parity_postgres_test //core:postgres_arm_test //core:sql_island_postgres_test //core:sql_parity_postgres_test \
  //core:store_writes_postgres_test --strategy=TestRunner=standalone \
  --run_under="/tmp/pgrun/as_unprivileged.sh ubuntu" --local_test_jobs=1
```

The wrapper must sit outside the workspace, in a directory `ubuntu` can read. Without the `ubuntu` argument it fails
with "No such file".

## 10. Traps and facts learned

**The SQL tree and its walks**

- **`SqlExpr.WindowCall.children()` omits a ranking function's arguments.** Its function is a `SqlAgg.RankingFn`
  (LAG, LEAD, FIRST_VALUE and the like), which is not an `SqlExpr`. A walk over `children()` misses LAG's argument;
  the `SqlRewriter` itself visits it. `SelectLayers.parts` handles this. Any new walk must too.
- **`SqlRewriter` is post-order.** The `select()` hook sees the inner select first, so a chain is offered to a pass
  bottom-up, once per level.
- **A window's reducer is an aggregate node** (`SqlAgg.aggregatesSelect` is true for it). A purity check must look at
  its arguments, not refuse the reducer.
- **`SqlExpr` records compare structurally, types included.** A `Column` compares its table, name, `TypeFact` and
  origin. Placeholder columns for canonical forms should come from the stamped door, `Column.derived`. A raw `new
  Column(...)` counts against the guard's 8.
- **`SqlExpr.raisesNothing`.** `SqlFn.RAISING` holds ERROR, PARSE_INT, PARSE_DATE, ACOS, ASIN, ADD_INTERVAL,
  ADD_INTERVAL_TEMPORAL, STRPTIME, GUID, TIMEZONE, MAKE_DATE and MAKE_TIMESTAMP. **Calendar ramps use `ADD_INTERVAL`,
  so they count as raising.** The rule from SelectMerge and SelectLayers: a raising value must still be evaluated on
  every row, never only inside a reader's branch.
- **`ScaleCostsDuckDbTest` pins the lowering fold's budget.** A chain that reads the level below twice must read the
  base column exactly 4 times. A pass that inlines without a budget blows up exponentially and runs out of memory; the
  first SelectLayers did, by counting distinct readers instead of reads. Count occurrences.

**The translator**

- **Name resolution turns the translator's graph into a tree.** `NameResolver.resolveQuery` rebuilds every node, so a
  shared subtree becomes copies, and the Typer types each copy. That cost spec-15 3 s on one relation. Bind sharing
  first (`TsToPure.bindShared`), then resolve. `TypedOnce` and `lifted` both do this.
- **Two values of one table with different points**, met with others, must be read from that table once, filtered by
  each value's points in turn (`met`: `points()` folded over the values). Otherwise `joined` meets the table with
  itself, and the second copy's columns clash ("the column '__ts_lag_15' already exists"). guide-13 hit this.
- **The prefix rule.** A table's rows that are not points of a value are always at its start: a word's ramp, a
  return's first row. This is why a rows form's row frame over a shared table equals the line form's frame over the
  value's own points. Keep it true: a construction that made non-points appear mid-table would break every rows form.
- **A value is none wherever it is not a point.** Rows forms mask their answer by points; source columns are points of
  every row.
- **`TsLines.joined` and the line forms' `helpers::meet`** both meet on (key, period), dated at the later date. That is
  why a pair rows form equals its line form. Under FILL they differ: the translator aligns, the line form meets
  unfilled.

**Bazel and generated files**

- **Bazel caches test results.** "(cached) PASSED" is a run of the same inputs. A library change is an input of the
  core timeseries tests, so they rerun.
- **Regeneration commands:**
  - `bazel run //datacube:update_generated` after the library changes;
  - `bazel run //parser-equivalence:update_ratchets` when a test file adds Pure snippets;
  - `bazel run //:update_generated` for native claims (it may need two runs);
  - the IQ showcase pin: `--jvm_flag=-Diq.showcase.update=<workspace>/core/src/test/resources/iq-parity/showcase` on
    `//core:iq_parity_test`; Cypher's is `-Dcypher.showcase.update=`.

**Upstream and data facts**

- **legend-pure's sqrt of a negative refuses** (PCT `testSquareRootError`). That is why spec-16 needed the clamp, not a
  change to sqrt.
- **pandas.** `x.rolling(n, 0).corr(x)` can be 1.0000000000000002, unclamped. `x.corr(x)` is 1.0, because it goes
  through `numpy.corrcoef`, which clips. gs-quant 2.2.4's `correlation` uses the first for counts and the second for
  calendar windows. Its `r_squared` is statsmodels' `RollingOLS.rsquared`, 1 − ssr/tss, never above 1 (register S133).
- **DuckDB.**
  - Preparing costs more as the SQL nests deeper.
  - Its profile is turned on with `PRAGMA enable_profiling='json'`, `SET profiling_mode='detailed'` and
    `PRAGMA profiling_output='<file>'`.
  - LAG over tied ORDER BY keys returns different rows from run to run.
- **MySQL and MariaDB take no expression as a frame offset** (S129), so calendar windows are refused there by name.

**Datacube tests**

- **The datacube tests** run their own build of the compiler. Run them after any core change that affects the SQL:
  `timeseries_ts_parity_test`, `stress_markets_test`, `timeseries_queries_test` and `pure_library_spike_test`.

## 11. The checks, with recent times

All of these were green at `8b948df5d`, run one at a time; the times are from 2026-11-04 to 2026-11-06.

| Check | Time |
|---|---|
| `//core:ts_documents_test` | ~10 s |
| `//core:ts_island_test` | ~46–50 s |
| `//core:timeseries_library_test` | ~21 s |
| `//core:ts_line_spike_test` | ~7 s |
| `//core:guardrails` | ~41 s |
| `//core:core_tests_sql` (`SelectLayersTest`) | ~8 s |
| `//core:core_tests_lowering` | ~10 s |
| `//datacube:timeseries_ts_parity_test` | ~51 s |
| `//datacube:stress_markets_test` | ~48 s |
| `//datacube:timeseries_queries_test` | ~107 s |
| `//datacube:pure_library_spike_test` | ~6 s |
| `//gates:core` and `//gates:checks` | ~50 min together |

The two gates include core_tests_integration (154 s), the H2 lane (45 s), the SQLite lane (41 s), `iq_parity` (106 s)
and `cypher_parity` (147 s). The 8 Postgres targets run unprivileged (§9): cypher_parity_postgres 146 s,
iq_parity_postgres 80 s, sql_parity_postgres 22 s.

The Python and view suites, run one at a time at `8b948df5d` on 2026-11-06, passed: `//python:engine_test` 194 s,
`//python:markets_test` 105 s and `//datacube:timeseries_view_dom_test` 32 s.

**The standing checks in the program's §8** are the commands to run before every commit. `//gates:local` is the full
gate, for A2, A10 and before any landing; a landing needs the user's word (§2).

## 12. Glossary

- **Base:** a source column's line, for example `close['KO']`. Every column added to it makes one growing table
  (R7, D5).
- **Line:** a table with a key (`part`), a date (`at`), `PERIOD` and `ORD`.
- **Item:** a column of the answer.
- **Word:** a library function in `timeseries::line::`.
- **Line form:** a word that makes its own line, joined back to the answer.
- **Rows form:** `timeseries::rows::<word>`, a column of an existing table.
- **Pair form:** the rows form of a two-line word, a column of the two lines' met table.
- **Met table:** two or more lines met on their periods, once per set of values (`Lines.met`).
- **Points:** the rows where a value is a point. They are recorded in the value's presence column, or `ORD` when it is
  every row. Not being a point differs from having no value: a return's first row is a point with no value.
- **Ramp:** the first rows of a window word, which have no value. gs-quant's `apply_ramp`.
- **Spine:** the answer's periods, gathered from every line (`TsLines.spread`).
- **D1, D2, D5:** the time model's rulings.
  - D1: duplicates within a line are allowed; met on one side, they are a lookup; on both sides, refused.
  - D2: the duplicates check is skipped where a key is declared (S132).
  - D5: words that keep their rows are columns of one table.
- **R6, R7:** the language spec's rules. R6: lines meet on dates. R7: one table per base.
- **S129:** frame offsets computed by the database.
- **S132:** a key's count is 1.
- **S133:** correlation is clamped.
- **C6.2b:** a frame bound spelled in integers is folded.
