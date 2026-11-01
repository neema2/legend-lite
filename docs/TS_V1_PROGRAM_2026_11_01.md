# Timeseries v1: the program, fixes first, then the features (2026-11-01)

**Status: proposed, not ratified.** Written 2026-11-01 because the user asked: "Create a roadmap for all the fixes first
and then all of these awesome features - write it down so we can run the program". The user decides the v1 cut (§2)
and the open rulings (§5). The design that governs the language is still `docs/TS_ONE_TREE_PLAN_2026_10_31.md`: its
decisions D1 to D13, and its rulings in §9. That plan's §6a is the state this program starts from. This document is
the order of the work from that point on, and covers everything past it as well.

A session picking the program up reads, in this order:

1. this document's §1 (the state) and §6 (the status table), to find the next item;
2. that item's brief, in §3 or §4;
3. §7, the rules;
4. §8, the checks.

Every item starts and ends green, and each commit updates §6.

## 0. What v1 is

A quant writes a timeseries one way, using gs-quant's words, in any of three places:

- in `#TS` text;
- in a Python notebook;
- in DataCube's timeseries tile.

They get the same numbers in all three. The database computes those numbers: the warehouse their data is in, at the
size their data is. The tile shows each series as the `#TS` statement and the Python program a person would write.
A page of charts behaves like a dashboard:

- a period chosen once moves every tile;
- a click on a chart filters the page, and so does a slicer;
- a title, a note and an image explain the page.

v1 is done when every one of the following holds, each proved by a test rather than claimed:

| # | v1 holds when | Proved by |
|---|---|---|
| V1 | **The language is the library's.** Every word means what gs-quant means, or is a ruled departure written in `docs/SEMANTICS_REGISTER.md` and the oracle's register. No rule of the language lives in Java. Core's parser is the only reader, on every surface | `TsTranslatorGuardrailTest`, extended (A1); the oracle; a guard that `datacube/src` holds no timeseries grammar (A3) |
| V2 | **The surfaces are one.** `#TS`, the Python DSL and the tile read and print one another. Show #TS and Show Python cover every series of every example page exactly (the text reaches a fixpoint and the rows are equal), with no refusals left | the parity tests' counts, pinned at zero |
| V3 | **It runs on real warehouses.** The frozen answers hold on DuckDB, H2, Postgres and one cloud warehouse | the warehouse lanes (B1) |
| V4 | **It holds at real size.** The scale benchmarks stay within their budgets | the measure targets (B2) |
| V5 | **Every word has a second opinion.** Each word has an outside oracle (gs-quant, TA-Lib, statsmodels or pandas), or is named as having none | the oracle's report (B5) |
| V6 | **Calendars and clocks.** A business calendar, its holidays, time zones, and `SAMPLE BY` under a day | B6's tests |
| V7 | **The page is a dashboard.** The page period, cross-filtering, slicers, drill-through and bookmarks; text, title, image and filter tiles | B7's and B8's tests |
| V8 | **The gate.** `//gates:local`, the Postgres lanes and the Python suites are green, and CI is green on every OS on the pull request that lands v1 | §8 |

## 1. Where it stands (2026-11-01)

The work so far is on the branch `claude/timeseries-language`. It contains PR #81's branch (`claude/timeseries-platform`)
plus 53 more commits, and is behind `main` by none. The commits are listed in the one-tree plan's §6a.

**Done:**

- **The words.** All 81 words are `timeseries::line::` functions in `projects/timeseries/model.pure`.
- **The line rules.** These are `timeseries::lines::` functions: a line, `sample`, `periodsOf`, `spine`, `someValued`,
  `latest`, `ordered` and `perYear`.
- **The binding.** The translator binds a word's parameters by type and position, never by name. It is guarded by
  `TsTranslatorGuardrailTest`.
- **Spellings.** A word's other spellings are tags in the library (`timeseries::Words.alias`).
- **Show #TS (D12).** All 210 series on the gallery and markets pages print exactly. On the class page, 7 of 10 print;
  3 are refused, because they read an enumeration as names.
- **The frozen answers.** 210 series (271,838 points) on the gallery and markets pages, and 10 series (17,749 points)
  on the class page.
- **The gs-quant oracle.** 145 series agree and 1 differs (register row S130). 39 have no gs-quant counterpart.
- **The vocabulary.** D3, R3, R14 and R15 are ruled and built.
- **Generated from core's export.** The guide (`docs/timeseries/WORDS.md`) and the tile's word list
  (`src/generated/ts-words.ts`).

**Not done.** Each of these is an item below:

- **Java still decides some rules of the language** (A1). The translator, `core/src/main/java/com/legend/protocol/ts/`,
  is 4,324 lines. `TsToPure` is 1,426 of them and `TsParser` 773. The rest are `TsWords` (406), `TsLines` (388),
  `TsShow` (370), `TsFunctions` (323), `TsPrinter` (315), `TsCatalog` (226) and `TsAst` (97). The rules it still
  decides are below. The rule numbers are the `#TS` spec's (`docs/TIMESERIES_LANGUAGE_2026_10_29.md`); they are not
  the one-tree plan's rulings, which reuse the letters.
  - **R2 and R6, how lines meet.** `TsLines` picks which `lines::` function to call, by what kinds of line they are
    and by `FILL`.
  - **R7, items that read items.** An item that reads an earlier item. `TsToPure` also folds a later item's line into
    an earlier one when it was built from it.
  - **R10, struct fields.** A field's answer column is named `<item>_<field>`.
  - **R5 and R9.** The rename to one date and the `latest` substitution.
  - **The column cap.** An answer has at most 12 columns, because the library's `someValued` has only 12 overloads.
  - **Refusals of meaning written in Java.** A grep for lines that throw or refuse finds 173 across `protocol/ts`:
    `TsToPure` 61, `TsParser` 51, `TsFunctions` 18, `TsShow` 18, `TsPrinter` 12, `TsWords` 8, `TsLines` 3 and
    `TsCatalog` 2. Not all of these are meaning rules; the census in A1 sorts them.
- **The plan's step 4 is not built** (A4). Under D2, `log`, `floor` and `ceil` are meant to be refused in text. Today
  `log(x)` is SQL's base 10 (`PostgresFunctions.java:99`). `Window(w, r)` and `type => logarithmic` are also missing.
  Step 4 needs rulings R1, R7 and R9.
- **One reader (S3) and the library's text (S4)** (A3, A5). The tile still has its own formula grammar and its own
  `TS_SIGNATURES`. It also sends 279 KB of library text with every model.
- **Show #TS** cannot yet print an enumeration or a calculated column (A6).
- **H2 runs out of memory on deep `SAMPLE BY` queries** (A7), so those tests run only on DuckDB.
- **No independent review and no full gate since commit `07ce4597`** (A2). The branch has never run CI: `gate.yml` runs
  on a pull request and on `main` only.
- **Not started.** The Python DSL (D8) and Show Python (D13), the warehouses, scale, calendars and time zones, an
  oracle for the words gs-quant lacks, page interactivity, and new tile kinds.

## 2. The cut (ruling P1)

| | In | Why |
|---|---|---|
| **Part A, the fixes** | A1 to A10 (§3) | finish what the one-tree plan set out to do before anything is built on it |
| **Part B, v1** | B1 to B8 (§4) | without these it is not a timeseries product a desk would adopt |
| **Part C, v1.1** | trend lines, forecasts and annotations on the general charts; units and currency (C2 of the parity note); time intelligence and the time spine (C3); live data; a crosshair linked across tiles; the service: share, permissions, versions, schedules, alerts | real value, but none of it blocks a quant's first week |
| **Part D, not v1** | AI and augmented analytics; maps; storytelling | the gap analysis's tier 5, and nothing timeseries-specific |

Part C's items come from `docs/DATACUBE_BI_GAP_ANALYSIS_2026_10_24.md` §4 (tiers 2 to 4) and
`docs/DATACUBE_BI_EXCEL_PARITY_2026_10_24.md` §3.6 and §3.10. Each gets its own brief when its turn comes.

## 3. Part A: the fixes, in order

### A1. The language's last rules move out of Java

- **Why.** The user's goal, 2026-11-01: "get all the logic out of java ... this program goal is not complete until
  everything works without the java". What remains is listed in §1.
- **What.** First take a census: every rule and every refusal in `protocol/ts`, each put in one of three classes.
  1. **Syntax.** The parser's, and it stays in Java.
  2. **A fact about a source or a column.** Name to source, column to line. It stays in Java.
  3. **A rule of meaning.** It moves into the library.

  Then move every rule in the third class:
  - **How lines meet (R2, R6).** Today `TsLines` picks the `lines::` meeting function by the lines' kinds and by
    `FILL`. Instead, the translator makes one call, and the library's overloads, chosen by the line's type, decide.
    `FILL` already reaches the library as `timeseries::Fill`.
  - **Items reading items (R7).** Today Java folds a later item's line into an earlier one. That fold is a library
    function over the items' lines, or the type checker's work.
  - **Struct fields (R10).** A field's name comes from the word's declared return type. The rename that the translator
    writes is bookkeeping, which the guard allows. The naming rule itself is either written in the library or
    recorded as the language's surface syntax, and the census says which, with its reason.
  - **The 12-column cap.** Either `someValued` takes a `ColSpecArray` (which needs a compiler that reads a row through
    one), or the cap stays as a named limit of the library. This is ruling P7.
  - **Refusals of meaning.** Each becomes a Pure `fail` in the library or a type error, keeping the same words.
- **Where.** `core/src/main/java/com/legend/protocol/ts/{TsToPure,TsLines,TsWords,TsFunctions}.java`,
  `projects/timeseries/model.pure`, and `TsTranslatorGuardrailTest`.
- **Done when:**
  - every rule is in the library, or is in the one-tree plan's §6a list of what stays in Java, each with its reason;
  - the guard is extended: no Java branch on a line's kind picks a library function, and the translator calls one
    meeting function;
  - the frozen answers are unchanged;
  - every refusal test passes with the same words.
- **Size.** Large.
- **Riskiest unknown.** Whether overloads chosen by a line's type can express the broadcast case: a line with no
  instrument, met with a line per instrument.

### A2. Checkpoint: an independent review and the full gate

- **Why.** No review and no full gate have run since `07ce4597`, and the branch has never run CI.
- **What:**
  - **An architecture review.** It is done by an agent that wrote none of the code, and it covers `07ce4597` to the
    head, checked against:
    - AGENTS.md's invariants: 1 to 8, and 3a;
    - the execution tenet: the translator computes no value;
    - the world map: natives, platform semantics, programs;
    - upstream fidelity: every divergence is a register row;
    - the guards.

    Its findings are verified, then fixed, each pinned by a test.
  - **The full gate.** `bazel test --lockfile_mode=error //gates:local`, the Postgres lanes, and the Python suites.
  - **CI on every OS.** CI runs only on a pull request, so this needs the user's word (ruling P3): a PR for this
    branch, either against `main` or stacked on #81.
- **Done when** the review's findings are fixed or answered, the gate is green, and CI is green or every failure is
  explained.
- **Size.** Medium.

### A3. One reader (S3, the plan's step 3)

- **Why.** D1: core's parser is the only reader. Today the tile's formula box has its own grammar
  (`datacube/src/timeseries-expr.ts`), its own signatures (`TS_SIGNATURES`) and its own translator
  (`datacube/src/timeseries-ts.ts`).
- **What:**
  - The formula box parses through the planner the tab already holds (WASM). This needs an entry point that parses
    one item and recovers from errors, so completions still work.
  - Core answers the facts a drawing needs (a series' unit, level, candle and problems) from the catalog.
  - The tile keeps each series as its item text, and "Build this tile" prints `#TS` items.
  - `timeseries-expr.ts`'s grammar, `timeseries-ts.ts` and `TS_SIGNATURES` are deleted, and
    `timeseries-query.ts`'s use of them is rewritten.
- **Done when:**
  - those files are deleted;
  - a guard shows `datacube/src` has no timeseries grammar;
  - the frozen answers hold, as do the tile's suites, the Python suites, and the bundle and cold-start budgets.
- **Size.** Large.
- **Riskiest unknowns.** The entry point's latency on each keystroke, and the roughly 25 analyses that have to move.

### A4. The words' remainder (the plan's step 4, plus the rest of #117)

- **Why.** D2 to D5 and §4 of the one-tree plan, as ruled, are not built. The 8 scalars are still per-point SQL, not
  words of the library. That is the remainder of #117.
- **What:**
  - **The scalars become library words** (`abs`, `exp`, `sqrt`, `ln`, `log10` and the rest), so the catalog, the
    Python stubs and the guide list them.
  - **D2's refusals.** `log(x)`, `floor(x)` and `ceil(x)` are refused in text, and the message gives both fixes:
    "write ln(x) (natural, gs-quant's log) or log10(x)", and "greatest(x, 0) for gs-quant's floor; round_down(x) to
    round".
  - **`returns`** takes gs-quant's values (`type => logarithmic`, with `log` as an alias).
  - **`Window(w, r)`** is written `w => 63, r => 10`.
  - **`beta`'s first three values.** Ruled under R1.
  - **`zscores` and `winsorize`** stay causal, each a named departure in the register.
  - **Two lines meeting.** Ruled under R9.
- **Needs** rulings R1, R7 and R9 (the one-tree plan's §9).
- **Done when:**
  - the oracle's rows turn to passes or named departures;
  - the frozen answers change only where a ruling says, each change with a dated reason.
- **Size.** Medium.

### A5. The library's text off the wire (S4)

- **Why.** A platform library belongs to the planner, never to a client's copy. Today
  `datacube/src/pure-library-text.ts` (279 KB) is sent with every model, both from the tile and from the headless path.
- **What.** The planner resolves `projects/timeseries` the way it resolves any project's dependency. Clients send their
  own model and nothing else. The file is deleted.
- **Done when:**
  - the file is gone;
  - the bundle budgets are pinned back down, with a dated reason;
  - the headless path and the tile both plan without it.
- **Size.** Medium.

### A6. Show #TS, complete

- **Why.** D12 promised every series. Today 3 class series are refused because they read an enumeration as its
  values' names, and any calculated column is refused too.
- **What:**
  - **Enumerations.** `#SQL`'s reader learns to read an enumeration cast to text as `toString`. That is a core change
    to `protocol/sql`.
  - **Calculated columns.** Each `extend` is printed as the Postgres expression that the reader reads back to the same
    meaning, one native at a time.
- **Done when:**
  - the class page's refused count is 0, and the pinned count is shrunk to 0;
  - a test page with calculated columns prints exactly.
- **Size.** Medium, and it grows with the number of natives.

### A7. H2's deep plans

- **Why.** H2 plans nested derived tables exponentially. As a result, `SAMPLE BY` queries with several items run out
  of memory, and those tests run only on DuckDB. That leaves the engine-style printer path untested on them.
- **What:**
  - Find the shape. `ProjectionWrapsFold` (commit `07ce4597`) folded one case.
  - Fold the rest in H2's dialect passes or in the lowering, without changing the engine-style printer's text.
- **Done when** the deep tests run on H2 within the lane's heap.
- **Size.** Small to medium.

### A8. The ratchets, reviewed

- **Why.** This program raised ceilings, each with a dated reason:
  - `BARE_NAME_ARMS_MAX`, from 194 to 197, for `TsShow`;
  - `SqlTextRatchetTest`'s register entry for `protocol/ts/TsPrinter.java`;
  - any others that `git log -S` turns up on the guard files.
- **What.**
  - Re-read each one.
  - Where a table can replace the arms, shrink the ceiling back. For example, `TsShow`'s protocol names could become
    one map keyed by `PlatformTypes`.
  - Re-pin each ceiling to its measurement.
- **Done when** every raise this program made has been reverted, or re-justified with its measurement.
- **Size.** Small.

### A9. The documents

- **What:**
  - Version 4 of the `#TS` spec (`docs/TIMESERIES_LANGUAGE_2026_10_29.md`). It replaces version 3's rules wherever the
    one-tree plan changed them, and its numbering stops clashing with the plan's rulings.
  - The guide for quants (`docs/TIMESERIES_GUIDE_FOR_QUANTS_2026_10_30.md`), with every example generated from the
    catalog and run.
- **Done when** every example in both documents runs in a test.
- **Size.** Small.

### A10. Close Part A

- **What:** a second independent review of A3 to A9, the full gate, and CI on the PR. Then the user decides whether
  Part A lands.

## 4. Part B: v1, in order

The order rests on two points:

- **Proving comes before building.** The warehouses and scale come first, because what they find can change the
  library's shapes, and everything after is built on those shapes.
- **Python comes before the oracle and calendars.** The DSL is the second surface, and both the oracle and the
  calendars are tested through it.

### B1. The warehouses

- **Why.** Today `#TS` is tested only on in-process engines (`TestBackends.inProcess()`).
- **What.** A lane that runs the frozen answers and `//core:ts_island_test`'s cases on three databases:
  - Postgres, embedded, through `tools/postgres/as_unprivileged.sh`;
  - MariaDB, which refuses a computed frame offset (register row S129); each such refusal must be named;
  - one cloud warehouse (ruling P4), from `docs/CLOUD_BACKENDS_PCT_2026_10_11.md`. Its credentials are an
    environment secret.
- **Done when** each lane is green, or every difference is a register row.
- **Size.** Medium.

### B2. Scale, with budgets

- **What.** These benchmarks:

  | Benchmark | Size |
  |---|---|
  | daily | 2,000 instruments × 20 years, about 10M rows |
  | intraday | 100 instruments × one year of minute bars, about 10M rows |
  | a wide query | 20 items |
  | sampled | `SAMPLE BY 1w` and `1mo` |
  | a basket | 500 members |

  For each, measure the plan time, the SQL's size and the run time, on DuckDB and on Postgres. Also measure the tile's
  cold open with 100,000 points (the tile's point limit, drawn with LTTB sampling).

  The budgets are pinned in a measure target, as `//datacube:timeseries_sql_measure` and `ScaleCostsDuckDbTest` pin
  theirs. The first measurement proposes them and the user rules (P5).
- **Done when** the budgets are pinned and met, and a regression fails the target.
- **Size.** Medium.

### B3. The Python DSL (D8)

- **What.** `legend_lite.timeseries`:
  - gs-quant's functions over series, with stubs generated from the catalog;
  - its sources: a grid, a class, a table, or a DataFrame (loaded first);
  - `frame()`.

  A program prints `#TS` text and runs through core's native library, the one reader.
- **Done when:**
  - gs-quant's own examples, ported, run and match;
  - the oracle also runs over the DSL;
  - the Python suites are green.
- **Size.** Large.
- **Riskiest unknown.** Expressing lines of several instruments in gs-quant's single-series terms.

### B4. Show Python (D13)

- **What.** The DSL program for each series, shown in the existing Python code box: no new tab and no change to the
  layout (the user, 2026-11-01).
- **Done when:**
  - every series on every example page prints, runs and gives the same rows, and refusals are counted;
  - a DOM test shows the program is in the existing box.
- **Size.** Medium.

### B5. A second opinion for the words gs-quant lacks

- **What.** The oracle (`tools/ts-oracle/oracle.py`) gains pinned references:

  | Reference | For |
  |---|---|
  | TA-Lib | RSI, MACD, ATR, stochastic, OBV, Keltner, Donchian, Bollinger |
  | statsmodels | OLS alpha, beta and r², ADF (`dickey_fuller`), half-life, cointegration |
  | pandas | the rolling and exponential families; percentile rank |
- **Done when** each of the 39 rows with no gs-quant counterpart (NO-GSQUANT) is either AGREE, a named departure, or
  listed with "no oracle" and the reason.
- **Size.** Medium.

### B6. Calendars and clocks

Split in two:

- **B6a, calendars and holidays.**
  - A business calendar is model data: an exchange's trading days and its holidays.
  - Windows and `SAMPLE BY` periods can be counted in business days (gs-quant's `b` tenors).
  - Ruling P6: where holidays come from (data the user supplies, or a shipped calendar).
- **B6b, time zones and intraday.**
  - A date-time source carries a zone, and a line's day is its exchange's day.
  - `SAMPLE BY` takes `h` and `min`.
  - gs-quant refuses to annualize several points a day (`econometrics.py:497`). `#TS` either refuses the same way or
    uses a ruled rule.

  The one-tree plan's §6 called intraday a later program. Putting it in v1 is part of ruling P1.
- **Size.** Large.

### B7. The page as a dashboard

- **What:**
  - **The page period** (ruling P2; option D is recommended in `docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md`'s Phase
    5 note). It comes in two steps:
    1. a linked window that every timeseries tile shows;
    2. grids and charts that opt in through a date column they are given.

    The gesture is the Measure box's "Show this period everywhere".
  - **Cross-filter.** A click on a series' instrument, or on a bar's category, filters the page's grids.
  - **Slicers.** These are B8's filter tile.
  - **Drill-through.** A point opens the rows behind it: its grid, filtered to that date and instrument.
  - **Bookmarks.** A named page state: the filters, the period and the zoom.

  The source is the gap analysis's tier 1.
- **Done when** each gesture works the same from the screen and from Python, through the same commands, as every
  timeseries change does.
- **Size.** Large.

### B8. Tile kinds

- **What:** text (Markdown), title, image, and filter. A filter tile, the slicer, offers a column's values or a range of
  dates. The source is the gap analysis's §2.6.
- **Size.** Medium.

## 5. The rulings asked

| # | Question | Recommended | Needed by |
|---|---|---|---|
| P1 | The v1 cut (§2), including intraday in v1 (B6b) | as written | now |
| P2 | The page period: options A to E, and whether the brushed tile zooms or keeps its whole view with the period shaded | D, shaded | B7 |
| P3 | A pull request for this branch, so CI runs on every OS, and its base | against `main` once #81 lands; stacked on #81 until then | A2 |
| P4 | Which cloud warehouse, and its credentials as an environment secret | Snowflake or BigQuery: whichever has an account | B1 |
| P5 | The scale budgets | proposed from the first measurement | B2 |
| P6 | Where holidays come from, and the time-zone rule | data the user supplies, the exchange's zone | B6 |
| P7 | The 12-column cap: lift it or keep it | lift it, if A1 shows the compiler can read a row through a `ColSpecArray` | A1 |
| R1, R7, R9 | The one-tree plan's §9: §4's rows, D2 as narrowed, and two lines meeting | as that plan recommends | A4 |

## 6. Status

Update this table in the same commit as the work.

| Item | State | Last commit |
|---|---|---|
| A1 the last rules out of Java | next | |
| A2 review and gate | waiting on A1; P3 for CI | |
| A3 one reader | | |
| A4 the words' remainder | waiting on R1, R7, R9 | |
| A5 the library's text off the wire | | |
| A6 Show #TS complete | | |
| A7 H2's deep plans | | |
| A8 the ratchets | | |
| A9 the documents | | |
| A10 close Part A | | |
| B1 to B8 | after Part A | |

## 7. The rules (the user's, each learned the hard way)

- **Branches.** Work on `claude/timeseries-language`; commit and push there. Never push to `main`. Open no PR without
  the user's word, and never merge #81.
- **Commit trailers.** A commit ends with the session's `Co-Authored-By` and `Claude-Session` lines. A model identifier
  appears in no repository artifact.
- **Upstream fidelity.** Every difference from gs-quant, legend-pure or legend-engine is posted to the user before it
  lands, then written in `docs/SEMANTICS_REGISTER.md`. A ruled decision is not reopened.
- **Guard ceilings** move only with a dated reason and a measurement.
- **Layout.** Don't change it without asking. Show #TS and Show Python go in the existing boxes.
- **Shell.** Edit files with the file tools, not with a shell edit. Run one plain command per call, with no chained or
  destructive commands, so the session does not stop to ask.
- **Green at every step.** Each item starts and ends green on §8's standing checks. A change of a number on purpose
  changes its frozen rows, with a dated reason.

## 8. The checks, exact

**Standing checks, every commit:**

```
bazel test //core:ts_island_test //core:timeseries_library_test //core:guardrails
bazel test //datacube:timeseries_ts_parity_test //datacube:stress_markets_test //datacube:timeseries_queries_test //datacube:timeseries_view_dom_test
bazel test //python:engine_test //python:markets_test
```

**Regenerate** after a change to the library or the catalog:

```
bazel run //datacube:update_generated
bazel run //docs:update_ts_words
```

**The Postgres lanes.** Copy the wrapper outside the workspace first (`docs/CLOUD_SESSIONS.md`):

```
bazel test <target> --strategy=TestRunner=standalone --run_under="$SCRATCH/as_unprivileged.sh ubuntu"
```

**The gs-quant oracle**, by hand (`tools/ts-oracle/oracle.py`'s header):

```
bazel test //datacube:timeseries_ts_parity_test
python3 -m venv /tmp/gsq && /tmp/gsq/bin/pip install gs-quant
/tmp/gsq/bin/python -I tools/ts-oracle/oracle.py bazel-testlogs/datacube/timeseries_ts_parity_test/test.outputs/oracle-input.json
```

**The full gate**, at A2, at A10 and before any landing:

```
bazel test --lockfile_mode=error //gates:local
```

**Traps already hit:**

- **H2 runs out of memory** on deep `SAMPLE BY` queries. Deep tests use the platform database, DuckDB, until A7.
- **A probe test left in a test file leaks into the gate.** The dialect-leak census flags a database named in a test.
- **Hygiene.** A library body's working columns are renamed fresh. So a column made inside a body can be read by name
  elsewhere only if it is in a declared return type.
- **`SAMPLE BY` dates.** Under `SAMPLE BY`, `date` is the period's day and `time` is the last tick's. A line's number in
  time is always seconds since 1970.
- **Multi-statement lambdas** need a trailing `;` when parsed through `Compiler.parseQuery`.
