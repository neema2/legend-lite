# H11: legend-pure's relation text printed in Java (the spike for slice 1.3), 2026-11-19

**Status: DONE for the time box (about four hours of one working day used). The prototype works; §9 lists what it did not cover.**

- **Branch:** `claude/compat-relation-text-spike`, from `main` `b75d6c73a`. Scratch: never proposed for merge as is.
- **Session:** `session_0195rQEjVtMXfZdcaQdweUzf`; reports to the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **The question:** slice 1.3 deletes `edge/RelationTextLowering.java` (791 lines, a relation's `toString`, `toCSV` and
  a TDS's `csv` composed in SQL) and writes the same text in Java after the run, from typed rows. No Java printer of a
  relation existed. 423 PCT tests assert legend-pure's `toString` of a relation. Does it work, byte for byte?
- **Nothing here moves a pin, a ceiling, a ledger, a roster, a golden or an allowlist.**

## 0. The answers on one page

**It works. legend-pure's relation text, printed in Java from typed rows, is PCT's expected text byte for byte, on
every text PCT has.**

**The counts (PCT channel B, the SQL builder switched OFF for a relation's `toString`, HOST judge):**

| Lane | Tests that assert a relation's text | Comparisons | Equal to PCT's text, byte for byte | Differ because of the printer | Suite pass count against `main` |
|---|---|---|---|---|---|
| DuckDB, five suites | 423 (relation 365, standard 58) | 431 | **431** | **0** | 1,240 of 1,249, the same |
| H2, relation | 334 reach the compare | 342 | **342** | **0** | 436 against 432: **4 more pass**, none fewer |
| SQLite, relation and standard | 412 reach it | 420 | 419 | **0** (1 is the product's rows, §8 P1) | relation 457 against 458; standard 205, the same |
| Postgres 16, relation and standard | 422 reach it | 430 | 427 | **0** (2 fail on `main` with the same text; 1 is P1) | relation 465 against 467; standard 198, the same |

- **Every failure, with its cause** (§4.5): 3 tests move from pass to fail, on SQLite (1) and Postgres (2), and all
  three are ONE product defect, not the printer: a window column read through a list operation gives wrong rows when
  the relation is read as rows at the root (§8 P1). The SQL text builder happened to hide it. It needs a product fix
  before 1.3 lands.
- **The print is at an assert side's root 431 times out of 431.** None is composed and none feeds, in PCT (§4.4: the
  source census and the run agree). The corpus is different: 41 tests compose a relation's CSV with `replace` (§8 P4,
  a source census, not run).
- **No new hook in the core.** All edited code is in `com.legend.edge`. The assert arm rides three hooks that exist
  (`EngineTestText`, `HostJudgment.EdgeJudged`, `EdgeHearing.judged`); the query-root path uses public doors only and
  plans in the PRODUCT's style (§3, §6). Those three hooks are the ones 2.2b reshapes, and the compatibility database
  judge cannot judge a presented side at all: under BOTH it refuses the 365 relation bodies. **So 1.3 must follow
  2.2**, as the plan orders it; measured here.
- **The typed facts (CP-6):** a relation's text needed the column's name, its Pure type and the typed cell. **It did
  not need a DateTime's written precision, and it did not need the SQL slot** (§7). One case is unproven: a mixed
  (tagged) column, which PCT never prints.
- **The edge cases (§5):** EDGE-1's Float texts are the same in Java, on DuckDB and H2, as legend-pure's and today's.
  EDGE-2 (`toCSV`'s DateTime) and EDGE-3 (`%r` of a newline) come out as UPSTREAM's answers (`2024-01-02 10:30:00`;
  `'a\nb'`), which today's SQL does not give: two listed golden moves.
- **Sizes (§10):** the printer is 213 lines (133 of code) for `toString`, `toCSV`, a TDS's `csv` and a value's print;
  with the arm and the record about 400 lines (245 of code), against the SQL builder's 791 (574 of code). The judge's
  SQL for the 365 relation tests shrinks by 73% (§4.6).
- **The estimate (§11):** the relation-text half of 1.3 is 4 to 5 days after 2.2 and 1.1. The value prints of 1.3
  were not prototyped; with them, all of 1.3 looks like 6 to 8 days against the plan's 4 to 6.
- **Not done (§9):** the relational corpus was not run (so `toCSV` is unit-tested only); seven database lanes were not
  run; no guard was run.

## 1. What was read

| Source | Where | Head or pin |
|---|---|---|
| The plan's slices 1.1 and 1.3, rulings CP-5, CP-6, CP-14 | `docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program` | fetched 2026-11-19 ~12:20 UTC |
| The log's §1 and its entries on 0.4 (f) and H6 | `docs/records/compat-program-log-2026-11-19.md`, same branch | same |
| H6, the judges' homework | `docs/records/compat-homework/H6-judges.md` on `claude/compat-judges-homework` | same |
| The old design's §3.6 | `docs/records/presentation-program-design-2026-11-18.md` on `claude/presentation-design` | same |
| The SQL builder and its facet | `edge/RelationTextLowering.java`, `lowering/RelationTexts.java`, `lowering/Lowerer.java:4300-4318` | `main` `b75d6c73a` |
| The host judge's rendered arm | `testplan/AssertVerdicts.java` (`renderForm`, `renderedPlan`), `testplan/HostJudge.planRendered`, `testexec/HostJudgments.renderedOf` | `main` |
| The edge's assert arm and its run hook | `testplan/EngineTestText.java`, `testplan/EngineTextGates.java`, `testplan/EngineTextPlanning.java`, `edge/EdgeEngineTestText.java`, `edge/Appeals.java`, `testexecutionplan/HostJudgment.EdgeJudged` | `main` |
| legend-engine's `toString` of a relation and its cell rule | `core_functions_relation/relation/functions/toString.pure`, `s.pure` | legend-engine 4.145.0 (the pin; archive sha256 matched) |
| legend-pure's value of a TDS cell and its print | legend-engine's `TestTDS.java`, `TestTDSInterpreted.java`; legend-pure's `DateFunctions.java`, `FloatCoreInstance.java`, `SimpleFloatCoreInstance.java`, `SimpleDecimalCoreInstance.java`, interpreted `ToString.java` | legend-engine 4.145.0, legend-pure 5.99.0 (the pins; archive sha256 matched) |

## 2. legend-pure's rule, read at the pin

**The text is Pure code, not a native.** `meta::pure::functions::relation::toString` (`toString.pure:19-35`):

```
'#TDS\n' + '   ' + <names joined ','> + '\n' + <one line per row: '   ' + cells joined ','> joined '\n' + '\n' + '#'
```

- **A name** is trimmed, then quoted with `'` unless it already starts with `'` or matches `^[a-zA-Z0-9_]+$`.
- **A cell** is `s(value, columnType)` (`s.pure:23-38`):
  - an empty cell is `null`, or `'null'` (quoted) under a Variant column;
  - a Variant is quoted, with `\` and `'` escaped;
  - a String holding `{` or `[` is quoted the same way; any other String is itself;
  - anything else is its `toString()`.
- **No rows:** the rows segment is empty, so the text is `#TDS\n   names\n\n#`.

**A cell's `toString()` is the interpreter's print of the value the TDS holds** (`ToString.java`: a primitive's
`getName()`), and what a TDS holds is decided by legend-engine's `TestTDS`:

| Column type | What `TestTDS` holds | Its print | Read at |
|---|---|---|---|
| Integer | a `long` | digits | `TestTDS.java:180-198` |
| Float | a `Double`; a `long` cell is cast to double | `BigDecimal.valueOf(double)`, at least one decimal, trailing zeros dropped, `toPlainString()`: never an exponent (`0.00001`, `25000000.0`) | `TestTDS.java:235-258`, `TestTDSInterpreted.java:96`, `FloatCoreInstance.canonicalizeBigDecimal`, `SimpleFloatCoreInstance.getName` |
| Number | a `Double`, a `long` cast to double | as Float | `TestTDS.java:332-356` |
| Decimal | a `BigDecimal` (`valueOf(double)` or `valueOf(long)`); under `Numeric(p, s)` set to scale `s`, HALF_UP | `toPlainString()`, no `D` suffix | `TestTDS.java:260-295`, `SimpleDecimalCoreInstance.getName` |
| any Date type | nanoseconds as a `long`, read back as `DateFunctions.fromDate(new Date(ns / 1000000))` | **always exactly three subsecond digits, truncated**, then `+0000`: `fromDate` is `fromInstant(instant, 3)` | `TestTDS.java:201-212`, `DateFunctions.java:257-268, 314-317` |
| Boolean | a `Boolean` | `true` / `false` | `TestTDS.java:215-229` |
| String, an enumeration | the text | itself | `TestTDS.java:356-367` |
| Variant | a `Variant` of the text | its `toString()`, quoted by `s()` | `TestTDS.java:296-331` |

**Measured over PCT's goldens** (`grep` of the `'   ...` lines of the relation and standard function trees at the
pin): 530 date cells, every one `yyyy-MM-ddTHH:mm:ss.SSS+0000`; none is date-only and none has more or fewer than three
subsecond digits. So `TestTDS.java:209` is what PCT expects, as the old design's §3.6 said.

**One difference from today's SQL builder, found by this reading.** `RelationTextLowering.dateTimeTextTds` writes "at
least three fraction digits": a nanosecond cell prints all nine (`.123456789`). legend-pure prints `.123`. No PCT
golden has a finer fraction, so neither form is tested; §5 says what the Java printer does.

## 3. The wiring: the edge's existing hooks carry it (no new hook in the core)

**Found, and built so: nothing in the product changed.** The spike edits six files, all in `com.legend.edge`.

- **At planning,** the test planner already asks the plan style's engine-text facet about every `assertEquals`
  (`testplan/AssertVerdicts.planArm`: `EngineTextGates.reachesUpstreamText`, then `EngineTextGates.armFor`, which is
  `EngineTestText.plan(TextAsk)`). Both answers are the edge's (`edge/EdgeEngineTestText`). The spike's arm
  (`edge/RelationTextArm`) answers them for one more shape: a side whose root, through the lets, is
  `meta::pure::functions::relation::toString(rel)` (or `toString(rel, false)`).
  - It asks for the RELATION (`EngineTextPlanning.ask(rel, false, true, hook)`: a TABULAR side, as the grid-pair arm
    asks for one) and for the other side as the String it is.
  - Its judgment is `HostJudgment.EdgeJudged`, holding the edge's own record (`edge/RelationTextJudgment`: which sides
    are printed, the form `TDS`) with no appeal (`RootLoop.NO_APPEAL`). The record is an INSTRUCTION ("present this
    side as `#TDS` text"), not a text: the plan holds no legend-pure text for it. `EdgeCodec` writes it under its own
    tag.
- **At the run,** the host judge hands an edge-judged comparison's sides to the run's hearing
  (`HostJudgments.judge`: `case EdgeJudged e -> run.judged(e, sides)`, `EdgeHearing.judged`). The edge's `Appeals.judged`
  sends the new record to `RelationTextArm.judge`, which prints each printed side's rows (`edge/RelationText.tds`) and
  compares the two Strings.
- **The switch:** `-Dlegend.spike.relationText=java`. It also makes `RelationTextLowering.relationToString` throw by
  name, so a `toString` that still reaches lowering (not at an assert side's root) fails and is counted.

**What this means for the standing rule ("a slice that needs a new hook in the core stops").** This part of 1.3 needs
none. It uses three hooks that exist: `EngineTestText` (C4), `HostJudgment.EdgeJudged` (C3) and `EdgeHearing.judged`
(C2). Two cautions for the real slice:

1. **The compatibility DATABASE judge cannot judge it.** Its gate for an edge-judged comparison is the edge's
   (`EngineTestText.gate`); the spike answers UNPLANNED. Measured: under BOTH, 365 of the relation suite's 469 bodies
   are then refused ("the program road does not plan this body: database judge's planner refused an assert"). So 1.3
   must follow 2.2 (the compatibility database judge deleted), as the plan already orders it. Under the HOST judge
   alone, nothing is refused.
2. **Guard rows, the user's word.** `ArchitectureTest.PLANNER_ENTRIES` lists each `EngineTextPlanning` member with
   its callers (today `com.legend.edge.SqlTextVerdicts` and `EdgeEngineTestText`). A new edge class calling `ask`,
   `check`, `planned`, `bare`, `letPrefix`, `specs` and `hook` needs a caller added to seven rows, or the arm lives
   inside a listed caller. The spike did not run `ArchitectureTest` and moved no row.
3. **These three hooks are the ones 2.2b reshapes** (the appeal's move, CP-14 (iii)). The relation-text arm should
   be written against whatever interface 2.2b leaves; it needs only: "claim an assert at planning, ask for its sides,
   be handed the sides' typed rows at the run".

**What was stubbed.**
- The failure message is legend-pure's `\nexpected: %r\nactual:   %r`, written by the spike itself and not through
  `EdgeHostWords`. Checked once: on Postgres two relation-text tests fail on `main` and under the spike for the same
  reason (§4.5), and the spike's message matches that lane's ledger text for both.
- `assertNotEquals` and `assertEquals` with `toString(rel, true)` are not claimed (PCT has neither over a relation's
  text; they would fail by name with the switch on).
- A messaged `assertEquals` goes through the planner's own `Messaged` wrapper, untested (PCT has none over a
  relation's text).
- The route's census line (`VerdictCensus.sqlRoute`) is not counted.

## 4. THE CHECK: PCT channel B with the SQL builder OFF

**Commands** (cloud container, Bazel 9.2.0, 4 cores): `bazel test //pct:pct_channel_b_host_<suite>
--jvmopt=-Dlegend.spike.relationText=java --nocache_test_results`. Each comparison the arm judges prints one `[H11]`
line (HELD or DIFFERS, the running test, the text's length); each (column type, carrier) pair the printer meets prints
one `[H11-cell]` line.

**Why the HOST lanes:** §3's caution 1. The HOST channel-B pass (`pct_channel_b_host_*`) is today's lane that runs
every body under the host judge alone, against the same ledgers.

### 4.1 DuckDB, the five suites, HOST judge

| Suite | Tests | Pass on `main` | Pass with the SQL builder OFF and the Java printer | Tests that assert a relation's text | Comparisons judged | Held byte for byte | Differ | Refused (print not at a side's root) |
|---|---|---|---|---|---|---|---|---|
| relation | 469 | 469 | **469** | 365 | 373 | **373** | 0 | 0 |
| standard | 205 | 205 | **205** | 58 | 58 | **58** | 0 | 0 |
| essential | 345 | 336 (9 ledgered) | 336 | 0 | 0 | 0 | 0 | 0 |
| grammar | 137 | 137 | 137 | 0 | 0 | 0 | 0 | 0 |
| unclassified | 93 | 93 | 93 | 0 | 0 | 0 | 0 | 0 |
| **All** | **1,249** | 1,240 | **1,240** | **423** | **431** | **431** | **0** | **0** |

- **Of the 423 tests that assert a relation's text, 423 pass; no failure to list.** The target was PCT's own expected
  text, and every one of the 431 texts is equal to it byte for byte (`String.equals`).
- All five targets PASSED their own pins and ledgers with the switch on.
- 423 is H6's number (relation 365, standard 58), reached independently here by a source census (§4.4).

### 4.2 H2, the relation suite, HOST judge (`pct_channel_b_host_h2_relation`)

| Run | Pass | Error | Comparisons judged | Held | Differ |
|---|---|---|---|---|---|
| `main` | 432 | 37 | n/a | n/a | n/a |
| SQL builder OFF, Java printer | **436** | 33 | 342 (334 tests) | **342** | 0 |

- **No test moves from pass to fail. Four move from error to pass:** `project::testSimpleProject`,
  `testSimpleProjectList`, `testSimpleProjectWithEmpty`, `composition::testFilterPostProject`. On `main` each fails
  with "DialectCapability: LIST_GET reached a dialect without a list encoding": the SQL TEXT of the relation needed a
  list form H2 has not; its ROWS do not. So the Java printer moves H2 toward PCT's expected values by 4.
- The lane's exact ledger reports those four as "PASSES, ledgered as failing", so the target is red until its ledger
  loses four rows. That is a listed ledger move for the real slice, not made here.
- The other 33 errors are the same on both runs: H2's own capability refusals (STRING_AGG 12, UNNEST 4, LIST_FILTER
  4, LATERAL 4, and 9 more list forms), none about a relation's text.

### 4.3 What the printer met

Every (column type, carrier) pair the 431 DuckDB texts and the 342 H2 texts held:

| Column type | Carrier | Where |
|---|---|---|
| Integer | `Long`, null | relation, standard |
| Float | `Double`, null | relation, standard |
| Number | `Double` | standard (`testSimpleWindowCorr`) |
| String | `String`, null | relation, standard |
| Boolean | `Boolean` | relation, standard |
| Date | `PureDateLiteral` at SUBSECOND precision (a TIMESTAMP's nine digits), null | relation, standard |
| Variant | `String` (its JSON text) | relation |

**Not met by PCT, so unproven by this check:** a Decimal column; a `BigDecimal` under a Float or Number column (the
SQL builder's "81.180 prints 81.18" case, which it reached through DuckDB aggregates: here DuckDB's typed read hands
a `Double`); a StrictDate column; a whole number under a Float column; a mixed (tagged) column; a pivot's keyed
columns. The printer has a rule for each of the first four (§2's table); they need their own unit tests in the real
slice.

### 4.4 Where the print stands (the source census)

`census.py` (scratch) over every `<<PCT.test>>` function of the relation and standard trees at the pin, each
`->toString(...)` classed by what encloses it:

| Suite | PCT tests | Tests with a `toString` | Sites | The second argument of `assertEquals`, at its root | Elsewhere |
|---|---|---|---|---|---|
| relation | 469 | 365 | 382 | 373 | 9, all a VALUE's `toString` inside a column lambda (`extend(~c: x \| ...->toString())`, `project`, `filter`): not a relation's text |
| standard | 205 | 58 | 59 | 58 | 1, a value's `toString` inside `extend` (`testDayOfWeek_Relation`) |

- **A relation's `toString` is at an assert side's root 431 times out of 431.** None is composed, none feeds.
- The receiver is `$res` (57), or `$res->sort(...)`, sometimes `->select(...)`, then `->toString()`.
- The run agrees: with the SQL builder switched off, no body anywhere in the five suites reached it (0 "SPIKE H11"
  refusals in the six logs).

### 4.5 Two more databases, HOST judge (`--jvmopt=-Dpct.channelb.judge=HOST` on the lanes' targets)

Each lane was run twice, `main`'s judging and the spike's, and the two outcome lists compared test by test. The lanes'
ledgers are written for BOTH, so only the comparison is read here.

| Lane | Suite | Pass on `main` | Pass with the Java printer | Comparisons judged | Held | Differ | Tests that moved |
|---|---|---|---|---|---|---|---|
| SQLite | relation | 458 (11 error) | 457 (12 error) | 362 (354 tests) | 361 | 1 | 1, pass to fail |
| SQLite | standard | 205 | 205 | 58 | 58 | 0 | 0 |
| Postgres 16 | relation | 467 (2 error) | 465 (4 error) | 372 (364 tests) | 369 | 3 | 2, pass to fail |
| Postgres 16 | standard | 198 (7 error) | 198 (7 error) | 58 | 58 | 0 | 0 |

**None of the four differing texts is the printer's.** Each was read:

- **Postgres, 2 texts** (`filter::testVariantColumn_filterOnKeyExtractionValue`,
  `extend::testVariantColumn_keyExtraction`): both tests fail on `main` too, with the same text. Postgres's `jsonb`
  answers `{"string":"hello","boolean":true,"integer":1}` where PCT expects the keys in source order. The Java printer
  and the SQL builder print the identical cell. Not a move.
- **SQLite 1 and Postgres 1, the same test** (`composition::testExtendJoinStringOnNull`), and one more Postgres test
  that now fails in the database (`extend::testOLAPAggStringWithPartitionAndUnboundedWindow`: "window functions are
  not allowed in functions in FROM"): **the relation's ROWS are wrong, a product defect the SQL text builder was
  hiding.** §8, finding P1.

So the three tests that move are one product defect, on the two databases whose list encoding is a subquery. With that
defect fixed, the expectation is no move on either lane; that was not run.

**Not run:** MariaDB, MySQL, ClickHouse, Trino, MongoDB and the three cloud lanes. The typed carriers differ by
driver, and P1 shows a rows plan can differ from a text plan, so each lane needs this same two-run comparison before
the real slice lands.

### 4.6 The SQL the judge sends gets smaller (G3)

`-Dlegend.diagnostics=sql-sizes`, DuckDB, the relation suite, HOST judge, session statements left out:

| | Statements | Characters |
|---|---|---|
| `main` | 475 | 1,005,717 |
| SQL builder OFF, Java printer | 475 | 496,617 |
| of which the 365 relation-text tests, `main` | 376 | 696,964 |
| the same tests, Java printer | 376 | **187,864** (73% less) |

The same number of statements, each a plain read of rows instead of `STRING_AGG` over `CONCAT`s of `CASE`s. The
suite's wall time is flat (13.9 s to 12.2 s, one run each: not a measurement).

## 5. The edge cases the audit found (EDGE-1, EDGE-2, EDGE-3)

The cases are `EdgeCasesGoldenTest`'s on `claude/presentation-b1`; "today" is its pinned answers
(`core/src/test/resources/edge/edge_cases_answers.tsv`, the same on DuckDB and H2 under both edge styles). Each was
run through the Java printer two ways: on hand-made cells (`pct/.../edge/RelationTextSpikeTest`, 7 tests) and END TO
END at a query's root on DuckDB and H2 (`RelationTextRootSpikeTest`, §6; 24 answers).

| Case | Today (the SQL builder) | legend-pure or legend-engine, read at the pin | The Java printer | Move |
|---|---|---|---|---|
| **EDGE-1**, a Float cell in a relation's `toString`: `0.00001`, `0.0005`, `25000000.0`, `10000000000000000.0`, `100000000000000000000.0` | those five texts (after PR #118's F7) | the same: `FloatCoreInstance.canonicalizeBigDecimal(BigDecimal.valueOf(d)).toPlainString()` | the same five, on both databases | none |
| **EDGE-1**, the same cells in `toCSV` | those five texts | the same: `toCSVString` is `toString()->escapeCSVString()` (`helperFunctions.pure:234`) | the same five | none |
| **EDGE-2**, a DateTime in `toCSV` (`2024-01-02T10:30:00.123+0000`) | `2024-01-02T10:30:00.123+0000` | **`2024-01-02 10:30:00`**: `formatDateTime` (`helperFunctions.pure:238-243`) formats a date with an hour by `SimpleDateTimeFormat()`, which is `'%t{yyyy-MM-dd HH:mm:ss}'` (`dateExtension.pure:384-387`); a date without one by `'%t{yyyy-MM-dd}'` | **`2024-01-02 10:30:00`**, on both databases | to upstream's answer: a listed golden move |
| **EDGE-3**, `format('%r', ['a\nb'])` | `'a` LF `b'`: the newline raw | **`'a\nb'`**, the two characters `\` and `n`: `toRepresentation.pure:43` escapes `\`, `'` and the newline | `'a\nb'` (`RelationText.representation`, one line of Java) | to upstream's answer: a listed golden move |

- EDGE-3 is a VALUE's print, not a relation's: the spike holds only its one rule, to answer the question. Its home in
  the real slice is the value presenter (`edge/Presenter`), with `format`'s other directives.
- **One rule the Java printer does NOT copy from the SQL builder, to be settled on the corpus.** For a column typed
  abstract `Date` over a TIMESTAMP, the SQL builder's `toCSV` prints the day alone when the time of day is midnight
  (`RelationTextLowering.timed`). legend-engine's rule at the pin is `hasHour`, a fact of the value, and a TIMESTAMP's
  value has an hour. The Java printer follows the carrier: a TIMESTAMP cell prints `yyyy-MM-dd HH:mm:ss` even at
  midnight; a DATE cell prints its day. No PCT test reaches it; the relational corpus will say which its goldens hold.

## 6. A query's ROOT: the door rewrite, through public doors alone

Slice 1.3: "the edge's door rewrites a root `toString` (or `toCSV`) into: answer the value; present it as text".
Prototyped in `pct/src/test/java/com/legend/edge/RelationTextRootSpikeTest.java` (scratch):

1. the query is typed once (`Compiler.query(ctx, parsed).expression()`) to read what its root IS, by its callee, a
   typed fact;
2. where the root is `relation::toString` or `toCSV`, the same lambda is typed again with the print's receiver in its
   place (`Compiler.query(ctx, ValueSpecification)`);
3. it is planned and run **in the product's own style** (`PlanStyle.PRODUCT`: no edge dialect, no edge SQL), its typed
   rows collected (`result.Collector`);
4. `RelationText` writes the text from the plan's columns and the rows.

- **24 of 24 answers equal upstream's text** (12 cases, DuckDB and H2): EDGE-1's ten, EDGE-2, and the same DateTime in
  a `toString`.
- **No new hook, and no edge SQL at all on this path.** Every call is a public door: `Compiler.compileModel`,
  `Compiler.parseQuery`, `Compiler.query`, `TypedQuery.expression`, `Execution.run`.
- **What the product's plan carried for each column** (printed by the test): `x type=Float slot=DOUBLE
  precision=null`, `id type=Integer slot=BIGINT precision=null`, `at type=DateTime slot=TIMESTAMP precision=INSTANT`;
  cells `Double`, `Long`, `PureDateLiteral.DateWithSubsecond`.
- **Not prototyped:** the real routes (`pure/v1` execute, the service tests' runner), a multi-statement lambda with
  lets, a print inside a returned structure (a column, a Pair's side, `^C(p = ...)`), and the envelope's declared type
  changing to String. The typing happens twice; a real door would keep the first typing.

## 7. The typed facts the printer needed (for CP-6)

| Fact | Needed by `toString` | Needed by `toCSV` and `csv` | Where it came from |
|---|---|---|---|
| A column's name | yes | yes | a judge side's `TypedSide.Tabular` column; the plan's `Column.name` |
| A column's Pure type | yes: Variant (the quoting and `'null'`); Float against Number or Decimal when the cell is a `BigDecimal` or a whole number | yes, for the same number cases | `Column.pureType`; the plan's `Column.type` |
| The typed cell (`Long`, `Double`, `BigDecimal`, `Boolean`, `String`, `PureDateLiteral`) | yes | yes | the run's typed read (`JdbcDecode`) |
| Whether a date cell has a time of day | no (see below) | yes | the cell itself: `PureDateLiteral.precision()` |
| **A DateTime's WRITTEN precision** | **no** | **no** | not needed: legend-pure's TDS re-reads every date at exactly three subsecond digits (§2), and `toCSV` drops the fraction |
| The SQL slot | no | no | not needed: the carrier already says what the SQL builder read the slot for (a DATE or a TIMESTAMP; a DECIMAL, a DOUBLE or an integer) |
| Whether the column is stored or computed | no | no | |

- **Nothing is missing for a relation's text.** A judge side's column record is three fields (name, type,
  multiplicity) and that was enough for all 431 PCT texts and the edge cases. CP-6's "a DateTime's written precision"
  is not this printer's need; it remains the VALUE presenter's (a DateTime literal's own `toString`, S118).
- **Unproven, because PCT never holds them** (§4.3): a mixed (tagged) column's cell, whose kind is per row; a list or
  a Map cell; a class instance's cell. The SQL builder has a per-slot arm for the first (`perSlot`). If a tagged cell
  arrives at the host as its winning value with its own carrier, the printer needs nothing more; if it arrives as a
  tag and slots, the printer needs the tag's kind. That is the one place a fact may be missing; it was not reached.

## 8. Findings outside the printer

**P1. A product defect on SQLite and Postgres, hidden by the SQL text builder (for the product's owner).**
- **What:** when a relation with a window column is read as ROWS at a statement's root, and a later column's
  expression reads that window column through a list operation, the lowering puts the window call INSIDE the list
  encoding's subquery. There it sees one row.
- **Witness:** `meta::pure::functions::relation::tests::composition::testExtendJoinStringOnNull`:
  `#TDS...#->extend(over(~grp), ~newCol:{p,w,r|$r.name}:y|$y->joinStrings(':'))->extend(~newColSorted:
  r|$r.newCol->toOne()->split(':')->sort()->joinStrings(''))->select(...)`.
  - SQLite sends `... FROM json_each((SELECT ... group_concat(_tds2.name, ':') OVER (PARTITION BY _tds2.grp) ...))`
    and answers `A` and `E` for group 2 where the answer is `AE` and `AE`. **Silently wrong rows.**
  - Postgres answers the same wrong rows for this test, and refuses
    `extend::testOLAPAggStringWithPartitionAndUnboundedWindow` outright ("window functions are not allowed in
    functions in FROM").
- **Why `main` passes:** the text builder wraps the relation as a subselect (`FROM (SELECT _tds2.*, group_concat(...)
  OVER (...) AS newCol ...) AS t10`), so the window is computed first and read by name. Both statements are in
  `logs/pct_channel_b_sqlite_relation.dbg_{base,spike}.log` (scratch; `-Dlegend.diagnostics=dump-sql,chb-only=...`).
- **DuckDB is right** (its list forms are not subqueries). **H2 refuses both tests by capability**, on `main` and under
  the spike alike ("collection reduction 'STRING_AGG' reached a dialect without a list encoding").
- **It matters to 1.3:** it must be fixed in the product first, or the SQLite lane loses 1 test and the Postgres lane
  2 when the relation text moves to Java. It also means a product user's query of that shape is wrong on SQLite today.

**P2. Today's SQL builder prints a finer DateTime fraction than legend-pure would.** `dateTimeTextTds` writes at least
three digits and keeps up to nine (`.123456789`); legend-pure's TDS holds three, truncated (`.123`, §2). The Java
printer writes three. No PCT golden has a finer fraction, so no test moves; one register row may be owed for the
difference in today's product.

**P3. H2 gains four tests** (§4.2): a relation whose TEXT needed `LIST_GET` in SQL has rows H2 can answer.

**P4. The corpus has one composed family this spike did not run** (a source census at the pin, not a run):
- legend-engine's relational tree has 225 `toCSV(` sites in 30 files. **41 of them are
  `toCSV($result.values)->replace('\n', ',')`**, one per test, all in
  `core_relational/relational/calendarAggregation/tests/testCalendarFunctions.pure` (41 of its 93 functions). Their
  goldens hold Float cells (`'hireType,annualized,Campus,9.812850678733032,Lateral,9.267692307692307,'`). 39 are in
  the DuckDB corpus's unordered register (`spec/src/test/resources/rcorpus/duckdb-unordered-register.txt`).
- That is a relation's text whose CONTENT is read (`replace`), over Floats. By slice 1.3's rule it is refused by
  name. The product's host judge already names this exact shape as one render form
  (`AssertVerdicts.renderForm`: `CSVJOIN:<sep>`).
- **A question for the owner, not answered here:** either "CSV with the rows joined by `<sep>`" is a presenter FORM
  (a parameter of the format, written by Java from the rows, like the header separator), or these 41 tests are
  refused. The log's 0.4 (f) says "0 of the 22 COMPOSED and FEEDS sites are over a Float, Decimal or DateTime"; these
  41 look like a counter-example unless that census counted value prints only. To be reconciled with B1.
- The other `toCSV` sites at a glance: 156 end a call (`...->toCSV())`), 16 are the first argument of an
  `assertEquals`, 11 are `let actualValue = ...->toCSV();` (carried), 13 are inside `println` (never asserted), 1 is
  inside `^CSVTestResult(result = ...)`. A TDS's `csv` property is read in 1 test (`testEnumInRelation`).

## 9. What the spike did not do

- **The relational corpus was not run.** `toCSV` and `csv` are written and unit-tested against upstream's rule, but
  no assert arm claims them and no corpus lane ran. The corpus is where `toCSV` lives (PCT has none), where the ruled
  rendered-text comparison is (S131, S132: 26 DuckDB tests and 3 H2 by H6 §6), and where P4's family is.
- **The VALUE prints of slice 1.3** (a Float's or a DateTime's `toString`, a list's `[a, b]`, `format`'s `%0Nd`,
  `toRepresentation`, an instance's id; `EdgePureOnlyTexts`, `PureListTexts`, `PureDateTimePrint`, the print half of
  `PurePrintForms`) were not touched. H6 §3.3 counts 10 PCT tests that assert them (7 a value's print, 3 refused by
  name today), beside the 16 that assert legend-pure's error words (slice 1.2's).
- **BOTH** cannot run it (§3): the compatibility database judge refuses the 365 bodies.
- **No guard was run:** `ArchitectureTest`, `PctDisciplineTest` (the two scratch tests sit in the PCT module's tree
  and its source scan may object), `HookRulingTest`, `EdgeOnlyDataTest`, the plan-JSON round trip (`decoded-plan`).
- **Seven lanes** were not run (§4.5).
- `toString(rel, true)` (types and multiplicities in the header) is not written.

## 10. Sizes

| | Lines | Code lines (no comments, no blanks) |
|---|---|---|
| `edge/RelationTextLowering.java` (the SQL builder, to delete) | 791 | 574 |
| `edge/RelationText.java`: `toString`, `toCSV`, a TDS's `csv`, a value's print, `%r` of a String (and 17 lines of the spike's own census) | 213 | 133 |
| `edge/RelationTextArm.java`: the claim, the plan, the run's compare | 141 | 104 |
| `edge/RelationTextJudgment.java`: the record | 16 | 4 |
| Edits to `EdgeEngineTestText`, `Appeals`, `EdgeCodec`, and the switch in `RelationTextLowering` | 32 | |
| **The Java side, all of it** | **about 400** | **about 245** |
| The two scratch tests | 237 | |

The SQL builder also keeps alive, in the product, the facet `lowering/RelationTexts.java` (61 lines), three `case`
arms of `Lowerer.scalar`, `PlanStyleFacets`' entry and `Render.csvOverRelation`; they go with it.

## 11. The estimate for the real slice 1.3

**The relation-text half: 4 to 5 days**, given 2.2 (the compatibility database judge deleted, the edge judge's
interface settled) and 1.1 (the presenter seam). The spike's own code is perhaps a day of it; the rest is the corpus
and the lanes.

| Piece | Days | Why |
|---|---|---|
| The printer, hardened: Decimal, a decimal under a Float, StrictDate, tagged and pivot columns, list and Map cells, `toString(rel, true)`; its unit tests from the pin | 0.5 | written; §4.3's unproven carriers each need a test |
| The assert arm on 2.2b's interface: `assertEquals`, `assertNotEquals`, a messaged assert, both sides printed, legend-pure's failure words through the edge's words; the codec | 0.5 to 1 | written against today's three hooks; rewritten once |
| A root's door rewrite on the real routes (`pure/v1` execute, the service tests' runner), the envelope's type as String, prints inside a returned structure (0.4 (f): 3 of 98) | 1 | §6 proves the mechanism on a test, not on a route |
| **`toCSV`, `csv` and the corpus:** the arm's CSV forms, the ruled comparison over a presented text, EDGE-2's goldens, the abstract-Date question (§5), P4's family once ruled | 1 to 1.5 | not run here; each corpus lane is about 500 s |
| Every lane compared as §4.5 does (seven more), the ledgers' listed moves (H2 loses 4 rows), the judge-SQL ratchets (they shrink, §4.6), the deletions and their guard rows | 1 | |

- **Before it:** P1 fixed in the product (not sized here; it is one lowering rule), or the SQLite and Postgres lanes
  lose 3 tests.
- **Not in this figure:** the VALUE prints (§9). The plan gives all of 1.3 four days, or six if many prints are
  nested. With the relation half alone at 4 to 5, **all of 1.3 looks like 6 to 8 days**, unless the value half is
  much smaller than the relation half. That half has no prototype.

## 12. Method

- **Setup** (cloud container, Bazel 9.2.0 through bazelisk, 4 cores, 15 GB): `tools/cloud-distdir.sh <targets>`; the
  two upstream archives packed from `git clone --depth 1` of the pinned tags (`legend-engine-4.145.0`,
  `legend-pure-5.99.0`), their sha256 equal to `release.MODULE.bazel`'s pins. The same clones were read for §2 and §5.
- **A lane:** `bazel test //pct:<target> --nocache_test_results [--jvmopt=-Dpct.channelb.judge=HOST]
  [--jvmopt=-Dlegend.spike.relationText=java]`; its `test.log` holds one `[chB-...] PASS|FAIL|ERROR <test>` line per
  test, one `[H11] HELD|DIFFERS <test>` line per comparison the arm judged, one `[H11-cell]` line per (type, carrier)
  pair. Two runs' outcome lines are joined by test name.
- **Postgres** as root: `--strategy=TestRunner=standalone --run_under="<a copy outside the workspace>/as_unprivileged.sh
  ubuntu"` (docs/CLOUD_SESSIONS.md).
- **The spike's own tests:** `bazel test //pct:h11_relation_text_test //pct:h11_relation_text_root_test` (two scratch
  targets added to `pct/BUILD.bazel`).
- **The source censuses** (`census.py`, `census_csv.py`, scratch): §4.4 and §8 P4 state their rules.
- Every run's log was kept in the session's scratch folder; none is committed.

## 13. The corpus half (second session, `session_0195rQEnceykGKzTfRUo7XWp`, from 13:15 UTC)

The orchestrator's four asks of 12:47 UTC: (1) the relational corpus on H2 and DuckDB with the Java printers and the
SQL builder off, the rosters and the verdict ledger against `main`'s; (2) the 41 calendar tests as a presenter FORM;
(3) the judge-side COMPOSED prints in the corpus and PCT, by shape; (4) the abstract-Date-at-midnight `toCSV` question.
The first session's unpushed corpus work was lost; this one started from `5c62ee85`.

### 13.1 Where every judge-side print stands (ask 3): the census

**Method.** `H11-probes/JudgeCensus.java.txt` parses every `.pure` file with lite's own parser (`ModelFromText`, as
H2's `TextCensus` does, compiled standalone with `javac`), walks each test's body with its lets bound and its TEST
helpers inlined (a function under a `tests` package or directory, its parameters bound to the call's arguments), and
records every print (`toString`, `toCSV`, a TDS's `csv`, `toRepresentation`, `format`, `formatDate`, `makeString`) on
an ASSERT's argument, never inside a query lambda (`execute`'s, `eval($f, ...)`'s, `toSQLString`'s and 25 more: those
are query-side, H2's and B1's census). Each print is classed by the frames between the assert's argument and it,
content-independent wrappers (`toOne`, `at`, a list literal) dropped: BARE (at the side's root), COMPOSED, FEEDS (a
content reader above it), MESSAGE (a failure message). `families.py.txt` groups the sides by their canonical shape.
The data: `H11-data/corpus-judge.tsv`, `H11-data/pct-judge.tsv`, one row per site. About 15 s per run.

**PCT (1,249 tests, five suites): 431 judge-side prints, all a relation's `toString` at an assert side's root**
(relation 373, standard 58). **0 composed, 0 feeding.** This is §4.4's number, reached by a second, independent census.
B1's one composed PCT print (`testPairCollectionToString`) is QUERY-side: inside the `eval` lambda.

**The corpus (2,761 tests): 980 assert sides in 828 tests hold a judge-side print** (4 more are failure messages).

| Family | Sides | Tests | What it is | Whose |
|---|---|---|---|---|
| A1 a relation's `toString` at the side's root | 253 | 127 | `#TDS...#->toString()` expected (125) against `$result.values->at(0)->sort(...)->toString()` (128) | the relation printer |
| A2 `toCSV` at the side's root | 160 | 145 | `assertEquals` 138 (15 with the CSV the FIRST argument), `assertSameElements` 22 | the relation printer |
| **A3 `toCSV(...)->replace('\n', sep)`** | **41** | **41** | the calendar family, all in `testCalendarFunctions.pure` | **the relation printer, a FORM (CSVJOIN)** |
| A4 a TDS's `csv` | 1 | 1 | `testEnumInRelation` | the relation printer |
| B1 each row's cells joined | 316 | 315 | `$result.values.rows->map(r\|$r.values->makeString(s1))`, then `->makeString(s2)` or `->sort()` | cell VALUES (makeString's per-cell `toString`) |
| B2 a row's text the test writes | 72 | 72 | `rows->map(r\|$r.getString('a') + ' ' + $r.getInteger('b')->toString())` | cell VALUES, user code |
| B3 every cell flattened and joined | 50 | 50 | `makeString(sort(map(rows, r\|$r.values)), s)` | cell VALUES |
| B5 one value's `toString` | 46 | 46 | `[min(dates)->toString(), max(dates)->toString()]` (calendar date ranges: StrictDate) | a VALUE |
| B6 the column names joined | 4 | 4 | `columns->map(c\|$c.name)->makeString(s)` | Strings (identity) |
| B7 other | 37 | 37 | property values joined (`makeString($result.values.name)`: Strings), 3 tests that build INPUT text (`'student_' + toString($i)`, Integers), 1 `dataTypeToSqlText` | mostly Strings |

**So, for the relation printer (slice 1.3's relation half):**
- **Judge-side COMPOSED relation prints: 41 in the corpus (A3, one shape), 0 in PCT.** That is the reconciliation
  with B1's 0.4 (f): B1 counted QUERY-side prints, where the corpus has 2 and none is a relation's text; the 41 are
  JUDGE-side, where B1's census did not look. No relation print FEEDS anything (no `contains`, no `==`, no `length`
  over one), and none is composed by `+` or `joinStrings`.
- **The one composed shape is covered by a presenter form:** `CSVJOIN:<sep>`, "the CSV with every newline written as
  `<sep>`" (Pure's `replace` replaces every occurrence, a cell's own quoted newline included; the Java form does the
  same, `String.replace`). It needs no refusal.
- **455 relation-text sides in all (A1-A4), every one at an assert side's root.** 22 of them sit under
  `assertSameElements`, which today's edge hook does not reach (§13.3).

**For slice 1.3's VALUE half (not this spike's printer):** 525 sides (B1-B7) print CELL VALUES that the test's own
code composes on the judge side. They are not relation texts: the product lowers them today (its `makeString`, `plus`,
`toString`), each cell printed by the product's value text (a Float by `LiteralSpelling.floatPrint`) or, for a few
kinds, by the plan style's (a DateTime's text, a list's, legend-pure-only texts). Which of them reach an EDGE text, and
so move when 1.3 deletes the edge's value texts, is measured by the run (§13.4). B1 (316) and B3 (50) are regular
enough to be presenter forms ("each row's cells joined by s1, the rows joined by s2, optionally sorted"); B2 (72) is
arbitrary user code in a lambda (getters and `+`), no form: it can only stay a lowered query with the product's text,
or be refused where a printed type's text differs by audience.
