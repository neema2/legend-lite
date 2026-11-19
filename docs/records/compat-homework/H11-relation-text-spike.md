# H11: legend-pure's relation text printed in Java (the spike for slice 1.3), 2026-11-19

**Status: IN PROGRESS. Written as it is found; §0 is filled last.**

- **Branch:** `claude/compat-relation-text-spike`, from `main` `b75d6c73a`. Scratch: never proposed for merge as is.
- **Session:** `session_0195rQEjVtMXfZdcaQdweUzf`; reports to the program's owner (`session_0195rQEoSpv1FxC3WndEk5ZH`).
- **The question:** slice 1.3 deletes `edge/RelationTextLowering.java` (791 lines, a relation's `toString`, `toCSV` and
  a TDS's `csv` composed in SQL) and writes the same text in Java after the run, from typed rows. No Java printer of a
  relation existed. 423 PCT tests assert legend-pure's `toString` of a relation. Does it work, byte for byte?
- **Nothing here moves a pin, a ceiling, a ledger, a roster, a golden or an allowlist.**

## 0. The answers on one page

(filled at the end)

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
- The failure message is `\nexpected: '<text>'\nactual:   '<text>'`, written by the spike, not legend-pure's words
  through `EdgeHostWords` (no failing case exists to compare it with: §4).
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
