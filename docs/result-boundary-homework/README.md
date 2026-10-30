# The result boundary program: the research record (homework), 2026-10-26 to 2026-10-28

Everything the program learned that was not yet in a document: the audit of 2026-10-27, the maps of the code made
for phases 1 to 5, the 51 writer differences, the census of core's tests, what legend-engine answered when asked,
the lanes' method, and every probe's source. Written 2026-10-28 so that nothing lives only in a session that can end.
The plan built on it is `docs/RESULT_BOUNDARY_START_HERE.md`; this folder is its evidence. Line numbers below are as
of the date given; search by name, not by line.

| File | What it is |
|---|---|
| this README | the findings, in order |
| `wire-differences-2026-10-27.tsv` | the 51 differences between the old database-written wires and the new Java writers, PCT DuckDB, phase 2 (§4) |
| `test-census-2026-10-28.tsv` | core's 140 test files still on the old door: 2,304 test methods classified (§5) |
| `probes/*.txt` | every probe's and script's source, not compiled (the rule: never commit a probe as code) (§8) |
| `WRITER_DIFFERENCES_BY_UPSTREAM_2026_10_29.md` | the 51 differences classified by upstream's answer, read from the pinned sources (S1.5) |
| `DECISIONS_LOG.md` | every decision made or wanted, per phase, with its upstream evidence (the user, 2026-10-29) |

---

## 1. The audit of phases 1 to 5 (2026-10-27)

The user stopped phase 5 and asked: "what did we actually do? did we do it right? did we stick to our core tenets?
how does the PCT harness look now? did we do both the host and the database judge? did we do channel B?" The answer,
measured against the code (not recalled):

**What each phase did.**

| Phase | Commit | Built | Deleted | Tested before landing |
|---|---|---|---|---|
| 1 | `51688f36` | typed results streamed to a sink in batches (`result` package: `ResultSink`, `Batch`, `Batcher`, `Collector`; `TdsResult.Shape`; plan format 12) | nothing | core |
| 2 | `12b89c7d` | the Java writers: Pure's text, legend-engine's JSON, lite's CSV and JSON (`CellText`, `PureV1Writer`, `CsvWriter`, `JsonRowsWriter`) | nothing | core; PCT DuckDB differential, 51 differences accepted (§4) |
| 3 | `1b0305f7`, `53cf5b6d` | `Execution.run` for `pure/v1` execute, the query service and the hosts' plan half | every CSV and JSON result the database wrote (`WireRender`, `Render`'s wires, `executeWire`, `executeStreaming`) | core, hosts, Postgres |
| 4 | `b3311cd6` | PCT channel A on typed rows | the TDS text path (`TdsTextWrap`, `Render.tdsText`, `TextResult`, `Format`, `TypedQuery.Output`) | PCT DuckDB, H2, SQLite, MongoDB; channel B DuckDB |
| 5 | `0fe63b42` (WIP, then `f883d42b` green) | effects as plan nodes (`ExecutionPlan.Effect`, `PlanRunner.update`/`effect`), value statements through `JudgePlans` | `exec.Executor` (1,175 lines) | nothing until `f883d42b` |

**Where we broke our own rules** (each since fixed or scheduled; the fix is named):

1. Deleted before measuring every lane (R7: measure, both, switch, delete, per lane). Phases 3 and 4 deleted the old
   paths after DuckDB, H2, SQLite and MongoDB only. Fixed by S1.1: six lane sessions measured BASE `7b8c89dc` against
   HEAD `b3311cd6` per test; every loss traced and fixed in `f883d42b` (GATES 2026-10-27, phase 5 entry, item 3).
2. No records: GATES, IN_FLIGHT, §9 and the register untouched across five commits; the 51 writer differences accepted
   by the session alone. Partly fixed (GATES entries from phase 5 on); the rest is S1.5.
3. The writers do more than carriage, and nothing guards them (R3 forbids comparing a value or choosing a spelling by a
   Pure rule). Found: `CellText.pureFloat` writes `-0.0` as `0.0` (fixed 2026-10-28 on the user's word: register
   S117, S114 until S1.0); the engine writer chose a date's form by its time of day (fixed `6811256b`: the engine writes a
   `java.sql.Timestamp` in full); a plan fact matched by slot-name strings
   (`"TEMPORAL_TEXT"`, `"LITERAL"`) in `JdbcDecode`, `RowDecode`, `MongoDbDecode`, `CellText`. The guards the program
   names (`WriterCarriageGuardTest`, `writersAreCarriageOnly`) were never built (the user had said "skip the guards" on
   2026-10-27; the audit recommended building them; approved 2026-10-28, "yes add this to the plan"). Open: S1.4.
4. A task marked done with "Arrow" in its title: no Arrow writer exists. Open: track A.
5. Phase 5's WIP commit had run no test. Fixed: `f883d42b`.

**What held up.** The database still computes every value; no new Java value computation beyond the writer items. No
new silent default: failures refuse by name (NaN, a null in a structure, an unknown cell). The phase 4 compiler change
is right (`21d` in a TDS literal is a Float, measured with the reference's own CSV reader). No PCT test went from
passing to failing on DuckDB (§2). The reference checkouts stayed test input only.

## 2. Did we lose passing PCT tests? The method, and the answers

**The method** (the user: "did we lose PCT tests that pass and just gloss over that?"). Never trust a count.
- Channel A: every suite's expected-failure list is exact both ways, so read each failing test case by its WORDING
  (`probes/compare_lanes.py.txt`): "Or remove the expected failure" is a pinned test now passing; "Or replace expected
  failure" a pinned test failing with a new message; any other failure a LOST pass.
- Channel B: diff the per-test `[chB...] PASS|FAIL|ERROR|DECLINED` lines; two suites sit exactly on their floors, so a
  count hides swaps.
- A route switch: count the crossing's log markers per suite (`[LegendLite PCT] engine JSON:` since `d8ab8434`).

**DuckDB, 7b8c89dc to b3311cd6:** no lost pass in either channel. Three already-failing tests changed message
(`testBigFloatAbs` `...780.0`; `testArcCosineError`, `testArcSineError` name the NaN refusal); four MongoDB date tests
started passing (unpinned).

**Every lane (S1.1, then f883d42b):** losses found and fixed (GATES 2026-10-27 phase 5 entry, items 2 and 3): MariaDB,
MySQL and BigQuery's window `mode`/`median`/`percentile` helper columns (13); BigQuery's Decimal scale (12); BC dates on
Databricks and Postgres; Trino's hidden error; the corpus's `testSQLComments` (the trace comment); about 1,300 Cypher
and SQL parity cases (JSON and time cells). One stays pinned with its probe: Snowflake's `testAdjustByMinutesBigNumber`
(its driver hands a far-BC `TIMESTAMP_NTZ` back as another instant and refuses every `java.time` getter;
`probes/BcTimestampProbe.java.txt`). At `f883d42b`: channel A exact on ten lanes, channel B identical per test (one
MySQL gain).

**The engine crossing, e8c341d0:** exact on every suite of every lane (GATES 2026-10-28, the switch).

## 3. How the PCT harness works (as of d8ab8434)

**Channel A: legend-pure's interpreter calls our plan.**
1. Each `Test_LegendLite_<Lane>_<Suite>_PCT` class builds the reference's own PCT suite with
   `PureTestBuilderInterpreted.buildPCTTestSuite(reportScope, expectedFailures, adapter)`.
2. A test's `$f->eval(|expr)` lands in the Pure adapter (`pct/src/main/resources/core_legend_lite_pct/pct_adapter.pure`,
   `testAdapterForLegendLiteExecution`): captured variables replaced, the lambda printed as grammar, the model elements
   it uses collected, then `executeLegendLiteQuery($pureText, $roots)`.
3. The native (`PctExecuteNative`) prints those elements from the reference's model (test fixtures only), then
   `PctPlanRoute.execute`: `Compiler.query` -> `query.executionPlan(StorelessRuntime.RUNTIME,
   Set.of(CORRECT_SQL_SUBSTRING_INDEXING), PctBackend.serverVersion())` -> the plan runner on the lane's sessions
   (`LEGENDLITE_PCT_BACKEND`, DuckDB by default).
4. By the plan's shape:
   - a RELATION streams through the server's own writer of legend-engine's execute answer (`PureV1Api.engineWriter`)
     and crosses as legend-engine's own PCT adapter reads it: `ValueBridge.engineTDS` (json-simple's typing of each
     value), then the adapter's Pure `engineTDS` (`getTDSResultFromProtocol`'s steps: the header from the function's
     relation type or the builder; a String cell `toCSVString`, `TDSNull` `'null'`, else `toString`), then
     `stringToTDS`. Before `d8ab8434` it crossed as TDS text written in Java (`PureTdsText`, deleted).
   - a VALUE becomes the reference's own typed instance (`ValueBridge.values`, by the query's result type).
5. The reference runs the test's own assertion; the suite compares failures with the lane's ledger both ways.

**Channel B: our platform runs the whole test.** `pct/src/test/java/org/finos/legend/lite/pct/channelb/ChannelB.java`
reads the reference's PCT sources, rewrites `$f->eval(|expr)` into `expr`, and runs the body through the OLD door:
`Execution.executeResolved(..., JudgeMode.BOTH)` -> `StatementExecutor` -> each assertion to `AgreementArm`: the HOST
judge (each side a typed plan, `JudgePlans.evalSide`, compared in Java by `Equality.ordered`; the verdict of record)
and the DATABASE judge (both sides composed into one verdict statement, `VerdictSql`, run as a plan); a disagreement
fails the test ("THE JUDGES DISAGREE"). Scoring: PASS/FAIL/ERROR/DECLINED per test, against floors.

**The corpus** (`spec/src/test/java/com/legend/rcorpus`, gates `//spec:corpus_duckdb`, `//spec:corpus_h2`) runs
legend-engine's core_relational tests through the same old door as channel B: `MinimalCorpus.run` ->
`core/src/main/java/com/legend/test/PureTestRunner.java` (`run`, `judge`) -> `Execution.executeResolved` (setups too)
-> `StatementExecutor`; verdicts through the `AssertListener`; the replay referee (`spec/.../harness/ReplayReferee`,
`RefereeVerify`) reads `ExecutionResult`.

## 4. The 51 writer differences (phase 2, 2026-10-27)

`WireDifferential` (`probes/WireDifferential.java.txt`) ran every expression of the five DuckDB PCT suites through the
old database-written wires and the new Java writers. 51 differences remained, all accepted by the session at the time
and recorded nowhere until now: `wire-differences-2026-10-27.tsv` (columns: kind, format/shape, expression, old, new).

| Kind | Count | What |
|---|---|---|
| VALUES | 42 | the value or its spelling moved: mixed lists typed instead of literal spellings (`'a'` -> `"a"`, `1` -> `1`), identity fields (`__id`, `__type`) no longer leaking, a Pair printed as Pure prints it (`<1, a>`), the zone on hour- and minute-precision dates, NaN as a JSON string, the engine wire's dynamic pivot cells (null before), random UUIDs |
| OLD-ERR | 6 | the old wire raised; the new answers (e.g. a class list concatenated: the old `struct_extract(JSON, ...)` binder error) |
| BOTH-ERR | 3 | both raise, with different messages (an object's identity read where not tracked; a structure of no Pure text) |

**Homework (S1.5):** classify each row as a correction (the old wire was wrong: most) or a spelling change, and put
every spelling change to the user case by case (the user, 2026-10-28: "any output spelling changes need to come to me
on case by case basis"), each with its expression, old output and new; the user's answer keeps it (a
`docs/SEMANTICS_REGISTER.md` row) or reverts it (in code). Among them: a Pair as `<1, a>`, NaN as a string, Floats
written in full without an exponent, the zone on hour- and minute-precision dates. Decided so far: lines 34 and 35
(`'-000.000'->parseFloat()`, `-0.0` written `0.0`) reverted on the user's word, 2026-10-28: the new writers write
`-0.0` again (register S117).

## 5. The census of core's tests on the old door (2026-10-28)

`test-census-2026-10-28.tsv`, made by `probes/test_census.py.txt` (a heuristic over each test method's text; counts
within a few percent). 140 files under `core/src/test` still call `Execution.execute*`, `QueryService`'s materialized
`execute` or read `ExecutionResult`; 23 files use the plan path directly.

| What the test asserts | Methods | Home in the plan |
|---|---|---|
| a value: rows or a scalar | 1,450 | the core twin (track T): judged on lite's LEAN output |
| ... of which a model-free expression (`|%2015-04-15T17->hour()`) | 410 | the twin of PCT |
| ... of which a query over a model or mapping | 608 | the twin of core_relational |
| ... of which the query is built by a helper (the heuristic cannot see it) | 432 | read per file |
| ... of which use a `let` | 20 | need the body runner (track B) |
| compile, structure, guards ("other") | 661 | stay Java unit tests; reach the compiler or the plan, not execution |
| a refusal (an error message) | 121 | the same |
| SQL text | 52 | assert on the plan's statement |
| column names or types only | 20 | assert on the plan's columns |

By directory: `integration` 1,858 methods (1,416 value), root 121, `exec` 93, `resolver` 92, `lowering` 68, `compiler`
44, `server` 15, `sql` 9, `normalizer` 4. 28 classes extend `integration/AbstractDatabaseTest`, whose helpers
(`queryService.execute`, `executeRelation`, `executePureQuery`) are the one place most value tests reach the old door;
it runs everything on the platform's DuckDB (`TestBackends.platform()`). Other helpers on the old door (2026-10-27 map):
`CheckerResults.collect`, `SqlParity.grid` and `SqlParity.Legend.run`, `CypherParity.Lane.run`, `PlanCases.todayTdsText`,
`testing/Rows.java`. Idioms: `.rows().get(N).get(` 898 sites in 30 files, `.rowCount()` 642 in 18, `.json()` 215 in 19.

## 6. Maps of the code made for phases 1 to 5 (2026-10-27; history where marked)

**The decided end state (2026-10-27, still the target).**
1. Every statement any path sends is in an `ExecutionPlan` node run by `PlanRunner` on a session: value roots as typed
   results; effects (DDL natives, CSV loads, setup seeds, raw statements) as plan steps.
2. `ExecutionResult` deleted from main (the judges' sides typed: collected rows plus a shape) -- *ruled otherwise 2026-10-29 (D36, D48): it stays as the judges' collected side, renamed `exec/TypedSide`; its old-door role is gone*; one production entry:
   plan, `PlanRunner.stream`, a writer. `Execution.execute*`/`executeResolved` deleted.
3. `StatementExecutor` survives only as the BODY DRIVER (lets, asserts and their judges, frames, effects) emitting
   plans, renamed if it stays, with no JDBC of its own.
4. The text outputs deleted (done in phases 3 and 4).
5. The `LITERAL` slot typed (mixed lists as tagged values); `LiteralText.parse` deleted (track L, L2, 2026-10-29: the probe reads zero); `conformValueEgress` not deleted (D51: the user's call).

**`StatementExecutor`'s kinds** (3,410 lines then, 3,270 now): `execute` -> `executeStatements` -> `BodyCompiler.execute`
(segment walk); a value root `executeTyped` -> `prelude` -> `lowerAndPrepare` -> the plan. What `PlanMaker` covers:
relation, scalar, collection, graph roots, the dynamic pivot (`Keyed`), write programs. What it does not (the body
runner's work): effects (`executeInDb`, `dropAndCreateTable/Schema`, `createTempTable`, CSV loads via `CsvLoad`, setup
seeds via `SeedSqlForms`, `print`), an effectful `map`, the DDL string generators, `planToString`/`toSQLString`, `$plan`
navigation, call frames (`buildFrame`, `spliceHook`, `aliasFrame`), test-data-generation statements, inline `from()`
seeds and frame options (`establishContexts`), assertions (`AssertVerdicts.tryAdjudicate`), `assertError`
(`AssertErrorNative`). Measured buckets: the root tail 393 lines (replaced by plans), sequencing 1,724, compile-time text
producers 953, judge-side planning 310, the test-data generator's view renderer 30.

**The judges' sides** (`JudgePlans`): host sides `sideResult` (GRAPH a typed OBJECTS plan; TABULAR `tabular`: late-bound
grids staticized, `ScanOrder.stabilize`, a rows plan; SCALAR/COLLECTION `valueLane`); the database judge `planValue` ->
`planStaged` + `wrapSide`, the verdict statement via `VerdictSql`, run by `JudgePlans.runner`. Constants answered from
`StatementExecutor.prelude`.

**History (deleted since, kept for the reasons):** the old `Executor`'s extras over `PlanRunner` that had to move
before it went: the execution-trace stamp (now `JdbcStatements.stamped`), SQL NULL against JSON null on Any
collections, a dynamic pivot's columns from JDBC metadata, a many-valued cell in a scalar slot refused, the census
hooks. The served routes before phase 3 (`QueryService.executeUpstream` -> `Execution.executeUpstream` -> `wireOn`).

## 7. What legend-engine answers (read from source, then asked)

**Read from its source (4.145.0):** `ValueTransformer` writes a `java.sql.Timestamp` in full
(`yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnZ`) and a `java.sql.Date` as `yyyy-MM-dd`; the TDS transformers are enum-only;
`pureTypeToDataTypeMap` maps Variant to SemiStructured and `alloyTypeToString` SemiStructured to `'SEMISTRUCTURED'`;
its PCT adapter (`pct_relational.pure`) reads a relation by `executePlanAsJSON`, `parseJSON`, `fromJSON` into
`RelationalTDSResult` (`nullReplacementInArray=TDSNull`; json-simple types an integral number Long, any other Double),
`getTDSResultFromProtocol`, `stringToTDS`.

**Asked (2026-10-28, PARK-34's probe; the answers recorded in `core/src/test/resources/upstream-api/e8-variant-*.json`
and `variant-probe.pure`):**
- a Variant column's builder: `"type":"meta::pure::metamodel::variant::Variant","relationalType":"SEMISTRUCTURED"`, a
  store column's and a computed `toVariant()`'s alike; the same plan on H2;
- a Variant cell: a STRING holding its JSON, re-written by the engine's JSON library (`2.50` answers `2.5`, spacing
  dropped); a JSON null the string `"null"`, SQL's NULL `null`; `toVariant(x)` on DuckDB is
  `ifnull(to_json(x), 'null'::json)`;
- on H2 the engine refuses executing a JSON column ("Unable to process variant result as JSON": H2's driver hands bytes);
- a midnight TIMESTAMP is written in full (`2024-01-02T00:00:00.000000000+0000`);
- computed columns: Boolean `BIT`, StrictDate `DATE`, DateTime and Date `TIMESTAMP`, Number `FLOAT`; store columns:
  INTEGER `Int`/`INTEGER`, BIGINT `BigInt`/`INTEGER`, FLOAT `Float4`/`FLOAT`, DECIMAL `Numeric`/`FLOAT`, VARCHAR
  `Varchar`/`VARCHAR(1024)`, and a `BIT` store column typed `TinyInt`/`INTEGER` while its cell is `true` (lite types it
  Boolean: open finding);
- refused before executing: a StrictTime column (`%10:30:00`, a match failure in `pureToSQLQuery`), a list column
  (`[1,2]`: "Cannot cast a collection of size 2 to multiplicity [1]"), a `VARBINARY` store column (the relation
  accessor cannot type it: "Match failure: VarbinaryObject instanceOf Varbinary").

**What legend-pure says about a Float's text (read from its source, 5.99.0, 2026-10-28;** the source is Bazel's
external repository `$(bazel info output_base)/external/+http_archive+legend_pure_src`, spec only**):**
- compiled mode, `CompiledSupport.primitiveToString(double)`: `(value == 0.0d) ? "0.0" : DECIMAL_FORMAT.format(value)`,
  where `DECIMAL_FORMAT` is `new DecimalFormat("0.0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))` with 340
  fraction digits at most: shortest digits, positional, a point always, and zero `0.0` for either sign (a
  `DecimalFormat` alone would write `-0.0`);
- interpreted mode holds a Float as a `BigDecimal` (`PrimitiveUtilities.getFloatValue`, `FloatCoreInstance`), which has
  no negative zero at all;
- `parseFloat.pure`'s `testParseZero` asserts `assertEq(0.0, $f->eval(|parseFloat('-000.000')))`: equality, so the
  PCT does not see a zero's sign; its documentation's example writes the result `0.0`.
- Measured with `probes/FloatTextProbe.java.txt` (2026-10-28): its formatter gives `0.0001` for `1.0E-4`, `0.00001`
  for `1.0E-5`, `10000000.0` for `1.0E7`, `1500000000000000.0` for `1.5E15`, `0.00000025` for `2.5E-7`.
Lite follows it for Pure's text (`FloatCanonText.ofText` in SQL) and departs for its own outputs' negative zero
(register S117, the user's ruling).

**What legend-pure and legend-engine say about a date's text (read from the pinned sources, 2026-10-29, for the
upstream-fidelity tenet):**
- legend-pure reads a SQL timestamp with all nine subsecond digits, always a `DateWithSubsecond`
  (`DateFunctions.fromSQLTimestamp` -> `DateWithSubsecond.fromInstant(timestamp.toInstant(), 9)`, m4
  `coreinstance/primitive/date/`), and its `toString` prints them all: its own `TestDateFunctions.testFromSQLTimestamp`
  asserts `2014-03-10T16:12:35.070004235+0000`, and `assertConvertsInstant` a whole second as
  `...T00:00:00.000000000+0000` (a `java.util.Date` three digits, `.000+0000`). A SQL DATE is a StrictDate, a day. A
  DateTime never prints as a day.
- legend-engine's JSON (`DEFAULT`) writes a `java.sql.Timestamp` as `yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnnZ` and a
  `java.sql.Date` as `yyyy-MM-dd` (`ValueTransformer`, read 2026-10-28 and again today); its CSV
  (`RelationalResultToCSVSerializer`, `SerializationFormat.CSV`) is commons-csv `CSVFormat.DEFAULT` (comma, CRLF, quotes
  only where needed) over `printRecords(resultSet)`, the driver's own text of each cell, with a header only when asked
  (`withHeader`); `CSV_TRANSFORMED` (`RelationalResultToCSVSerializerWithTransformersApplied`) is the same with each
  cell through the column's transformer and `ValueTransformer`'s two date spellings. The other formats:
  `PURE_TDSOBJECT`, `PURE`, `RAW`, `ARROW_IPC`.
- The PCT relation suite (legend-engine's `core_functions_relation`, the RelationFunctions tests) asserts TDS text
  with THREE fraction digits in about 950 places (the four largest files 942: `over.pure` and `reduce.pure` 416 each,
  `asofjoin.pure` 82, `composition.pure` 28; the count corrected by the S1.4 audit, 2026-10-29) and nine in 848 (their input literals): the harness's own spelling, not a product's --
  `TestTDS.java:209` (`legend-engine-pure-runtime-java-extension-shared-functions-relation`) holds a Date column as
  millisecond `java.util.Date`s (`DateFunctions.fromDate(new Date(value / 1000000))`), so `toString` prints `.SSS`
  whatever digits the adapter sends; `assertTdsEquivalent` compares values. Lite's deleted `PureTdsText` had copied the
  three-digit form. The engine's own `core_relational` tests expect nine digits for database-read timestamps (138
  places), three or four only in TDS literals and a milestoning constant, and a fraction-less `+0000` only in
  Pure-printed property keys (`synonymsMilestoned(2020-10-15T00:00:00+0000)`). Checked 2026-10-29 on the user's
  question ("are you sure it's always 9 digits expected?"): for a product's relation cell, yes; for a value Pure
  printed, no -- the precision written.
- Lite's LEAN writers (`CellText.pure`) diverged from both on a TIMESTAMP cell until 2026-10-29: the fraction's
  trailing zeros trimmed and an abstract Date at midnight written as its day. Now a relation's cell keeps its nine
  digits (INSTANT) and a value root's the precision its digits show (MINIMAL, register S118); LEGACY
  (`CellText.engine`) matches `ValueTransformer` exactly. The decisions log D4 to D6.
**Read for S1.5's classification (2026-10-29; the whole table is `WRITER_DIFFERENCES_BY_UPSTREAM_2026_10_29.md`):**
- a date's zone: legend-pure's `DateFormat.append` writes `+0000` inside its `hasMinute` branch, so an hour-only date
  prints `2014-03-10T16` (`TestDateFormat:791`) and a minute `...T16:12+0000`; legend-engine's `PureDateSerializer`
  writes `toString()`. Lite's LEAN wrote the zone from an hour on until S1.5 (`PureDateLiteral.DateWithHour.toString`
  too); lite's SQL-side text of a minute-precision date carried as text writes none (`dateTimeTextOfText`; track T).
- NaN: legend-pure has none (`acos(2.0)` fails by name); legend-engine's relational JSON writes a NaN or infinite
  Float cell as `null` (`RelationalResultToJsonDefaultSerializer.processRow` -> the column's transformer, the identity
  for a Float (`SetImplTransformers.buildTransformer`) -> `ExecutionResultObjectMapperFactory.convertPurePrimitiveToJson`,
  `null` for `isNaN`/`isInfinite`); phase 2's `"NaN"` string in LEGACY named the engine without reading it. Register S119.
- an instance's text: `CompiledSupport.pureToString(Object)` and `JavaCompiledCoreInstance.toString(es)` print
  `ModelRepository.possiblyReplaceAnonymousId(getName())`: `Anonymous_StripedId` for every anonymous instance; a
  packageable element prints its name (`AbstractCompiledLazyConcreteElement.toString(es)`: `getName()`). Lite prints
  `@_` and the instance's id, and an element's path (the lowering's `Scalars.pureToString`): the user's (D16, D17).
- a Pair: `Pair.toString()` in `anonymousCollections.pure` is `'<' first ', ' second '>'`; the engine's JSON of an
  Eclipse `Pair` is `{"first":..,"second":..}` (`EclipsePairSerializer`). Lite's CSV and JSON match each.
- the engine's CSV has no spelling of its own: `CSV` is commons-csv over the driver's text, `CSV_TRANSFORMED` the
  value's `toString` with `ValueTransformer`'s two date forms; a Double there is Java's `E` form, legend-pure's print
  the positional layout: the twin question D10 stays the user's.

**How to ask it again (the recipe).** No server is needed: the engine runner's jars are the engine.
1. `bazel build //tools/engine-runner:testable` (fetches `@maven_runner`, legend-engine 4.145.0 and its execution
   stack, about 600 MB of jars; clear `$(bazel info output_base)/sandbox/*` first if the disk allowance is tight).
2. The runner's classpath is too long for a command line: symlink every entry into one directory and use `dir/*`.
   `grep -o 'CLASSPATH="${RUNPATH}[^"]*"' bazel-bin/tools/engine-runner/testable` gives the entries; replace
   `${RUNPATH}` with `$PWD/bazel-bin/tools/engine-runner/testable.runfiles/_main/`.
3. Compile a scratch main against it with the system JDK: `javac -cp "cp/*" -d classes VariantProbe.java`
   (`probes/VariantProbe.java.txt`: parse with `PureGrammarParser.parseModel`, compile with `Compiler.compile(pmcd,
   DeploymentMode.TEST, null)`, build the lambda with `HelperValueSpecificationBuilder.buildLambda`, plan with
   `PlanGenerator.generateExecutionPlan(lambda, null, null, null, pureModel, "vX_X_X", PlanPlatform.JAVA, null,
   extensions, LegendPlanTransformers.transformers)` where `extensions` is
   `PureCoreExtensionLoader.extensions().flatCollect(e -> e.extraPureCoreExtensions(pureModel.getExecutionSupport()))`,
   run with `PlanExecutor.newPlanExecutorBuilder().withAvailableStoreExecutors().build().execute(plan)`, write with
   `((StreamingResult) result).getSerializer(SerializationFormat.DEFAULT).stream(out)`).
4. A model with a DuckDB connection `specification: DuckDB { path: '<file>'; };` over a file seeded by
   `probes/Seed.java.txt` (the duckdb_jdbc jar from the same classpath), or H2 `LocalH2 { testDataSetupSqls: [...] }`.
5. Run: `java --add-opens=java.base/java.nio=ALL-UNNAMED -Xmx3g -cp "classes:cp/*" probe.VariantProbe model.pure '<lambda>' ...`.
   Record the answer under `core/src/test/resources/upstream-api/` with the model and the provenance, and quote it.

## 8. The probes and scripts (sources in `probes/`, never compiled)

| File | Measured | When | Result recorded in |
|---|---|---|---|
| `VariantProbe.java.txt`, `Seed.java.txt` | legend-engine's answer for Variant and other column types | 2026-10-28 | §7; GATES 2026-10-28 PARK-34; `upstream-api/` |
| `WireDifferential.java.txt` | old wires against the Java writers, PCT DuckDB | 2026-10-27 | §4; `wire-differences-2026-10-27.tsv` |
| `BcTimestampProbe.java.txt` | a lane driver's getters on a far-BC date-time (Snowflake, Databricks) | 2026-10-27 | GATES 2026-10-27 phase 5 entry (the Snowflake pin) |
| `SnowflakeFloatTextProbe.java.txt`, `SnowflakeTextProbe.java.txt`, `SnowflakeDecimalProbe.java.txt` | Snowflake's Float text, format models, NUMBER casts | 2026-10-21 to 24 | `docs/RESULTS_AND_RENDERERS_2026_10_22.md` §10.5 |
| `BigQueryFloatTextProbe.java.txt`, `BigQueryRaiseProbe.java.txt` | BigQuery's Float casts and `FORMAT('%.17g')`; its raise | 2026-10-21 to 24 | the same §10.5; GATES |
| `FloatTextProbe.java.txt` | `CellText.pureFloat`'s text before and after the negative zero, the `0.00010` fix, against legend-pure's own formatter | 2026-10-28 | §7, §9; the start page's S1.4 item 2; register S117 |
| `test_census.py.txt` | §5's census | 2026-10-28 | `test-census-2026-10-28.tsv` |
| `oldpath_census.py.txt` | every old-path symbol's sites per step (the deletion census) | 2026-10-27 | rerun before each deletion |
| `compare_lanes.py.txt` | a lane's per-test comparison of two commits, channels A and B, with the route markers | 2026-10-27/28 | every lane report |
| `compare_times.py.txt` | per-test wall time between two commits' test.xml | 2026-10-21 | the timing notes in GATES |

A lane probe is a JUnit class in `pct/src/test/java/org/finos/legend/lite/pct/extension/`, run with the lane's PCT
target and `--test_arg=--select-class=...`, opening its session through `PctBackend.connect()` or
`com.legend.testbackend.TestBackends.named("<lane>").open()` (never a credential itself), printing prefixed lines, and
deleted after.

## 9. Open findings, with their evidence

| Finding | Evidence | Where it goes |
|---|---|---|
| the doors (the user, 2026-10-29: "close all the doors here as part of this PR so we know that it can be done"): the ways a statement reached a session around the plan runner -- the probes and the pivot's key type on `Statements.columns`/`describe`, the seeds, a program's session statements and the system store's rows on `Statements.execute`/`load` (`SetupRunner`), the generator's own JDBC, the last side materialized (`ProgramReport.last`) | the map in the decisions log ("The doors"); the start page §4, "The doors" | doors 3, 4, 5 built 2026-10-29 (D61: `PlanRunner.columns`/`describe`/`setUp`, the guard `theRunnerIsTheOneCallerOfTheSessionsStatements`); door 2 built 2026-10-29 (D62: the generator's effects, described typed rows and reported columns through the runner; no JDBC in `testdatagen`); door 1 built 2026-10-29 (D63: the last statement streamed to the caller's sink, `ProgramReport` deleted, the service test runner and the twin on the boundary's writers). Every door in the map closed; the audit of the doors named three raw sends inside the chartered seams and twelve findings, folded 2026-10-29 (D64: the records corrected, the effect-last answer ordered, the dead arms deleted); the two readings ruled upstream-exact and kept, the three routings built 2026-10-29 (D65: the plan path's seeds, the server's fill and the MariaDB raise recovery through the seam; in core's main sources the seam and its bulk loader the only JDBC senders; the audit folded); PR #80 merged 2026-10-30; the java.sql revamp's step 1 built 2026-10-30 (D66: a target's session statements once per connection, a rollback clearing the memo); step 2a built 2026-10-30 (D67: the execution front door on sessions, `QueryService` and `OutputFormat` deleted, the free tier; the JDBC census shrunk by nine files); step 2b built 2026-10-30 (D68: the test runners, the observer and the corpus harness on sessions; three more files off the census); step 3 built 2026-10-30 (D69: the census a table of classes and their acts, the funnel on that one list, no JDBC type in a public signature outside `exec`); the seam's step A built 2026-10-30 (D70: one way in, the connection the seam's alone, the state the owning session's, the server's lease a session; four more files off the census) |
| `toVariant()` of an empty value is SQL's NULL on DuckDB (`to_json(x)` alone), an empty cell under `Variant[1]`; the engine's is JSON's null | register S116 (pending); `datacube/test/python-engine/execute.test.ts`'s Variant case reads only rows with a quantity | FIXED 2026-10-29 (track F; S116 'outcome'): the lowering of `toVariant` coalesces to JSON's null, upstream-exact |
| the engine-parity H2 dialect (`H2`, servers 2.1 and 2.2) spells `fromJson(text)` `CAST(x AS JSON)`, which H2 answers as a JSON STRING: a silent wrong answer | found 2026-10-28 by `PureV1ApiTest`'s first draft; `H2Modern` parses (`JSON '...'`, `x FORMAT JSON`) | FIXED 2026-10-29 (track F): `H2.jsonParseCast` -- `JSON '...'` for a literal, `(x FORMAT JSON)` for text, `CAST(NULL AS JSON)` for a typed null (`(NULL FORMAT JSON)` is JSON's null, measured); both spellings measured on 2.1.214 and 2.2.224; `H2Modern` inherits it |
| a `BIT` store column: the engine types it `TinyInt`/`INTEGER`, lite Boolean/`BIT` | §7 | a ruling: register row or a typing change |
| Snowflake's far-BC `TIMESTAMP_NTZ`: the driver hands another instant | `BcTimestampProbe`; the pinned `testAdjustByMinutesBigNumber` | Snowflake's dialect returns such dates as the database's own text; a lane measurement |
| `RootNumericTypes` guesses an "unbounded Decimal" from `DECIMAL(38,18)`, so a declared `Decimal(38,18)` is skipped too | the phase 3 audit (2026-10-27), item 4; `sql/dialect/RootNumericTypes.java` unchanged since | CLOSED 2026-10-29 by design (D58 (8)): `DECIMAL(38,18)` IS the platform's unbounded Decimal carrier (`Type.PrecisionDecimal.DEFAULT_DECIMAL`, its javadoc of 2026-10-27); a declared `Decimal(38,18)` column holds its scale already, the skipped cast changes no cell |
| the tab's Arrow nested values (a BigNum's `toJSON` quoted unscaled; dates as epoch milliseconds) may be mis-decoded by `HostRows` | the phase 3 audit, item 2 (the Variant and tagged-slot halves fixed in `b3311cd6`) | verify with a DataCube case |
| lite's JSON writes an absent to-many property as `null` (Pure's is `[]`) | the phase 4 notes | NOT REPRODUCED 2026-10-29 (track F, D58 (7)): the LEAN route writes `[]` (`GraphFetchCheckedIntegrationTest.anEmptyToManyPropertySerializesAsAnEmptyList`); the evidence was not found; the probe stays as the pin |
| `CellText.pureFloat` writes a double below `1e-3` whose shortest digits are one digit with a trailing zero (`1.0E-4` as `0.00010`); legend-pure and the Float rule's shortest digits give `0.0001` | found 2026-10-28 with S117 (S114 then); `probes/FloatTextProbe.java.txt` (the function against legend-pure's formatter, and the fix) | ruled 2026-10-28, a defect to fix: S1.4 item 2; **built 2026-10-28** (GATES, the S1.4 entry, item 1: `0.0001`) |
| nits from the phase 3 audit: `OutputFormat.id`/`contentType` dead; `FloatPrintText.ofText`'s function parameter vestigial; `CsvWriter`'s "no CSV wire" text; `PureV1Api`'s builder-column three-way choice; the tab prints `-0` as `0`; a REAL widened on hosts but narrowed by `JdbcDecode` | the phase 3 audit | swept 2026-10-28 (GATES, the S1.4 entry, item 4): `ofText` deleted; `OutputFormat`, `CsvWriter`'s text and the REAL were already fixed at `b3311cd6`; the builder choice stays (it needs the compiler's `Type`, which the plan names as text); the tab's `-0` (`datacube/src/format.ts`, `signDisplay: 'negative'`) is a display spelling change, put to the user |
| PCT channel A JVMs peak at 4,064 to 4,096 MB of their 4,096 MB heap before a collection (live peak after one 2.1 to 2.3 GB) | the Databricks lane, 2026-10-28, at both commits | watch; raise or split before it fails |
| the `asOfJoin` emulation on a database without `ASOF JOIN` (SQLite, H2) answers matches against an EMPTY right side where DuckDB's `ASOF JOIN` (an inner match) answers none: a wrong answer, not a refusal | the lanes' floors (`lanes/sqlite.txt`, `lanes/h2.txt`: `AsOfJoinCheckerTest$MatchOnly#testAsOfJoinEmptyRight`, expected 0 rows, got 2), measured 2026-10-29 at T-d; the T-d audit's finding 4 | CORRECTED 2026-10-29 (track F): the emulation answers the SPEC (a LEFT join: upstream's `JoinKind.LEFT`, the PCT keeps an unmatched left row); DuckDB's 0 rows is DuckDB's own defect against a derived table its planner proves empty (measured on 1.3.0 and 1.4.4, `probes/asof_empty_right.txt`: an empty TABLE keeps the rows) -- register S126, the user's ruling; the test's comment and the floors' reasons say so. RESOLVED 2026-10-29 by the driver (D60): core's DuckDB 1.4.4.0 -> 1.5.6.0 on the user's ask (1.5.5 and later keep the left rows, `probes/duckdb_1_5_6.txt`); the test asserts the spec on every lane, the floors lose their line |
| a `concatenate` of relations whose column counts differ reaches SQL: the DATABASE refuses it (DuckDB's and H2's own set-operation error texts), where the compiler should (AGENTS.md invariant 4: the typer owns every type) | the T-d audit's finding 3: `ConcatenateFlattenCheckerTest$ConcatenateErrors#testConcatMismatchLeftExtra` and `RightExtra` assert a message containing `mismatch` or `column` and pass on every lane by the DATABASE's text (DuckDB's set-operation error, H2's `Column count does not match`; on the T-d H2 floor by its case alone) | CLOSED 2026-10-29 (track F, upstream-exact as it stands): legend-pure's `_RelationType.isCompatibleWith` admits a relation whose columns are a subset of the other's, so `concatenate<T>(Relation<T>, Relation<T>)` COMPILES a width mismatch there too and the database refuses the union; the legacy TDS `concatenate` is schema-erased (`ConcatenateChecker.positional`'s own note). Invariant 4 yields to the upstream-fidelity tenet here |
| a mixed list across comparison groups (numbers beside strings) sorts by a RANK STRUCT ({@code {g, n, d, s}}), which SQLite and MariaDB, having no struct, order as its JSON TEXT (a number's digits as text: `10` before `9`); the spelling carrier's `rankedElems` ordered the same key, so the PCT cases (single digits) pass by luck | track L, 2026-10-29 (`MixedEncoding.taggedList`'s comparable; a one-group list orders by its scalar channel, the fix for `[342, 5.0, -2.0, 171, 1]->sort()` on SQLite) | SIZED 2026-10-29, not built (D58 (9), the user's call): an ordered list aggregate over SEVERAL keys (`SqlExpr.OrderedListAgg` takes one; ten dialect arms, `MirJson`: plan format 20 and the readers' pins), so the rank is `g, n, d, s` on every database; measured on the MariaDB lane CI alone runs |
| the typer refuses a collection literal with an element of multiplicity [0..1] ("Collection element must have a multiplicity [1], found [0..1]": `[[7]->first(), 'a']`), where Pure's grammar admits it (the collection's multiplicity sums); `MixedEncoding.compacts` and the tagged list's present positions wait on it | the L1 audit's finding 10, measured 2026-10-29 | CLOSED 2026-10-29 (track F, upstream-exact): legend-pure's `InstanceValueValidator.validateNoNestedCollections` refuses it too (`Required multiplicity: 1, found: [0..1]`), as legend-engine's `ValueSpecificationBuilder` does with lite's own message; the finding's premise ('Pure's grammar admits it') was wrong |
| Pure equates two NUMBERS of different kinds by their texts (compiled legend-pure's `CompiledSupport.eq(Number, Number)`: `equals`, then `toString`), so `3 == 3D`, `[3, 'a']->contains(3D)` and `1.5 == 1.5D` are true there and `1 == 1.0`, `1.5 == 1.50D` false; lite's tagged list compares within a kind and REFUSES by name the pairs that can cross (an Integer against a Decimal of scale zero, a Float against a Decimal: `TaggedLists.refuseCrossingNumbers`), where a static false stood until the L2 audit; the pairs whose texts never agree keep their false | the L2 audit's finding 4 (2026-10-29); `exec/TaggedListTest.numbersOfDifferentKindsAreRefusedWherePureEquatesTheirTexts` | LEDGERED 2026-10-29 (track Z, the audit's finding 3): register S127, pending the user's ruling |
| the Batch holds a tagged cell as its slot's own carrier, the TAG read with the row and dropped (`RowDecode.cells`): a writer that must name the kind (track A's Arrow writer, a union member per slot) can only infer it from the carrier's class, and two slots of one carrier (an enumeration beside a String; two `DateTime(n)`) leave it unknown | track A's design (2026-10-29) | FIXED 2026-10-29 (track F, D58 (3)): `TaggedCell`, `Batch.tag` |
| `exec/TaggedListTest.aRelationsCellsAreATaggedListOverItsRows` (L1, renamed `anUntypedGridsCellsAreReadOnTheVariantLane` at the L2 audit) asserted the tagged flatten over an `executeInDb` grid, whose columns are UNTYPED and take the variant lane: the flatten it named was never reached, and its sorted-star-frame crash (the L2 audit's finding 1) went unseen until `aTypedRelationsCellsKeepTheRelationsOrder` | the L2 audit (2026-10-29), found by dumping the test's SQL | fixed with the gate that reaches the flatten (the L2 audit's fold) |
| a mixed list's sort breaks ties arbitrarily where upstream's `toSortedList` is stable (`[1, 1.0]->sort()` keeps 1 first there): `SqlExpr.OrderedListAgg` orders by ONE key, and a struct key would order as text on SQLite and MariaDB | the L1 audit, finding 11 (2026-10-29) | SIZED 2026-10-29, not built (D58 (9)): the same multi-key aggregate, the position as its last key |
| the tagged list's gate (`exec/TaggedListTest`) runs on the platform's DuckDB alone; the SQLite and H2 lanes select `com.legend.integration`, so no lane measures the list's operations there beyond PCT's essential suite (SQLite) and the corpus's flatten (H2) | the L1 audit, finding 10 (2026-10-29) | NOT BUILT 2026-10-29, posted (D58 (10)): an integration twin on the two lanes is an hour's rewrite onto `AbstractDatabaseTest`; PCT's essential suite (SQLite) and the corpus's flatten (H2) cover the operations there meanwhile |
| a tagged slot's KIND is a string (`DateTime(3)`, an enumeration's path), read by six affix tests across `MixedEncoding` and `TaggedLists`; the identity guard moved once for it (D50d) | the L1 audit, finding 12 (2026-10-29) | DEFERRED 2026-10-29, posted (D58 (11)): a typed `Kind` value in the slot (the plan format spells it; the readers dispatch on the value) |
| an Any list holding an INSTANCE keeps the VARIANT lane (as instances always did): at a root its elements are JSON. (An enumeration's value was on it at L1's first cut; the corpus's H2 lane caught it, and it is a kind of the tagged list: D50d.) | track L, 2026-10-29 (`MixedEncoding.taggedList` returns null for an instance) | an instance kind is undesigned: a class's value in a mixed list stays JSON |
| H2's planner is exponential in the depth of nested derived tables: a chain of twenty calls of a window-extending generic function (twenty nested selects) exhausts a 512 MB heap in `Select.getPlanSQL` (`QueryExpressionIndex.getQuery` re-plans every level per candidate plan), and JUnit rethrows the `OutOfMemoryError` as unrecoverable, ending the run; DuckDB and SQLite plan it | `lanes/h2.txt`'s not-run pin (`UserFunctionIntegrationTest$RelationGeneric#aChainOfCallsIsTypedOnceEach`), measured 2026-10-29 at T-d2 | a lowering question (a chain of extends over the same window could flatten into one select) or H2's limit recorded; the pin moves with it |
| the two LEAN writers disagree on a DAY under a TIMESTAMP column (a session hands one by design, `JdbcDecode.temporal`, SQLite's text day among them): the JSON route spells its day (`CellText.pureDate`, legend-engine's `java.sql.Date` rule), the Arrow route refuses by name (a timestamp holds no day without a midnight invented) | the track A audit (D57 (4)), 2026-10-29; pinned by `ArrowWriterTest.aValueTheColumnCannotHoldIsRefusedByName` | the user's word: refuse in both, or carry in both (utf8 of Pure's text where the precision fact allows a day) |
| the JSON route's INSTANT spelling drops a TENTH subsecond digit silently (`PureDateLiteral.java:105`, `(subsecond + '000000000').substring(0, 9)`; `validateSubsecond` admits any count), where legend-pure keeps every written digit; the Arrow writer refuses it by name (`ArrowWriter.nanosDigits`) | the track A audit (D57 (10)), 2026-10-29: `.1234567891` -> JSON `...123456789+0000`, Arrow refused | a spelling: the user's word (refuse in JSON too, or keep the digits as text) |
| the Arrow route refuses a tagged column whose slots share a carrier -- every mixed-precision DateTime list (`TAGGED<DateTime(3) TIMESTAMP, DateTime TIMESTAMP>`, D53 (2)), an enumeration beside a String (two VARCHAR slots, D50d) -- while the JSON route writes them: the batch drops the tag | the track A audit (D57 (5)), 2026-10-29 | FIXED 2026-10-29 (track F, D58 (3)): `TaggedCell` in the batch; `Batch.tag`; the union reads it; `ArrowWriterTest.twoSlotsOfOneCarrierReadByTheirTags` |
| the MongoDB lane of core's integration tests (track T's item 3 names it) is not built: the integration base opens a JDBC session for its seeds (D47; track D kept it) and the MongoDB backend has none; `_CORE_LANES` is sqlite and h2 | the Z audit (finding 4b), 2026-10-29; `core/BUILD.bazel`'s lane note | the seeds through the session seam (`Statements.load`), then the lane with its floor (the user's call: an hour or two) |
| track T's queued items left pending S121's ruling: D18's four hour-on-zone sites (`exec/CanonicalForm`, `lowering/Scalars` the DATE_TIME literal, `LiteralSpelling.dateTimeTextOfText`, `resolver/GraphEmission`), D17's element-name print, D7's host-judge trim by the slot's text (`JudgePlans.valueLane`) | the Z audit (finding 4a), 2026-10-29 | S121 rules them; the trim becomes a read of the precision fact when the judges' canon is re-measured |
| the Arrow gate compares one batch of every kind against its JSON rows, not the twin's tests (track T's item 5 wanted Arrow in the LEAN judge with a gate that the two agree) | the Z audit (finding 4c), 2026-10-29 | the twin reads a second sink (Arrow) beside JSON for one lane, or the item is struck: the user's call |
| D44's two findings never reached this table: the plan path through `Execution.run` on a given session routes no system metamodel store read (`MetamodelStoreTest` goes through `testing/Program`); `PureV1Api.tdsColumns` reads a TDS's columns positionally | the Z audit (finding 5), 2026-10-29; D44 | a session seam for the system store; the columns by name from the plan |
| D27's engine-runner probe (a NaN cell, `ACOS(2.0)`, 'the next time the runner is built') was never run; D41's note (`Compiler.programFacts` types a body on a second compiler, `PureTestRunner.java:248, 427`) was neither done nor re-queued | the Z audit (finding 5), 2026-10-29 | run the probe when the runner is next built (S119 waits on it); one compiler per body |
