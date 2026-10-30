# The result boundary program: a typed plan, writers at the edge, the text path deleted

**Status: RULED, revision 3, 2026-10-26; landing the branch on `main`, then step 0.** The user ruled on
2026-10-26: the design (D1 to D5), the formatting-at-the-edge clause with its guards, lean JSON's numbers,
collecting into memory as a test helper only, and the order are accepted as written in revision 2 ("everything else
is okay"); **the database judge is never deleted** ("a crown jewel"); the Float rule and the Arrow question were
sent back for the expert version, which revision 3 gives (§8, items 3 and 4), and both were then accepted the same
day ("arrow look good"; "sounds great"). Revision 1 was written the same day at the
user's ask ("this is now becoming a huge blocker to our streaming/deephaven/real-time ambitions; how can we move this
forward sanely AND delete all our historical cruft"); revision 2 folded in the user's three questions that followed.
It is the execution plan for `docs/RESULTS_AND_RENDERERS_2026_10_22.md` (D1 to D6) and for the plan path program's
steps 7 to 11 (`docs/PLAN_PATH_PROGRAM_2026_10_17.md` §7), made one program. Nothing is built; the handoff for a
session with no context is `docs/RESULT_BOUNDARY_START_HERE.md`.

## 0. The plan in plain words

**The goal.** A query produces a table of typed values. The database computes every value. Our SQL never formats
anything for output. Whoever consumes the result formats it at the edge, their way: Studio and the other legend
clients get legend-engine's exact JSON, written in Java; the PCT and corpus harnesses get Pure values, no text; lite's
own clients get lossless typed data (Arrow, JSON, CSV); the browser formats its own grid. Every old path that did it
differently is deleted, not kept beside the new one. The product keeps zero third-party dependencies in the compiler,
the WebAssembly planner and the native image: no Arrow library enters the product (§2).

**What we have today.**

- The compiler makes a plan: the SQL plus, for each result column, its Pure type and multiplicity, the SQL type the
  database answers with, and the store column's declared type. This is right and stays.
- Two executors. The old one runs SQL and builds an in-memory result with its own decoding. The plan runner runs the
  plan and reads each cell by the plan's column types. Two decoders; one must go.
- On most routes the SQL itself is made to write the final text: Pure's TDS text for PCT, legend-engine's JSON rows
  for the served API, CSV. That is what turns one addition into a 48 KB statement on Snowflake for PCT and 6 MB for
  the served API, makes every new database re-derive Pure's spellings before one test passes, and is the wrong shape
  for anything that streams or ticks. This goes.
- Values SQL has no type for. A mixed `max` now comes back as a tag plus one typed column per kind (landed
  2026-10-23). Mixed lists still come back as text and must be typed the same way.
- A Float turned into a String inside a query (`toString`, `format`): the database writes it, as the shortest decimal
  that reads back as the same double. On Snowflake and BigQuery that costs a 5 KB expression because their own
  formatters are lossy (measured 2026-10-21 and 2026-10-24). This stays, because it is part of the query's meaning
  and nothing cheaper is lossless there; §8 item 3 says how to make it cheap where it can be.
- The warehouse server already writes Arrow IPC streams with no Arrow library: its own framer
  (`warehouse/.../duck/ArrowStreams.java`, `FlatBuilder`) over the buffers DuckDB builds, inside the native image,
  checked against pyarrow in its tests. The lean Arrow writer reuses that framing over our own batches; the batch
  itself stays our own representation, typed arrays behind one accessor (§2: why not Arrow's layout inside core).

**What we will do, in order.** Measure first. Make the plan answer typed rows and nothing else. Define the result
as a stream of batches of typed rows, our own typed arrays behind one accessor. Write legend-engine's JSON in Java and prove it byte
for byte, then delete the SQL that wrote it. Feed PCT and the corpus from typed rows, lane by lane, then delete the
SQL that wrote TDS text. Add the lossless writers, the Arrow one over the warehouse's framer. Type the mixed lists and
delete the text carrier. Delete the old executor and make "collect into memory" a test helper rather than a
production method. The database judge and the host judge both stay, reading typed rows.

**Is it correct?** Yes. The decision that matters, typed results with formatting at the edge, is what every engine
and every driver does, and the measurements of 2026-10-18 and 2026-10-22 show the alternative's cost on our own
lanes. The risks are known and each has a gate that finds it (§6).

**Is it tractable?** Yes. Seven to nine weeks of one session's work, the cloud lanes' turnaround included. Every
step lands on its own with its own gate and its own deletion; no step needs the ones after it; the Deephaven line
needs only steps 1 and 6. The risk is concentrated in step 4 (the lanes) and step 7 (test expectations that move
when the old decoder's quirks are corrected), and both are made visible by running old and new side by side before
switching.

## 1. The rules, each with the test that keeps it

AGENTS.md: guard files are the rules' mechanical form. A rule without a guard is a wish.

- **R1. The database computes every value; our code derives none.** Unchanged (the execution tenet; the charter's
  Clauses 1 to 6). Guard: `JavaEvalLedgerTest`, `ArchitectureTest.theInterpreterPerformsNoJdbc`.
- **R2. The plan describes types; it never spells values and never names an output format.** Every result column
  carries its three facts. A fact that only Pure knows (a Decimal's scale, a DateTime's written precision, a tagged
  winner's kinds) travels in the plan or in the data, never in a text. Guard: `PlanJsonTest` (format pin),
  `PlanMakerTest.aColumnCarriesItsThreeTypeFacts`, `testcases/PlanRoundTrip`; new: the planner makes no `TextResult`.
- **R3. Formatting at the edge is serialization, not computation** (ruled 2026-10-26). A writer spells the typed
  value it received, losslessly, in its consumer's layout. It does no arithmetic, no comparison, no conversion
  between kinds, no default for a null, no zone shift, and chooses no spelling by a Pure rule. The test: swap the
  writer and every value is unchanged. Guard, new: `ArchitectureTest.writersAreCarriageOnly` (the writer package
  depends on the plan's column facts and the typed batch, on no `compiler`, `lowering`, `sql` or `exec` class) and
  `WriterCarriageGuardTest` (a source scan for arithmetic and comparison on numbers and dates in that package, zero
  allowed; representation operations such as setting a scale without rounding are allowed and listed). A scan
  catches drift, not an adversary; that limit is the same the existing guards carry. The charter clause is D3's.
- **R4. One statement, one typed result, several writers.** The legacy writer makes legend-engine's bytes and is
  judged by bytes (`OutputMatrixTest`); the PCT and corpus harnesses take Pure values through `ValueBridge`; the lean
  writers are judged by the value round trip. Execution is never forked for a format (D6).
- **R5. A String the query itself builds is computed in the query, lossless, with one spelling.** The shortest
  round-trip decimal in Pure's layout: the database's own text where its cast writes shortest digits, the exact
  expression where it does not (Snowflake, BigQuery), a platform-owned SQL function only if measurement ever shows
  that expression on a hot path (§8 item 3). A Float printed as the last thing a result column does is not a String
  the query builds: it is carried as the Float and spelled by the writer, which is Pure's own implementation
  (§8 item 3, the edge rule). This is D7. Guard: the PCT suites; a dated register row for any deliberate difference.
- **R6. A value SQL has no type for rides as a typed encoding with a plan fact, never as a spelling.** The tagged
  winner is landed; mixed lists follow (step 6) as a list of tagged values, which is Arrow's list of a union, so the
  plan's vocabulary, the tagged carrier and the lean wire are one type system. Guard: `TaggedSelectionTest` and its
  successor for lists; a shrink-only census of `LITERAL` slots in the test corpus's plans, zero at step 6.
- **R7. Measure, run both, switch, delete, in that order, per lane.** Nothing is deleted until its replacement has
  produced the same bytes (legacy) or the same values (lean, PCT) on every lane and the gate pins it. No parallel
  path survives its switch. Guard: a GATES entry per switch; a shrink-only census of database-written result text,
  zero at step 4.
- **R8. A result is a stream of batches of typed rows** (ruled 2026-10-26). A batch is up to N rows, flushed when
  full, when the source has no next row ready, or at the end; a batch of one row is a batch. Memory is bounded by one
  batch. Collecting a whole result into memory is a writer like any other, used by tests and the judges, and never a
  production method. Guard: `TenetRatchetTest` (JDBC accessor sites at 21); the exec package's closed register; new:
  no production method in `Execution` returns a materialized result.
- **R9. Zero third-party dependencies stay zero in the compiler, the WebAssembly planner and the native image**
  (the user, 2026-10-26: "I don't want to compromise on that"). The Arrow format is written by our own framer; the
  Arrow and pyarrow libraries are test-only readers. Guard, new: a Bazel query test in `tools/guards` that
  `deps(//core:plan_side)`, `deps(//wasm:...)` and the native image's deps contain no `arrow` repository; the
  existing C2a pin (`ArchitectureTest.rootJavaSqlSurfaceIsPinned`: the planner is `java.sql`-free).
- **R10. Both judges stay** (the user, 2026-10-26; plan path R2). The host judge compares typed values in Java; the
  database judge composes both sides into one verdict statement and compares inside the database, over the
  platform's own canonical spellings (`FLOAT_CANON`, the tagged canon per slot), which are query-side constructs
  like `toString` and are never deleted. The two must agree. Guard: `VerdictChannelRegisterTest`, channel B's floors.

## 2. The shape, once

```
Pure query ──▶ PLAN: the statement + one column record per result column (type, multiplicity, slot, store)
                 │        no output format, no spelling, no writer named in it
                 ▼
            EXECUTE: the database computes everything that has meaning
                 │
                 ▼
            TYPED BATCHES: the session reads cells by the plan's facts into typed arrays, up to N rows, one accessor
                 │        (a row driver fills small batches; the warehouse's native DuckDB path keeps its own Arrow buffers)
     ┌───────────┼──────────────────────────┬──────────────────────────┬────────────────────────┐
     ▼           ▼                          ▼                          ▼                        ▼
 lean writers   legacy writer            ValueBridge              a collector              the tab's own reader
 Arrow IPC      legend-engine pure/v1    (PCT, corpus):           (tests, the judges;      DataCube on DuckDB in the
 (our framer),  JSON, byte for byte      Pure values, no text     never a production API)  browser, and on Deephaven
 JSON, CSV
```

The right-hand consumer is why the plan is the only contract that matters for real time: on the Deephaven route no
Java of ours runs between the plan and the grid. What it needs from this program is R2 and R6, nothing else.

**Row databases and batches (the user's worry, 2026-10-26).** Batches are not a plan concern and not two execution
modes; the batch size is a session setting. A row-at-a-time driver (Postgres, MySQL, H2, SQLite) produces small
batches and flushes on idle, so the first row is not delayed and a tick arrives as a batch of one. Moving a row's
cells into columns is a copy per cell, cheaper than today's typed read plus formatting per cell; the driver's own
wire cost is paid either way, and most row drivers fetch in chunks underneath. A one-value result pays microseconds
of fixed cost against a round trip of milliseconds. Choosing rows as the unit instead would de-vectorize every
columnar source and re-vectorize it at the Arrow writer: slowest exactly where the databases are fastest.

**Where the Arrow format lives, and where no library does (the user's question, 2026-10-26).** The compiler
(`//core:planner`, exported by `//core:plan_side`, the WebAssembly planner's only core input) has no execution
dependency and no Arrow; that is C2a, pinned today, and R9 adds the Bazel query guard. The execution side
(`//core:exec`, the JDBC drivers in `//core:drivers`, the writers) gains no Arrow library either. **The batch is our
own representation: one typed Java array per column (`long[]`, `double[]`, `String[]`, `BigDecimal[]`, the date and
nested kinds likewise) with a null mask, behind one accessor interface** (column count, row count, is-null, a typed
read per cell) that every writer uses, so no writer knows the representation. The user asked (2026-10-26) whether
the warehouse server's own Arrow layout should be that representation instead, and the honest answer is no, for
one decisive reason: in core there is no Arrow-native source to pass through. The warehouse gets DuckDB's Arrow
buffers through the foreign-function API, in the native image, a path of its own; core reads DuckDB through JDBC,
whose Arrow export needs the Arrow library's allocator. So an Arrow layout inside core would be built cell by cell
from JDBC rows (bit-packed validity, offsets, 128-bit decimals, nested children, by hand) only to be read cell by
cell again by the row-oriented writers that are the bulk of the consumers (legend-engine's JSON, `ValueBridge`, the
collector), and its one benefit, no copy at the Arrow writer, would be paid for on every other route. Typed arrays
are the natural form for those writers, trivial to build from a driver row, and the Arrow writer converts them in
one pass, which costs about what any formatting pass costs. What is shared with the warehouse is its IPC framing
(`FlatBuilder` and the schema and record-batch messages of `ArrowStreams`), not its buffers: the core writer builds
Arrow buffers from the arrays and hands them to the same framer. The plan's three facts per column become Arrow field
metadata at that writer. If a measured gap ever shows on a route that could hand over Arrow buffers as they are
(DuckDB through the foreign-function API in core, a cloud driver's Arrow results), an Arrow-backed batch implements
the same accessor and no writer changes. The Arrow Java library and pyarrow stay what they are today: readers in
tests (`arrow_matches_json`), which read back what our writer wrote. The native image carries exactly what it
carries today.

## 3. Where things stand (honest)

> **Brought up to date 2026-10-29 (S1.5).** The live state, the step-by-step map, the commits and what is left are
> `docs/RESULT_BOUNDARY_START_HERE.md` §3 and §4, kept current by every phase; this table is the program's own view of
> its steps (§5), by step. The table of 2026-10-26 (the state before phases 1 to 5) is in git history (`d102b35c`).

| Step (§5) | State | Where |
|---|---|---|
| 0. Measure first | half: the clouds' M5 to M8 (GATES 2026-10-26); M1 to M4 never recorded; the lanes' per-test baselines of S1.1 replaced them as the gate | GATES 2026-10-27, phase 5, item 3 |
| 1. One result shape, the batch stream | done: `result/` (`ResultSink`, `Batch`, `Batcher`, `Collector`), `TdsResult.Shape`, `PlanRunner.stream` | `51688f36` |
| 2. The shared Arrow framer | built (track A, 2026-10-29): `//arrow_ipc`, on java.base and `//base`; the warehouse delegates its framing byte for byte (its tests unchanged); no Arrow library in any product closure (`ArrowClosureTest`) | track A; GATES, the track A entry |
| 3. The legacy writer in Java; the SQL-written wires deleted | done: `CellText.engine`, `PureV1Writer`, `Execution.run`, every served route; `WireRender`, `Render`'s wires, the engine Float spellings, `server/serial` deleted | `12b89c7d`, `1b0305f7`, `53cf5b6d` |
| 4. PCT and the corpus from typed rows | PCT done: a relation crosses as legend-engine's own adapter reads it (engine JSON), a value as the reference's instances; the TDS text path deleted. The corpus: not yet (track C, on track B) | `b3311cd6`; `ac4bb7d3`, `6811256b`, `4eaf1b07`, `d8ab8434` |
| 5. The lean writers | JSON and CSV done (`JsonRowsWriter`, `CsvWriter`, `CellText.pure`/`pureJson`); the writers carry only, the precision fact on the plan column, the two guards (S1.4); Arrow built (track A, 2026-10-29: `ArrowWriter`, the server's ARROW_IPC execute; the Python route and the tab not switched, D56); `Fold.conformValueEgress` not deleted (track L, D51) | `12b89c7d`, `f58f37ed`, `d102b35c` |
| 6. Mixed lists typed | not started | track L |
| 7. The old executor goes | half: `exec.Executor` deleted, its effects plan nodes (phase 5); the old door (`Execution.execute*`, `StatementExecutor`, `ExecutionResult`) remains for the corpus, channel B, the service tests and core's 140 test files | `0fe63b42`, `f883d42b`; tracks B, T, C, D |
| 8. The question parked for the user | parked (the engine-style SQL-text dialects) | -- |

**The spellings, under the upstream-fidelity tenet (2026-10-29):** the 51 writer differences classified by upstream's
answer, `docs/result-boundary-homework/WRITER_DIFFERENCES_BY_UPSTREAM_2026_10_29.md`; the decisions, made and wanted,
`docs/result-boundary-homework/DECISIONS_LOG.md`; the rows S117 to S120.

## 4. Step 0: measure first (one week, nothing built)

Every number is produced before step 1 and recorded in `docs/GATES.md` under one entry, "The result boundary
program, step 0". Each is the baseline a later step is judged against.

- **M1. The census of database-written text.** For every plan the test corpus makes (the integration suite, PCT's
  five suites on the platform's engine, the corpus, `OutputMatrixTest`): which result node, and which consumer asked.
  Its `TextResult` count is R7's shrink-only pin and must read zero at step 4. Local. Half a day.
- **M2. The value round-trip gate, built and run.** Every Pure kind of RESULTS_AND_RENDERERS §5 through every
  carrier: each local database's driver read, then Arrow and JSON once they exist. Compared by value, never text. Its
  failures are the real work list of steps 1 to 3 (time zones, BigQuery's BIGNUMERIC scale, nested values per
  driver, a Decimal whose scale varies by value, an Integer past 64 bits). Local first, the clouds through the lanes.
  Two days.
- **M3. Host writing speed.** Ten million rows on DuckDB: typed batches plus a Java JSON writer and a CSV writer,
  against today's database-written wires (2026-10-18: typed 10 to 12 s, the JSON wire 50 to 61 s). Never measured;
  step 3 does not switch without it. One day.
- **M4. The collector over the plan path, once.** `ExecutionResult` filled from the plan runner's typed rows on H2
  and DuckDB, the 101 test files run against it: how many expectations move, and why each moves (the old decoder's
  quirk or the new one's bug). Sizes step 7. Half a day.
- **M5. Statement size and lane time per suite.** Per lane and suite: the largest statement, the count over 5 KB,
  wall time; the lanes' histories collected into one table with the local lanes. Half a day.
- **M6. How often a Float becomes a String inside a query** in PCT standard and essential, the corpus and the
  Deephaven line's 74 trading queries, per lane. Decides whether R5's cheaper form is worth building. Half a day.
- **M7. What legend-engine itself prints for a Float's `toString` on Snowflake and BigQuery**, and which JDK's
  `Double.toString` the `OutputMatrixTest` bytes came from (before JDK 19 it was not always shortest: 8.41E21 printed
  as `8.409999999999999E21`). Decides what legacy byte parity means on a lossy engine and whether the writers ship
  their own shortest-digits routine or pin the JDK. Half a day.
- **M8. Where a Float becomes a String: at the edge or inside the query.** Of M6's occurrences, how many are the
  last operation on a result column (a report column printed; the String is consumed by nothing in the query) and
  how many feed a filter, a join, a group, or a longer String. Decides how much of §8 item 3's edge rule covers,
  and whether the inline expression is ever on a hot path. Folded into M6. No extra time.

Step 0 ends with §5 re-sized from the numbers.

## 5. The steps

Each step has the same five beats (R7): measure (its M items), build, run both (old and new, every lane, differences
counted), switch, delete (in the same step, never later). A step lands as one GATES entry and one line in §9.

### Step 1. One result shape, and the batch stream (three days)

- Build: the planner makes a `TdsResult` for every relation and value root; `TypedQuery.Output` leaves the planning
  input (`TypedQuery.plan`, `PlanMaker`, `ExecuteOptions`, `Diagnostics` take no format); `PlanJson` format 11 writes
  no `TextResult`. The result API: `Execution.batches(...)` answers a stream of typed batches (R8's flush rule) to a
  writer; `executeStreaming` becomes a writer; `executeWire`'s `List<String>` is marked for step 3's deletion. The
  batch type: our own, one typed array per plan column in the slot vocabulary with a null mask (§2), behind one
  accessor interface so no writer knows the representation and an Arrow-backed batch can implement it later.
- Delete: nothing yet (the `TextResult` node stays until its last consumer switches at step 4).
- Gate: `PlanJsonTest` 11, `PlanRoundTrip`, the core lanes, M1 showing every planner-made result typed; R9's query
  guard added now, so it watches every later step.

### Step 2. The shared Arrow framer (two days)

- Build: `ArrowStreams` and `FlatBuilder` move from the warehouse server to a dependency-free module both the
  warehouse's SQL API and core's writers use; the warehouse's tests (`arrow_matches_json`, pyarrow reading what the
  framer wrote) move with them and stay its judge. No Arrow library anywhere in the product (R9).
- Gate: the warehouse's Arrow tests unchanged; R9's query guard green.

### Step 3. The legacy writer: legend-engine's rows in Java (one week)

- Build: one writer package (`com.legend.sink`, name the implementer's) under R3's guards. `PureV1Writer`: a row's
  cells as legend-engine spells them from the plan's facts and the typed cell: a Float by the shortest-digits routine
  (M7 decides: the JDK's `Double.toString` pinned, or our own), a DateTime by the nine-digit `+0000` form at the
  plan's precision, a Decimal at its own scale (S104), a tagged winner by its slot's kind, an Integer past 64 bits as
  digits; the envelope `PureV1Api` already writes. CSV and the JSON array the same way, batch by batch.
- Run both: every `OutputMatrixTest` cell (192 on DuckDB and H2) and every served-route integration test, old route
  and new, zero byte differences or a dated register row each (as S104). M3's speed beside it.
- Switch: `pure/v1` execute, the streamed JSON and CSV routes read the batch stream through the writer. `jsonPlan`'s
  `run` (the tab running the server's wire SQL) becomes the base statement plus the plan's columns: the tab types
  its own rows, as it already does for Arrow and DuckDB in the browser.
- Delete: `WireRender`; `Render.jsonWire`, `csvWire`, `jsonWireRows`; `LiteralSpelling.floatPrintJava` and
  `dateTimeTextEngine` with `SqlFn.FLOAT_PRINT_ENGINE` and every dialect's arm; MariaDB's and MySQL's `floatLayout`;
  `Execution.executeWire`; `Format.JSON`, `JSON_PER_ROW`, `CSV`. Snowflake's served statement goes from 5.9 KB to 44
  characters; MySQL's and MariaDB's from megabytes to the base query.
- Gate: `OutputMatrixTest` unchanged byte for byte; the served routes' tests; `writersAreCarriageOnly`; the local gate.

### Step 4. PCT and the corpus from typed rows (one week, the lanes' turnaround included)

- Build: `ValueBridge` gains a relation built from typed rows (column facts to Pure's TDS column types, each cell by
  its kind, a tagged winner by its slot, a list by the identity channel until step 6); `PctBackend` runs the batch
  stream and hands rows to it; `PctPlanRoute`'s text path and `PctShadowPlan` go (plan path R8's referee overlap ends).
  The database judge reads both sides from typed plans as it does today; its canonical spellings stay in the query
  (R10).
- Run both: PCT both ways on the platform's engine and MongoDB locally, then each cloud lane (Snowflake, BigQuery,
  Databricks) and the server lanes through channel B's floors. The pass set must be identical test by test; a test
  that moves has its cause named before the switch. A difference here is a bug, never a register row.
- Switch: lane by lane; each lane's counts re-baselined in `docs/GATES.md` (the merge rule's floors move only here).
- Delete: `TdsTextWrap`, `Render.tdsText` and its per-row form, `Format.TDS_TEXT`, `ExecutionPlan.TextResult` and
  `Format`, `ResultType` where only text used it, `KeyedPivot`'s text outputs (its probe stays). M1's census reads
  zero.
- Gate: the five PCT suites exact on the platform's engine and MongoDB; every lane at or above its previous count;
  M5 re-measured: no PCT statement carries output text; Snowflake's standard suite at or below the merge-base's 269 s;
  channel B's floors unchanged on every lane.

### Step 5. The lean writers: Arrow IPC, JSON, CSV, lossless (one week)

- Build: `ArrowWriter` over the shared framer (one Arrow schema from the plan's columns; an Integer past 64 bits and
  a Decimal whose scale varies by value carried as M2 decided; a mixed value as a union), `JsonWriter` (numbers
  written as exact JSON numbers with the column types beside the rows, ruled 2026-10-26; NaN and the infinities as
  the strings `NaN`, `Infinity`, `-Infinity`, since JSON has no token for them), `CsvWriter`. The Python route
  (`arrowPlan`) and the tab's readers consume them.
- Gate: the value round-trip gate over every writer and every kind (pyarrow and the Arrow Java library as test-only
  readers). No bytes to compare.
- Delete: `Fold.conformValueEgress`.

### Step 6. Mixed lists as typed values (two weeks; runs beside steps 5 and 7)

- Build: a mixed list (`sort` over mixed kinds, the Any-kind carrier, a mixed collection at a root) as a list of
  tagged values, one tag and one typed slot per element, Arrow's list of a union; the decoders read the tag then the
  slot as the tagged winner's do; the database judge's canon per slot. Partial dates (year, year-month) as their own
  kinds with the precision in the slot's kind.
- Delete: the `LITERAL` slot, `LiteralText.parse`, the spelling carrier's remaining machinery in `MixedEncoding`
  (`rankedElems`' text ranks) and the dialects' literal-text arms. R6's census reads zero.
- Gate: `TaggedSelectionTest`'s successor for lists; the PCT suites; every lane's channels A and B.

### Step 7. The old executor goes; collecting becomes a test helper (one to two weeks)

- Build: `Execution.execute(...)` as a production method returning a materialized result is deleted.
  `server/QueryService`, the one real route on it, writes to a writer. The collector (`ExecutionResult`'s shape,
  filled from the batch stream) moves to test code as a helper, and the channel B judges, harness code in main, keep
  it as their named writer. The 135 test call sites call the helper; their assertions do not change.
- The real work, sized by M4: every expectation that moves because the old decoder read a value differently (a Long
  where it read an Integer, a Decimal keeping its scale, a date's precision) is either a bug in the new decoder,
  fixed, or a correction of what the old one silently did, recorded. This step is the test of the new decoder.
- Delete: `Executor` (1,195 lines), `StatementExecutor`'s legacy order; `JavaEvalLedgerTest`'s exec register shrinks
  by those rows; `TenetRatchetTest`'s JDBC accessor pin drops with `Executor`'s reads.
- Gate: the integration suite on the plan path; the corpus; the local gate; both judges green and agreeing.

### Step 8. One question parked for the user

The engine-style SQL-text dialects (`EngineStyleH2`, `EngineStyleDB2`, `EngineStyleComposite`): matching
legend-engine's generated SQL byte for byte is a test oracle, not a product contract, once results are judged by
value. Not this program's to delete; named here because it is cruft of the same kind. (Revision 2 also listed the
database judge here; the user ruled it stays, R10, and it is withdrawn.)

### What stays, by name, and why

A String the query builds (R5); the database judge and the host judge with their canonical spellings in the query
(R10); kind B's encodings until step 6 types them; the exact-digits Float text on Snowflake and BigQuery for a String
built inside a query (R5; the edge rule takes the common case off it); the engine-style dialects until step 8 is
ruled.

## 6. Gates for the program

1. **The value round-trip gate** (M2): the judge of every lean writer and of the typed seam on every lane.
2. **`OutputMatrixTest` byte for byte**: the judge of the legacy writer; a deliberate difference is a register row.
3. **PCT and the corpus, test by test, every lane, no lane below its previous count**; the floors move only at step
   4's re-baseline, in the GATES entry that names it. **Both judges agree** on every lane (R10).
4. **Two shrink-only censuses:** database-written result text (M1) and `LITERAL` slots (R6), both zero by step 6.
5. **`writersAreCarriageOnly` and `WriterCarriageGuardTest`** from step 3 on; **R9's dependency query guard** from
   step 1 on.
6. **The size and time table** (M5) re-measured at steps 4 and 7: Snowflake's standard suite at or below 269 s; no
   PCT statement over 5 KB from output text; Snowflake's served statement for `|1.5 + 2.25` at 44 characters.
7. **No production method returns a materialized result** (R8), from step 7 on.

## 7. Order, dependencies, size

```
step 0 (1 wk) ─▶ 1 (3 d) ─▶ 2 (2 d) ─▶ 3 (1 wk) ─▶ 4 (1 wk, lanes) ─▶ 5 (1 wk) ─▶ 7 (1-2 wk) ─▶ 8 (ruling)
                                                          └─▶ 6 (2 wk, beside 5 and 7; may start after 1 if Deephaven is the priority)
```

Before step 0: land the current branch on `main` (every lane at or above the merge-base but `testFilterInstance`,
which main loses too; Snowflake standard +21% same-hour, the TDS text wrap this program removes). The program
starts from `main`, on its own branch, with nothing else on it: the last merge of `main` into this branch cost 39
conflicts, and the two text pipelines this program deletes are exactly where other lines' edits land.

Step 3 before step 4 because the legacy writer's cell spellings are the same cell reads `ValueBridge` needs, proven
once against bytes. Step 6 is independent after step 1 and the longest. The Deephaven line needs steps 1 and 6 and
nothing else from here.

Seven to nine weeks of one session's work, the lanes' turnaround included. The deletion is about 3,500 lines of
Java plus one render arm per dialect, the four kinds of output text out of every statement on every lane, and one
of the two decoders.

## 8. The rulings

Ruled 2026-10-26 ("everything else is okay"): **1** D1 to D5 as written in RESULTS_AND_RENDERERS §2; **2** D3's
charter clause (R3) with its two guards, the writers registered carriage sites in `JavaEvalLedgerTest`; **5** lean
JSON writes exact JSON numbers with the column types beside the rows, Arrow for any reader that needs exactness in
JavaScript; **6** collecting into memory is a test helper and the judges' writer, never a production method, and the
old executor goes at step 7; **7** the database judge is never deleted (R10; the engine-style dialects stay parked);
**8** the order of §7, mixed lists in parallel after step 1 if Deephaven is the priority.

Sent back for the expert version, given here, waiting on the user's yes:

**3. A Float, the version that is best on every side and makes the lean experience good. Ruled 2026-10-26
("sounds great"), the last of the eight.** Three places, and
the user's reminder (2026-10-26) that the legacy writer and the lean writer may hold different ideas of a Float's
text where that is better. The plan, in one table:

| Where a Float meets text | LEGACY (legend-engine's bytes) | LEAN | PCT (Pure's values) |
|---|---|---|---|
| A. In a result | its 64 bits; the writer spells `Double.toString` | its 64 bits; Arrow float64; JSON and CSV the shortest digits | its bits, a Pure Float |
| B. Printed as the last thing a column does | the bits carried; the writer spells `Double.toString`, which is Pure's own implementation | the same bits, the same spelling | the bits, spelled by the bridge with the same function: the reference, byte for byte |
| C. A String built inside the query (feeds a filter, a join, a group, a longer String) | one spelling, in SQL: shortest digits in Pure's layout; a native cast on nine engines, the exact-digits expression on Snowflake and BigQuery (a register row where legend-engine printed the database's lossy text, M7) | **the same SQL** (the recommendation); the alternative, the database's own native text at zero cost, is lossless on nine engines but engine-dependent in layout and lossy on Snowflake, so it is offered, not recommended | the same SQL as LEGACY: Pure's text |

Places A and B are where real code meets a Float's text and both cost nothing on any engine. Place C is the rare
one, and the only place where LEGACY and LEAN could differ; the recommendation keeps one SQL because the
difference buys nothing measurable and costs a String that changes meaning by database.

**"At the edge" is a property of a plan's root, not of the Pure function** (the user's question, 2026-10-26: does an
`assert` pushed to the database turn place B into place C?). Yes, and by design. The edge rule applies when the
plan's root projection is the printed Float and nothing in the statement consumes the String. An assertion run by
the host judge reads the two operands as the plan's results and compares them in Java, so the printed Float is at
the root there, carried as its bits, and the host judge spells it with Pure's own implementation. The same
assertion composed by the database judge into one verdict statement consumes the String inside the SQL (`expected
= actual`), so there it is place C: the database spells it, with the exact-digits expression on the two lossy
engines, as channel B does today. R10 then requires the two judges to agree, which makes every such test a
cross-check of the Java spelling against the SQL spelling on every lane: if either were wrong, the judges would
disagree and name the test. That is the database judge doing exactly what it is kept for. The cost is test-only and
already paid (channel B's statements carry the canon today); a product query whose root prints a Float pays
nothing. PCT's channel A is place B throughout: `ValueBridge` builds the String with Pure's own function and
legend-pure runs the assertion over it.

The detail of each place:
- *In a result.* A Float is its 64 bits, never a text: the DOUBLE slot, the driver's exact read, Arrow's float64.
  Nothing is lost, nothing is spelled, no database is involved. Where a lean writer must spell it (JSON, CSV), it
  writes the shortest decimal that reads back as the same double: 0.1 is `0.1`, a third is `0.3333333333333333`,
  never `0.10000000000000001` and never a lost digit. That routine is pinned (M7): the JDK's `Double.toString` from
  JDK 19 on, or our own shortest-digits routine (about 300 lines, no dependency) if the pinned JDK is older. Decimals
  print at their own scale; integers exactly; NaN and the infinities as named strings in JSON and natively in Arrow.
- *Inside a query* (`toString`, `format`, a String built from a Float). One spelling everywhere, so the same program
  gives the same String on every database: the shortest round-trip digits in Pure's layout. Nine engines write it
  with one cast and a layout fix (DuckDB, Postgres, H2, ClickHouse, Trino, MariaDB, MySQL, Databricks, MongoDB);
  Deephaven is Java and writes it natively. Snowflake and BigQuery cannot (measured: sixteen digits and a
  mis-rounded `%.17g`), so they compute the exact digits in SQL today, about 5 KB per Float printed, correct.
- *Making those two cheap, practically, with no code running inside the database that is not SQL* (the user,
  2026-10-26: no JavaScript in the database; `toString` inside a query is rare in real code). Four options, in the
  order they should be used:
  1. **The edge rule: a Float printed as the last thing a column does is carried as the Float and spelled by the
     writer.** When `toString` (or `format` of one Float) is the final operation on a result column and nothing in
     the query consumes the String, the plan keeps the DOUBLE slot and records the column's Pure type as String; the
     writers spell it with the shortest-digits routine. This is not an approximation: Pure's reference
     implementation of a Float's `toString` is Java's `Double.toString`, so the writer in Java is byte-identical to
     the reference by construction, and `ValueBridge` builds the PCT value the same way. Covers the common real
     case (a report column) at zero SQL cost on every engine. The database judge canons such a column as the String
     the reference would print (the exact-digits expression inside the judge, test-only).
  2. **Inside the query, the exact-digits expression stays, inline.** A String that feeds a filter, a join, a group
     or a longer String is computed where it is used, correct, about 5 KB per Float on two engines, zero elsewhere.
     Pay only when used; M6 and M8 say how rare that is.
  3. **A platform-owned SQL function, installed once, as the escalation if 2 is ever on a hot path.** The same
     expression as the body of a SQL function (`CREATE FUNCTION`, SQL language only, deterministic) in the
     platform's own schema, the way the platform already ships views over its own system tables (`docs/WORLD_MAP.md`);
     each statement then spells a Float as one call and the engine inlines it. Needs a schema and the privilege to
     create a function; a deployment that refuses DDL falls back to 2. Built only on M6's number.
  4. **A compile-time diagnostic** when a Float becomes a String inside a query on an engine that cannot print it
     natively, naming the cost. Cheap, informative, changes nothing.
- *Rejected and why:* JavaScript or any non-SQL function in the database (the user's ruling); a fifteen- or
  sixteen-digit standard (lossy by design on the lossless engines, and Snowflake's own fifteen is double-rounded);
  Snowflake's sixteen digits as a per-database exception (one database would answer differently, and it is not
  lossless); fixed seventeen digits (shows binary noise on ordinary values, which is why every modern runtime moved
  to shortest).

**4. Arrow: where the dependency would sit (nowhere), and what the batch is (our own). Ruled 2026-10-26 ("arrow
look good").** The compiler never sees
it: the planner has no execution dependency (C2a, pinned), and the WebAssembly planner takes only `//core:plan_side`.
The execution side does not take it either. The batch is our own representation, typed arrays with a null mask
behind one accessor interface (§2). The user asked whether the warehouse server's own Arrow layout should be the
representation instead; it was a suggestion, and the honest answer is no: core has no Arrow-native source to pass
through (the warehouse reaches DuckDB's buffers through the foreign-function API in the native image; core reads
DuckDB through JDBC, whose Arrow export needs the library), so an Arrow layout in core would be built by hand from
driver rows and read back cell by cell by the row-oriented writers, paying on every route for a saving on one.
Typed arrays are the natural form for those writers and the Arrow writer converts them in one pass. Shared with the
warehouse: its IPC framing (`FlatBuilder`, the schema and record-batch messages), not its buffers. The Arrow Java
library and pyarrow remain test-only readers. R9 pins it with a Bazel query guard over the planner, the WebAssembly
planner and the native image. Reversible: an Arrow-backed batch behind the same accessor, if a measured gap ever
shows on a route that can hand over Arrow buffers as they are; never in the compiler or the native image, never the
default.

**Ruled later (2026-10-27 and 2026-10-28; the order of work they set is `docs/RESULT_BOUNDARY_START_HERE.md` §4):**

**9. The execution order and the audit (2026-10-27).** "Skip the guards - let's actually do the real work of making
the plan real and deleting everything else including the string spellings and old execution - keep going in phases,
do expert work, audit after each phase": phases 1 to 5. Then "we need to pause and do an audit": the audit
(`docs/result-boundary-homework/README.md` §1) and its three-stage plan, answered "keep it, and keep going": the
paused phase 5 commit kept and made green. The plan's other three questions, answered 2026-10-28: "any output
spelling changes need to come to me on case by case basis" (no spelling change of any output is accepted or
registered by a session: each is put to the user, alone, with its before and after, and the user's answer recorded);
"yes keep arrow writer at the boundary in this program (but no arrow dependencies in compiler!)" (the Arrow writer is
track A, at the boundary only; R9's guard keeps every Arrow artifact out of the compiler, the WebAssembly planner and
the native image); the writer guards: the user asked what they are for ("writer guards are for what exactly"; the
start page's S1.4 keeps the answer) and then approved them, "yes add this to the plan": R3's two guards
(`ArchitectureTest.writersAreCarriageOnly` and `WriterCarriageGuardTest`) are built in S1.4, right after the typed
precision fact, which supersedes 2026-10-27's "skip the guards" for them. The first spelling change put to the user
case by case, a negative zero ("what should we do about the negative zero? what is the right move", then "okay"):
lite's own outputs (LEAN JSON and CSV) write it `-0.0`, item 3's shortest decimal that reads back as the same double;
Pure's `toString` of a Float stays `0.0` for either sign, legend-pure's own (register S117; S114 until S1.0). Then "keep the fix, fix
the plan wording including adding that we need to fix 0.00010": `b944a42e` kept, and the trailing zero lite's own
outputs write after a small double's one digit (`1.0E-4` as `0.00010`, where legend-pure and item 3's shortest digits
give `0.0001`) ruled a defect, fixed in S1.4.

**10. A PCT relation crosses as legend-engine's own adapter reads it (2026-10-28, "Do the probe", then "Do it").**
The relation is streamed through the server's own LEGACY writer (`PureV1Api.engineWriter`) and read by a Pure copy of
the engine adapter's `getTDSResultFromProtocol` steps, then `stringToTDS`; the TDS text written in Java
(`PureTdsText`) is deleted. Chosen over the typed `TestTDSInterpreted.addColumn` route the audit had proposed: that
is not the reference's own crossing, its reader holds a date at the millisecond, and its classes sit in jars the
upstream pool does not list. So the PCT judges the JSON the server serves (three production defects found by it).

**12. Upstream fidelity (2026-10-29).** "wherever possible we should stick to upstream legend-pure and legend-engine
as possible - every answer should be 'can we do exactly as pure/engine does, or as close to as possible where too
expensive to be exact but with an explicit posting to me and a ledger for diverging' every decision should be settled
with that tenet." Recorded as AGENTS.md's upstream-fidelity tenet, the start page's first rule (§5) and the register's
header: upstream's answer, read from the pinned sources or asked of the engine runner, is the default; a divergence is
taken only where exactness is too expensive, posted to the user before it lands, and ledgered with the ruling. It
settles S1.4's two open questions (the start page's §4) and governs S1.5's classification of the 51 writer differences
and every track after.

**11. Our own core tests are the LEAN twin of PCT and the corpus (2026-10-28).** "i would prefer not to delete the
overlapping tests but use the exact same logic but with platform output that we think users would actually run/use
instead of backwards compat mode - so its almost the platform twin of PCT/core-relational". Replaces step 7's "test
helper with assertions unchanged": no test is deleted for overlapping PCT or the corpus; each value test keeps its
query and logic and is judged on lite's own LEAN output (lite's JSON rows first, Arrow when built), on every lane like
PCT. PCT and the corpus remain the LEGACY judges. Track T of the start page.

## 9. Status

- **2026-10-29, the three routings** (the decisions log D65; GATES, the three routings' entry): the plan path's
  seeds and the server's held-database fill run as plan setup steps through the runner's `setUp` (the target's
  session statements first, the order upstream's test connections keep); the MariaDB raise recovery is two
  statements its connector declares as data, read by the seam after a failure; `DatabaseConnector.raisedMessage`
  and `Sessions.raisedMessage` deleted. In core's main sources the seam `JdbcStatements` and its DuckDB bulk loader
  are the only JDBC senders. The independent audit of the routings is folded in D65 (the held raise read only
  where the error's own text carries none, as before; the per-step session statements and the counts stated). The two service test runner readings of door 1 were read against the pinned legend-engine sources and
  kept (the presented column name: `pureToSQLQuery.pure`'s `mayQuotePivotColNames`; the one-object graph fetch bare:
  `ExecutionNodeSerializerHelper.writeTo`). Next: the independent audit of the routings, the decisions post.
- **2026-10-29, the doors' audit folded** (the decisions log D64; GATES, the fold entry): the records corrected (with
  door 1); an effect statement that ends the body answered after its script runs; every generator temp dropped; the
  dead arms deleted (`PrepTrace`'s timed prepare/execute, `SqlTypeCensus`'s wire arm, `TypedSide`'s accessors,
  `Lean.ofCollectedSide`), both files off the JDBC census; the stale sentences. Posted to the user, not built: two
  readings of the service test runner (the presented column name; the one-object graph envelope), the routing of the
  plan path's seeds and the server's fill through the runner. Next: the decisions post.
- **2026-10-29, the doors, 1** (the decisions log D63; GATES, the door 1 entry): a program's last statement streams
  to the sink the caller hands for its shape; `Execution.ProgramReport` deleted, `TypedSide` off the public surface;
  the service test runner reads its answer from the boundary's writers (`PureV1Writer.objects`, the new
  `TdsObjectsWriter`), the twin's `Program` from `Lean.Capturing`. Every door in the map is closed: a body's every
  statement is the plan runner's, and nothing materialized leaves a public entry. Three raw sends remain inside the
  chartered seams, named by the doors' audit (D64): the plan path's seeds (`PlanSessions.runSetup`), the server's
  fill of a held database (`ConnectionResolver.filled`), the MariaDB connector's own session reads. Next: the
  audit's fold (D64), the decisions post.
- **2026-10-29, the doors, 2** (the decisions log D62; GATES, the door 2 entry): the test-data generator on plans --
  its temps and drops as effects, its read-backs as typed rows of the columns the database describes, the CSV's
  text columns as reported columns; `testdatagen` holds no JDBC (the funnel tightened, the census shrunk). Next:
  door 1 (the last statement streamed, `ProgramReport` deleted), the audit, the post.
- **2026-10-29, the doors, 3-5** (the decisions log, "The doors", D61; GATES, the doors 3-5 entry): the user's ask
  after Z ("close all the doors here as part of this PR so we know that it can be done"). The probes and the pivot's
  key type as plan statements the runner runs (`PlanRunner.columns`, `describe`); the seeds, a program's session
  statements and the system store's rows as setup steps it runs (`PlanRunner.setUp`; `SetupRunner` deleted); the
  guard: outside the seam, no class calls a session's statements. Next: door 2 (the generator), door 1 (the last
  statement streamed, `ProgramReport` deleted).
- **2026-10-29, the DuckDB driver 1.5.6.0** (the decisions log D60; GATES, the D60 entry): core's pin 1.4.4.0 to
  1.5.6.0 on the user's ask, measured first on the jars (the pin's LIMIT shape correct on all three versions; the
  as-of defect of S126 gone from 1.5.5 on); the as-of test asserts the spec on every lane; the chain green on the bump
  alone, no PCT or corpus pin flipped. Open: the warehouse's own pin (1.5.5.1) as the same version, the user's call.
- **2026-10-29, track Z: the final audit folded; the program's work on the branch is complete** (the decisions log
  D59; GATES, the track Z entry): twelve findings, none on the code's correctness; the service test runner's cells
  through the boundary's writer (its own spelling and `String.valueOf` fallback gone); the crossing-numbers refusal
  ledgered (S127); the records corrected where stale; the open questions in one table (the start page's §10a). Left
  to the user: the rulings of §10a and the landing of PR #80 (no session pushes `main` or merges).
- **2026-10-29, track F: the open findings** (the decisions log D58; GATES, the track F entry): built S116 (the
  engine's `toVariant` of an empty), the H2 JSON parse (63 corpus tests gained), the tag in the batch; closed the
  `concatenate` width, the `[0..1]` element and the unbounded Decimal by upstream's own rule; posted `asOfJoin`'s empty
  right side (S126), the multi-key ordered list aggregate (sized), the lane twin and the typed kind. Next: track Z.
- **2026-10-29, track A audited and folded** (the decisions log D57; GATES, the track A entry): eighteen findings,
  two blockers (the union's type ids for the browser's reader; the decoders' 63-bit bound) fixed before the commit;
  three questions posted (a day under a TIMESTAMP column, the JSON route's tenth subsecond digit, the server's Arrow
  format against S28: register S125).
- **2026-10-29, track A: the Arrow writer at the boundary** (GATES, the track A entry; the decisions log D55, D56):
  `//arrow_ipc`, the warehouse's framer shared; `result/ArrowWriter`; the server's ARROW_IPC execute; the embedded
  hosts' entry points; R9's closure guard; the pyarrow round-trip gate. Steps 2 and 5's Arrow half done; the
  consumers' switch (Python's route, the tab) posted for the user (D56). Next: track F.
- **2026-10-29, track L, the L2 audit's fold** (GATES, the L2 audit entry; the decisions log D54; the register
  S123, S124): the flatten's numbering of a sorted star frame and of an ordered source; crossing numbers refused by
  name; `filter` over a mixed list; the cells' size; the gate over typed columns. Next: track A.
- **2026-10-29, track L, L2: the spelling carrier deleted** (GATES, the L2 entry; the decisions log D52): the
  `LITERAL` slot, `LiteralText`, the decoders' and dialects' arms, the lowering's spellers; the census zero; plan
  format 19. Open: `Fold.conformValueEgress` (step 5's delete) waits on D51's ruling.
- **2026-10-29, track L, L1: a mixed list as a list of tagged values** (GATES, the L1 entry; the decisions log
  D50): `SqlExpr.TaggedList`, parallel typed arrays by position, rows of a tagged value at a root; the operations in
  `TaggedLists`; the PCT essential suites on every local lane and channel B at their counts. Next: L2, the deletions.
- **2026-10-29, track T, T-d2: every integration test on the lanes; the floors re-measured** (GATES, the T-d2
  entry; the decisions log D49; the ledger, batch T-d2): sqlite 89 on its floor, h2 269 and one not-run pin (268 since track F; H2's
  planner on a twenty-call chain), the platform 0 of 2,232; a test's own connection text re-spelled to the
  session's backend; the T-d and track D audits' fixes. Track T built but for Arrow's judge (A). Next: track L.
- **2026-10-29, track D built: the old door deleted, the judges' side and the body driver named for what they are**
  (GATES, the track D entry; the decisions log D48): `Execution.execute*`, `executeResolved` and the service's
  materialized `execute` gone; `ExecutionResult` -> `TypedSide`, `StatementExecutor` -> `BodyDriver`; the old-path
  census zero on every step-7 symbol; AGENTS.md and core/README.md corrected. Step 11's 2026-10-17 wording superseded
  by track B (the driver stays); its closure on step 9's residue, the user's rulings. Next: tracks L, A, F, then Z.
- **2026-10-29, track T, T-d: the twin's lanes** (GATES, the T-d entry; the decisions log D47; the ledger, batch T-d):
  core's integration tests on SQLite (15 on its floor) and H2 (169), each lane a Bazel target in the core gate held to
  its recorded floor (`testing/LaneFloor`); the MongoDB lane deferred to D's session-seam move. Track T built but for
  Arrow's judge (A). Next: track D.
- **2026-10-29, track C built: the corpus runner, the service test runner and channel B on `Execution.runProgram`**
  (GATES, the track C entry; the decisions log D39): the body typed once, the session the caller's, nothing
  materialized returned to the corpus runner or channel B; the referee keeps `ExecutionResult` as the judges' side
  until D (D36). The plan path program's step 10 is this entry. Next: track T (core's tests as the LEAN twin).
- **2026-10-29, track T, T-b2 and T-c: the other packages, the parity harnesses and lanes on the twin; two product
  bugs fixed** (GATES, the T-b2/T-c entry; the decisions log D45, D46; the ledger): `Json.parse`'s negative zero and
  `RowDecode`'s identity fields, both found by the twin; the reader in `:test_cases`; `QueryService.execute(...,
  sinkOf)`. 7 files, 21 lines remain on the old door, track D's. One divergence posted for the user (the plan path's
  egress lower bound). Next: T-d, then D.
- **2026-10-29, track T, T-b: the integration package's remaining 31 files; `testing/Program.java`; a plan-path bug
  fixed** (GATES, the T-b entry; the decisions log D44; the ledger, batch T-b): a navigate's slot column is flattened
  once for the plan's columns and the judges' (`TypedRows.flat`); six files whose bodies only the program entry runs
  go through `Program`; 2,232 of 2,232. Findings queued to D and F: the plan path's missing metamodel-store route on
  a given session; `PureV1Api`'s positional tdsColumns. Next: the other packages (85 files by the counting rule).
- **2026-10-29, track T begun: the LEAN reader and the first batch** (GATES, the T-a entry; the decisions log D42;
  the ledger `docs/result-boundary-homework/T_EXPECTATIONS_2026_10_29.md`): `testing/Lean.java` reads lite's JSON
  rows back through the product's own row decoder; the 28 `AbstractDatabaseTest` subclasses (2,232 tests) judged on
  it, no expectation moved. Next: the remaining 115 test files on the old door, the helpers, the per-lane targets.
- **2026-10-29, track B, B1a and B1b built: the body driver sends nothing itself and holds no connection** (GATES, the
  B1a and B1b entries; the decisions log D33, D35). B1a: the session seam answers a statement's reported columns
  (`Statements.columns`, `describe`) and loads rows (`load`); the three schema probes, the seeds and the static pivot
  read and send through it. B1b: the driver's environment holds a `Sessions.Session` (`statements(trace)`, `state()`,
  `version()`), never a `java.sql.Connection`; the judges, the verdict batch, the referee's SPI and the generator's
  natives the same; `Execution` is the one JDBC-aware caller; the guard `ArchitectureTest.theBodyDriverAndTheJudgesHoldNoJdbc`
  pins thirteen classes by name. `main` merged a second time (PR #79, D34) for PR #80's CI. **B2 built the same day**
  (D37): `Execution.runProgram`, the one entry for a program, the old door's entries shims over it. The order changed
  (D36): B3 (the judges' typed side) is track D's rename, after C and T remove `ExecutionResult`'s other two roles.
  Next: track C.
- **2026-10-29, S1.6: draft PR #80 opened** (`https://github.com/neema2/legend-lite/pull/80`, from `a0fe9d21`; the
  first CI on every OS since phase 1; under the user's "keep going", D11; the merge the user's). Track B begins
  (the start page's track B block: the design D29, the Java residue D30).
- **2026-10-29, S1.5 done: the 51 writer differences classified by upstream's answer; two moves toward upstream;
  the S1.4 audit's findings fixed** (the records commit `3e9e739a`, the code commit after it; GATES, the S1.5
  entry). Eleven groups, each read from the pinned sources
  (`docs/result-boundary-homework/WRITER_DIFFERENCES_BY_UPSTREAM_2026_10_29.md`): 29 corrections, 4 old defects
  fixed, 2 ruled, 5 random, 3 findings, 4 moves toward upstream, 4 for the user; built: an hour-only date without a zone (legend-pure's `DateFormat.append`
  writes `+0000` from a minute on), a LEGACY NaN cell as `null` (the engine's `convertPurePrimitiveToJson`);
  wanted of the user, each with a recommendation: LEAN's NaN (S119), an instance's text (the interpreter's `@_` shape
  against compiled's `Anonymous_StripedId`), a metamodel element's name against its path, a Float with an exponent
  (D10), nested dates (D9); the SQL-side zone rule queued for track T (D18; register S121 names the four). Register S119, S120; the §3 table rewritten by step; GATES entries for phases 1 to 4. The S1.4 audit (an
  independent agent, 16 findings) found the writers sound and the records stale in seven places, fixed. The
  decisions log D12 to D27. Next: S1.6, a draft PR under the user's "keep going" (D11), then track B.
- **2026-10-29, S1.4 corrected under the tenet; the decisions log opened; the program continues without stopping**
  (the user: "no need to stop, just keep going all the way until done with whole program and all old ways to execute
  are deleted - do a deep audit after every phase and keep a log of decisions"). A relation's TIMESTAMP cell in
  lite's own outputs keeps its nine digits as legend-engine and legend-pure spell a SQL timestamp (the trim and the
  midnight rule gone); a value root's keeps the value lane's minimal digits (register S118); the PCT's three-digit
  texts traced to its harness's `TestTDS`. `docs/result-boundary-homework/DECISIONS_LOG.md` holds every decision made
  or wanted, per phase, with an audit after each. Next: S1.5.
- **2026-10-29, the upstream-fidelity tenet ruled** (§8 item 12; AGENTS.md; the start page's §5 first rule and §10;
  the register's header): exact upstream by default, a divergence only where too expensive, posted and ledgered.
  S1.4's two open questions settled by it and written up with their upstream evidence (the start page's §4, S1.4): a
  TIMESTAMP cell in lite's own outputs spelled as legend-pure prints it (nine digits, never a day; the writer's trim
  and midnight rules to go) and the tab's negative zero (no pinned upstream governs a grid's text; keep and ledger
  recommended); both await the user's go, LEAN spelling changes being the user's case by case. S1.5's brief now
  classifies the 51 writer differences by upstream's answer. Next: the go on the two, then S1.5.
- **2026-10-28, S1.4 done: the writers carry, never decide** (GATES, the S1.4 entry). The `0.00010` trailing zero
  fixed in lite's own outputs (`1.0E-4` as `0.0001`, legend-pure's text; ruled); the plan column's fourth fact, its
  temporal cells' written precision (`ExecutionPlan.Precision`: WRITTEN, DAY, INSTANT; format 17), set by
  `ColumnFacts` and read by the writer in place of the slot's spelling it matched (the decoders keep the slot as their
  decode instruction); R3's two guards built, `ArchitectureTest.writersAreCarriageOnly` and `WriterCarriageGuardTest`
  (58 representation operations registered by name), and the result package a closed register; the audit's nits
  swept. Open for the user: the midnight rule, the one decision by a value's clock left in a writer (kept, registered
  pending the ruling), and the tab's negative zero. Next: S1.5, then S1.6 on the user's word.
- **2026-10-28, S1.0 done: `main` merged into the branch.** The next session (branch `claude/wizardly-dirac-rw6tim`,
  from `7a9a368b`) merged `main` at `369bb176` (PR #77: 50 commits, 114 files, 12 on both sides) with a merge commit,
  both sides kept -- `PureV1Api.execute` runs the branch's typed route over main's held-model hook; the register keeps
  main's S112 to S114 and renumbers the branch's three rows S115 to S117 with every citation (28 lines, 11 files); main's
  `without` tests take the `Long` the plan runner decodes. Measured locally: core (all 25 packages), the guards, the
  census, PCT discipline, PCT channel A on DuckDB, SQLite and MongoDB (all five suites, exact, the crossing counted),
  the DataCube Python test, the local gate (GATES, the S1.0 entry). Not the lanes: unreachable from this session.
  Next: S1.4.
- **2026-10-28, S1.4 begun; the handoff completed.** The negative zero built (`b944a42e`: LEAN JSON and CSV write
  `-0.0`, Pure's `toString` stays `0.0`; register S117, S114 until S1.0; `//core:core_tests`, the guards and the census green); the
  writer guards approved; the `0.00010` trailing zero ruled a fix. `main` moved 105 commits past the branch's base
  (PR #77) and took register ids S112 to S114 too, so a new first step: S1.0, bring `main` in and renumber the
  branch's three rows. The start page's §12 is the message that opens the next session. Next: S1.0, then S1.4.
- **2026-10-28, the handoff rewritten for a session from any account.** `docs/RESULT_BOUNDARY_START_HERE.md` is now
  the whole roadmap in order (stage 1's S1.4 to S1.6, then tracks B, T, C, D, L, A, F, P and Z, a brief each), and
  `docs/result-boundary-homework/` holds the evidence that had lived only in a session: the audit, the code maps,
  the 51 writer differences, the census of core's tests, legend-engine's answers and how to ask it, every probe's
  source. Rulings 9 to 11 added to §8 (the twin among them). Next: S1.4.
- **2026-10-28, stage 1, step 3 done: a PCT relation crosses as legend-engine's own adapter reads it.** Every lane
  measured the engine crossing at e8c341d0 exact on every suite (channel A; GATES, 2026-10-28); the switch is made and
  the Java TDS text (`PureTdsText`), its wrapper and the adapter's text branch are deleted. The PCT now judges the
  engine JSON the server serves. Next: S1.4 (the writers carriage only), then S1.5's records and S1.6's draft PR.
- **2026-10-28, PARK-34 closed by probing legend-engine 4.145.0** (stage 1, step 3 waited on it): a Variant
  column's builder is `SEMISTRUCTURED`, its cell a string holding its JSON; the writer spells both so (register S115, S112 until S1.0
  for the JSON's digits), and every other type the engine has no answer for stays refused, said so (GATES,
  2026-10-28). Next: the engine crossing re-measured, then the switch decided.
- **2026-10-27, phase 5's first half green; the audit's plan under way.** Phases 1 to 4 landed on the branch
  (51688f36 typed results; 12b89c7d the writers; 1b0305f7 and 53cf5b6d the served routes, the database-written wires
  deleted; b3311cd6 PCT from typed rows, the TDS text path deleted). The user paused phase 5 for an audit; its plan
  (three stages: make phase 5 green and measure every lane per test; one entry point, the old door deleted; the
  mixed lists typed and the Arrow writer) runs now. This head: the old executor deleted, the body driver's effects on
  the plan runner, and every loss the six lanes measured between 7b8c89dc and b3311cd6 fixed (GATES, 2026-10-27).
  Next: the lanes re-measure this head; the PCT relation's crossing decided by measurement (stage 1, step 3).
- **2026-10-26, all eight ruled; the landing begins.** The Float rule accepted ("sounds great") after the batch
  ("arrow look good"). Next, in order: the branch on `main` under AGENTS.md's pushing rule (fresh against `main`,
  the local gate on the tip, "local gate: //gates:local green" in the message, `main`'s CI watched, revert on red);
  step 0 announced in `docs/IN_FLIGHT.md`; its eight measurements; their one GATES entry.
- **2026-10-26, revision 3, ruled in part.** The user's rulings recorded (§8): items 1, 2, 5, 6, 8 as written; the
  database judge stays (R10); no JavaScript or other non-SQL code in the database. The Float rule answered with the
  edge rule plus the inline expression, a platform SQL function only on a measured hot path, one table for LEGACY,
  LEAN and PCT across the three places a Float meets text (§8 item 3); the batch is our own representation, typed
  arrays behind one accessor, the warehouse's IPC framing shared and its buffers not, no library anywhere (§8 item
  4, R9; the user's "warehouse Arrow" was a suggestion, answered no with the reason). **Item 4 ruled the same day
  ("arrow look good").** Item 3 waits on the user's yes, its last question answered (an assertion pushed to the
  database is a consumer: place C there, place B for the host judge and PCT, the two judges cross-checking the two
  spellings). Next: the yes, then landing the branch on `main`, then step 0.
- **2026-10-26, the handoff written:** `docs/RESULT_BOUNDARY_START_HERE.md`.
- **2026-10-26, revision 2.** After the user's three questions: batches as the unit; collecting into memory a test
  helper, never a production method; M4 and M7 added to step 0.
- **2026-10-26, revision 1, proposed.** Written after the Snowflake Float-text measurement (RESULTS_AND_RENDERERS
  §10.5) closed the last open fact under D7.
