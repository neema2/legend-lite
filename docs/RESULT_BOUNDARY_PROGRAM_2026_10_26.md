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

| Piece | State today | Step |
|---|---|---|
| Three facts per column, typed tree in the plan JSON (format 10) | landed (2026-10-17/18; tagged slots 2026-10-23) | kept |
| The typed seam: `ExecutionPlan.TdsResult`, `PlanRunner.rows`, `JdbcDecode`/`RowDecode` by the plan's facts | landed; used by the typed callers and `TaggedSelectionTest` | step 1 makes it the only result |
| `ExecutionPlan.TextResult` with `Format {CSV, JSON, JSON_PER_ROW, TDS_TEXT}`; `TypedQuery.Output` as a planning input | live: PCT (`TDS_TEXT`), the served routes (`JSON_PER_ROW`, `JSON`, `CSV`) | steps 1 to 4 retire; deleted at step 4 |
| `WireRender` (177 lines), `Render`'s text wires (in 1,360), `TdsTextWrap` (268), `FloatPrintText`'s engine layout (in 269), `LiteralSpelling.floatPrintJava` and `dateTimeTextEngine` (in 1,052), each dialect's `FLOAT_PRINT_ENGINE` arm, MariaDB's and MySQL's `floatLayout` | live | deleted at steps 3 and 4 |
| `PureV1Api` builds the envelope in Java and takes the rows as the database wrote them (`Execution.executeWire`, a `List<String>` of lines) | live | step 3 |
| `ValueBridge` builds Pure values, lists and class instances from typed values; a relation arrives as TDS text | live | step 4 adds the relation from typed rows |
| `Executor` (1,195 lines, the old executor) and `ExecutionResult` (an in-memory result: columns and rows), produced by it and returned by `Execution.execute` | live; 101 test files and 28 main files use the type; one real route (`server/QueryService`), the channel B judges, loaders, and type references | step 7 |
| `Fold.conformValueEgress` (re-types a value root's egress) | live | step 5 deletes with the text wires |
| `LITERAL` slot, `LiteralText.parse`, the Any-kind carrier, mixed `sort` | live for lists | step 6 |
| The Arrow IPC framer | live in the warehouse server, dependency-free, native-image-proven | step 5 shares it |
| `CanonicalRenderSql` (1,085 lines) and each dialect's `FLOAT_CANON`: the database judge's canonical spellings | live (channel B) | stays (R10); simplifies per slot as carriers are typed |
| A Float's String inside a query on Snowflake and BigQuery | the exact-digits expression (correct, ~5 KB per Float printed) | stays (R5); §8 item 3 measures the cheaper form |

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

## 9. Status

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
