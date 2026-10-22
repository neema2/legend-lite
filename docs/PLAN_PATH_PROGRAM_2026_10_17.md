# The plan path program, 2026-10-17: everything executes through a plan

**The user's word (2026-10-17): "i think we should take over the plangen work and fix it here and make it actually
work"; "is it achievable for everything to always run against plan (in engine mode at least)"; "lets write it all down
and then actually do it end to end and lets go!"**

This document is the plan and the record. It continues `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` (the boundary,
its §8 the program as agreed on 2026-10-05, its steps 1 to 3 landed) and takes that program's step 4 -- the callers
switch and the old paths are deleted -- to its end: every execution in legend-lite is a plan made by the planner and
run by the model-free runner. The tenet it serves is the execution tenet of `AGENTS.md`: Java orchestrates, the
DATABASE executes. A runner that holds no model cannot compute a value, so moving every caller onto it is the
mechanical form of that tenet, and every value Java still computes today has nowhere to hide on the plan path: it goes
to the database or is refused by name.

## 0. The rulings this program rests on (the user, 2026-10-17)

| # | ruling | where it bites |
|---|---|---|
| R1 | Everything runs against a plan, in engine mode at least: the server's execute, the query service, the test runners, the PCT lanes, the corpus. | §7, §8 |
| R2 | Both judges stay. The host judge (World 1 of the charter's Clause 2c, the verdict channel) and the database judge (both sides composed into one verdict statement) both read the plan's results; they must agree. | §6 |
| R3 | The plan carries every type: the Pure type and multiplicity, the SQL type the database answers with, and the store's declared type -- each present or explicitly absent, never defaulted. | §3 |
| R4 | The lossless text carrier for a non-primitive value is Pure's own literal grammar, spelled by the database and parsed by Pure. JSON is not required anywhere the engine's API does not demand it. | §4 |
| R5 | The lite plan's representation stays JSON (the decision and its reasons: §2). | §2 |
| R6 | Nothing in this program computes a value outside the database. The database executes, the runner carries, the judges judge. | everywhere |
| R7 | The order: types first, the text carrier second (the audit of 2026-10-17). | §7 |
| R8 | Deletion is mandatory: the legacy executor, the legacy PCT route, the shadow plan and the Java struct-to-instance rebuild all go. A referee overlap -- the old path kept beside the new until the new is proven -- is allowed, and MUST end in deletion. | §7 |
| R9 | The corpus rule and the PCT rule are the same: no roster row and no pin moves unless a verdict genuinely changed, and both judges must agree for a pass to count. | §6, §7 |
| R10 | The engine-compatible plan is as compatible as it can be made: where the engine carries a fact (a column's declared type), ours carries the same fact, read from the plan, never spelled from a table. | §3 |
| R11 | A `let` lives in the database or nowhere (§5): dissolved by the compiler, or a temporary table in the body's workspace; never a value Java holds between statements. | §5 |

## 1. Where we are (measured 2026-10-17)

**Landed.** The planner makes lite plans (`PlanMaker`, `ExecutionPlan`, `PlanJson`; step 2 of the boundary program,
2026-10-09). The runner runs a plan with no model (`exec.PlanRunner`; step 3, 2026-10-10): its values are typed and
checked in the plan's own words, parameters bind through the plan's slots, the one designed hole (a placeholder's type
written by the runner at a recorded place) is `TypeHole`. The server's `executionPlan` endpoint renders the
engine-compatible plan shape; its `execute` still binds parameters through `PureV1Api.boundParameters` and runs the
legacy path (step 7's work). The MongoDB lane runs PCT
channel A end to end through the plan path (`PctPlanRoute`, `TypedQuery.Output.TDS_TEXT`, 2026-10-17): 1,145 of
1,249 tests, every pin named. A differential answers the same query through today's path and through a plan on DuckDB,
H2 and Postgres.

**Still on the legacy executor** (`Execution.execute` / `executeResolved` / `StatementExecutor`), by file in
`core/src/main/java/com/legend`: `server/QueryService.java` (the wire and streaming paths), `test/PureTestRunner.java`
and `test/ServiceTestRunner.java` (Pure test bodies, service tests), `CsvLoad.java`, the verdict arms
(`AssertVerdicts.java`, `DatabaseJudge.java`, `HostJudge.java`, `SqlTextVerdicts.java`, `AssertErrorNative.java`),
`BodyCompiler.java`, `PlanAllocations.java`, `PlanEnvelope.java`, `SeedSqlForms.java`, `exec/CanonRider.java`,
`exec/InstanceIds.java`, and three lowering-side callers (`builtin/Pure.java`, `lowering/Scalars.java`,
`lowering/CollectionLanes.java`). Twenty files; three of them matter (the query service and the two test runners), the
rest follow.

**What the plan carries today** (`executionplan/ExecutionPlan.java`): a parameter's name, Pure type, multiplicity,
enum values, bind type and hole spellings; a tabular result's column name, Pure type and SQL type (`TdsColumn`); a text
result's column name and Pure type only (`Column`); the typed SQL tree in memory (`Sql.statement`) but NOT in the lite
JSON ("a plan read back has none", `PlanJson`'s header); no store-declared type anywhere. The engine-compatible
renderer spells a column's `relationalType` from the Pure type through a table (`plan/UpstreamRelationType.
relationalSpelling`: every String and every enumeration `VARCHAR(1024)`, a Decimal `FLOAT`), with the comment "lite
has no physical provenance". The engine's own TDS header carries the declared type (`VARCHAR(30)`; the corpus goldens
show it).

**How upstream runs PCT** (read in `finos/legend-engine` at `3593037`, a blobless read-only clone at
`/home/user/finos/legend-engine`; spec and test input only, per the reference-checkout tenet). legend-pure's own lanes
(`testAdapterForInMemoryExecution`) are `$f->eval()`: no plan. legend-engine's relational lanes
(`core_external_test_connection/pct_relational.pure`, `testAdapterForRelationalExecution`) go through plan generation
every time: the test function is rewritten so its literal data becomes CSV-seeded tables (constant pre-evaluation off on
purpose), `executionPlan(...)` builds the plan, the setup nodes are prepended, the plan is bound to the Java platform,
serialised to the protocol JSON and run by `executePlanAsJSON` (the Java `PlanExecutor`), and the result JSON is read
back by the function's declared return type (`resultToType`): a List, Map, Variant or to-many primitive arrives as JSON
text in one cell and is parsed with `fromJson`; a class result through `processRelationalClassResult`. Upstream's DuckDB
manifest (`core_relational_duckdb_pct/.../EssentialFunctions_manifest.json`, `GrammarFunctions_manifest.json`) excludes
`testFindInstance`, `testFilterInstance`, `testEqNonPrimitive`, `testAssignNewInstance`, `testConcatenateMixedType` and
`testLetChainedWithAnotherFunction`: the instance-identity tests cross no wire upstream either.

**The corpus.** 2,798 `<<test.Test>>` functions of legend-engine's `core_relational`, consumed as data
(`spec/.../rcorpus/Corpus.java`), run by `PureTestRunner` statement by statement through `Execution.executeResolved`
after the package's setups; the gate is a roster per database (`spec/src/test/resources/rcorpus/*-roster.txt`: 108
known DuckDB failures, 14 skipped) that has not moved in weeks.

## 2. The representation: JSON stays (decided 2026-10-17)

The question put: "decide if you want new plan in json too or some other representation that might be
better/faster/cheaper/more bulletproof -- if json is already good then no need to change."

**JSON stays.** The reasons, measured against the alternatives (a binary encoding such as CBOR or protobuf, Java
serialisation, a textual DSL of our own):

1. **Nothing numeric travels as a JSON number.** A plan's values are the statement's text, typed parameter slots, and
   type spellings; the one place JSON is weak (Long and Decimal fidelity) never arises, so a binary format's one real
   advantage buys nothing.
2. **Plans are small** (kilobytes), made once per query and run once; encoding cost is not on any measured path.
3. **The writer and the reader exist** (`PlanJson`, format `legend-lite-plan`, versioned since 2026-10-08), every
   `_type` an enum constant read by an exhaustive switch, an unknown tag refused by name; its consumers are the runner
   (`exec/PlanSessions`) and its tests. A second format would be a second reader to keep honest.
4. **Inspectable and diffable.** A plan is read in a terminal, pinned in a test, and compared in a differential; the
   planner also runs in the browser (TeaVM) where JSON is the native carrier.
5. **The engine's protocol is JSON**, and the compatibility mode (the boundary program's last phase) is a projection of
   the lite plan; one carrier for both keeps the projection a function, not a translation.

**What makes it bulletproof is content and a gate, not a format**: the typed SQL tree written into the lite JSON (§3,
format version 5; the lite JSON is at 4 today), and a round-trip gate -- every plan the tests make, written to JSON, read back, and rendered again,
must render the same SQL on every dialect and run the same in the runner. The gate is the bulletproofing; a binary
format would not add to it.

## 3. The types in the plan (R3)

**Per result column, three facts**, each present or explicitly `absent`:

| fact | what it answers | who reads it |
|---|---|---|
| `pure` -- the Pure type and multiplicity, as the typer names it | how a value is decoded and printed: the TDS header, Pure's print forms, the host judge's declared-kind equality | the runner, the judges, the text wires |
| `slot` -- the SQL type the database answers with (the lowering's type fact) | the bind kind, the hole spelling, Decimal scale, DATE against TIMESTAMP (the `datePart` fix of 2026-10-17 was this slot type deciding the text) | the runner, the dialects' holes |
| `store` -- the column's declared type in its `###Relational` store, when the value is a store column read through | the engine renderer's `relationalType` without a table; widths and scales the engine shows; the DDL of setup steps; the template a dynamic pivot's value columns inherit | the engine renderer, the corpus goldens, the pivot mode |

A computed column has no store type and says `absent`. The planner asserts the three agree (a store `VARCHAR` behind a
Pure Integer is a model defect the typer refuses; the plan is where the compiler's facts are asserted one last time)
and refuses by name when they do not: invariant 4, no defaulting, on the plan.

**The engine renderer reads `store` where the engine carries the physical type.** Measured on the recorded 4.145.0
answers (`core/src/test/resources/upstream-api/e8-groupby-sort.json`, `e9-groupby-sort.json`; the audit of
2026-10-17): the plan's `resultColumns[].dataType` is the store's declared type for a column read through
(`VARCHAR(32)`) and `""` for a computed one; the TDS builder's `relationalType` is the engine's spelling of the PURE
type even for a store column (`VARCHAR(1024)` for that same `VARCHAR(32)` column; `pureTypeToDataTypeMap`,
`pureToRelational.pure:48-56`, probed 2026-09-27). So `resultColumns.dataType` reads `store` (the parity test's
recorded difference 2 closes) and the builder keeps `UpstreamRelationType.relationalSpelling`, which is the engine's
own rule, not a lite guess; an unprobed type stays refused.

**Parameters** already carry theirs (Pure type, multiplicity, enum values, bind type, hole spellings).

**The typed SQL tree in the lite JSON** (format version 5): the MIR (`SqlQuery`, `SqlSource`, `SqlExpr`, `SqlAgg`,
`SqlType`, `DateFmt`, `TypeFact`) written as JSON, every variant its own `_type`, read back by exhaustive switches. A
plan read back is then fully typed: re-renderable for another dialect, diffable, inspectable, and the engine protocol
renderer becomes a projection of the lite plan (the boundary program's §8, "the lite JSON carries ... the typed SQL
tree"). The gate: §2's round trip.

**Dynamic pivots** need one more piece than the types, because on a text wire the database writes the header line
itself and a single static statement cannot name columns it does not know. The shape is a two-plan sequence: a PROBE
plan that asks the database for the distinct pivot values, then the main plan with an IDENTIFIER HOLE the runner fills
from the probe's rows -- the mechanism the type hole already uses (the dialect writes nothing at a place the writer
records; the runner writes there, before it prepares the statement, what the plan says). The pivot columns' types are
the template column the plan carries (`pure`, `slot`, `store` of the aggregate). The database computes every value;
the runner writes names where the plan recorded a hole. Today's refusal ("a pivot's columns are known only once the
database is asked") retires with the sequence. (Corrected in the audit of 2026-10-17: "names from the result" alone
is not enough.)

**Corrected again at step 4's landing (2026-10-18): no sequence, no hole.** The database names its own pivot's columns:
their names are its keys -- the distinct values of the ON column over the pivot's source, in the order the database
orders the columns -- and each row's values are readable by those names from the row's own JSON
(`SqlExpr.RowJson`, DuckDB's `to_json(alias)`). So the header is a keys subquery, each row's cells a correlated
subquery over the same keys, the types the template's (the typer's dynamic column per USING and the USING's lowered
slot), and the whole text is ONE static statement the database writes (`Render.tdsTextOfDynamicPivot`); the JSON wire
is each row's own JSON (`jsonWireOfRows`). The runner does nothing -- the stronger form of R6. The probe plan and the
identifier hole were never built.

## 4. The text carrier for non-primitive values (R4)

**The problem.** On the text route (the TDS text output, the PCT lanes through the plan) a value root whose type is not
a primitive -- a class instance, a List, a Map, a Pair, `Any`, a `Type` -- has no written form, and upstream's TDS
reader (`TestTDS` in `legend-engine-pure-runtime-java-extension-shared-functions-relation`) refuses any header column
whose type is not a primitive ("Pure Type: 'X' is not supported yet", the text behind 7 MongoDB pins). The legacy route
passes such tests only through ~400 lines of Java rebuilding instances from JDBC structs (`structToInstance`, the PCT
audit's largest compensation): value work in Java, the opposite of R6.

**The carrier: Pure's literal grammar, spelled by the database, parsed by Pure.** The forms:

| value | the cell's text | how it is spelled | how it is read |
|---|---|---|---|
| a class instance | `^my::CO_Person(firstName='Fabrice', lastName='Smith')`, nested instances and collections inside | the TDS writer's concatenation over the instance's columns (the graph shape the JSON wire already lowers); on MongoDB the nested-document writer that writes the Variant JSON today, with Pure's brackets | `compileValueSpecification(text).result->reactivate()` (both natives on the PCT classpath: `compileValueSpecification` is legend-engine's, `legend-engine-pure-functions-unclassified-pure`'s `core_functions_unclassified/meta/compileValueSpecification.pure` with its interpreted native, which the adapter module declares as a dependency; `reactivate` is the platform's, `platform/pure/essential/meta/reflect/reactivate.pure` -- corrected 2026-10-18: the pinned legend-pure has no `compileValueSpecification` of its own) |
| a List, a Pair, a Map | `list([1, 2])`, `pair(1, 'a')`, `newMap([pair('k', 1)])` | the same concatenation over the collection's elements | the same |
| an `Any`-typed value | each element in its own literal spelling by the type fact of its branch: `1`, `1.0`, `1.0D`, `'a'`, `%2015-01-01`, `true` | the TDS writer, by the lowering's type fact per branch (a union's branches are typed) | the same |
| a `Type` or another model element | its path, `my::TestClass` | a string literal | the same; it reactivates to the element |

The adapter already bypasses the TDS reader for value roots and reads lines by the function's return type; a class,
`Any`, `Type` or Pair return type takes the new branch. The input direction already uses this form: the adapter writes
captured instances into the query as `^Class(...)` (`objectToGrammar`, `CapturedInstance`).

**What stays named.** The identity tests (`find` and `filter` returning the instance passed in, `eq` on instances,
`deactivate`, a `new` instance's generic type) compare by reference; no wire carries that, upstream's manifests exclude
the same tests, and they stay pinned by name on every lane with that evidence. The `zip` of empty lists ("Type argument
mismatch for the class Pair") is a typer defect about an empty list's type arguments, fixed in the compiler, not a
carriage question.

## 5. A `let` lives in the database or nowhere (R11)

A Pure test body is not one expression: setups, `let`s, several asserts. On the plan path a body is a SEQUENCE of
plans (the `Sequence` node exists): the setups as DDL and DML plans (`SqlDdl`, `SqlDml`, the session tables and
`SetupStep`s already there), then the statements. A `let` is a name for a value, and the only question is where that
value lives between statements. The tenet allows two answers:

1. **Nowhere: the compiler dissolved it.** `let x = <expr>;` then `assert($x->size() == 3)` becomes
   `assert(<expr>->size() == 3)`: the expression substituted into every statement that uses the name, the database
   computing it where it is used (G½'s β-inlining, applied to statement lets). Inside one statement the uses share a
   CTE; across statements the database recomputes, which is correct for anything deterministic. The default.
2. **In the database: materialized.** The let's own plan runs `CREATE TEMPORARY TABLE __let_x AS <expr>` in the body's
   private workspace (bodies with inline CSV seeds already get one), and every later plan reads it. The value never
   leaves the database. Required when the value must be computed exactly once (a `now()`, a `generateGuid()`, a side
   effect); chosen by the planner when a relation is reused by many statements and recomputing would be wasteful --
   a plan-time choice, which is planning, not Java doing work.

**Never in Java.** No value is fetched after one statement and bound into the next (legend-engine's allocation node
does exactly that; it is a carriage site and we do not build one). The runner runs the sequence and carries nothing
but the final result. A let that can be neither dissolved nor materialized is refused by name; none is known.

**Plan-time lets** are a different thing: a body that writes `let plan = executionPlan(...)` and asserts on the plan's
SQL text binds a value the PLANNER produces (a plan, a SQL text, a type), read only by plan-time verdicts
(`SqlTextVerdicts`). Those live in the planner, as they do today; they are not runtime carriage.

The planner picks between 1 and 2 from two facts it has: whether the expression is deterministic, and how many
statements read the name.

**As built (step 5, 2026-10-18).** Answer 1 is the platform's on both paths, and was before this program: a query
let is dissolved by the expression inliner (G½, `UserCallInliner.inlineBody`, its scope substituting the value where
the name is read), a program let by the statement inliner (`StatementInline`), an inlined function body's let by the
same inliner (`reduceStatements`); no let reaches the lowering, and no value is fetched and bound by Java. A clock
read (`now`, `today`) is one value for the statement on every database, so a let over it dissolves. The one native the
database computes anew at each call, `generateGuid`, is the case answer 2 exists for: a let over it read more than once
cannot be dissolved (each read a new value) and is refused by name (`refuseUndissolvable`) until materialization is
built -- no body in the PCT's 921 or the lanes demanded it (every lane green on the rule), so answer 2 stays
unbuilt, and the refusal is its demand signal. The inliner's remaining known trade stands: a let over a row set with
no total order, read twice, may be two different row sets where Pure's single binding was one (CTE sharing is its
fix); it is a trade, not a wrong value. The judges (§6) do not care which was chosen; they care what they are handed. This is what
the channel B homework sized (`docs/MONGODB_PLAN_2026_10_16.md` §9: single- against multi-statement bodies) and what
`docs/DEFERRED_TEST_EXECUTION.md` charters.

## 6. Both judges on plans (R2)

The charter's Clause 2c (ratified 2026-08-19) names two worlds: World 1, the host adjudication layer (`PureAsserts`,
the verdict channel, pinned by `VerdictChannelRegisterTest`), and World 2, the compiler; verdicts are World 1's job,
in-query computation World 2's, neither reimplements the other. Both judges stay:

- **The host judge** receives each side as a plan's result and decodes it by the plan's declared result type (the Z2
  ruling's carrier-decode bridge), then compares. Nothing computed; read and compared.
- **The database judge** composes both sides into one verdict statement; on the plan path that statement is a plan whose
  one row is the verdict.
- **The agreement gate.** Every assert gets two verdicts from two routes; a disagreement is a finding on a named test
  (a verdict statement's composition bug, a host decode misreading a carrier), never a quiet wrong pass. This is the
  parallel-referee role the host judge's javadoc already describes, stronger with both arms on plans.

**As built (2026-10-18, step 6).** A typed-rows plan (`ExecutionPlan.TdsResult`, the three-fact `Column`) is a
statement that writes each row as one JSON object; the runner decodes each cell by the plan's column alone
(`exec.RowDecode`). The database judge's verdict statement is such a plan whose one row is the verdict (`JudgePlans`,
`VerdictBatch.Runner`); the host judge's sides are such plans too (`JudgePlans.evalSide`: the legacy executor still
PLANS a side exactly as its host route prepared it -- the let prefix, the inliner's splice, the natives' staging, the
store resolution, no wire reconciliation, a literal folded only where that route folded -- until step 8's runners emit
plan sequences and step 11 deletes it), written in the JSON rows wire's CARRIER spelling (each cell as its slot holds
it), decoded by the plan's columns (the slot first; a string token under a numeric slot is the column's own text
unless the Pure type is numeric: a store may declare a column INT over a VARCHAR, and the engine's test store does)
and shaped as the executor shaped them. The byte channel that rode the host side's query (V11's canon rider) is retired:
every side declines it by name, the database judge is the referee. The gate is `ExecuteOptions.JudgeMode.BOTH`
(`AgreementArm`): the host's verdict of record; the database's must agree; a disagreement fails the assert naming both;
a decline, an UNJUDGED outcome or the database arm's own wall is told to the listener and the host decides; a database
statement that errors is a failure. Named residue: a host side whose plan holds a dynamic pivot keeps the legacy route
(step 9); the Any lane cannot tell a SQL NULL cell from a present JSON null in a row's text (the legacy executor
could: it drops the one and keeps the other) -- a null is kept, measured by the corpus at step 10.

## 7. The callers (R1): the order, and what is deleted

1. **Types in the plan** (§3): the three facts per column, the engine renderer on `store`, the typed tree in the lite
   JSON (format 5), the round-trip gate. Lands first because every later step reads it.
2. **Printed values** (PARK-24, the boundary program's step 3b): Float arithmetic in decimal, a whole Float's `.0`, a
   Decimal literal's scale, a DateTime's text. The user's ruling (2026-10-10): an answer is Pure's value, its text the
   served API's. The value root's date precision (2026-10-17) was the first slice.
3. **The text carrier** (§4): the forms, the adapter's reader, a differential case per form on DuckDB against MongoDB.
4. **Dynamic pivots** (§3's last paragraph): names from the result, types from the template.
5. **A `let` in the database or nowhere** (§5): dissolve or materialize, never carry.
6. **Both judges on plans** (§6), the agreement gate.
7. **The query service**: the wire and streaming paths plan and run; `Execution.executeWire` / `executeStreaming`,
   `Execution`'s `wireOn` / `streamOn`, `PureV1Api.boundParameters` and `ConnectionResolver`'s reading of the compiled
   model are DELETED (the boundary program's step 4, rule 15). The differential between the two paths is the gate, and
   is itself deleted with the old path. **Built on `docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`** (ratified
   2026-10-18): the typed session seam, the result as a stream with sinks, per-row formatting for every text format,
   the engine-protocol plan as a rendering of the one plan record; its §8 measurement precedes the design's final
   form, its §9 says what lands in which step.
8. **The test runners**: `PureTestRunner` and `ServiceTestRunner` emit a sequence of plans per body; `CsvLoad` a DML
   plan; the small seams follow.
9. **The PCT lanes**: `PctBackend.throughThePlan` for every lane, the DuckDB lane first (its shadow run agreed on 81%
   before this program; the pins say the rest); the legacy route (`PctExecuteNative`'s query-service path,
   `ExecuteLegendLiteQuery`'s struct-to-instance bijection) deleted; `PctShadowPlan` deleted with it.
10. **The corpus**: the 2,798 bodies through plan sequences, both judges; the rosters are the gate and no row moves
    unless a verdict genuinely changed.
11. **The deletions**: `StatementExecutor` and what only it called; `JavaEvalLedgerTest`'s pins only go down; the
    `exec` guard reaches zero (the boundary program's §8: `exec` depends only on `//base`, `//json`, the plan records and
    `java.sql`).

**Step 9's named residue, each with its exit (2026-10-19).** A residue pinned by name is loud both ways (the pin fails
when the test passes or its message moves) but can sit; so each one names the step that resolves it, and step 11
does not close while any of them has neither a fix nor a recorded ruling by the user:
- **S82, a type value's namesake** (`testLetChainedWithAnotherFunction`): resolved by the path convention -- a type
  value carries its path on the wire, every printer takes the name, equality compares paths (a namesake equality
  bug today) -- landed with the corpus on plans (steps 8 and 10), where its fifteen comparing bodies judge it.
- **S81, a heterogeneous class collection** (`testConcatenateTypeInference`): resolved by the instance carrier --
  the class riding every JSON-carried instance (`__type`) and a layout-driven speller from JSON dispatching on it
  over the ancestor's subclasses -- when a corpus body or a user needs it; else a recorded ruling that it stays.
- **A Float literal beyond a double** (`testBigFloatAbs`): the value lane keeping a DECIMAL slot exact under a Float
  root; the judges' numeric canon decides whether it lands or is ruled engine parity (the engine's adapters pin it).
- **The inverse trigonometric NaN** (`testArcSineError`, `testArcCosineError`): engine parity (its relational
  adapters ledger both; H2 answers NaN); stays unless ruled otherwise.
- **An uncast dynamic pivot's text on a dialect without a row-to-JSON form** (ten relation cases per lane, the
  `testPivot_*` family without a cast; found 2026-10-19 on H2 and SQLite, "a row as JSON reached a dialect without a
  row-to-JSON form", "variant navigation reached a dialect without JSON support"): the database builds such a
  pivot's TDS text from each row's own JSON (step 4), a form only DuckDB's dialect writes today. Resolved by each
  dialect's own row-to-JSON arm where the database has one (H2 and MariaDB's `JSON_OBJECT`, SQLite's `json_object`,
  Postgres' `row_to_json`, the others' measured when the arm lands), a render item per dialect; a lane whose database
  has none pins them by name (the engine refuses every pivot on H2, S101). The cast form is not residue: it answers
  on every dialect since 2026-10-19 (S102).
- **A declared SERVER connection opened by the plan's runner with its credentials** (found 2026-10-19 by the server
  lanes: `Sessions.openingFor` admits no credential -- every authentication but `NoAuth`, `DefaultH2` and `TestAuth`
  is refused by name -- and has no opening for Trino, ClickHouse, Snowflake, Databricks or BigQuery, so a plan on
  such a database has no session of its own, and on Postgres, MariaDB and MySQL the driver logs in as the OS user).
  The PCT runs a server lane's plan on the harness's own session meanwhile (`PctBackend.planSessions`, as the legacy
  route did), which judges everything but the opening. Resolved by legend-engine's own form: the connection's
  authentication (`UserNamePassword` with vault references, `ApiToken`, `SnowflakePublic`,
  `GCPApplicationDefaultCredentials`) resolved through a vault the host configures (properties, environment, a
  secrets service), the driver properties built from it, and an `Opening.Url` arm per server type from its
  specification; the MongoDB connector's vault reference for its password is the one precedent. A product item of
  the database owner, before a user's plan runs on a server.

**What never goes through a plan, and should not**: compiling a model, the language server, diagrams, tests that
assert a compile error, the corpus buckets that exercise the engine's own machinery (routing internals,
pre-evaluation, protocol transforms, dynamic compilation), and the PCT identity tests.

## 8. How each step lands

- Announced here (a dated line in §9) and in `docs/IN_FLIGHT.md`'s thirteenth line before it starts.
- The local gate (`bazel test --lockfile_mode=error //gates:local`), the step's own judges named in the step, one full
  CI on the branch, then pushed; the GATES entry names what judged it.
- A guard pin, roster row or allowlist entry moves only with a dated justification naming this program.
- Every deliberate difference from legend-pure or legend-engine is a `docs/SEMANTICS_REGISTER.md` row; "deliberate
  differences" that are defects are fixed, not registered.
- A plan never references a harness; the plan path serves the product, the PCT and the corpus are its judges.

## 9. Status

- **2026-10-20, the parity phase, second batch: twenty-five more lane pins removed** (GATES "the parity phase, second
  batch"): Jaro-Winkler on MySQL, MariaDB and SQLite; MongoDB's dynamic pivot through the probe plan (its markers bound
  by place, the keyed plan's cells named in the plan, JSON v9) and its divide as doubles; the metamodel's tables inline
  where a database has no temporary table (Trino, MongoDB); MD5, SHA-1, SHA-256 and `hashCode` written in SQLite's SQL;
  ClickHouse's LEFT join with a residual as its matches. ClickHouse's date arithmetic past its years now raises by name
  (six pins re-recorded from a silent clamped value). Per lane: MongoDB 12, SQLite 7, MySQL 2, MariaDB 2, Trino 1,
  ClickHouse 1. Next: a Variant's key order on MySQL and Postgres (a carrier change), a named zone where the database
  has no zone rules (a design question put to the user), then the SQL optimization passes.
- **2026-10-20, the parity phase, first batch: fifty-two lane pins removed at their causes** (GATES "the parity phase,
  first batch"; the user's order of the day, "before we move on we need to have 100% parity on PCT across all
  backends"): `format`'s argument list lowered element by element (658,608 characters of SQL to 1,596 on DuckDB); a
  Float root of exact-digit literals kept DECIMAL; SHA-1 as pgcrypto's `digest()` on Postgres; a windowed string
  aggregation on MariaDB and MySQL as a correlated subquery over the FROM numbered; a window call inside an expression
  computed below it on MySQL; an ordered derived table given a LIMIT on MariaDB and MySQL; two dates without a time
  compared on their day numbers; Trino's unset struct fields typed and its ROWs compared null-safe; MongoDB's raising
  operators' literal operands read through the row and its null tests by type; on H2 a lateral over a constant list as
  the list's rows and a literal list's filtered length as a sum of counts. Per lane: MySQL 15, MariaDB 14, MongoDB 8,
  H2 5, Trino 5, Postgres 2, ClickHouse 1, DuckDB 1, SQLite 1. Channel B's server lanes stay below their floors as
  measured at the merged head (step 10, the judges on plans, their home). Next, batch 2: Jaro-Winkler on MySQL,
  MariaDB and SQLite; MongoDB's dynamic pivot through the probe plan (positional markers); the metamodel's tables inline
  where a database has no temporary table (Trino, MongoDB); MongoDB's decimal divide as doubles; then the variant key
  order (MySQL, Postgres), ClickHouse's outer join with a residual condition, SQLite's digests.
- **2026-10-20, a dynamic pivot's probe plan, on every database** (GATES "a dynamic pivot's probe plan"; register S103;
  the user's rulings of the day: the probe plan, a plan step whose rows fill the next statement's per-key fragment,
  and "before we move on we need to have 100% parity on PCT across all backends", then passes of deep SQL
  optimization): `ExecutionPlan.Keyed` -- a keys probe, a body with per-key regions the runner writes once per key
  binding each key's text, the outputs with their facts, one snapshot, the database's column limit; every output,
  every database but MongoDB; the row-as-JSON form and its three builders deleted; plan JSON format 8. The ten uncast
  pivots answer on H2, SQLite and the server lanes this container runs, their pins removed. Next, before step 7: PCT
  parity on every backend (the triage of 610 pins: what is neither the engine's failure on that database nor a proven
  limit of it is fixed, the merged head's MongoDB `testFormatFloat` first), then the SQL optimization passes (the
  Cypher line's lesson first: a value read more than once computed once).
- **2026-10-20, WITHDRAWN the same day (the user: "what if I just want the output of a pivot as a relation?"):**
  the cross join packs each group's cells into one text value, so it cannot give a pivot's result as a relation, which
  is the main consumer; the premise below ("a dynamic pivot's only consumers are the serialized outputs") was wrong.
  Nothing landed. What was announced:
  **the dynamic pivot's outputs by the cross join, on every engine** (the user's decision after
  the pivot discussion of 2026-10-19/20; §7 step 9's residue "the uncast dynamic pivot's text road"): a dynamic pivot's
  only consumers are the serialized outputs (no Pure code names its generated columns without a cast, and a cast
  makes it static; DataCube pivots by its own two ordinary plans), so the TDS text, the CSV and the JSON wires are
  built from the pivot's SOURCE as cells (grouped by the static columns and the key, each aggregate once), keys and
  groups, the groups cross-joined with the keys and left-joined to the cells, one ordered string aggregate per line
  -- one statement, existing MIR, no JSON navigation, no probe, no row-as-JSON; DuckDB and Snowflake included (native
  PIVOT stays the spelling of a STATIC pivot where the engine has it). The row-as-JSON builder and `SqlExpr.RowJson`
  go; the ten uncast-pivot pins per lane go where the lane runs here. Judged by the DuckDB lane (exact), H2's and
  SQLite's relation suites, `PlanMakerTest`, the core lanes.
- **2026-10-19, step 9's merge completed: the trino-h2 branch in, the guard lanes reconciled, the verdicts on the
  final head** (GATES "step 9's merge completed"): the sixth branch merged (`1616963`: Trino 5 of 5 exact on its pins,
  H2 469 of 469 with the ten uncast pivots pinned, the harness's Trino session without PREPARE, the platform's Trino
  opening reviewed and kept within §7's credentials residue); channel B on the two sibling containers measured, floors
  unmoved (Databricks relation 457 of a 459 floor: distinct's unresolved column, two datatype mismatches, `typeof` in
  the judge statement; Trino essential 323, relation 386, standard 203: 84 "the judges disagree" with equal values on
  the typed-rows judge seam, `typeof`, Trino's own date and ROW refusals -- the parent's tree measures the same); the
  guard lanes reconciled with dated reasons (`4e35539`). Verdicts on `4e35539`: `//gates:core` 45 of 45 (the six
  Postgres lanes under the unprivileged wrapper), the PCT lanes this container runs 23 of 23 (DuckDB exact, H2 469,
  SQLite 469, channel B at its floors), both corpus lanes exact. Open: the Snowflake and BigQuery lanes (their
  containers refused the credential read; the user's call), the pivot's one-query form (the user's decision), step 10
  (the judges on plans, the channel B residue's home), step 8.
- **2026-10-19, step 9: the siblings' lanes merged; channel B on the server lanes measured, four causes fixed, the
  rest named** (GATES "the siblings' lanes merged"): channel A green with pins on every lane (DuckDB exact, H2 but
  the ten uncast pivots until the trino-h2 branch lands, Postgres, SQLite, MongoDB, MariaDB, MySQL, ClickHouse,
  Databricks, Trino on its branch); channel B found below its floors on every server lane, this program's doing
  (steps 6 and 7, judged by the local gate's DuckDB and H2 alone): the typed rows' map and jsonb arms, the judges'
  server version, the leniency's doubles, Databricks' ordered ranking window, all fixed; the rest named per lane with
  its cause, no floor moved, the judges on plans (step 10) its home. Next: the trino-h2 merge, the corpus lanes and
  the core gate on the merged head.
- **2026-10-19, step 9: the system metamodel store on a server engine as the session's own temporary tables**
  (GATES "the system metamodel store on a server engine"): `Class.all()` on every server lane was refused by the
  runner (the plan's setup on the user's database); the referenced tables are now the target's session statements on
  a server engine (the legacy system database's form, the user's design of 2026-10-11), read by the query under their
  session names, the setup untouched; Trino refuses by name. Verdicts: the plan maker's test on DuckDB, Postgres and
  Trino, four core lanes, the DuckDB grammar suite, and `testBasic` passing on the embedded MariaDB (its grammar
  suite 134 of 137 here); the other server lanes judge it at the siblings' merge.
- **2026-10-19, step 9: every lane flipped, a plan written for the session's server version, six sibling sessions
  started, the cast over a dynamic pivot landed on every dialect** (GATES "step 9: every PCT lane flipped"):
  `throughThePlan` true for every lane (`83f2810`); the H2 lane's 414 version refusals fixed by planning for the
  session's server (`2b7bad3`); the siblings (`docs/STEP9_LANE_BRIEF_2026_10_19.md`) burn their lanes down on their
  own branches while this session landed the shared item: the keys a cast names are the pivot's named values and
  every static emulation pivots on them (S102), so the four composition cases pass on every lane. Verdicts: nine core
  lanes pass, DuckDB's relation suite exact, H2 459 of 469 and SQLite's relation suite 459 of 469 (the ten uncast
  dynamic pivots each, §7's residue). Next: the siblings' merge, the corpus lanes and the core gate on it.
- **2026-10-18, step 9, first landing: the DuckDB PCT lane exact on the plan route** (GATES "step 9, first
  landing"): the flipped lane's first run failed 12 of 1,249; fixed at the root: a Number-typed value read by the
  wire header's type and a Number root labelled by its cell's own numeric slot, its Decimal spelled with its D by the
  text writer (six), a type value's path from the static type where it names one class (one), the system metamodel store's tables written into the
  plan's setup by the plan maker (`Class.all()`); pinned by name with the reason: a Float literal beyond a double, the
  inverse trigonometric NaN (the engine's adapters ledger them), a heterogeneous class collection (S81), a type's
  namesake (S82). Verdicts:
  1,249 of 1,249, channel B 5 of 5, the core gate 45 of 45, both corpus lanes exact. Next: the other JDBC lanes.
- **2026-10-18, step 9 announced and begun: the DuckDB PCT lane flipped onto the plan route** (the user: "flip
  and let's fix everything", after the session's account of steps 1 to 7 switching no consumer). `PctBackend
  .throughThePlan` is true for the DuckDB lane as for MongoDB's: every channel A case planned to Pure's TDS text and
  run by the plan runner (`PctPlanRoute`), the legacy route no longer taken there. The first run's failures are the
  work list, fixed at their root in the plan path; a residue is pinned by name, shrink-only. The other JDBC lanes
  follow once this one is exact; the shadow plan is deleted with the legacy route (R8). Step 7's second half
  (the service's switch, the whole-text wires, H2's lazy execution, the streaming gate) follows steps 8 to 10.
- **2026-10-18, the pivot homework against upstream's real code** (the user's question after step 7's first
  landing; `docs/PIVOT_UPSTREAM_HOMEWORK_2026_10_18.md`; GATES "The pivot homework"): the engine's generated column
  name carries the single quotes everywhere, only a cast pivot executes there, H2 refuses every pivot; the group rule,
  the absent cast key and the composite key hold. The defect found: the CSV wire's header and the JSON wires' keys
  printed the bare physical name while the TDS text presented the engine's -- fixed, every wire presents the name as
  the engine labels it, and the dynamic pivot's JSON wire is built from its cells as its CSV is (step 4's named
  residue retired). Register S99 to S101. Verdicts: the core gate 45 of 45, PCT channels A and B 11 of 11, both
  corpus lanes exact (every judge 0). Step 7 continues as announced below.
- **2026-10-18, step 7 announced; its first landing: the judges onto the typed seam, the dynamic pivot's holes**
  (GATES "step 7, first landing"): `Statements.rows` (typed cells by the plan's columns) on JDBC (`exec.JdbcDecode`)
  and MongoDB (`exec.MongoDbDecode`); `PlanRunner.rows` through it; the judges' plans the statements themselves, the
  JSON rows wire's carrier spellings deleted; a timestamp read carries the engine's nine digits, trimmed in the
  judges' value lane as its database canon is; the Float that is no number spelled `NaN`; the typer removes every
  column a pivot's aggregate reads, and a cast over a dynamic pivot projects the columns it names; the structured
  slots' spelling read back in one place. Verdicts: the core gate 45 of 45 (the six Postgres targets under the
  unprivileged wrapper), PCT channels A and B 11 of 11, the H2 corpus lane exact, the DuckDB lane exact but for its
  host engine-order register, six bodies fewer by an accounting correction (re-pinned with the reason). Next in step 7: the query
  service's switch and the deletions (§7), the whole-text wires retired, the H2 session's lazy execution (D2), the
  streaming gate.
- **2026-10-18, the §8 measurement taken** (GATES "The plan and the result, the §8 measurement"; the design note's
  §8.1): the seam's cost per row on DuckDB (10 million rows) and H2 (2 million) by each path a result can cross the
  session seam, the whole text's ceiling under a 256 MB heap, the plans' sizes (452 judged plans; the corpus's
  16,810 statements); Arrow not measured (the build has no Java Arrow dependency; adding one is a decision of its
  own). Decided: the typed seam lands in step 7 (D1); per-row formatting replaces the whole text (F1, F2); an H2
  session takes lazy execution (D2, new); no compression and no binary format (P4 stands). The measurement is a
  manual target (`//core:seam_benchmark`, `//core:seam_benchmark_small_heap`), never a gate. Step 7's first landing
  is next.
- **2026-10-18, the plan and the result ratified** (`docs/PLAN_AND_RESULT_MODEL_2026_10_18.md`, with the user): one
  plan record and three renderers (the tree only when asked; JSON stays, no binary format, compression only with a
  measured number); one result model, a stream of rows (typed cells, a text row, a document) consumed by one sink,
  the runner accumulating nothing; the session seam text in, rows out (D12 amended); per-row formatting for every
  text format, the whole-text wires retired; the fast path with no plan JSON and no data held; the judges onto the
  typed seam as step 7's first landing; the §8 measurement before the design's final form. Found on the way, from
  the channel A shadow run on DuckDB (2,621 cases, 74% textual agreement, about 91% once the text carrier's own
  spellings are read as values): the Float text writes `nan.0` for NaN, and a dynamic pivot through the TDS text
  output has no template for its columns; both go into step 7's first landing. Step 7 is announced when its
  first landing is cut.
- **2026-10-18, step 6 landed: both judges on plans, the agreement gate** (GATES "step 6, both judges on plans"): the
  runner's typed rows, the verdict a plan's row, each host side a plan's result, `JudgeMode.BOTH` with its gate;
  channel B under the gate found two of step 2's unjudged cuts (a TDS DateTime cell's three digits, a sort key the JSON
  rows wire dropped) and one raw wall of the database judge, all fixed. The corpus lanes on DuckDB and H2 (host and database passes) and `//gates:core`,
  the Postgres targets and the four PCT lanes are green on the final build; the gate on DuckDB is EXACT with 0
  disagreements, on H2 it loses the database arm's registered H2 errors (GATES). Next: step 7, the query
  service (§7).
- **2026-10-18, step 6 announced: both judges on plans, the agreement gate** (§6): the runner reads typed rows
  (`ExecutionPlan.TdsResult` on the three-fact `Column`: each row one JSON object the database writes, decoded by
  the plan's columns in `exec.RowDecode`, format 6); the database judge's verdict statement is such a plan whose one
  row is the verdict (`JudgePlans`, `VerdictBatch.Runner`); the host judge receives each side as a plan's result
  decoded by its declared types (`JudgePlans.evalSide`, the legacy executor still planning the side); the agreement
  gate is `ExecuteOptions.JudgeMode.BOTH` (`AgreementArm`: the host's verdict of record, the database's must agree,
  a disagreement a failure naming both). Judges: `RowDecodeTest`, `AgreementArmTest`, `JudgesOnPlansTest` (both
  judges on DuckDB and H2), the core targets, channel B's lanes, the corpus in `both` mode measured.
- **2026-10-17** -- the program written (this document), the rulings R1 to R6 recorded, the representation decided
  (JSON stays, §2). Step 1 announced: types in the plan.
- **2026-10-18, step 5 landed: the lets** (GATES "step 5, the lets"): dissolution measured as the rule already in
  force on both paths (the inliners; §5 "as built"); the one undissolvable case -- a `generateGuid` let read more
  than once -- refused by name in the inliner; materialization unbuilt, undemanded; `LetsTest` (new). Next: step 6,
  both judges on plans (§6).
- **2026-10-18, step 4 landed: dynamic pivots** (GATES "step 4, dynamic pivots"): announced and landed in one
  landing, as its design shrank to one MIR expression (`SqlExpr.RowJson`) and one wrapper: the database names its own
  pivot's columns (§3, corrected), so the plan path's refusal retires; judged against the legacy probe route's text
  (`PlanMakerTest.aDynamicPivotsTextIsBuiltByTheDatabase_asTheLegacyProbeBuildsIt`). Next: step 5, the lets (§5).
- **2026-10-18, step 3 landed: the text carrier** (GATES "step 3, the text carrier"): every form spelled by the
  database and read by Pure; the MongoDB lane's pins 29 deleted and 9 moved to what stays named (the identity tests,
  a user element by its simple name, an instance past the layout's depth); the text-carried date's `+0000` corrected
  (after seconds only, never twice); `PlanJson` reads a plan as deep as it nests. Next: step 4, dynamic pivots.
- **2026-10-18, step 3 announced: the text carrier** (§4): the forms spelled by the database in
  `lowering/LiteralSpelling.pureValueText` (a class instance over its canonical layout, a Pair, a List, a Map, a
  Type's path, an enum value's path inside a value; the TDS text's cell arm in `lowering/Render.tdsTextCell`; the
  classes' layouts handed through `TdsTextWrap.wrap` by the plan maker and the legacy executor), the adapter's
  reader (`pct_adapter.pure`: the TDS reader for relations alone; an enumeration by `extractEnumValue`; every other
  value `compileValueSpecification(text).result->reactivate()`, the adapter module depending on
  `core_functions_unclassified`), a differential case per form (`MongoDbDifferentialTest`), `TextCarrierTest` (new)
  on DuckDB; judged by the MongoDB lane (its pins on "Pure Type: 'X' is not supported yet", `Any`, `Pair` and the
  user elements by simple name expected to go) and the DuckDB and H2 lanes (unchanged: the legacy route).
- **2026-10-18, step 2, `pure/v1`'s text** (GATES "step 2, `pure/v1`'s text"): the JSON wire has a spelling
  (`WireRender.Spelling`, `PURE` on lite's outputs, `ENGINE` at the two engine-API seams); two more owners,
  `floatPrintJava` (Java's layout over the database's digits) and `dateTimeTextEngine` (nine digits, `+0000`); the
  values Pure's (register S97, S98). The design's five steps are landed; PARK-24 closes. Step 2 is complete; step 3
  (the text carrier) is next.
- **2026-10-18, step 2, the DateTime's text** (GATES "step 2, the DateTime's text"): one owner,
  `LiteralSpelling.dateTimeText` (`T`, the fraction read at nine digits and trimmed of trailing zeros by the database,
  `+0000`), on the CSV, the JSON wires, Pure's TDS text and `toString`; a text-carried date value takes `+0000` when
  timed. The matrix's last 44 pins deleted: 192 of 192 cells write Pure's text on DuckDB and H2. The design's steps 2
  to 4 are landed; step 5 (`pure/v1`'s text in the engine's conventions) is next, then the program's step 3.
- **2026-10-18, step 2, the Float's text and the JSON wires from their cells** (GATES "step 2, the Float's text"):
  `floatPrint` finds the exponent in either case (H2's `E`); the JSON wires are assembled from their cells' texts, a
  Float's Pure's, a Decimal's its own scale, the rest the database's own JSON token read off a one-element array --
  32 pins deleted (76 -> 44). What is left is the DateTime's text: the next landing, one owner (`T`, the fraction's
  digits with trailing zeros trimmed, `+0000`) for every output, the text-carried value root included.
- **2026-10-18, step 2, the matrix measured** (GATES "The plan path program, step 2 begins"): `OutputMatrixTest`, 192
  cells on DuckDB and H2; 116 write Pure's text, 76 pinned by name (the JSON wires' native number tokens, H2's
  exponent forms, every DateTime text but the TDS text's whole second and the TDS text's millisecond column, a
  binder error on DuckDB's CSV wire for a value root's fractional date-time). The landings that follow delete pins:
  the Float's text owner, the Decimal's, the DateTime's, then `pure/v1`'s text.
- **2026-10-18, step 2 announced: the printed values** (PARK-24; `docs/OUTPUT_LAYER_DESIGN_2026_10_10.md`, whose
  five steps this step is): first the matrix, measured -- a test recording, for each kind (a whole, a fractional and
  an extreme Float; a Decimal literal and after arithmetic; a DateTime with no, three and nine fraction digits; a
  StrictDate) and each output (CSV, JSON, a stream of JSON rows, Pure's TDS text, `pure/v1` execute) on DuckDB and H2,
  lite's text beside interpreted Pure's (`probes/engine-reference/pure-interpreted-results.txt`) and the engine's;
  today's deviations pinned by name and shrink-only, so each later landing (the Float's text owner, the Decimal's, the
  DateTime's, `pure/v1`'s) is judged by the pins it removes.
- **2026-10-18, step 1 landed** (the three facts per column, the typed tree in the lite JSON, the round-trip gate;
  GATES "The plan path program, step 1"). The tree: `MirJson`, every MIR variant its own tag, written into the
  plan's `tree` and read back; the gate: `testcases/PlanRoundTrip`, every plan the plan maker's and the MongoDB tests
  make reads back equal and renders the same statement. Measured on the way: a column no output is named after is
  one of three said things (a pivot's generated column, a nested column flattened into prefixed outputs, a value
  root's one output), found by the integration suite's first run.
- **2026-10-17, step 1a** (the three facts per column): `ExecutionPlan.Column` carries `type`, `multiplicity`,
  `slot` and `store` (null when computed); `ColumnFacts` reads the slot from the lowered query's outputs and the store
  type by provenance (a projection reading a base table's column through subselects, CTEs, joins and a pivot's keys;
  `SqlExpr.StoredRead` carries it outright); `PlanColumns` joins them with the typer's columns; `PlanJson` format 5;
  `QueryPlan` carries the columns and `PureV1Api`'s `resultColumns.dataType` is the store type, so the parity test's
  recorded difference 2 is closed and the engine's recorded plan is compared verbatim there. Judged by
  `PlanMakerTest.aColumnCarriesItsThreeTypeFacts`, `PlanJsonTest`, `PureV1ApiTest.e9`, the local gate's core lane and
  `//core:mongodb_plan_test`.
- **2026-10-17, the audit with the user** -- R7 to R11 recorded; four corrections (the server's `execute` is still on
  the legacy path; the lite JSON's next version is 5; a dynamic pivot is a probe plan and an identifier hole; a `let`
  is dissolved or materialized, never carried). Step 1a starts: the three facts per column, the engine renderer on the
  store's declared type.
