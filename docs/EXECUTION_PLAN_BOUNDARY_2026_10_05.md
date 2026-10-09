# The execution plan as the boundary between planner and executor (2026-10-05)

**Status: DESIGN AGREED by the user 2026-10-05 (§5, §7) — no code yet; §6 homework before code.** Asked by the user 2026-10-05 ("Is TypedQuery our version of
legend-engine Plan? Want to make sure we can execute old legend-engine Plans"). The user moved the rebuild's W6.1
(staged plan IR) and W6.2 (the runner) to this line (the database-owner line,
docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md) the same day. The user agreed §5 P1, P2, P4, P5,
P6 and the decisions in §7 the same day.

## 1. The question, precisely

After C2a/C2b the planner (`//core:planner`, no database) and the executor (`com.legend.Execution`) are separate
libraries, but **nothing passes between them that could be stored, sent or replayed**: `Execution.execute(model text,
query text, runtime, …)` compiles again, and `StatementExecutor` re-runs the compile phases per statement. legend-engine's
boundary is an artifact — the **execution plan** — that `generatePlan` produces and `executePlan` runs later, with no
model. We want the same boundary, in a form that lets lite run **legend-engine's own plans** as well as ours.

`TypedQuery` (C2b) is NOT a plan: it is the planner's in-memory handle on a typed query, tied to its compiled model —
what a plan is made FROM. `plan.QueryPlan(sql, rootType, shape)` is too thin to be one (no connection, no node tree, no
parameters, not serialisable). `PureV1Api.executionPlan` EMITS legend-engine-shaped JSON for the simplest TDS case only;
lite has no `executePlan`.

## 2. What legend-engine's plan is (from source, file:line in the homework transcript)

- `SingleExecutionPlan` (`_type: "simple"`): `rootExecutionNode`, `templateFunctions` (FreeMarker functions prepended to
  every template), `globalImplementationSupport` (shared generated Java classes), `serializer`, `authDependent`,
  `kerberos`. `CompositeExecutionPlan` (`"composite"`) picks one single plan by a parameter's value.
- Every node: `resultType`, `executionNodes` (children), `resultSizeRange`, `requiredVariableInputs`, `implementation`
  (generated Java: `{_type: "java", classes[{package, name, source|byteCode}], executionClassFullName, …}`).
- **Nodes that run WITHOUT Java** (legend-engine's own executor): `sql`, `relationalTdsInstantiation`,
  `relationalDataTypeInstantiation`, `relationalRelationDataInstantiation`, `relationalBlock`, `sequence`, `allocation`,
  `constant`, `freeMarkerConditionalExecutionNode`, `function-parameters-validation`, `error`, `varResolution`,
  `platformMerge`.
- **Nodes that NEED compiled generated Java**: `platform`/`pureExp` (storeless and M2M expressions),
  `relationalClassInstantiation` (any class result), every graph-fetch node, in-memory/M2M nodes,
  `createAndPopulateTempTable` (its non-variant path), external format, service store, Mongo, Deephaven.
- A `sql` node carries its **whole connection** (`RelationalDatabaseConnection`: `type`, `datasourceSpecification`
  `h2Local|static|h2Embedded|duckDB|…`, `authenticationStrategy`), `sqlQuery` and `sqlComment` **FreeMarker templates**
  (`'${name?replace("'", "''")}'`, `in (${inFilterClause_x})`), and `resultColumns`.
- Result types: `tds` (`tdsColumns[{name, type, relationalType, enumMapping, doc}]`), `class`, `partialClass`,
  `dataType`, `void`, `relation`.

## 3. Real plans from the pinned engine (4.145.0 on :6300; `docs/execution-plan-boundary-2026-10-05/plans/`)

One model (relational on H2 + a JSON M2M mapping), 13 queries, every one planned:

| query | plan tree | generated Java |
|---|---|---|
| project (TDS), relation accessor, group-by, sort/limit, `let`, relation group-by | `relationalTdsInstantiation` → `sql` | none |
| a `String[1]` parameter | `sequence` → `function-parameters-validation`, `relationalTdsInstantiation` → `sql` | none |
| a `String[*]` parameter in `in(...)` | `relationalBlock` → validation, `allocation` → `freeMarkerConditional` (temp table or inline list), `relationalTdsInstantiation` → `sql` | 3 classes (temp-table loader + string helpers) |
| a class query (`Person.all()`) | `relationalClassInstantiation` (Java) → `sql` | 5 classes (~30 KB: `Person_Impl`, `Helper`, …) |
| graph fetch (flat, nested) | `platform` (Java) → `storeMappingGlobalGraphFetch` → temp-table graph-fetch nodes | 6–10 classes |
| M2M graph fetch | `platform` (Java) → `inMemoryRootGraphFetch`, `storeStreamReading` | 10 classes |
| storeless `1 + 1` | `platform` (Java) | 1 class |

So: **TDS and relation queries are pure SQL plans; anything producing objects is Java.** 22 more serialized plans are in
legend-engine's test resources (e.g. `SimpleRelationalService.json`, `tempTableExecutionPlanWithPostProcessor.json`,
`TestTDSJoin.json`, `singleExecutionPlan.json`) — fixtures for an executor.

## 4. Lite today, against that

| lite | role | gap to a plan |
|---|---|---|
| `TypedQuery` | typed query, in memory | is the INPUT to planning |
| `plan.QueryPlan` | one SQL + root type + shape | no connection, nodes, parameters, serialisation |
| `PureV1Api.executionPlan` | emits `relationalTdsInstantiation → sql` JSON | output only; one shape |
| `plan.PlanNode` / `PlanText` | the `executionPlan()` Pure function's TEXT | printing, never executed |
| `Execution` / `StatementExecutor` | run from model text, recompiling | no plan in between |

Lite builds class and graph results IN THE DATABASE (JSON composed by SQL) — it has no Java instantiation; and it binds
connections through `exec.Sessions` from the compiler's `Target` (C3b), which maps directly onto a `sql` node's
connection.

## 5. Proposed design (decisions for the user)

- **P1 — The plan format IS legend-engine's protocol** for every node kind lite runs: typed Java records in the planner
  library mirroring the protocol 1:1 (`SingleExecutionPlan`, `SqlNode`, `TdsInstantiation`, `Sequence`, `Allocation`,
  `FreeMarkerConditional`, `FunctionParametersValidation`, `Constant`, `RelationalBlock`, `CreateAndPopulateTempTable`,
  result types, connection), serialised as its JSON. One artifact for `generatePlan`, `executePlan` and lite's own
  execution. *Alternative: our own IR plus converters — two formats to keep in step; not recommended.*
- **P2 — Lite never compiles or runs generated Java** (the execution tenet). For node kinds legend-engine fills with
  Java: (a) where the node's DATA fully specifies the work (`createAndPopulateTempTable`: variable, table, columns), lite
  executes it natively and ignores the Java; (b) where the semantics live only in the Java (`platform`/`pureExp`,
  `relationalClassInstantiation`, graph fetch, M2M), an upstream plan is **refused by name**. Lite's OWN class and graph
  results stay database-built JSON, carried by a **lite node kind** (e.g. `relationalJsonInstantiation`) recorded in
  SEMANTICS_REGISTER — such a plan runs on lite, not on legend-engine.
- **P3 — Parameters: typed slots, no FreeMarker dependency (AGREED).** Lite's own plans carry each statement with typed
  parameter SLOTS (placeholders plus a typed parameter list) that the executor binds through the store's own API (JDBC
  bind parameters for SQL; a command document's values for a future Mongo-like store) — never values spliced into text.
  Values keep their type, nothing is escaped by template (no injection class), the statement text is stable (the
  database reuses its plan). Variable-length `in` lists are decided at plan time in the target's form (`= ANY(?)`, a
  list parameter, or the temp-table node). **No FreeMarker library dependency, ever:** a FreeMarker SUBSET is
  reimplemented ONLY to run legend-engine's legacy plans, sized from evidence (§6), frozen to legacy. `generatePlan`'s
  upstream-shaped export prints parameters in upstream's FreeMarker spelling (printing slots as templates is easy; lite
  never parses its own plans back from templates).
- **P4 — Connections.** A `sql` node's connection is the `ConnectionDefinition` the compiler's `Target` names;
  `Execution` opens each through `exec.Sessions` (a caller's own session checked against it, as C3b). Nothing new.
- **P5 — Scope, in phases.** (1) the query path: `Execution.execute*` = plan, then run the plan — no recompiling;
  `generatePlan` emits the full node tree. (2) `executePlan` (`executionPlan/v1/execution/executePlan`) for upstream
  plans of the no-Java subset, judged on legend-engine's own fixture plans and the §3 plans. (3) W6.2: Pure test
  bodies (`StatementExecutor`: lets, asserts, effects) become plans per statement — the largest step, last.
- **P6 — Result shaping without the model.** A plan carries what decoding needs (TDS column Pure types, relational
  types, enum mappings; lite's JSON result schema) so the executor never consults a compiled model.

- **Lineage (the user's goal, 2026-10-05).** Upstream post-processors are opaque lambdas over its SQL metamodel or
  template text (`${roleSpecificTable(...)}`), so lineage is lost; lite applies post-processors as rewrites of its own
  typed SQL tree (`lowering.SqlPostProcessors`, unknown shapes loud) and has a lineage library over typed trees. Real
  lineage for lite's plans holds on three conditions: typed parameter slots (P3); every post-processor a rewrite of the
  typed tree, never text editing (a rule from now on); the plan carries the typed tree (§7). Not promised: upstream plans
  (text), raw SQL a user writes (`executeInDb`, hand-written tabular SQL), shapes decided at run time (a dynamic pivot's
  columns).

## 6. Homework still open before code

1. The FreeMarker feature census: every construct legend-engine's `templateFunctions`, its 22 fixture plans and the §3
   plans use (interpolations, built-ins such as `?replace` `?c` `?number`, `!` defaults, `<#if>` `<#list>`
   `<#function>` `<#assign>` `<#return>`, the plan functions `renderCollection` `collectionSize` `instanceOf`) — the
   compatibility subset's exact size.
   **DONE 2026-10-05 (`docs/execution-plan-boundary-2026-10-05/plans/fm/`).** Sources: all 201 template texts in legend-engine's 22 fixture plans plus
   the 13 plans of §3, and every `.pure` file in legend-engine that writes templates (70 files: relational and its
   database extensions, core, service, Mongo, Elasticsearch — the upper bound a legacy plan can contain). The subset is
   a small FreeMarker INTERPRETER, not a function list — the standard functions are themselves FreeMarker:
   - **directives:** `<#function>`/`<#return>`, `<#assign>`, `<#if>`/`<#elseif>`/`<#else>`, `<#list x as v>` and the
     hash form `<#list m as k, v>`;
   - **expressions:** `${…}` interpolation; string, number and boolean literals; sequence literals and concatenation
     (`[a] + result`); `+` (strings and numbers), `==` `!=` `>` `<` `&&` `||` `!`; the default operator `x!` / `x![]`;
     calls of template functions;
   - **built-ins:** `?number`, `?replace`, `?c`, `?json_string`, `?then`, `?join`, `?map`, `?size`, `?has_content`,
     `?is_string`, `?is_enumerable`, `?is_sequence`, `?split`, `?reverse`, `?sort`, `?date`, `?eval`;
   - **the standard template functions** (each plan's `templateFunctions`; lite already emits their text,
     `plan.PlanSupportFunctions`): `renderCollection`, `collectionSize`, `varPlaceHolderToString`,
     `optionalVarPlaceHolderOperationSelector`, `equalEnumOperationSelector`, `GMTtoTZ`, `renderCollectionWithTz`;
     plus PER-PLAN generated functions (`enumMap_<mapping>_<enum>`, an enum parameter's value-to-source map) and
     user templates such as `roleSpecificTable` — so the interpreter runs whatever a plan defines, not a fixed list;
   - **two Java-registered extras** in legend-engine's executor: the method `instanceOf(x, "Stream")`
     (`FreemarkerInstanceOfMethod`) and the custom date format `?date.@alloyDate` (used by `GMTtoTZ`).
2. lite's plan-text and this JSON plan: one model or two? **DONE 2026-10-05: today lite has THREE representations
   of a plan** — `plan.PlanText` (1,136 lines) builds the `executionPlan()`/`planToString` TEXT directly as strings
   (it does not use `PlanNode`); `plan.PlanNode` is a loosely typed node model (kinds as strings) behind the Pure
   plan-navigation rows (`PlanRows`: `$plan.rootExecutionNode->allNodes(...)`), whose own javadoc says it was meant to
   feed "later, the JSON wire serializer"; and `PureV1Api.executionPlan` builds the JSON as raw maps. Upstream prints
   ONE plan object both ways (`planToString` over the same tree `generatePlan` serialises). **Recommendation:** the
   P1 typed plan records are the one tree; the JSON serialiser, the text printer (`planToString`, whose engine-exact
   SQL spelling belongs to C6's per-family decision) and the plan-navigation rows all read it; `PlanNode` and the map
   builder are deleted, `PlanText` becomes a printer of the tree.
3. Which corpus and showcase queries produce which node kinds under legend-engine (the §3 harness over the corpus), to
   order phase 2.

## 7. The plan's shape (AGREED 2026-10-05)

- **One plan type, and it is PHYSICAL.** The planner (control plane) renders every SQL statement at PLAN time for its
  target; the executor (data plane) is a pure executor — it opens or checks each session through `exec.Sessions`
  (the session must be the node's target type), binds the typed slots, runs, shapes results from the plan's result types,
  and runs the control nodes. No compiler, no dialect, no lowering in the data plane: what runs is what was reviewed or
  authorised; two executors never render differently.
- **The typed SQL tree rides in the plan as METADATA** — for lineage, explanation and re-targeting — produced by the
  planner in the same step as the text; the executor never reads it; a TEST-time check re-renders it and compares.
  The text is the truth for execution, the tree for lineage.
- **The target is per SQL node** (its connection and dialect), as upstream's `sql` nodes carry their connection: a plan
  can already hold statements for different databases (the cross-store future), from day one.
- **No logical plan type, no multi-target plan, for now.** The logical plan lives inside the planner (`TypedQuery`, the
  dialect-free SQL tree); re-targeting is the planner re-rendering the tree, never the executor. A target-free plan, or
  a composite plan pre-rendered for several targets and picked by the session's database type (upstream's
  `CompositeExecutionPlan` is that mechanism), is added only when a real use case asks.
- **Upstream plans** fit the same type: `sql` nodes with text only, run through the FreeMarker compatibility subset,
  no lineage.

## 8. The program, as agreed 2026-10-05

**Decisions (user, 2026-10-05):**
- **One `exec`, made light.** No second execution library. `exec` today holds planning work (seed and metamodel
  rendering, probes, wire-type decisions), decoding with compiler types, and the test judges (measured: 20 files touch
  the compiler, lowering, dialect or plan libraries). The program moves each out: seeds and probes to the planner (plan
  time), decoding to a result description carried in the plan (per-database value quirks owned by `Sessions`), the
  judges to the testing side. A shrink-only guard pins `exec`'s references to those libraries at today's counts; at zero
  it becomes a build rule: `exec` depends only on `//base`, `//json`, the plan records and `java.sql`.
- **Parameter validation in the runner, model-free** (legend-engine's own `FunctionParametersParametersValidation`
  checks only type NAMES, multiplicity and enum value lists the plan carries). The plan records per parameter: name, Pure
  type, multiplicity, and for an enum its allowed values and their database values (a table, where legend-engine
  generates `enumMap_…` FreeMarker functions). The runner checks missing parameters, multiplicity, and converts each
  value to its slot's Java type (String, Long, Double, BigDecimal, Boolean, LocalDate, timestamps, the enum's database
  value; lists to arrays) — the check and the binding conversion are one step. Pure date forms through the `values`
  library (no compiler). Tested against legend-engine's validator cases.
- **One plan in memory, the clean LITE format first.** Typed records; the lite JSON carries typed parameter slots, the
  typed SQL tree, the in-memory identity, and lite's own node for database-built JSON results — no contortions to fit
  upstream shapes, and nothing lite-specific in upstream shapes.
- **A REAL compatibility mode, LAST.** A full serialisation in legend-engine's protocol that its `executePlan` runs:
  parameters as FreeMarker in `sqlQuery` (a second rendering mode in the dialects, from the typed tree), list parameters
  as `renderCollection`, JSON results as `relationalDataTypeInstantiation`, enum mappings as `enumMap_…` functions, the
  standard `templateFunctions`. Gate: our compatible plans executed by the real 4.145.0 engine return the same answers.
  No partial compatibility before it: the existing upstream-shaped `generatePlan` (`PureV1Api.executionPlan`, the TDS
  shape only) is left untouched until this mode replaces it.

**Phases:** (1) the query path on lite plans; (2) legend-engine's plans run on lite (`executePlan`, the FreeMarker
subset, temp-table and allocation nodes); (3) the runner for Pure test bodies (W6.2: `StatementExecutor`, typed-row
decoding, the judges out of `exec`, the guard at zero); (4) the compatibility mode.

## 9. Phase 1 — the query path

**Scope.** The paths whose result is TEXT THE DATABASE BUILDS: the server's `pure/v1/execution/execute` (with
`parameterValues`) and `Execution.executeWire` / `executeStreaming` (JSON, CSV, streamed rows), with `QueryService`'s
wire and streaming paths. In these the executor never decodes a value, so phase 1's runner is pure from the start.
`generatePlan` is NOT changed (its upstream shape waits for the compatibility mode, phase 4).

**Facts it rests on (measured 2026-10-05).** The SQL tree already has typed parameter slots (`SqlExpr.PlanParam`;
today only the engine-text printer renders them). The server's `execute` binds `parameterValues` by rewriting the
lambda into `let`s and recompiling per request (`PureV1Api.boundParameters`). A connection's declared test data is
rendered from the MODEL at execution (`CsvSeed.declaredSteps`), and the server's in-memory database identity is a hash
read from the model at execution (`ConnectionResolver.storesKey`).

**Steps, each landed on the full chain:**
1. **The plan records** — a small library `//core:execution_plan` (`com.legend.executionplan`; deps `//base`, `//json`),
   read by both the planner and `exec`: the plan, `Sequence`, the parameter declarations (name, Pure type,
   multiplicity, enum values and their database values), `TdsInstantiation`, lite's `JsonResult` (the database builds
   the JSON), `Sql` (statement text with bind placeholders, ordered typed slots, result columns, the typed SQL tree as
   metadata, the target connection with its setup statements and in-memory identity), result types. The lite JSON
   format, with round-trip tests. No behaviour change.
   **Step 1 DONE 2026-10-05:** `com.legend.executionplan.ExecutionPlan` (records) and `PlanJson` (the lite format,
   `{"format":"legend-lite-plan","version":1,…}`), deps `//base`, `//json`, `:model`, `:sql`. The connection is lite's
   own `ConnectionDefinition` (database type, 9 specification and 12 authentication records), not upstream's protocol
   shape — that shape belongs to the compatibility mode (phase 4), and reading upstream connection JSON to phase 2.
   Every `_type` is an enum constant shared by writer and reader, read by an exhaustive switch, an unknown tag refused
   by name. The typed SQL tree is held in memory and not written yet (its serializer is a later step). `PlanJsonTest`:
   every node kind, parameters with enum values, all 10 × 12 specification/authentication pairs read back equal.
2. **The planner makes plans** — `TypedQuery.executionPlan(runtime, output)` (`output`: JSON, CSV, streamed JSON —
   the wire statement the database builds, `lowering.WireRender`). Each dialect renders `PlanParam` as a bind placeholder
   and returns the slots in order; a list slot in the target's array form (`= ANY(?)`, a list parameter, H2's array);
   the target per `Sql` node from `Compiler.executesOn`; setup statements computed at plan time (the in-memory
   identity this step first named is gone: decision A below shares by the target's own content).
   **Step 2's first piece DONE 2026-10-08 (`847b41df4`; `docs/GATES.md`):** a connection's setup — its `CREATE
   TABLE`s with each column's type, and its rows — moved from `exec` to the plan-side `//core:setup`, so the planner
   can write it into the plan and the Studio tab can take the server's exact statements; running the steps stays in
   `exec` (`SetupRunner`). No behaviour change.
   **Step 2, landing 1 (2026-10-08): the records and the one parameter list.** `ExecutionPlan`: a parameter's enum
   values are names; `TextResult` (format CSV, JSON or one JSON object per row; result type a relation's columns or a
   value's type) replaces `JsonResult`; a target's setup is a list of steps, a statement or rows for the bulk loader
   with every statement it needs; the identity is gone (decision A). `PlanJson` version 2 (version 1 refused).
   The audit's fixes are in: the third parameter reader (`PlanAllocations`, the enum template functions) reads
   `QueryParameters` too, and PARK-15 covers every place the legacy plan picks an enumeration mapping with no place of
   use.
   `QueryParameters` reads a query's declared parameters once: the legacy plan's text and walkable forms read it
   (their two readers deleted, a dead flag with them) and `TypedQuery.parameters()` reads the same declarations for
   the lite plan. PARK-15 parks the legacy plan's per-parameter enum map (`docs/PARKED_WORK_LEDGER.md`).
   **Step 2's decisions (the user, 2026-10-07 and 2026-10-08; evidence `docs/execution-plan-boundary-2026-10-05/`):**
   - *One list of a query's parameters.* Today the legacy (legend-engine-shaped) plan builds its own, twice: its text
     form (`StatementExecutor.sequencePlan`) and the form Pure code walks (`planModel`). One planner function reads
     a typed lambda's parameters (name, Pure type, multiplicity, an enum's allowed names) and all three read it — the
     legacy plan's two forms and the lite plan.
   - *Values reach the database as values.* The dialect renders each `PlanParam` as `?` and returns the slots in
     order; a list is one array (`= ANY(?)`). At run time nothing edits the SQL text: the runner hands typed values to
     the driver. Measured on the pinned drivers: a quote, a list, an empty list, strings, a null — all three
     databases (`bind-results.txt`). The typed SQL tree rides in the plan for lineage. A slot names a parameter, not a
     SQL spelling, so a non-SQL store spells it its own way. Parameter values are plain values only — literals,
     lists, enum values — as legend-engine's `execute` accepts (`PrimitiveValueSpecificationToObjectVisitor`). A
     function value (`today()`) belongs in the query: legend-query-builder moves it there as a `let`
     (`LambdaParameterState.ts`, `getExecutionQueryFromRawLambda`), and the Query app will do the same before step 4
     (`query/src/builder/build.ts` sends them as values today; agreed with the Studio line, 2026-10-07).
   - *Setup at plan time, one form per database.* The planner picks by the target's database: DuckDB gets the rows
     to load in bulk (the measured reason: one giant INSERT cost DuckDB most of a 61 s first query, commit
     `2c4c57816`), the others the INSERT statement they run today. Step 1's `Target.setup` becomes a list of steps,
     a statement or rows. Upstream's form (`testDataSetupSqls`, text) is written only by the compatibility mode.
   - *Which runs share an in-memory database: decision A (the user, 2026-10-08; measured,
     `docs/execution-plan-boundary-2026-10-05/sharing/results.md`).* Three rules. (1) Two runs share a database the
     runner opens when their connection and setup statements are the same, as legend-engine keys a local H2
     (`LocalH2DataSourceSpecificationKey`: a checksum of the setup statements); setup runs once, when it opens. The
     setup statements carry every column's type, so two models declaring a table differently get different
     databases (the 2026-08-26 leak, D100, stays closed). (2) A database the runner opened for sharing that a run
     changed is thrown away; the next run opens a fresh one. A database the caller hands the runner (the corpus
     harness's one per test package) is the caller's, never thrown away. (3) The server tests that create tables
     with raw SQL (`Seed.sql`: 17 calls in 3 files) move those tables into their models' test data, so every table
     in a shared database comes from its setup. The model-derived hash (`ConnectionResolver.storesKey`) and
     `Target.identity` go. Checked first: nothing in the product creates tables outside setup (the raw-SQL route
     went 2026-09-29; the paths phase 1 changes run one SELECT); only those test files do. Against today: the stress
     corpus already shares this way (25.5 s; a fresh database per test took 58 min, the same answers); the server
     loads setup once instead of on every request, and its streaming path no longer depends on an earlier request
     having built the tables; the in-place reload after a write, which keeps a table the run created, is replaced —
     it ran no statement in either corpus (124,148 reloads in the relational corpus's DuckDB pass, every one empty).
     Rejected: a fresh database per run (C: 58 min against 25.5 s), and adding the table declarations to what decides
     sharing (B: redundant once every table comes from setup, and legend-engine's plans carry no declarations).
   - *`JsonResult` becomes `TextResult`* with a format — CSV, JSON, or one JSON object per row (the runner writes
     the brackets and commas): the database builds the finished text, the runner passes it on. The format is fixed
     when the plan is made (legend-engine chooses it at run time because Java formats its rows; here the database
     does), so a CSV and a JSON request are two plans.
   - *An enum parameter is translated by the database, at each place it is compared, keeping the column's index.*
     Each place carries its own value table, rendered into the SQL at plan time:
     `STATUS IN (SELECT code FROM (VALUES ('A','ACTIVE'), ('X','ACTIVE'), ('C','CLOSED')) m(code, name) WHERE name = ?)`.
     The runner passes the name unchanged; the plan lists the enum's allowed names, so a wrong one is refused by
     name. Per place, because one model can store an enum two ways in two tables. Measured (`enum-index-results.txt`,
     400,000 rows): this form uses the index on DuckDB, H2 and Postgres and is right for a value stored under two
     codes; decoding the column instead (`CASE STATUS WHEN 'A' THEN 'ACTIVE' … END = ?`) scans the table on all
     three (Postgres ~24 ms against ~0.4 ms). Step 1's `EnumValue` database values leave the parameter.
     The legacy printer picks one translation per parameter, the first enumeration mapping over the enum
     (`PlanText.enumMapFnOf` → `enumMappingOf`, and `PlanAllocations.planTemplateFunctions`), and a result column's
     the first declared when its mapping names none (`PlanText.enumMappingIdFor`) — wrong rows when an enum is
     mapped twice; parked as PARK-15 with step 2's landing 1.
   **Step 2, landing 2: the planner makes lite plans — LANDED 2026-10-09 (`682c6fedf`, run 37988343620; `docs/GATES.md`)
   (design, 2026-10-09; homework below, evidence `probes/`).**
   `TypedQuery.executionPlan(runtime, output)` returns an `ExecutionPlan` whose one `TextResult` node holds the statement
   the database answers with finished text (`lowering.WireRender`: CSV, JSON, one JSON object per row; a graph fetch's
   JSON array), every parameter written as a `?` with its slot, and the target it runs on. Nothing runs it yet (step 3).

   What the homework found and decided:
   - *Parameters reach the lowering as slots.* `TypedQuery.lower` binds no parameter today (`$x` of a declared parameter
     is an "unresolvable variable"); `executionPlan` binds each declared parameter as a `PlanParam` (the one parameter
     list, `TypedQuery.parameters()`), so `$x` lowers to it wherever it is used. A parameter whose type is a class is
     refused by name: parameter values are plain values (§9, step 2's decisions).
   - *A placeholder answers as the literal it replaces* — today a request's values become `let`s and the database types
     each literal; a bound value must answer the same. Measured on the pinned drivers (`probes/TypingProbe.java` →
     `typing-results.txt`; `probes/LiteralProbe.java` → `literal-results.txt`, slice (b)): DuckDB and Postgres type a
     bare `?` by the bound value, and every type answers as its literal does in every position. H2 does not: it types a
     parameter when it prepares the statement, by its neighbour — `ID * ?` with 1.5 answers `[2, 6]` where the literal
     answers `[1.5, 4.5]` — and refuses one that stands alone (`Unknown data type`). So H2 writes every placeholder
     typed, `CAST(? AS T)` with T the parameter's type: for Integer, String, Boolean, StrictDate and DateTime the text
     is the literal's (an Integer's value is a `BIGINT` where a small literal is an `INTEGER`, on all three: the same
     values and text, an overflow at 64 bits as Pure's own `long`, not at 32). A Float value is bound as a decimal, never a double, on every database (Rule 1 of the numeric
     charter: a Float literal is a decimal in the database; bound as a double, `PRICE * ?` answers
     `1.6500000000000001`). A decimal (Float or Decimal) on H2 has no exact type: a literal's type is its own digits'
     (`2.50` is `NUMERIC(3,2)`), and no type a statement can name keeps a value's own scale — `NUMERIC` rounds to none,
     `NUMERIC(38,2)` pads, `DECFLOAT` keeps the value but drops trailing zeros (`3.75` where the literal answers
     `3.7500`). A parameter whose value decides its type (`Date`, `Number`) has no one placeholder type either. The
     runner converts each value by its declared type (§8: Integer → long, Float and Decimal → BigDecimal, StrictDate →
     LocalDate, DateTime → LocalDateTime in UTC); an absent optional value is a typed null.
   - *An optional parameter is compared null-safely.* Pure's `==` holds for two empties; the legacy printer writes an
     optional parameter's equality `is not distinct from` (`EngineStyleH2.optionalParamEquality`, a pattern it
     recognises). The lowering already writes `NULL_SAFE_EQUAL` for two optional columns (`NullSemantics.equalNullArms`);
     it will for an optional parameter too, so every dialect writes it from the tree (`IS NOT DISTINCT FROM ?`, measured
     on all three), and the legacy printer reads the same node (its text unchanged, judged by the census; its recogniser
     still serves the plain `=` a verbatim context keeps).
   - *An enum parameter: a value table at each place* (§9, decided 2026-10-08): the comparison the lowering writes over
     the column's decode (`CASE col WHEN 'A' THEN 'ACTIVE' ...`) is rewritten, tree to tree, to
     `col IN (SELECT code FROM (VALUES ...) m(code, name) WHERE name = ?)`, the pairs taken from that decode.
   - *A list parameter: one array.* `->in($xs)` and `$xs->contains(...)` write `= ANY(?)` and bind the list as one array
     of the element's SQL type (measured, `bind-results.txt`: all three, the empty list included); other uses of a list
     parameter are refused by name until measured.
   - *The statement's spelling is fixed in the plan, and checked.* Today the dialect is chosen after the session is open,
     from the server's version (`H2.forServer`: 2.1/2.2 the engine-parity spelling, later versions `H2Modern`). A plan is
     written before a session exists, so its target records the server versions its text is written for, and the runner
     refuses a session outside them by name, as `Sessions.check` refuses another database. The product's H2 is 2.1.214
     (`tools/deps/jars_table.bzl`; 2.4.240 is test-only, one PCT lane on the snapshot path, phase 3).
   - *The target is a declared connection or the platform's own engine.* `Compiler.executesOn` decides a declared
     database (one or more connection names) or the platform's in-process DuckDB for a runtime binding only model data
     (S27); the plan's target holds one or the other, and a runtime whose connections are different definitions is refused
     when the plan is made (today the server refuses it when it opens the session, `ConnectionResolver.open`).
   - *Per-connection statements ride in the target.* DuckDB and Postgres set `TimeZone='UTC'` on every connection
     (`SqlDialect.sessionSetup`); the target carries them apart from its setup (once per database).

   Slices, each judged by the census (no statement of today's paths changes) and a differential test (the same query's
   answer through the plan's statement, bound by the test, and through today's path, on DuckDB, H2 and Postgres):
   (a) the plan for queries without parameters, every output, its target whole (a declared connection or the platform's
   engine, the server versions, the per-connection statements); (b) scalar parameters; (c) optional; (d) enum value
   tables; (e) lists.

   *Slice (a), landed 2026-10-09.* `TypedQuery.executionPlan(runtime, Output)` (`CSV`, `JSON`, `STREAMED_JSON`)
   through `PlanMaker`: a relation's, a value's and a graph fetch's text in each form today's paths write
   (`Execution.executeWire`/`executeStreaming`), a graph's CSV refused by name, a query with parameters refused until
   (b). The records (lite format version 3): a target is a `Database` (`Declared` connection, or the `Platform`'s
   engine), its `Servers` (`Every`, or `Versions` — H2's `2.1`, `2.2`, named once, `H2.SERVERS`, which `forServer`
   reads too; `Databases.servers`), its `session` statements and its setup; a text result's relation columns are a
   name and a Pure type, no SQL type. The setup is final at plan time: a connection's SQL split and adapted to the
   database, each table's rows for DuckDB's bulk loader with their staging statements (`RowLoad.staging`, the one
   owner, which `Executor.load` now reads too) or one INSERT elsewhere (`Databases.loadsRowsInBulk`). A runtime
   binding different connection definitions is refused when the plan is made. `PlanMakerTest` and
   `PostgresArmTest.aPlanAnswersAsTodaysPaths`: on DuckDB, H2 and Postgres, every query's plan run step by step on a
   fresh database answers byte for byte as today's path does (a relation, an empty one, a projection, a graph fetch,
   a model-data runtime on the platform's engine; every output). Census: no statement of today's paths changes, only
   the new tests' own are added (`render-census/landing2-result.txt`).

   *Slice (b), landed 2026-10-09.* A query's declared parameters lower to slots (`TypedQuery.lower` with
   `QueryParameters.Declared.slot()`), each typed as a literal of its declared type is (Integer `BIGINT`, String
   `VARCHAR`, Boolean, StrictDate `DATE`, DateTime `TIMESTAMP`; a Float's, a Decimal's, a Date's and a Number's unknown:
   its value decides), and the plan declares them. A dialect writes the placeholder (`AnsiSqlRenderer.placeholder`):
   bare on DuckDB and Postgres, typed on H2 (`CAST(? AS T)`), which refuses a slot of no one type by name (PARK-19,
   proposed: `docs/PARKED_WORK_LEDGER.md`). Optional, enumeration and list parameters stay refused by name until
   (c)-(e). `PlanMakerTest` and `PostgresArmTest`: a query's plan, run with its values (a Float's and a Decimal's as
   decimals), answers byte for byte as the same query with each parameter a `let` of its value, on DuckDB, H2 and
   Postgres and in every output — an integer, a string with a quote, a decimal and a float in a filter, in arithmetic
   and projected, a date, a boolean, a Date and a Number (DuckDB, Postgres), one parameter written twice, two
   parameters. Census: no statement of today's paths changes (`render-census/landing2-result.txt`).

   *Slice (c), landed 2026-10-09.* An optional parameter (`x: T[0..1]`) is a slot like any other, its absence bound
   as a null of its declared type. Its equality is null-safe (`NullSemantics.equalNullArms`: an optional parameter's
   `==` is `NULL_SAFE_EQUAL`, so `!=` is `NULL_SAFE_NOT_EQUAL`), as legend-engine's plans write it
   (`optionalVarPlaceHolderOperationSelector`: `col is null` when the value is absent) and as the legacy printer already
   spelled it by recognising the shape (`is not distinct from`). `PlanMakerTest` and `PostgresArmTest`: with its value,
   the plan answers as the query with a `let` of it; with none, as the query written `[]` (`$r.NAME == []`, lowered to
   the engine's null check) — on DuckDB, H2 and Postgres, every output. Found on the way: today's server path, which
   turns a request's empty value into `let x = [];`, writes `NAME = NULL` and answers no rows where the engine answers
   the rows whose `NAME` is empty; the plan answers as the engine does, and step 4 retires that path. An enumeration's
   comparison keeps its enum routing (never null-safe, as the engine's); census: the legacy printer's 31 optional-
   parameter equalities unchanged, no statement of today's paths changes (`render-census/landing2-result.txt`).

   *Slice (d), landed 2026-10-09.* An enumeration parameter is a slot holding its value's NAME (`VARCHAR`). Compared
   with a mapped column — the lowering's decode, `CASE WHEN ST = 'A' OR ST = 'X' THEN 'ACTIVE' ... END = ?` — the
   lite plan reads the stored column through that place's value table (`sql/EnumValueTables`, applied by `PlanMaker`
   after lowering, beside the legacy plan's own `PlanEnumForm`): `ST IN (SELECT code FROM (VALUES ('A', 'ACTIVE'),
   ('X', 'ACTIVE'), ('C', 'CLOSED')) AS _enum(code, name) WHERE name = ?)`, the pairs from the decode itself
   (`DecodeShapes.codesAndNames`), `!=`'s comparison its `NOT IN` beside the null arms the lowering writes. Decoded or
   tabled, the rows are the same for a present value; tabled, `==`, `in` and `contains` read the stored column and
   keep its index (`probes/enum-index-results.txt`), and `!=` keeps the decode in the null arms beside its `NOT IN`.
   `PlanMakerTest` and `PostgresArmTest`: a name stored under two codes, a missing value and an unknown code, compared
   `==` and `!=` and written as a value of its own, answer as the query with a `let` of the value, on DuckDB, H2 and
   Postgres, every output. Census: no statement of today's paths changes.

   *Slice (e), landed 2026-10-09.* A list parameter is a slot holding ONE array of its element type
   (`QueryParameters.Declared.slot`: `BIGINT`, `VARCHAR`, `BOOLEAN`, `DATE`, `TIMESTAMP`, an enumeration's names), and
   `->in($list)` and `$list->contains(x)` write `x = ANY(?)`, bare on every database (`AnsiSqlRenderer.anyOf`; measured,
   `probes/ListProbe.java` → `list-results.txt`: each answers as the literal `IN (...)`, the empty list included, and H2
   reads a cast inside `ANY(...)` as its boolean aggregate); the slot names its element type for the driver's array. An
   enumeration list compared with a mapped column reads the stored column through the value table, its names filtered
   by `name = ANY(?)`. A list of decimals, Dates or Numbers is refused by name (PARK-20): DuckDB's driver makes a
   decimal array of three places (`0.1234` read as `0.123`, the wrong row). `PlanMakerTest` and `PostgresArmTest`: an
   integer list with `in`, the empty list, `contains`, a string list with a quote, an enumeration list — as the query
   with a `let` of the list, on DuckDB, H2 and Postgres, every output.

   *The audit's fixes (2026-10-09; an independent audit of the whole landing).* An optional enumeration parameter is
   refused by name: its absence has three candidate answers — legend-engine's plan text (`0 = 1`), Pure's `[] == []`,
   today's let path's NULL — and the value table answers as the engine's text reads where the decoded form answers as
   the let path; none is measured against the engine yet. A class instance and a Byte, LatestDate or StrictTime value
   are refused by name too (all three PARK-21). A list parameter written where one value goes is refused by name
   (`AnsiSqlRenderer.oneValue`), and `x = ANY(?)` is parenthesized under an operator that binds tighter (`=` does not
   chain). The shared test cases are their own test library (`:test_cases`: `PlanCases`, `ReservedNames`), so the
   Postgres target reads them, not all of core's tests. New cases: a DateTime value, `!in` over a list, a scalar's
   and a collection's text in every output.

   Before step 4 (switching callers), two consumers of `PureV1Api.boundParameters` besides `execute` to settle:
   `arrowPlan` (Python's host runs the plan's SQL itself, so it must bind the values: agreed with the DataCube + Python
   line first), and the `execute` answer's activity, which reports the statement that ran (with its `?`s).
3. **The runner, in `exec`** — `exec.PlanRunner.run(plan, parameterValues, Sessions.Source, out)`: validates and
   converts parameters (§8), opens or checks each node's session through `Sessions` (running its setup once; shared by
   the target's own content, decision A — `Sessions.Source` no longer takes the model), binds, executes, streams the
   database's text.
   The shrink-only guard on `exec`'s planning-library references starts here (slice c, below).
   **Step 3's design (2026-10-09; homework: legend-engine's own validation, read from its source,
   `FunctionParametersParametersValidation`, `FunctionParameterTypeValidator`, `FunctionParametersNormalizer`,
   `PrimitiveValueSpecificationToObjectVisitor`, and its tests `TestParametersValidation`, `TestEngineDate`,
   `TestQueryExecutionWithParameters`).**
   - *The runner's input is legend-engine's.* A caller hands the runner each value as legend-engine's execute API turns
     a request's protocol value into a Java value: a string, a `Long`, a `Double`, a `BigDecimal`, a `Boolean`, a date
     as its text (no `%`), an enumeration value as its name, a list for a collection. The server's `execute` makes them
     from `parameterValues` in step 4; the runner knows no protocol.
   - *Checked as legend-engine checks, with its messages.* A declared parameter with a lower bound above 0 and no value
     is `Missing external parameter(s): name:Type[m]` (several joined by `,`); so is one given a null value, a
     deliberate difference (legend-engine counts it present, and its template then writes no statement a database
     runs). A value for no declared parameter is ignored; a one-element list for a parameter of upper bound 1 is its
     element. Each value is checked by its declared type — the right Java type, or a string that parses (Integer:
     `Long`, `Integer`; Float: `Double`, `Float`, `Integer`, `Long`; Decimal: `BigDecimal` only; Boolean; String;
     StrictDate `yyyy-MM-dd`; DateTime its six formats, an `Instant` or a `ZonedDateTime`, an offset converted to UTC;
     Date either) — and an enumeration's name against the plan's names. Every failure is collected into `Invalid
     provided parameter(s): [...]`, each in the engine's words (`Unable to process 'Integer' parameter, value: true.`,
     `Invalid enum value X for path, valid enum values: [A, B]`): a list's failure names its failing element, as
     legend-engine validates element by element; an enumeration's names the whole value. A type legend-engine has no
     validator for (a Number) is its `Unknown external parameter type: T, valid external parameter types: [...]`, the
     ten types it validates in the order it declares them (it prints its map's hash order, which none of its tests
     asserts). The runner's own refusals are collected with them, by name: a list for a parameter of upper bound 1
     (legend-engine passes it to its template, which writes no SQL a database runs), a Float that is not finite (NaN,
     Infinity: no SQL literal; legend-engine writes `NaN` into its statement), and a Byte or a Variant, which no plan
     binds yet (PARK-21).
   - *Bound as the plan's slots say.* Each slot binds its parameter's checked value, converted for the driver: an
     Integer a `long`; a Float as its literal is typed (the numeric charter's Rule 1: its plain digits a `BigDecimal`,
     at an extreme magnitude — at least 1e15, or below 1e-6 — a `double`; one owner, `SqlTyping.floatDecimal`, which
     the dialects spell a Float literal by); a Decimal a `BigDecimal`; a StrictDate a `LocalDate`; a DateTime a
     `LocalDateTime` in UTC; an enumeration its name; an absent optional value a null of its type; a list one array of
     its element type (`createArrayOf`; a timestamp element as `java.sql.Timestamp`). Nothing edits the statement.
   - *Sessions by the target (decision A).* `exec.PlanSessions` gives out a session for a plan's target. An in-memory
     database (the platform's engine, an in-memory or `LocalH2` connection) is shared by every run whose target is equal
     — the connection, its server versions, its session statements and its setup, keyed by the target's whole content
     as its plan JSON spells it (`PlanJson.writeTarget`: a cell holding a comma and a null cell are spelled apart from
     their look-alikes), hashed on each run in time proportional to the setup's rows, as making the plan is. Its setup
     runs once, when it is opened, outside the store's lock (`HandleStore.getOrOpenSlowly`: other runs of the same
     target wait for it, and no other target's are held up). A setup that fails closes what it opened and is
     forgotten: the next run sets up afresh (an H2 database the runner names is new at each attempt and lives only
     while its keeper connection is open). A database that is the user's is never set up by a plan, which is refused
     by name if it has setup (only an in-memory `LocalH2` connection declares test data): one reached by URL, opened
     for the run and closed after it, and an in-memory one the user named (an `EmbeddedH2`), shared by design. A caller's own
     connection is the caller's (`PlanSessions.given`; `PlanSessions.setUp` runs a target's setup on a test's fresh
     one). Every session is checked against the target before the run's statements: the database it is
     (`Sessions.check`) and, where the statements are written for some server versions (H2), its version
     (`Servers.Versions`), refused by name otherwise. Each run then runs the target's session statements on its session,
     then the statement.
   - *The text the database writes, passed on.* A CSV or JSON result: the one row's one cell; one JSON object per row:
     each row's cell, the array's brackets and commas written by the runner, flushed per row (no row: `[]`).
   - *Model-free by construction.* `PlanRunner`, `PlanParameters`, `PlanSessions` and the bulk load read the plan
     records, `java.sql` and the session opener only; an architecture rule pins that they reach no compiler, lowering,
     SQL tree, dialect, setup or plan-text class, with one call excepted and pinned: `SqlTyping.floatDecimal`, a Float's
     binding, which only its value decides, at run time.
   - *Slices:* (a) the runner and its sessions for plans without parameters — `PlanCases`' hand-written run replaced by
     `PlanRunner`, every landing-2 case then the runner's test on DuckDB, H2 and Postgres; (b) parameters checked,
     converted and bound, legend-engine's validation cases ported; (c) the guard.
   **Step 3, on branch 2026-10-09** (`dbowner/plan-runner`): `exec.PlanRunner`, `PlanParameters`, `PlanSessions`. Every
   landing-2 case runs through the runner (`PlanCases.run`) and answers byte for byte as today's path on DuckDB, H2 and
   Postgres, its values as legend-engine's execute API makes them (dates as text, an offset DateTime, a DateTime list
   bound as timestamps, Floats at both extreme magnitudes); landing 2's `Number` case left `PlanCases` with the runner,
   which refuses it before opening any session (`PlanRunnerTest`). `PlanRunnerTest` ports legend-engine's
   `TestParametersValidation` case for case — each type's valid values, with what the runner binds, and each invalid
   value with its exact message (to-many too, where legend-engine's test asserts only the prefix) — and holds the
   sessions' rules: a target's database shared and set up once, by eight runs started together too; another target's its
   own, look-alike cells included; a failed setup leaving nothing, on DuckDB and H2; a URL or a user-named database with
   setup refused, the URL never opened; a platform target; a session of another database or H2 version refused. One
   shape for a bulk load: a plan's `SetupStep.Rows`, which `RowLoad.staged` writes and the loader takes (`BulkLoad`,
   `BulkLoads`; `RowLoad.Staging` gone). `ArchitectureTest.theRunnerIsModelFree` pins the runner's classes as above (it
   fails on a planted reference). The guard (slice c): `ArchitectureTest.execsReachIntoPlanningOnlyShrinks` names the 16
   `exec` classes that still reach a planning library — the compiler, the resolver, the lowering, the dialect, the plan
   side's setup and plan packages, and the builtin, parser, normalizer, platform and database packages; the SQL tree is
   not counted, being the plan records' own vocabulary — and fails when one joins, or when one that stopped stays
   listed. Audited before landing (one blocker, eight should-fix, four nits; all fixed): the list message, the type
   list, the null difference named, Floats bound as their literal is typed and NaN refused, the target key, failed
   setups, setup outside the lock, this paragraph's claims, and the guard. The render census
   (`render-census/step3-result.txt`): no statement of today's paths changes, every Float literal included.
4. **Switch the callers, delete what they replace** — `pure/v1/execution/execute`: plan once, run with
   `parameterValues`; `Execution.executeWire` / `executeStreaming` and `QueryService`'s wire and streaming paths: plan +
   run. DELETED (rule 15): `PureV1Api.boundParameters`, `Execution`'s `wireOn` / `streamOn`, and `ConnectionResolver`'s
   reading of the compiled model.

**Gates.** The full chain at each step; `PureV1ApiTest`, `PureV1HttpTest`, the server and streaming tests with unchanged
answers; the same query answered identically through the plan and through today's path on DuckDB, H2 and Postgres (a
differential test kept until step 4 deletes the old path); `:planner`'s closure without execution libraries; the `exec`
guard's counts only going down.

## 10. E — the dialects write through one writer (the user, 2026-10-08)

**Why.** Step 2's landing 2 needs, for each `?` in a statement, the parameter that fills it: JDBC binds by position, and
Postgres's driver accepts only positional `?` (numbered `$1` refused, measured 2026-10-08; DuckDB and H2 accept both).
The dialects build SQL as returned strings and paste pieces: 25 places render a piece once and place it twice (DuckDB's
XOR is `(a | b) - (a & b)`), so recording a parameter as its `?` is written would miss the second. The alternatives were
measured and set aside: numbered placeholders (Postgres), binding every parameter once in a `WITH params` header (keeps
the index for scalars and enums everywhere, but a list loses it on H2 or reads 0 rows), and markers turned into `?`
after rendering (exact, but a text step). The user: "would rather do this correctly and use this to build dialect
correctly — if not now, then when!"

**What E is.** Every render method writes into ONE ordered `SqlWriter` — text, and each bound parameter at the place it
is written — and returns nothing, the way jOOQ, Calcite and Hibernate generate SQL. Not typed fragments: Java's `+`
turns any object into text, so a missed site would compile; a `void` cannot be joined, so the compiler finds every one.
A piece placed twice is written twice, so its parameter is recorded twice, by construction. The writer can also record
which tree node wrote each span of the SQL — lineage from text back to the typed tree.

**The homework (2026-10-08).** The renderers are ~8,300 of the dialect package's 11,575 lines, one hierarchy under
`AnsiSqlRenderer` (DuckDb; H2 → H2Modern; Postgres; EngineStyleH2 → EngineStyleDB2 → EngineStyleComposite); the
`SqlRewriter` passes are tree to tree and untouched. 250 methods return rendered SQL, 13 write a shared buffer; 525
recursive render calls; ~740 lines join with `+`, 45 stream joins, 42 `StringBuilder` sites. Of 133 text operations, 129
quote, escape or parse names and values; 4 edit rendered SQL, all in the legacy engine-text printer (lowercasing `OVER`,
`PARTITION BY`, a function name; escaping a rendered expression into a FreeMarker argument). The real dialects never edit
their output. `SqlDialect` has three entry points, not one — `render(SqlQuery)`, `render(SqlDdl)`, `render(SqlDml)`
(AGENTS.md invariant 3 was stale until E-1 restated it) — with 54 callers; E keeps them and adds one entry returning a statement and its ordered
parameters.

**The stages, each landed alone and each rendering exactly what its parent renders:**
- **E-0, the judge — DONE 2026-10-08.** The render census (`docs/execution-plan-boundary-2026-10-05/render-census/`):
  every statement the JVM suites render (core, stress, the four PCT lanes, both relational corpus lanes), compared byte for
  byte. Baseline on `daa78d0eb`: 5,706,332 renders, 47,904 distinct texts. Three runs on unchanged code agree on all
  52,085 entries once three things are normalised — a quoted temporary path, the activity comment's random
  `executionTraceID`, and a lambda's scope id that varies between runs (a product defect in
  `resolver/FunctionBodyRows.scopeId`, reported to the resolver's owner, who will fix it with this census as judge).
- **E-1, the writer and a bridge — LANDED 2026-10-08 (`f804dba9a`; `docs/GATES.md`).** `SqlWriter` (text, and `bind` writing `?` and recording the
  parameter) and `RenderedStatement` (text and parameters in placeholder order); `SqlDialect.renderStatement` beside the
  three `render`s (the legacy engine-text printer refuses it: its parameters are template variables). The clause layer
  — `query`, `select`, `source`, `subselectSource`, `valuesSource`, `pivotSource`, `appendQualify`, `nl`, in
  `AnsiSqlRenderer`, `DuckDb`, `H2` and `EngineStyleH2` — writes into the writer; expressions still arrive as strings
  through the bridge (`inline`, and `writer.append(expr(...))`), where a parameter is refused as before. AGENTS.md
  invariant 3 restated. Census: 0 of 52,085 entries differ (`render-census/e1-result.txt`). The census probe became
  `probe.py` (by signature; E-1 reshaped `render`, so the E-0 patch no longer applied), and records `renderStatement`.
- **E-2, expressions — LANDED 2026-10-09 with E-3 and E-4 (`46fc131b8`, run 37965399077; `docs/GATES.md`, "E").** `expr`, `call` (with Postgres's `postgresCall`) and `membership` write
  into the writer in every dialect — `AnsiSqlRenderer`, `DuckDb`, `H2`, `H2Modern`, `Postgres`, `EngineStyleH2`,
  `EngineStyleDB2`, `EngineStyleComposite`; the clause layer writes its expressions there too (`WHERE`, `GROUP BY`,
  `HAVING`, `JOIN ... ON`, `QUALIFY`, `VALUES` rows; the legacy printer, which binds nothing, still spells its own
  `WHERE` and `GROUP BY` as text); subqueries (`EXISTS`, `IN`, scalar, quantified) are written into the same writer. The
  string forms of `expr`, `call` and `membership` are `final` bridges in the base, so a stale override cannot compile. A
  plan parameter reaching `expr` is BOUND as one value (`renderStatement` lists it; `render` refuses it); what one value
  cannot carry yet — a RAW splice, an optional or enum parameter, a collection as IN's whole list — is refused by name,
  for step 2's landing 2. `SqlWriterTest` runs bound statements on DuckDB: two parameters under AND/NOT, one written
  twice by XOR (bound twice), one inside an IN subquery. The other composing helpers (CASE, casts, windows, aggregates,
  list and JSON functions, projections, sort keys), and about 25 arms that paste a sub-expression they built as text
  (acos's domain guard; Postgres's regexp, date and JSON arms), still build strings through the bridge, where a
  parameter is refused (tested) — the next stages. **A render method returns the writer it wrote into**, so the code
  keeps its shape: an arm is one chain (`case SQRT -> writer.append("sqrt(").expr(a.get(0), 0).append(")")`),
  `C ? A : B` stays a conditional over writers, and a dispatching switch stays a `return switch` EXPRESSION, whose cases javac
  checks (AGENTS.md invariant 3; measured: deleting `GUID`'s arm from `postgresCall` fails to compile, "the switch
  expression does not cover all possible input values"). The first conversion had written one statement per piece inside
  switch STATEMENTS: javac stops checking an enum switch statement's cases, so a new function without a Postgres arm
  would have written nothing, and two methods grew past the 250-line guard (`call` 219 → 370 lines, `postgresCall` 229 →
  435). Returning the writer restored both (`call` 229, `postgresCall` 242). Checked by the compiler and the census:
  every statement rendered before renders identically (`render-census/e2-result.txt`).
- **E-3, the composing helpers — LANDED 2026-10-09 with E-2 and E-4 (`46fc131b8`).** Every dialect that executes (`AnsiSqlRenderer`,
  `DuckDb`, `H2`, `H2Modern`, `Postgres`) writes all of a query into the writer: CASE, casts, windows, aggregates, the
  list, JSON, variant and struct functions, projections, sort keys, and the arms E-2 left pasting text (acos's domain
  guard; Postgres's regexp, date and JSON arms). Three writer forms do it without text: `function(name, args)` writes
  `name(a, b)`; `join(items, separator, each)` writes each item by a function (`CAST(x AS INTEGER)` per argument); and a
  `Piece`, a piece of SQL that writes itself, is what a helper takes or returns where it used to take or return text it
  did not build (Postgres's `listOf(xs)`, `decode(json, …)`, `naive(ts)`) — written where the helper writes it, as often
  as it does, its parameters with it (jOOQ's QueryPart). The base's text helpers `fn` and `list` are gone, and so are
  the string forms of `call` and `membership`, without callers once every override writes (`expr`'s and `inline`'s
  remain the bridge). A base method changed, so did every override, the legacy engine-text printer's included; that
  printer's own helpers and its text edits (which now edit a piece it wrote itself: it binds nothing) are the next
  stage. `SqlWriterTest`: a parameter under acos is bound at both places it is written and runs on DuckDB; a parameter
  in a DML row (still text) is refused. Census: every statement E-2 renders, E-3 renders identically
  (`render-census/e3-result.txt`).
- **E-4, the rest — LANDED 2026-10-09 with E-2 and E-3 (`46fc131b8`); E is complete with it.** Four commits, each judged by the
  census (`render-census/e4-result.txt`):
  - *The legacy engine-text printer's helpers write* (`EngineStyleH2`, `EngineStyleDB2`): its pattern recognisers
    (enum selectors, optional-parameter equality, the date-diff folds, the decode chains) return a `Piece` or nothing,
    its WHERE and GROUP BY write. Census: 0 of 52,095 entries differ.
  - *DML's rows write; the bridge goes.* `render(SqlDml)`'s rows were the last text path; a parameter in a row (a row
    holds values) is refused by name. The `expr` text form, with no caller left, is deleted (`inline`'s went with
    E-4a). DDL spells
    only names, types and keywords: text, as any spelling. Census: 0 differ.
  - *PARK-16, the product half.* DDL and DML spell a table or schema name through `physicalName`, as queries do (and
    as legend-engine's own H2 DDL does, through `tableToString`); Postgres's two overrides go. A default-schema table
    `order` and a table in a schema `select` seed and answer a query on DuckDB, H2 and Postgres (`ReservedNamesSeedTest`,
    `PostgresArmTest`; both failed before: `Drop table if exists order;` is refused). The census: the 27 entries that
    differ are those tests' new statements; no existing statement changed. The test-data generator's hand-built SQL
    stays parked (PARK-16, restated: the user, "product now, generator later").
- *The legacy printer exact (E-4b; the user: "for backwards compatibility/legacy mode we need to be fully exact,
    implemented cleanly ... a single exact backwards compatibility mode").* Measured against legend-engine 4.145.0
    itself (`legacy-text/`: 14 shapes through its `generatePlan`, and its source): the printer's two text edits
    (lowercasing a window's keywords by find-and-replace, which also changed string literals; lowercasing an aggregate's
    name) become direct writes through two spelling hooks, `keyword` and `aggregateName`, the engine's own design (its
    SQL dialect translation's `keyword()`). And where the printer was not exact it now is: an aggregate's own `order by
    ... asc`, `count(distinct ...)`, `rank()`, a window frame, and the string aggregate `listagg`, ordered `listagg(x,
    sep) within group (order by k)` — over a window with the `within group` after the `over (...)`, as the engine writes
    it. The enum selector's quoting (each `'` as `\'`) was already the engine's. `LegacyTextTest` holds the spellings to
    the engine's text. `EngineStyleDB2` and `EngineStyleComposite` inherit these spellings; the one DB2 statement the
    suites render moved to `count(distinct ...)`, as the engine's DB2 golden spells it (`testIsDistinctSQLGeneration`,
    testToSQLString.pure:724); DB2's window and string-aggregate text is unmeasured and follows H2 until it is. Recorded
    where they are owned: an explicit `nulls first`/`nulls last` (PARK-18: the IR cannot yet tell a query's own
    placement from pure's); `reduce` and a `joinStrings` over a window's partition (lowering gaps, not spellings); and a
    relation-API query's statement structure (the engine's dialect translation aliases `t_0` and lists every column,
    lite's printer follows the TDS goldens' `root`): the compatibility mode's, §8 phase 4.
- **Then step 2's landing 2**: a parameter is `bind(...)`.
