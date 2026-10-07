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
   the target per `Sql` node from `Compiler.executesOn`; setup statements and the in-memory identity computed at plan
   time.
   **Step 2's decisions (the user, 2026-10-07; evidence `docs/execution-plan-boundary-2026-10-05/probes/`):**
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
   - *Which requests share an in-memory database: the plan's own content* (the connection and its setup), as
     legend-engine keys a local H2 (`LocalH2DataSourceSpecificationKey`: a checksum of the setup statements). The
     model-derived hash (`ConnectionResolver.storesKey`) and `Target.identity` go. NOT YET CHECKED, and checked before
     step 2's code: that nothing creates tables outside the setup statements; if something does, this comes back to
     the user.
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
     The legacy printer picks one translation per parameter and, unsure, the first declared
     (`PlanText.enumMappingIdFor`, "Falls back to first-declared") — wrong rows when an enum is mapped twice; it goes
     on the parked-work ledger with step 2's first commit.
3. **The runner, in `exec`** — `exec.PlanRunner.run(plan, parameterValues, Sessions.Source, out)`: validates and
   converts parameters (§8), opens or checks each node's session through `Sessions` (running its setup once; the
   identity from the plan — `Sessions.Source` no longer takes the model), binds, executes, streams the database's text.
   The shrink-only guard on `exec`'s planning-library references starts here.
4. **Switch the callers, delete what they replace** — `pure/v1/execution/execute`: plan once, run with
   `parameterValues`; `Execution.executeWire` / `executeStreaming` and `QueryService`'s wire and streaming paths: plan +
   run. DELETED (rule 15): `PureV1Api.boundParameters`, `Execution`'s `wireOn` / `streamOn`, and `ConnectionResolver`'s
   reading of the compiled model.

**Gates.** The full chain at each step; `PureV1ApiTest`, `PureV1HttpTest`, the server and streaming tests with unchanged
answers; the same query answered identically through the plan and through today's path on DuckDB, H2 and Postgres (a
differential test kept until step 4 deletes the old path); `:planner`'s closure without execution libraries; the `exec`
guard's counts only going down.
