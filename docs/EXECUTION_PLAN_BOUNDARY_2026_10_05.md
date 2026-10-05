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

## 3. Real plans from the pinned engine (4.145.0 on :6300; `runs/plans/`)

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
   **DONE 2026-10-05 (`runs/plans/fm/`).** Sources: all 201 template texts in legend-engine's 22 fixture plans plus
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

## 8. Phase 1 — the query path (PROPOSED 2026-10-05, for the user's review before code)

**Scope.** The paths whose result is TEXT THE DATABASE BUILDS: the server's `pure/v1/execution/generatePlan` and
`execute` (with `parameterValues`), and `Execution.executeWire` / `executeStreaming` (JSON, CSV, streamed rows). In
these the executor never decodes a value (the wire statement returns the finished JSON or CSV), so phase 1's executor is
pure from the start. Typed-row decoding into `ExecutionResult` (the in-process callers, Pure test bodies) is phase 3.

**Facts it rests on (measured 2026-10-05).**
- The SQL tree already has typed parameter slots: `SqlExpr.PlanParam(name, kind, optional, enumMapFn, type)` — today
  only the engine-text printer renders them (as FreeMarker), the real dialects refuse them.
- The server's `execute` binds `parameterValues` by REWRITING the lambda (`PureV1Api.boundParameters`: each value
  becomes a `let`) and recompiling — per request.
- `exec` uses the dialect 43 times (seed DDL rendering, `normalize` decoding) and compiler types (decoding, metamodel
  seeding), so the pure executor is a NEW library, not `exec` renamed.
- Connections' declared test data (`CsvSeed.declaredSteps`) is rendered from the MODEL at execution today; the server's
  in-memory database identity (`ConnectionResolver.storesKey`) is a hash read from the model at execution today.

**Steps, each landed on the full chain:**
1. **The plan records** — a new library `//core:execution_plan` (`com.legend.executionplan`; deps `//base`, `//json`
   only): `SingleExecutionPlan`, `Sequence`, `FunctionParametersValidation`, `TdsInstantiation`, a lite
   `JsonInstantiation` (class, graph, scalar and collection results the database builds as JSON), `Sql` (statement text,
   ordered typed parameter slots, result columns, the typed SQL tree as metadata, the connection), result types (`tds`,
   `dataType`, lite's JSON result), the connection (upstream's `RelationalDatabaseConnection` shape) carrying its SETUP
   statements (upstream's `testDataSetupSqls`, rendered at plan time) and the in-memory database's identity (a lite
   field, computed at plan time). JSON in legend-engine's protocol shape; round-trip tests. No behaviour change.
2. **The planner makes plans** — `TypedQuery.executionPlan(runtime, output)` (`output`: rows, JSON, CSV, streamed
   JSON — the wire statement the database builds, `lowering.WireRender`, decided at plan time). Each dialect renders
   `PlanParam` as a bind placeholder and returns the slots in order; a collection slot is rendered in the target's array
   form (`= ANY(?)` on Postgres, a list parameter on DuckDB, H2's array) with the element's SQL type name in the slot;
   the target per `Sql` node from `Compiler.executesOn`; setup statements rendered from the connection's declared data.
   Tests: plans for the §3 queries, checked node by node against legend-engine's (kinds, result columns, connection).
3. **The pure executor** — a new library `//core:plan_runner` (deps: `:execution_plan`, `//base`, `//json`, and the
   session owner — `exec.Sessions` moves to a dependency-light library it can use, since `exec` itself depends on the
   dialect and the compiler). `PlanRunner.run(plan, parameterValues, Sessions.Source, out)`: validates parameters
   against the plan's declared ones, opens or checks each node's session (running its setup once), binds slots by type
   (arrays through `Connection.createArrayOf`), executes, streams the database's text. Its BUILD has no `:compiler`,
   `:lowering`, `:sql_dialect`, `:planner` — the data plane is pure by construction, as the planner is (C2a).
4. **Switch the callers and delete what they replace** — `generatePlan` returns the plan (upstream-shaped export:
   `sqlQuery` printed with FreeMarker-spelled parameters for outside consumers); `execute` = plan once, run with
   `parameterValues` (no lambda rewriting, no recompiling per value); `Execution.executeWire` / `executeStreaming` /
   `QueryService`'s wire and streaming paths = plan + run. DELETED (rule 15): `PureV1Api.executionPlan` (the map
   builder), `PureV1Api.boundParameters`, the wire/streaming bodies in `Execution` (`wireOn`, `streamOn`), and
   `plan.QueryPlan` where the plan replaces it (the WebAssembly planner's `plan(...).sql()` reads the plan's statement).
   `ConnectionResolver` opens from the node's connection and identity instead of the compiled model.

**Not in phase 1:** upstream plans (`executePlan`, the FreeMarker subset, temp-table and allocation nodes — phase 2);
typed-row decoding and Pure test bodies (`StatementExecutor`, phase 3); moving `normalize`'s per-database decoding out
of the dialect into the data plane (phase 3, when rows are decoded); the plan text printer and `PlanNode` (with C6's
engine-text decision).

**Gates.** The full chain at each step; `PureV1ApiTest`, `PureV1HttpTest`, the server and streaming tests with
unchanged answers; for the §3 queries, our plan's node kinds and result columns equal legend-engine's; the same query
answered identically through the plan and through today's path on DuckDB, H2 and Postgres (a differential test kept
until step 4 deletes the old path); `:planner` and `:plan_runner` dependency closures checked by
`tools/deps` (no execution library in the first, no compiler, lowering or dialect in the second).

**Open questions for the user.** (a) The lite `JsonInstantiation` node and lite fields (`identity`, the typed tree,
typed slots) in the plan JSON — under a `lite` key, or top-level fields? (b) Parameter validation: legend-engine's
`function-parameters-validation` checks types and multiplicity — the runner does the same from the plan's declared
parameters; agreed?

