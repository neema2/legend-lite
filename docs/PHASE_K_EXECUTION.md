# Phase K — Execution + the QueryService bridge (corpus-as-acceptance)

Status: DESIGN + build in progress. Decided 2026-07-05 (user): do NOT hand-port
the engine test corpus — build core's K layer, bridge engine's `QueryService`
to it, and run the ~94-class engine suite AS-IS as core's acceptance
scoreboard. Failures over unbuilt territory (mappings/class sources) MEASURE
Phase H, they don't block (the V2-parity measurement pattern, this time
against a stable lowering).

## THE BRIDGE RULE (load-bearing)

The bridge contains ZERO decisions — a bijection: core record → engine record
field-by-field, plus one static type-vocabulary table (core Type ↔ engine
Type). Anything with a rule in it (value representation, precision, null
handling, JSON envelopes) is CORE's contract, built and pinned in core's own
tests. **Litmus: if a corpus test fails and the tempting fix is in the bridge,
the fix is in the wrong place.** When the engine module retires, the bridge
dies and NOTHING is ported; surviving tests get mechanical import swaps
(representations were identical all along).

## Representation spec (core adopts engine's, verified from source)

- `ExecutionResult` sealed quartet, all variants expose `returnType()` /
  `columns()` / `rows()` / `toJsonArray()`:
  - `ScalarResult(Object value, Type)` — synthesizes columns=[value], 1 row
  - `CollectionResult(List<Object> values, Type elementType)` — N rows × 1 col
  - `TabularResult(List<Column>, List<Row>, schema, Type)`
  - `GraphResult(String json, Type)` — `toJsonArray()` returns json VERBATIM
    (it is already a JSON array built by the DB); columns()=[json]
- `Column(String name, String sqlType, String javaType)`, `Row(List<Object>)`
- Cell values are RAW JDBC objects (`rs.getObject`) — no conversion at the
  execution layer; the Type on the result is the semantic carrier and
  consumers (PCT, serializers) convert. writeJsonValue rules: null→null,
  Boolean→bool, integral Number→long, other Number→double, else toString().

## Engine flow facts (the seam, verified from source)

- Corpus entry: `QueryService.execute(pureSource, query, runtimeName,
  Connection)` (tests hold their own Connection and seed data via JDBC —
  `AbstractDatabaseTest.connection`); a 3-arg overload resolves the
  Connection from the model's Runtime (`PureModelBuilder.resolveConnection`).
- Engine internals: PureModelBuilder → PlanGenerator.generate(model, query,
  runtimeName, SNAPSHOT) → SingleExecutionPlan(sql, ResultFormat) →
  PlanExecutor.execute(plan, conn). ResultFormat = {Graph, Tabular, Scalar}
  tags stamped at plan time (Collection folded into Scalar handling).

## Core K layer (com.legend.exec — permanent code)

1. `ExecutionResult` — the quartet with CORE types (`Type` from
   compiler.element.type), representation-identical to engine's.
2. `ResultShape.of(ExprType root)` — the closed 4-way switch
   (PHASE_HIJ_LOWERING.md table): RelationType→TABULAR; ClassType[*] /
   graphFetch/serialize→GRAPH; mult many scalar→COLLECTION; else SCALAR.
3. `Executor` — rendered SQL + Connection + (shape, output Pure types) →
   ExecutionResult. TABULAR: columns from the query's typed outputs (names +
   Pure types; sqlType informational). COLLECTION: N×1 flatten. SCALAR:
   single value. GRAPH: single json cell verbatim.
4. `Compiler.execute(model, query, runtimeName, Connection)` — the core
   QueryService: compileQuery → Lowerer → DuckDb → Executor. (3-arg
   runtime→Connection resolution lands with Phase H runtime handling; the
   corpus's 4-arg form doesn't need it.)

## Bridge (engine module, throwaway by design)

Swap `QueryService.execute(...)` internals to call core's
`Compiler.execute`, mapping core ExecutionResult → engine ExecutionResult
1-1 (type table + re-wrapping). `executeSql` trivially bridged; `stream`
stays engine-backed initially.

## Baseline protocol

Run the engine suite; record pass/fail per class; triage into
(a) passes on core, (b) Phase-H territory (mappings/class sources/services),
(c) genuine core lowering bugs — bucket (c) is the mining target and gets
fixed IN CORE. The scoreboard number becomes Phase H's parity target.
