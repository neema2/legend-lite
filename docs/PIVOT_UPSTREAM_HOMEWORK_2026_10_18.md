# Pivot homework against upstream's real code (2026-10-18)

The user's question, 2026-10-18, after step 7's first landing of the plan path program
(`docs/PLAN_PATH_PROGRAM_2026_10_17.md`): "Do we need more homework for pivot on upstream real code?" Four items were
named -- the group rule, the generated column's name, the cast over an absent key, the composite key -- and the user
said "do the homework". This note is the reading, the findings, the one defect found and fixed, and the register rows.

The reference checkouts are spec and test input only (AGENTS.md, the reference-checkout tenet): every path below is
relative to `$(bazel info output_base)/external/+http_archive+legend_engine_src`, read to know what the engine does,
never loaded.

## 1. What was read

| What | Where |
|---|---|
| The five `pivot` signatures, each `:Relation<Any>[1]`; the static form takes one `ColSpec`, the values and one `AggColSpec` | `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-functions-relation-pure/src/main/resources/core_functions_relation/relation/functions/transformation/pivot.pure:18-32` |
| The PCT's dynamic pivot test: the expected `#TDS` literal's header `city,country,'2000__|__newCol',...`, compared to `$res->cast(@Relation<(..., '\'2000__|__newCol\'':Integer, ...)>)` | `pivot.pure:33-62` |
| The PCT's static form: `->pivot(~year, [2000, 2011], ~newCol : x \| $x.treePlanted : y \| $y->plus())` | `pivot.pure:365-393` |
| The composite key's names, `'UK__|__LDN__|__sum'` | `pivot.pure:97-126` |
| `assertTdsEquivalent` compares the column NAMES verbatim (`$oneCol.name == $twoCol.name`) before any value | `.../core_functions_relation/relation/functions/tdsEquivalent.pure:25-32` |
| The relational lowering: `processStaticPivot`, `processDynamicPivot`; a cast's column aliases, where a name holding `__|__` that is not already quoted is quoted (`mayQuotePivotColNames`) | `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/pureToSQLQuery/pureToSQLQuery.pure:6368`, `:6426`, `:3186-3194` |
| DuckDB's pivot: the dynamic form pivots `on concat('\'', <column>, '\'')`; the static form aliases each aggregate `'"_|__<name>'"'` over `on <column> in (...)`, so DuckDB's `<value>_<alias>` joins into `'<value>__|__<name>'` | `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/core_relational_duckdb/relational/sqlQueryToString/duckdbExtension.pure:608-631` |
| H2 and the default extension refuse every pivot: `assertEmpty($s.pivot, 'pivot is not supported')` | `.../core_relational/relational/sqlQueryToString/dbSpecific/h2/h2Extension2_1_214.pure:600`, `.../sqlQueryToString/extensionDefaults.pure:360` |
| The interpreter's pivot column: `columnName = "'" + values.with(aggName).makeString("__|__") + "'"` | `legend-engine-core/legend-engine-core-pure/legend-engine-pure-code-functions-relation/legend-engine-pure-runtime-java-extension-shared-functions-relation/src/main/java/org/finos/legend/pure/runtime/java/extension/external/relation/shared/TestTDS.java:901-914` |
| The executor's column names: the driver's labels; for a TDS result, each label looked up among the plan's TDS columns by name, `requireNonNull` ("Column '...' not found among ...") | `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-execution/legend-engine-xt-relationalStore-executionPlan/src/main/java/org/finos/legend/engine/plan/execution/stores/relational/result/RelationalResult.java:170-176`, `:291-292`; `legend-engine-core/legend-engine-core-base/legend-engine-core-executionPlan-execution/legend-engine-executionPlan-execution/src/main/java/org/finos/legend/engine/plan/execution/result/builder/tds/TDSBuilder.java:49-58`; `.../nodes/helpers/ExecutionNodeTDSResultHelper.java:36-43` |
| The CSV serializer: Commons CSV's `DEFAULT` format with the result's names as the header (a single quote is no CSV special: the header carries `'2000__|__newCol'` verbatim); the row-object JSON serializer's keys are the TDS columns' names | `.../stores/relational/serialization/RelationalResultToCSVSerializer.java:47-57`, `RelationalResultToPureTDSToObjectSerializer.java:41-62` |

On lite's side: `compiler/spec/PivotChecker` (the group rule, the static columns, the templates),
`compiler/element/type/Type.RelationType` (`presentPivotName`, `pivotColumnType`), `lowering/Pivots`,
`lowering/Fold.pivotIdentity` and `pivotColumn`, `lowering/Lowerer.relationCast` and `castOverDynamicPivot`,
`lowering/Render` (the TDS text, the CSV and JSON wires), `lowering/WireRender`, `sql/dialect/DuckDb.pivotSource`,
`sql/dialect/H2.needsStaticPivot`, `sql/dialect/MongoDbPipeline.pivot`, `exec/Executor` (the legacy route's columns).

## 2. Findings

1. **The group rule -- confirmed.** The engine's group columns are the input's minus the pivot columns minus every
   column the lowered aggregates reference (`processDynamicPivot`'s `originalAggColumns`); DuckDB's `PIVOT` shares
   the rule (a column the `USING` references is no implicit group key). Lite's `PivotChecker` removes every column
   an aggregate's map reads (`columnsRead`, anywhere in the map's body), landed in step 7's first landing from the
   channel A shadow run. No further difference found.

2. **The generated column's NAME carries the single quotes in the engine, everywhere.** On DuckDB's SQL the label
   is `'2000__|__newCol'` (the dynamic form's `concat('''', year, '''')`, the static form's alias); the interpreter
   names its column the same way (`PivotColumnInfo`); the PCT's expected text and its casts carry them; the
   executor's serializers print the label as it is. Lite's physical column and typed name are bare
   (`2000__|__newCol`: the typer's static columns, the DuckDB `PIVOT`'s own label), presented quoted by one rule
   (`Type.RelationType.presentPivotName`, engine-verbatim from `mayQuotePivotColNames`) on the TDS text
   (`Render.tdsHeader`, `resolveDeferredTds`), on the legacy route's columns (`Executor`) and on a cast's label
   (`Lowerer.castOverDynamicPivot`); the typer reads a column by either spelling (`pivotColumnType`,
   `Fold.pivotIdentity`). **The defect:** the CSV wire's header (`Render.csvWire`, `csvWireOfDynamicPivot`) and the
   JSON wires' keys (`jsonRowOf`, and the dynamic pivot's `jsonWireOfRows`, each row's own JSON) printed the bare
   name, so the same result named its columns two ways across its carriers, and the wires' names were not the
   engine's. **Fixed (this note's landing):** every wire presents the name as the engine labels it -- the static
   wires through `presentPivotName`, the dynamic ones through `Render.generatedName` (the key's text, the
   separator and the alias between literal quotes); the dynamic pivot's JSON wire is now built from its cells as
   its CSV and TDS text are (`jsonWireOfDynamicPivot`: the keys subquery, each member read from the row's own JSON
   by its physical name and tokenised by its template's type), which also retires step 4's named residue (the
   database's own tokens on that one wire). Pins moved with the reason: `PlanMakerTest
   .aDynamicPivotsTextIsBuiltByTheDatabase_asTheLegacyProbeBuildsIt` (CSV and JSON, dynamic; a static pivot's CSV and
   JSON added).

3. **Only a cast pivot executes in the engine.** Every `pivot` is typed `Relation<Any>`; the plan's TDS columns are
   the cast's; the executor refuses a label absent from them (`ExecutionNodeTDSResultHelper.getTDSColumn`), so an
   uncast pivot has no engine execution to compare against -- the PCT casts every pivot result before
   `assertTdsEquivalent`. Lite types a static pivot's columns and a dynamic pivot's templates, and an uncast pivot
   answers (its TDS text, CSV and JSON built by the database: step 4). Register S106.

4. **H2.** The engine refuses every pivot by name ("pivot is not supported"). Lite answers a static pivot on H2 by
   emulation (`H2.needsStaticPivot`, the staticization pre-pass) and refuses a dynamic pivot's text road at render
   by the row-JSON capability (`DialectCapability`: "a row as JSON reached a dialect without a row-to-JSON form"),
   a refusal that does not name the pivot. Register S108; the wording is a later item, not a defect of outcome.

5. **A cast naming a key no row has.** Both fail at the database: the engine projects `"root"."'2013__|__newCol'"`,
   lite reads the physical column by name (`castOverDynamicPivot`), and DuckDB refuses the unknown column either
   way. No difference.

6. **Composite keys.** The engine joins the pivot columns' values with the separator (`'UK__|__LDN__|__sum'`); lite
   synthesises the same composite key before pivoting (`Pivots.lower`), and the name rule applies to it the same
   way. The database-built texts spell one ON column only (`Render.PivotKeys.of`, refused by name for more); the
   engine has no static form over several columns either (the values overload takes one `ColSpec`).

7. **The static-pivot probe.** The probe that failed to type in the homework's first pass used
   `pivot(~[year], [2000, 2011], ~[...])`: no such overload exists upstream (the values form takes one `ColSpec` and
   one `AggColSpec`), so lite's refusal ("no overload ... matches 4 argument(s)") is the engine's own behaviour.
   The engine's form, `pivot(~year, [2000, 2011], ~newCol : x | ... : y | ...)`, types and runs here (the PCT's
   `testStaticPivot_SingleSingle` is green on DuckDB; the static case added to `PlanMakerTest`).

## 3. What changed

- `lowering/Render`: `csvWire`'s header and `jsonMembers`'s keys through `presentPivotName`; `PivotKeys` (the one
  keys subquery of the three dynamic builders), `generatedName`, `generatedCell`; `jsonWireOfDynamicPivot` replaces
  `jsonWireOfRows`.
- `lowering/WireRender.wrap`: the dynamic road dispatches both wires from the one pivot, its static columns and its
  templates.
- `PlanMakerTest.aDynamicPivotsTextIsBuiltByTheDatabase_asTheLegacyProbeBuildsIt`: the two pins, the static case.
- `docs/SEMANTICS_REGISTER.md`: S106, S107, S108.

## 4. Verdicts

See GATES "The pivot homework" (2026-10-18).

## 5. Open

Whether lite's physical and typed name should carry the quotes as the engine's does: `presentPivotName` would become
the identity, DuckDB's `ON` would spell the engine's `concat`, the static aliases, `Fold`'s matching, H2's
staticization and MongoDB's projection would follow. No carrier observes the difference today (every text presents
the engine's name; the plan's `ExecutionPlan.Column` names an uncast pivot's column bare, and the engine cannot run
one), so it waits for a consumer of the plan's column names that needs the engine's spelling.
