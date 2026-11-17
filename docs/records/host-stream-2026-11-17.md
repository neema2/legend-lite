# The HOST stream of the edge program (2026-11-17)

The TypeScript worker's record of the HOST stream, on branch `claude/total-wall-host`. Its base is
`claude/total-wall-d-stage` at 93e6ab886. The plan of record is `docs/records/total-wall-edge-program-2026-11-16.md`
(§1-§11) and `docs/records/edge-audit-2026-11-17.md` (section A), both on the orchestrator's branch.

## What landed

| Commit | Item | What |
|---|---|---|
| 78ed11f969 | N4 | Query builds `groupBy(~[], aggs)` for an aggregate with no key, as lite types it. legend-engine's `aggregate()` bend is gone. |
| 72d42608c2 | lite routes | Lite's grammar and typing routes on `/api/lite/v1`: `grammar/modelElements`, `grammar/parseLambda`, `grammar/printLambda` and `compilation/lambdaRelationType` (`server/LiteV1Planning`). |
| b1c34f7626 and 24ac6394a8 | H3 + H7, one piece (ruling (b)) | Studio and Query run on lite's own plan, run and API, in the tab (`TabEngine`) and on a server (`LiteServer`). |

## The rulings this piece applies

- **(b)**: no legend-engine adapter in the product apps, not even a temporary one. No product app uses `HttpEngine`,
  `RoutedEngine`, `BrowserEngine` or `LegendEngineExecutor`.
- **Q2, bind at RUN**: the hosts' runner takes each run's parameter values and binds them to the plan's declared
  parameters. It checks them as Java's `PlanParameters` does: `plan-runner.ts` `checkParameters` and
  `declaredParameter`.
- **Q-a**: a one-valued parameter whose value is a function (`today()`, `now()`, an adjusted date) is taken out of the
  lambda's parameters and set by a `let` ahead of the body. Upstream does exactly this: finos/legend-studio master,
  `packages/legend-query-builder/src/stores/shared/LambdaParameterState.ts`, `buildExecutionQueryFromLambdaFunction`,
  read 2026-11-17. A `$variable` value is refused by name.
- **Q-b**: the server-mode user is the store's own. `GET /api/lite/v1/query/currentUser` is on lite's store mount
  alone; the edge's `/api/pure/v1/query` path reads `currentUser` as a query's id.
- **Q-c**: the results cube's server rows come through the app's engine (`query/src/app/cube.ts` `liteRows`).
- **H7 placements**:
  - additive values-taking overloads in `Execution.runOnProjectDatabases` and `ProductRun.run`/`onProjectDatabases`;
  - a second runner interface, `LiteV1Planning.Runner`, handed in by `LegendHttpServer`'s mount line
    (`LiteV1Api.Runner` untouched);
  - lite's own JSON value reader, keyed by the lambda's declared types;
  - the typed-rows writer, `LiteV1Planning.Answered`, an `Output.Rows` sink in `result.HostRows`' cell form. Its
    state is a final frame object.

## Guard files moved (each with a dated note at the pin)

- `tools/guards/PlannedSqlTest.java` `MINT_REFERENCES`: `engine-client/src/legend/wasm-grammar.ts` 15 -> 16, the
  `executionPlan` answer typed as read.
- `FETCHES`: `engine-client/src/doors/lite-server.ts`'s one send is added. `query-store/src/client.ts` is folded to
  one send, `fetch(url`.

## Evidence (outside Bazel: the container's disk was full)

- javac, with `-sourcepath` and `-implicit:none`, of every touched class against the current sources.
- JUnit through a small launcher, with an emulated runfiles tree (`RUNFILES_DIR` whose `_main` is the repository;
  `-Dlegend.sources` and `-Dinventory.files` list `git ls-files`):
  - `LiteV1PlanningTest` 11/11;
  - `EdgeServerTest` 3/3;
  - `CodeShapeGuardrailTest` and `JavaEvalLedgerTest`, all pass;
  - `PlannedSqlTest`, `DataMechanismCensusTest` and `PlanVersionsAgreeTest`, 22/22.
- `engine-client/test/plan-runner.test.ts` 50/50 under node `--experimental-strip-types`. It runs on the WASM planner
  built earlier the same day (`WASM_PLANNER` pointing at `bazel-bin/wasm/planner_dir`) and node DuckDB.
  `node_modules` was a temporary link to `bazel-bin/engine-client/node_modules`, removed after each run.
- `query/test/run.test.ts` 4/4 and `studio/test/run.test.ts` 4/4.
- `studio/test/engine.test.ts` 12/13. Its in-tab compile case needs a `//wasm` build newer than the
  `compileErrorsOrError` export.
- `tsc --noEmit` is clean for query, studio and datacube.
- Each change is red on revert:
  - exec dropping the values;
  - the execution mount;
  - the store's user route;
  - the tab's column narrowing;
  - the declaration check;
  - Query's `let` rule;
  - Studio's signed-number and enumeration checks.

Not run here: `ArchitectureTest` (it imports Bazel's per-library jars), `TsLayeringTest`, the Query and Studio bundle
budgets, the verify e2e lanes, and `core_tests_server` in full. They are handed to the D stream.

## Open

1. A connection that declares its own test data (`testDataSetupCSV`) puts loads on the plan's target. The tab's run
   refuses it by name, since the tab's project database carries the project's Data alone. A probe with the real
   planner found that a project's Data puts no loads on the target.
2. The warehouse plane now sends the target's session statements through the runner. The warehouse lane has to
   confirm that.
3. `claude/total-wall-b3-batch`'s T-D (`studio/src/ui/editor.ts` `rowsTable`) meets this piece's `resultView` edit.
   The merge is mechanical: T-D's `rowsTable` over `r.columns.map((c) => c.name)` and `r.rows`.
4. H6 (the query store onto `/api/lite/v1/query`, its own page entry) is next.

## Lite API v1, the host train (2026-11-17)

The spec of record is `docs/records/lite-api-v1-2026-11-17.md`, on `claude/total-wall-h-datacube` at c075f4a32. The
orchestrator's go was "Go on the apps". This stream owns the server binding, the TS client, the apps' re-point and a TS
reader of DataCube's conformance cases.

| Commit | What |
|---|---|
| a7fedfc878 | The server binding. Lite's routes are `/api/lite/v1/<operation>` (LiteV1Api, LiteV1Planning). Lite's one error shape is `{kind, message, where?, raise?}`, its status by kind (J18). legend-engine's envelope moved to the edge (`edge/EngineEnvelope`). Also `queries/` and `user`, `query/run` in json or arrow, and the edge-off boot test. |
| e86ceb2c40 | The TS client by operation: `LiteServer` (a server) and `TabEngine` (the tab), lite's error shape (`src/lite-error.ts`), the Arrow read (`legend.shape`, `legend.type`), and 64-bit Integer parameters given as bigints. Query, Studio and query-store's `currentUser` are re-pointed. |
| 684ce57010 | The server binding's reader of `lite-api-cases.json` (`//engine-client:lite_api_server_test`). |
| 6afe479900 | Query's cube planner prints through `query/print`, in the cube's own words (after DataCube's 5d9b9583ee). |
| (next commit) | The guards' findings fixed, listed below, and `query/run` through `LiteServer` in Arrow and in JSON over a real server. |

### Guard findings and what was done

- `PlannedSqlTest.UNKNOWN_CASTS` (shrink-only): no new `as unknown as`.
  - `arrowIpcTyped` reads through `arrowIpcAnswer`, which now also gives each field's metadata.
  - `WasmGrammar.relationType`, which is the edge's Grammar shape, is answered by the edge's own route in the tab, as
    `returnType` is.
  - Query's cube types its columns through lite's `query/type`.
  - No pin moved.
- `PlannedSqlTest.FETCHES`: `LiteServer`'s one send keeps its pinned text, `${route}`. No pin moved.
- `PlatformNamesGuardrailTest` (bare-name arms): `query/print`'s style and `query/run`'s format are each read through
  one enum table, `LiteV1Planning.spelled`, not a switch over words. The count is back to 188. No pin moved.
- `JavaEvalLedgerTest`'s funnel register and `ErrorShapeGuardrailTest`'s broad catches: `edge/EngineEnvelope.java` is
  registered, each with a dated note. It is the file the envelope moved to. Its catch is the engine half of the pure/v1
  boundary; LiteV1Api keeps lite's own.
- The tab's DuckDB (DuckDB-WASM) binds values through JSON. Measured on 1.33:
  - a JSON integer past 2^53 is refused ("Invalid column type encountered");
  - a bigint cannot be stringified;
  - a JSON number is bound as a DOUBLE, even `1`.

  `duckdb.ts` `jsonBound` binds a bigint within 2^53 as its exact number. One past 2^53 is refused by name before
  anything is sent. A server binds it exactly (`lite-api-server.test.ts`).

### Open, for others

1. **native/ does not build on the train.** `nativelib/Compiler.java` calls the Boundary methods that DataCube's 2c
   removed. It is Python's binding, and Python has been told. `//core:guardrails_edge` cannot build until it moves.
2. **`RaiseEnvelopeConformanceTest` is red.** DataCube's H9 put lite's words in the shared fixture, and Java's
   `RaisedErrors` still prefers upstream's (the audit's K10). That is the Java half of H9.
3. **DuckDB's Integer placeholder is bound bare (D1).** On DuckDB-WASM, every Integer parameter is therefore a DOUBLE.
   A typed placeholder (`CAST(? AS BIGINT)`, the value bound as its text) would bind it exactly in the tab. That is a
   planner ruling, not this stream's.
4. **H6 is staged, not landed.** The query store's own routes, with a product page entry in lite's words, are saved as
   a patch for after the train.
