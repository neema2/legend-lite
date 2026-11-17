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
| b0d46482ba | The guards' findings fixed, listed below, and `query/run` through `LiteServer` in Arrow and in JSON over a real server. Its commit message also says Query's remote cube had printed 'pretty' as standard. That was wrong: that print implements engine-remote's `RemoteExecutor`, whose style is legend-engine's `'PRETTY'`. The next commit restores the mapping. |
| f99a5c25de | H6, whole (the orchestrator's ruling of 2026-11-17), plus DataCube's 507a0afe0 merged and the server reader on its `bindings` and `answerShape`. See "H6" below. |
| 5b462bd207 | H11's in-train de-imports, and Studio's Save Query fixed. See "H11" below. |
| 80c4bc0e99 | `EdgeBoundary.refused`, for Python's native edge half. The user route's blank check is written portably. |
| d6671ab1ce | On the merge point: the app-runs census shrinks by five sites, and `ParameterValue`/`PlanValue` become records (`requests.ts`). Query's server-mode cube reads an enumeration column as String. The plan-runner test reads `PLAN_VERSION`. |
| a9a6ac4b53 | The warehouse's statement API takes positional parameters, bound at run (the orchestrator's ruling (a)). See "The warehouse's parameters" below. |

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

### H6: the query store at lite's route, in lite's words

- The page's store module is the product's. Its entry, `query-store/java/com/legend/queries/page/QueriesPage.java`,
  answers `/api/lite/v1/queries[...]` in `SavedQueries.LITES_WORDS`.
  - It moved by `git mv` from `com.legend.edge.queries`, census A3. Its exports are unchanged: `answer`, `cubes`.
  - Its target is `//query-store:page_entry`. It is now one of the build's edge-separation roots
    (`_EDGE_SEPARATION_ROOTS`), so the separation query proves it reaches no edge target. The query reads empty, and
    the planted bite still shows.
- `query-store/src/wasm-server.ts` answers lite's `queries` and `user`. A refusal is `{kind, message}` with the store's
  status, as the server's store mount answers it. A store's not-found stays 404, kind request, pending the user's word
  on a `notFound` kind.
- `query-store/src/client.ts` asks `/api/lite/v1/queries`.
- The edge's `/api/pure/v1/query` stays the server's, byte-exact.
- `query-store/test/conformance.ts` is one suite in two dialects:
  - LITE, which the server and the page both run;
  - ENGINE, legend-engine's path and words, which the server runs.
- `LiteV1Api.read` words a request that may hold no keys as "(it holds none)".

### H11: the in-train de-imports

- `legend/wasm-grammar.ts` (`WasmGrammar`, lite's planner in the tab) imports none of `legend/engine-api`, `grammar`
  or `planning-client`. `doors/lite-server.ts` imported none already.
- legend-engine's shapes over the same planner moved to `legend/wasm-edge.ts` (`WasmEdgeGrammar implements Grammar`):
  modelText, relationType, returnType, generatePlan, compile, execute's two halves, and the pure/v1 fetch over the
  planner. The edge's `BrowserEngine` takes a `WasmEdgeGrammar`.
- `wasm-edge.ts` is in engine-client's `_PLANNER` list until `//engine-client:edge` exists.
- `PlannedSqlTest`'s `MINTS` and `MINT_REFERENCES` moved with execute's plan half: a file move, dated
  (wasm-grammar.ts 16 -> 15, wasm-edge.ts 2).
- One DataCube test (`datacube/test/python-engine/execute.test.ts`) builds the edge's `BrowserEngine` on the edge
  grammar, a one-line edit. DataCube was told.
- Studio's Save Query (`studio/src/backend/query-builder.ts`) compared the parse answer, read by the protocol's
  reader, with the model's raw JSON. That never matched after e86ceb2c40, and `//studio:query_builder_test` was red.
  Both sides now go through `readLambda`.
- After the host train, as this stream's own piece: split `legend/wire.ts`, `legend/engine.ts` and `legend/pmcd.ts`.

### Red on revert (each shown, then restored; the tree clean after)

| Mutation | Target | What goes red |
|---|---|---|
| `duckdb.ts`: bind `params`, not `jsonBound(params)` | `//engine-client:plan_runner_test` | the 64-bit run case, and TabEngine's query/run |
| `lite-server.ts`: ask `format: 'json'` always | `//engine-client:lite_api_server_test` | "the same typed rows in each format" |
| `wasm-server.ts`: a store refusal as the module answered it, not lite's shape | `//query-store:wasm_test` | the page's suite in lite's dialect, and its own cases |
| the guards (before their fixes) | `//tools/guards:planned_sql_test`, `//core:guardrails`, `//core:census` | FETCHES, UNKNOWN_CASTS, MINTS, bare-name arms, the funnel register, broad catches |
| Studio's Save Query before its fix | `//studio:query_builder_test` | both of its write-back cases |
| the edge-separation query | `//core:edge_direct_dependents_planted` | the planted reach still shows; the real query, with the page entry a root, reads empty |

### The warehouse's parameters (the orchestrator's ruling (a), 2026-11-17)

- **Why.** A run's values are bound at RUN (Q2). The warehouse's SQL API took SQL text alone, so Query's three
  parameterized warehouse steps failed.
- **Ruled (a).** The API carries the values and the server binds them, so lite's API v1 §4 holds on every plane. (b),
  planning the values in, and (c), refusing, were rejected.
- **sqlapi.** `StatementRequest.parameters` holds each value as a String, a Long, a Double, a Boolean or null. Any
  other value is refused by name, never stringified. A whole number is 64-bit; one past it is refused by name.
- **Server.**
  - DuckDB's binder gains double and boolean.
  - A statement with no parameters runs as before, several statements included.
  - Passthrough and grant statements refuse parameters by name.
  - The native image's metadata gains the two downcalls.
- **TS driver.** `refuseParams` is gone. A bigint goes in the arguments' JSON as its exact digits.
- **Corpus.** `sqlapi-corpus.json` has a 14th scenario. Python's driver moves in its own piece (told).
- **Evidence.** `//query:verify_test` is green on every plane. `//warehouse:tests` and the corpus replay go red on
  revert.

### Open, for others

1. **native/ does not build on the train.** `nativelib/Compiler.java` calls the Boundary methods that DataCube's 2c
   removed. It is Python's binding, and Python has been told. `//core:guardrails_edge` cannot build until it moves.
   *Closed 2026-11-17:* with `EdgeBoundary.refused` on the train, `//core:guardrails_edge` builds and passes.
2. **`RaiseEnvelopeConformanceTest` is red.** DataCube's H9 put lite's words in the shared fixture, and Java's
   `RaisedErrors` still prefers upstream's (the audit's K10). That is the Java half of H9.
3. **DuckDB's Integer placeholder is bound bare (D1).** On DuckDB-WASM, every Integer parameter is therefore a DOUBLE.
   A typed placeholder (`CAST(? AS BIGINT)`, the value bound as its text) would bind it exactly in the tab. That is a
   planner ruling, not this stream's.
4. **`PLAN_VERSION = 57`** comes from DataCube's merge point (the plain-Decimal plan type). K10a and K10b are off
   this train. `RaiseEnvelopeConformanceTest`'s K10b half goes with them.

### DataCube's two findings (2026-11-17)

- **query/parse reads lite's own grammar.** `LiteV1Planning.queryParse` parsed at `GrammarLevel.LEGEND_ENGINE`, so
  lite's islands (`#Cypher{...}#`) were refused by lite's own route. That was a product bug.
  - It now parses at `LEGEND_LITE` through a dialect-named overload of the parse door (`Parse.lambdaJson(text, si,
    dialect)`, over `SpecParser.lambdaJson`/`parseLambda` at a level).
  - legend-engine's `grammarToJson`, the edge's, keeps `LEGEND_ENGINE`.
  - `PhaseEntriesRefusedInsideARunTest` pins the new door's refusal inside a run.
  - Witness: `LiteV1PlanningTest.parseReadsLitesGrammar`. It parses a `#Cypher` island and shows that the engine
    grammar refuses it. It goes red on revert.
- **The plan runner decides no binding by a type's name.** `parameterValue`'s `case 'Integer': ...` switch is now a
  switch on `types.ts`' `parameterKind` (the plain type's kind, read from `TYPE_FACTS`). The unknown arm is
  `case undefined`, refused by name as before.
  - Witness: `//datacube:guardrails_test`, "decides nothing by comparing a type name". It goes red on revert.
- **Green:**
  - `//core:core_tests_server`, `//core:core_tests_root`, `//core:guardrails`, `//core:guardrails_edge`;
  - `//core:lite_api_conformance_test`, `//engine-client:lite_api_server_test`, `//engine-client:plan_runner_test`;
  - `//datacube:guardrails_test`, `//datacube:cypher_spike_test`, `//datacube:typecheck_test`;
  - `//studio:run_test`, `//query:run_test`, `//tools/guards:planned_sql_test`,
    `//tools/deps:engine_client_run_alone_test`.
