# Lite's native protocol: the design (DRAFT for the user's review, 2026-11-11)

> **The user, 2026-11-11:** "I think we should actually have lite clean sheet protocol that can work local or remote
> super fast and typed and always arrow no matter if local or remote. Then we should have remote compatibility mode that
> is drop in for engine or ours ... Basically make everything speak lite native / fast / shaped how we want. Then again
> at edge a purev1 compatibility mode that lite or engine could answer." And: "We can design now and then do it after
> this PR once we are 100% total separation."
>
> So this is a design, reviewed now and BUILT in the next program, after the total-wall PR. It comes from a read-only
> research read of `origin/claude/total-wall` at 5d18a7b9 (nothing built or run).

## 0. The idea in four lines

- **One native protocol (product):** about 16 typed operations; the same messages through a worker (the browser tab),
  in process (Python) or over HTTP (lite's server).
- **Every table answered as an Arrow IPC stream, typed exactly by the plan;** small JSON only where no table exists.
- **Errors typed:** a closed kind and where in the query, a fixed text known at planning, never a value.
- **`pure/v1` at the edge:** lite's server answers it (a drop-in for engine clients) as an adapter over the native
  operations; lite's apps reach a real legend-engine through a `pure/v1` client adapter.

## 1. What the apps call today (the inventory)

| where | operations | shape | local or remote |
|---|---|---|---|
| `engine-client` `legend/engine.ts:97-175` (HttpEngine) | grammar both ways, relationType, compile, lite compile (`:139`, probes for a 404), returnType, execute, generatePlan, currentUser | pure/v1 JSON | remote |
| `legend/wasm-grammar.ts:70-108` | the same over the worker (`pureV1` kind, answered `OK\n<status>\n<type>\n<body>`) | pure/v1 through the worker | local |
| `wasm-grammar.ts:177-249`; `browser-engine.ts:63-68, 94` | in-tab execute: jsonPlan, DuckDB, rows re-encoded as JSON (hostRows), jsonAnswer, TDS JSON, `JSON.parse` | rows encoded three times | local |
| `pure-v1.ts:129-195`; `engine-remote.ts:175, 206` | parse, print, relationType, elements, generatePlan, execute, executeArrow (zstd frame) | pure/v1 | remote |
| `datacube` `planner.ts:184-218` | generatePlan, its SQL lifted out and run in the tab | pure/v1 | planned remotely, run locally |
| `datacube` `lite-planner.ts:492-718` | planJson, executionPlan (writes), grammar via `pureV1`, model writes via `/api/lite/v1/model/*`, tableModel, catalogColumnsSql, sessionSetup, projectData, hostOperations, testData | the planner protocol's kinds | local |
| `datacube` `runner.ts:260`; `duckdb.ts:427-458` | PlanThenRun: the plan's SQL on DuckDB-WASM; the Arrow copied into JS arrays, then typed by the plan | Arrow, then arrays | local |
| `datacube` `upload.ts:254-395`; `remote.ts:103-184` | a file in, a remote view, the catalog, a model (S3 keys written into SQL text: B3's finding) | host operations | local |
| `services/python-host.ts:212-350` | `pureV1` over HTTP, `/plan.json`, `/plan-and-run.json` (already Arrow IPC with `legend.plan` and `legend.receipt` metadata), ~15 private `.json` routes | private JSON + Arrow | loopback |
| `cube-store.ts:191`; query-store `client.ts:57-86`, `wasm-server.ts:17` | saved cubes and queries at `pure/v1/query(/dataCube)` | pure/v1 JSON | either |
| `warehouse.ts:339-395` | SQL to the warehouse's SQL API, Arrow back | SQL in, Arrow out | remote |
| query `demo/main.ts:82-151`, `app/run.ts:67-79`, `ui/results.ts:288` | execute (TDS JSON drawn), generatePlan ("Show SQL"), lambda text/JSON, relationType | pure/v1 | either |
| studio `app/app.ts:37-135`; `backend/run.ts:95-114` | modelJson, compileErrors, execute, a raw SQL playground on the tab's DuckDB | pure/v1, raw SQL | either |
| python `_planner.py:18-36`, `compiler.py:101-272`, `frames.py:89, 248`, `engine.py:1482-1649` | the planner protocol's 17 kinds as C functions; Frames plan and run in process to a typed `pa.Table`; an HTTP server answering pure/v1 and ~15 private routes; files sent as 4 MiB base64 parts | C strings; Arrow from duckdb | in process, loopback |

The planner protocol has 17 kinds (`engine-client/src/legend/planner-protocol.ts:10-28`). DataCube's grid is its own
treegrid (`datacube/src/grid/grid.ts:1`).

## 2. What lite already has

- **The plan:** `ExecutionPlan.java` (sealed nodes; `Column` carries name, type, multiplicity, slot, store, precision);
  `PlanJson.VERSION` 42; `plan-runner.ts` matches it (`PlanVersionsAgreeTest`).
- **The Arrow writer:** `ArrowWriter.java` types each column from the plan with no Arrow library: timestamp[ns, UTC], exact
  decimal128, unions, List/Struct/Map, `legend.*` field metadata (S125's lite typing). But `Output.Arrow` is engine-shaped
  (`Arrow(OutputStream, PureV1Writer.Facts)`, refusing OBJECTS and VALUE answers): no lite-native Arrow output yet.
- **The tab and native boundaries** pass strings only, so Arrow crosses as base64 (`Folded.BASE64`,
  `lite_execute_arrow_answer`); `HostRows` exists only for the JSON round trip.
- **Held models** keyed by the SHA-256 of the model text (`HeldModels`, `cache/Hash.java`).
- **The server:** `LegendHttpServer.java:137-163` sends `/api/pure/v1/` and `/api/lite/v1/` to one dispatcher
  (`PureV1Api.route`, 10 pure/v1 routes and 4 lite routes), in `com.legend.server`.
- **Errors:** `Folded` answers `ERR\n<kind>\n<message>`; `DataError` carries the database's own text.
- **Credentials:** B3's named credential and planned `CREATE TEMPORARY SECRET` with bound values. **Files:** the server's
  `--sources` fence.
- **DuckDB natively:** DuckDB-WASM `query()`/`send()` answer Arrow, `send()` streams batches and `cancelSent()` cancels;
  `insertArrowFromIPCStream` loads Arrow; duckdb-python `.arrow()`/`fetch_record_batch()` answer Arrow, pyarrow converts
  to polars without copying.

## 3. The protocol, v1

**Operations (a closed list):**
- `hello`: versions and capabilities.
- `model.hold` {text} -> {key: sha256}; `model.release`.
- `model.check` (every error), `model.elements`, `model.print`, `model.change` (createClass, createAssociation,
  createDataSpace).
- `query.parse`, `query.print`.
- `query.type`: the columns {name, type, multiplicity, slot, precision, arrow} (replaces relationType and returnType).
- `query.plan`: the plan as data plus its SQL for display (replaces plan, planJson, planModel, executionPlan,
  generatePlan, executePlan).
- `query.run`: answers Arrow (replaces execute, plan-and-run, executeAnswer).
- `data.load` (bytes in) -> the table's model; `data.mount` {url, credential: NAME}; `data.forget`; `data.catalog` -> Arrow.
- `store.query.*`, `store.cube.*` (saved queries and cubes).
- `cancel`.

Seven of today's planner kinds stop being protocol operations (sessionSetup, testData, projectData, hostOperations,
tableModel, catalogColumnsSql, warm): they become the local host's internals serving `data.*` and a plan's loads.

**Messages:** a request is JSON `{"lite": 1, "op", "id", ...}`; a load's bytes travel as a binary body. Every table is an
Arrow IPC stream (`application/vnd.apache.arrow.stream`) typed by the plan, with the writer's field metadata and
`lite.protocol` / `lite.plan` on the schema; IPC buffer compression optional, never upstream's outer zstd frame. An answer
with no table is small JSON (`application/vnd.lite+json`). After the stream's end marker, a JSON trailer {rows, receipt,
error?}: a standard Arrow reader stops at the marker; a lite client reads the trailer (a failure part-way through a stream
lands there).

**Errors:** `{"error": {"kind", "phase", "at": {element, line, col, endLine, endCol} | null, "code", "text"}}`; kinds SYNTAX,
MODEL, TYPE, MAPPING, PLAN, UNSUPPORTED, DATA (`code` the SQLSTATE), REFUSED (fence, limit, credential), VERSION,
NOT_HELD, CANCELLED, INTERNAL; `text` a fixed sentence written at planning, no value interpolated (the rulings of
2026-11-10). The edge maps them to pure/v1's error JSON as `PureV1Api` does today.

**Transports:**
- **Tab:** a `LiteClient` whose local transport posts the same messages to the planner worker and runs plans on
  DuckDB-WASM; ArrayBuffers in postMessage's transfer list, not copied.
- **Python:** `Lite()` in process: the native library plans through one `lite_call(op, json)`, duckdb runs the plan, the
  answer is a `pa.RecordBatchReader`.
- **Remote:** `Lite(url)` sends `POST /api/lite/v1/<op>` and reads a chunked stream; the 200 goes once the first batch
  exists, so planning and statement errors come back typed; cancel by an aborted fetch or a `cancel` message; limits
  (JSON size, a per-file load cap, rows by `limit`); auth by a bearer token.

**State:** a request names its model by content key; an unheld key answers NOT_HELD and the client holds the model and
asks again (a typed step, not a silent fallback); held models leased and evicted least-recently-used; credentials named,
never values; `--sources` governs `data.mount` and `file:` sources.

**Exact typing:** the writer's type table moves into the plan as each column's `arrow` fact; the plan's SQL makes the
database produce that representation (G6); a host may only relabel (a time zone, metadata) or refuse by name, never
convert a value.

**Versioning:** `LITE_PROTOCOL = 1` declared in Java, TypeScript and Python and held together by a census test (as
`PlanVersionsAgreeTest`); the op list compared across the three (`planner-protocol.ts`'s `Same<>`,
`_headless.check_interface`).

## 4. pure/v1 at the edge

- **Server:** `PureV1Api` moves to `com.legend.edge.purev1` as an adapter over the native operations; `PureV1Writer` writes
  the JSON and TDS answers; ARROW_IPC is re-typed to upstream's typing and wrapped in its zstd frame; `Output.Arrow` becomes
  lite's own (S125: the native typing is lite's, the edge does exactly what upstream does).
- **Client:** `engine-client/src/edge/pure-v1.ts` implements `LiteClient` over a real legend-engine: it sends the model text
  on every call; `model.check` gives the first error only; `model.change` and `data.*` are refused by name; `query.run`
  goes through ARROW_IPC, conformed by `query.type`.
- **Engine-shaped only there:** ExecuteInput, TDS JSON, generatePlan's JSON, errorType, the zstd frame.

## 5. Migration order (the next program)

0. The spec, the constants, the guards.
1. The server's native operations beside pure/v1; then pure/v1 rebuilt on them, with the parity suite.
2. Python (closest already: plan-and-run answers Arrow with its plan).
3. DataCube.
4. Query and Studio (their results as Arrow in the CubeApp they embed).
5. Deletions: the `pureV1` worker kind and plannerFetch; executePlan/executeAnswer with jsonPlan, jsonAnswer, HostRows and
   hostRows; arrowPlan, the native Arrow entry points, Folded's HTTP and base64 encodings; BrowserEngine and RoutedEngine
   (HttpEngine to the edge); UpstreamPlanner and serverPlan; the base64 file parts; Python's `execute_json_*`.

**Guards:** `pure/v1` appears only in edge files (a census: 87 app-source lines today); the protocol version and op list
agree across Java, TypeScript and Python; a parity suite (the tab on Node WASM + DuckDB, Python and the remote server answer
byte-identical Arrow for every corpus query; the edge's answers match `core/src/test/resources/upstream-api`'s goldens); no
credential value in any request or plan; error texts from a register.

## 6. Questions for the user (each with a recommendation)

1. **The hosts' runners** (`plan-runner.ts` in the tab and Python's) are a second exec, against G1 (one exec).
   Recommended: keep them as bounded host runners held to the parity suite (byte-identical Arrow), rather than compiling
   exec to WASM and native.
2. **A database error's text** carries the database's own words, values included. Recommended: answer the SQLSTATE plus a
   planned text; keep the database's text in the server log only.
3. **A failure part-way through a stream:** recommended the JSON trailer after the Arrow end marker, rather than a lite
   framing around the IPC messages.
4. **Python's DataCube-only services** (page keeper, join, snap, opened, source, warehouse): recommended a `datacube.*` group
   in the same protocol and version.
5. **Saved stores:** recommended keeping upstream's Query and DataCubeQuery records, under native operations.
6. **The warehouse** takes SQL from the tab today: recommended leaving it out of v1; it takes `query.run` later.

## 7. Not determined (measure when building)

Whether DuckDB emits timestamp[ns, UTC] without a relabel; whether Safari hosts DuckDB-WASM inside the planner worker
(nested workers); whether TeaVM can export bytes without base64; how DuckDB-WASM hands IPC buffers to the page.
