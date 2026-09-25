# DataCube ↔ engine: the API contract — 2026-09-25

**The rule (user ruling, 2026-09-25):** legend-lite serves the *exact*
legend-engine HTTP APIs DataCube uses — same paths, same request and
response JSON, no legend-lite-specific shapes — so the **same client
code** runs against either server. Our DataCube speaks only this API, and
a saved cube is upstream's `DataCubeSpecification` JSON.

- **Client spec:** legend-studio `c5b2f2c78` (2026-09-14): the calls made by
  `legend-data-cube`, `legend-application-data-cube`
  (`LegendDataCubeDataCubeEngine`, the builder store, the sources) and
  `legend-query-builder/stores/data-cube`, resolved through
  `legend-graph`'s `V1_EngineServerClient`.
- **Server spec:** legend-engine `4.145.0` (`230c159196d`, 2026-09-09), the
  checkout at `~/legend/legend-engine`.
- Both are spec only (reference-checkout tenet): read, never run inside
  legend-lite.

Every path below is relative to the engine's base URL, which for
legend-engine is `http://host:port/api`.

## 1. The endpoints

| # | Endpoint | Request | Response | DataCube uses it for | legend-engine 4.145.0 | legend-lite today |
|---|---|---|---|---|---|---|
| E1 | `POST pure/v1/grammar/grammarToJson/lambda` (query params `sourceId`, `lineOffset`, `columnOffset`, `returnSourceInformation`) | Pure text | lambda protocol JSON | parsing the cube's query and calculated-column code | `GrammarToJson.java` | ❌ no endpoint; the parser + `ProtocolEmitter` already produce these bytes for function bodies (PMCD parity) |
| E2 | `POST pure/v1/grammar/grammarToJson/model` | Pure text | `PureModelContextData` JSON | a source's model text (freeform, local file) | `GrammarToJson.java` | ❌ no endpoint; **byte-exact PMCD emission exists** (`ProtocolEmitter`, 5,259-source parity) |
| E3 | `POST pure/v1/grammar/grammarToJson/valueSpecification` | Pure text | value-spec JSON | filter values, parameter values | `GrammarToJson.java` | ❌ |
| E4 | `POST pure/v1/grammar/jsonToGrammar/lambda` (`renderStyle`), `…/lambda/batch`, `…/valueSpecification` | protocol JSON | Pure text | showing the cube's query, the calculated-column editor, View Source | `JsonToGrammar.java` | ❌ no protocol-JSON **reader**, no grammar **composer** |
| E5 | `POST pure/v1/compilation/lambdaRelationType` (and `/batch`) | `LambdaReturnTypeInput {model, lambda}` | `RelationType {columns:[{name, genericType}]}` | the column set and types of a query, **including validating a calculated column before it is applied** | `Compile.java` | ❌ no endpoint; the compiler computes relation types |
| E6 | `POST pure/v1/compilation/lambdaReturnType` | `LambdaReturnTypeInput` | `{returnType}` | the type of an expression | `Compile.java` | ❌ |
| E7 | `POST pure/v1/compilation/autofix/transformTdsToRelation/lambda` | lambda + model | lambda JSON | opening a legacy TDS query in DataCube | `Autofix.java` | ❌ |
| E8 | `POST pure/v1/execution/execute` (`serializationFormat`) | `ExecuteInput {clientVersion, function, mapping, runtime, context, model, parameterValues}` | the engine's result JSON (relation: `builder` + `activities` with the SQL + `result.columns/rows`) | running the cube on the server | `Execute.java` | ⚠️ `/engine/execute` exists with a legend-lite shape — not the contract |
| E9 | `POST pure/v1/execution/generatePlan` | `ExecuteInput` | `ExecutionPlan` JSON | upstream's CACHED path: take the SQL out of the plan, run it in DuckDB-wasm | `Execute.java` | ⚠️ `/engine/plan` exists with a legend-lite shape; no `ExecutionPlan` JSON |
| E10 | `POST pure/v1/codeCompletion/completeCode` | `CompleteCodeInput` | `CodeCompletionResult` | typeahead in the code editors | **absent from 4.145.0** — the client has called it since 2024-10 and gets no answer from open-source engine | ❌ — to match 4.145.0 exactly, also absent; see §4 |
| Q1 | `GET pure/v1/query/{id}`, `POST pure/v1/query/search`, `GET pure/v1/query/batch` | — / `QuerySearchSpecification` | `Query` / light queries | the **Legend Query source**: load a saved query's lambda, mapping, runtime, parameters | `ApplicationQuery.java` | ❌ no store |
| Q2 | `POST pure/v1/query/dataCube/search`, `GET …/batch`, `GET …/{id}`, `POST …/dataCube`, `PUT …/{id}`, `DELETE …/{id}` | `DataCubeQuery {id, name, description, content, owner, createdAt, lastUpdatedAt, lastOpenAt}`, `QuerySearchSpecification` | the same | **save / load / delete DataCubes**; `content` is a `DataCubeSpecification` | `ApplicationQuery.java` (Mongo-backed upstream) | ❌ no store |
| S1 | `GET server/v1/currentUser` | — | user id | owner of a saved cube; "mine only" | server info resource | ❌ |

**Model contexts.** E5, E6, E8 and E9 take a `PureModelContext`. Two
variants matter:
- `PureModelContextData` (the model inline): what our DataCube sends today.
  legend-lite must read it, which needs the protocol-JSON reader.
- `PureModelContextPointer` (project coordinates): what a **Legend Query**
  or **function** source sends. The engine resolves it through the
  **metadata server (Depot)**, which is a second contract: `getProjects`,
  `getVersions`, `getVersionEntities`, `getVersionEntity` in
  `legend-application-data-cube`. Supporting pointers means legend-lite
  either speaks Depot's API as a client or serves a Depot-compatible one.

## 2. What legend-lite is missing, as pieces of work

| Piece | Serves | Existing base |
|---|---|---|
| **P1 Protocol-JSON reader** — engine protocol JSON (lambda, value specification, PMCD) → our `Protocol` records | E4, E5, E6, E8, E9, and every endpoint that takes JSON | `FromProtocol` (records → model) and `ProtocolEmitter` (records → bytes) are its two neighbours; this is the mirror of the emitter |
| **P2 Grammar composer** — `Protocol` records → Pure text, byte-matching legend-engine's composer (`renderStyle` STANDARD / PRETTY) | E4 | none: legend-lite parses text, it never writes it |
| **P3 Endpoints** under `/api/pure/v1/...`, beside `/lsp` and `/engine/*` in `LegendHttpServer` | E1–E3, E5–E8 | parser, emitter, compiler, planner, executor all exist |
| **P4 Result JSON** — the engine's execution-result serialization (relation / TDS result, `activities` carrying the SQL) | E8 | `server/serial/*` has legend-lite's own |
| **P5 `ExecutionPlan` JSON** — at least the single-store relational plan upstream's cache path reads | E9 | the planner has the SQL; the plan document is new |
| **P6 Query store** — saved queries and DataCubes, the `ApplicationQuery` API, stored in DuckDB on the server | Q1, Q2, S1 | none |
| **P7 Model pointers** — resolve `PureModelContextPointer` through a Depot-compatible source | Legend Query / function sources | none |

And on the DataCube side:
- **C1 One client.** Every server call goes through an engine-API client
  matching `V1_EngineServerClient`'s requests. `/engine/plan` and
  `/engine/execute` stop being called.
- **C2 The in-tab planner speaks the same JSON.** The WebAssembly planner
  takes `ExecuteInput` and returns `ExecutionPlan`, exactly as E9 does, so
  running in the tab is the same code talking to a different transport.
  (This is upstream's cached path: plan, take the SQL, run it in
  DuckDB-wasm.)
- **C3 Saved cubes as `DataCubeSpecification`** (query text, upstream's
  `DataCubeConfiguration` field names, typed source). Our snapshot and
  configuration map to and from it; fields upstream lacks (heatmaps,
  median, weighted average) ride where upstream's serializer tolerates
  them, or they are named as a divergence.

## 3. How "exact" is proven

A **differential**: record every request our DataCube sends while the
browser harness drives it, replay each against a running legend-engine
and against legend-lite, and
diff the responses byte for byte. Only fields that are
nondeterministic by nature (ids, timestamps, timings) are masked, and
each mask is named. The inverse also holds: upstream's own client
(`V1_EngineServerClient`) pointed at legend-lite gets the answers it
parses. A response legend-lite cannot yet match is a red row, never a
normalised one.

## 4. Decisions for the user

1. **Code completion (E10).** Open-source legend-engine 4.145.0 does not
   serve it. Match 4.145.0 exactly (no endpoint; typeahead comes from
   legend-lite's own completion inside the page), or serve the path
   upstream's client calls, which is a shape we cannot verify against a
   real engine?
2. **Model pointers (P7).** The Legend Query and function sources send
   project coordinates, which a real engine resolves through Depot. Does
   legend-lite need a Depot-compatible store now, or do these sources
   wait until the server phase?
3. **Which engine is the referee.** The runnable engine on this machine
   is `legend-engine-server-4.138.5-shaded.jar`
   (`/Users/neemsandv/legend/engine-dist`); the source checkout read for
   this contract is 4.145.0. The differential must run against the
   engine whose source defines the contract, so either build the 4.145.0
   shaded jar from the checkout or pin the contract to 4.138.5.
4. **The query store's home (P6).** Upstream keeps it in MongoDB behind
   the engine. Ours would be DuckDB on the same server as the server-side
   DuckDB work.

## 5. Order

1. **P1 reader + E1–E3** (grammar ↔ JSON over what exists) and the
   differential harness itself, so every later endpoint lands with its
   proof.
2. **E5/E6 relation and return types**: unblocks validating calculated
   columns before they are applied, as upstream does.
3. **E8 execute + P4 result JSON**; **E9 plan + P5** and **C2** (the tab
   planner speaks `ExecuteInput`/`ExecutionPlan`); **C1** (our DataCube on
   the engine API only).
4. **P2 composer + E4.**
5. **P6 store + Q1/Q2/S1 + C3**: save/load, exactly upstream's.
6. **P7 pointers**, with the server phase.
