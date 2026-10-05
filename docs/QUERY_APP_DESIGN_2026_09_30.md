# Legend Query, rebuilt light: design and parity plan (2026-09-30)

**What:** one browser app, `query/`, that replaces legend-studio's Query application
(`legend-application-query` + `legend-query-builder` + the data-space viewer). It is
built on legend-lite and legend-lite's WASM planner, and speaks **only upstream APIs** so
that pointing it at a real legend-engine later is a configuration change.

**Parity checklist source:** `query/docs/UPSTREAM_QUERY_CENSUS.md`, read from
finos/legend-studio master `8dc8e4ce` (2026-09-29). Every feature below cites its section.

**Out of scope:** DataSet Browser (DSB) is not open source and is not a reference.
Lakehouse, data-product access requests, ingest, the SQL playground and service
registration are marketplace or Lakehouse features, not query features; they are listed
last and deferred.

## v1 status (2026-10-01, the first push to main)

- **Three planes, one app** (`query/demo/config*.json`): in the browser (legend-lite's planner as WebAssembly
  plans, DuckDB-WASM in the tab runs; saved queries in IndexedDB -- no server at all), the warehouse
  (DataCube's Live plane: the warehouse's DuckDB as the signed-in user; run by `//query:verify` on the native warehouse it starts, every step as in the other planes),
  and legend-lite's server (it plans, executes and keeps saved queries; `--query-store DIR`).
- **Server: thin API only** (`PureV1Api`, `SavedQueries`): execute with `parameterValues`, graphFetch results,
  `compilation/compile`, `lambdaReturnType`, the query store, `currentUser` -- each routed to existing lite
  functions. The analyses (G6, G9) are not ported (the user, 2026-09-30).
- **Graph fetch, one tree** (core): text, protocol JSON and the compiler share `GraphFetchLiteral`; the parser's
  second (character) parser and the column-list disguise are gone (`docs/IN_FLIGHT.md`, fourth line).
- **Results:** the plain grid by default; a Grid | DataCube switch opens a `CubeApp` over the query (its source
  is the query the form built; its own operations are Pure on top, planned by the same worker). DataCube opens
  with its controls hidden (its own mode; "Show Controls" is its right-click menu's last entry) and follows
  Query's light/dark (DataCube's dark theme, opt-in by the page).
- **Proof:** `bazel run //query:verify` -- its steps in the browser plane and on the server, in CI's browser lane;
  `//query:tests`, `//query:typecheck_test`; core's `PureV1ApiTest`, `SavedQueriesTest`, `ProtocolReaderTest`.
- **Upstream's look, feel and layout** (2026-10-01, after v1): legend-art's tokens (default-dark,
  legacy-light behind the switch), Roboto/Roboto Mono/Raleway, the same icons (react-icons 5.5.0
  paths, `query/tools/icons.mjs`), the app bar and builder header, the resizable four-panel
  workspace with upstream's panel chrome, the explorer/fetch-structure/filter/results interiors,
  `/` as the builder and `/setup` as the setup page. Measured from legend-studio 821c74c.
- **Next:** the open items below (relation-accessor sources, coverage G6, Depot model source,
  UI polish), then Studio/LSP; constants, milestoning, percentile/wavg and derived-property arguments are in (2026-10-01).

---

## 0. Decisions

| # | Decision | Why |
|---|---|---|
| D1 | **Upstream wire only.** Engine calls are legend-engine's `/api/pure/v1/...` paths and shapes; saved queries are the `/pure/v1/query` store's `Query` shape; project lookup (later) is Depot's. No legend-lite-only endpoint. | The user's rule; also the repo's standing ruling (`UPSTREAM_ENDPOINTS_DESIGN_2026_09_27.md`). |
| D2 | **Where a server is missing, a local implementation of the same contract.** The saved-query store has an HTTP implementation (`/pure/v1/query`) and an IndexedDB one with the same interface and the same `Query` records. The model source is `.pure` text today (`PureModelContextText`, which upstream accepts) and Depot entities later. | Nothing invented; swapping backends swaps an implementation, not a data shape. |
| D3 | **WASM where it can, server where it must.** Grammar, typing (`lambdaRelationType`) and planning run on legend-lite's WASM module in a worker; execution goes to the server. The routing is fixed by configuration per operation, never a fallback on failure (AGENTS.md: no fallbacks). | The user's ask; also makes typing and "show SQL" instant. |
| D4 | **Relation API for tables, graphFetch for objects.** New tabular queries are Relation (`project(~[...])`, `groupBy(~[..],~[..])`, `extend(over(..))`, `sort`, `limit`...). graphFetch + `serialize` is a first-class mode (it is not legacy -- it is how JSON/object graphs are fetched). Legacy TDS (`project([..],[..])`, `tds::filter`, `olapGroupBy`) is read: opened, run, shown as text, never produced by the form. | User decision (2026-09-30). |
| D5 | **Queries are protocol JSON, never built Pure text.** The form builds V1 lambda JSON with `pure-protocol`; text is only for people, via `jsonToGrammar`. | Same rule DataCube keeps (guardrails); Pure's `&&`/comparison precedence makes built text wrong-by-construction. |
| D6 | **Plain TypeScript and DOM, no framework, no new npm dependency to start.** Same idiom and toolchain as DataCube (esbuild, node's test runner, jsdom, strict tsc). | Lightweight; one idiom in the repo; the npm lock lives in `datacube/`, which this line does not touch. |
| D7 | **DataCube is used, not forked.** Query runs on DataCube's engines (DuckDB-WASM, the warehouse) and embeds a `CubeApp` as its optional result view (the plain grid stays the default, census §7). Changes inside `datacube/` are DataCube features cleared with its line and announced in `docs/IN_FLIGHT.md` (the controls-hidden mode, the dark theme). *(Was: "does not edit `datacube/`", 2026-09-30; superseded by the user's asks of 2026-10-01.)* | One DataCube; a host owns only what it hosts. |
| D8 | **Parameters travel as upstream's `parameterValues`.** legend-lite's `execute` takes them (bound as `let`s server-side, as upstream binds function-valued ones, census §5.5); the browser plane binds them the same way before planning, and the DataCube source substitutes them. *(Was: "bound as `let` statements by the client", before lite took `parameterValues`.)* | One request shape for both engines. |
| D9 | **Routes are upstream's** (`/edit/:id`, `/create/manual/:gav/:mapping/:runtime`, `/extensions/dataspace/:gav/:path/:ctx?`, `/create-from-service/:gav/:service`, `?p:name=value`), under a hash so the app is a static site. | Deep links from upstream tools keep working. |

## 1. What legend-lite gives today (probed 2026-09-30 against `query/demo/models/trading.pure`)

Works through `/api/pure/v1`: `grammarToJson/model`, `grammarToJson/lambda`,
`jsonToGrammar/lambda`, `lambdaRelationType`, `generatePlan`, `execute` (TDS/relation
results). Verified queries: class `project(~[...])` with to-one navigation, derived
properties (`fullName()`, `notional()`), enum filters, `exists` over to-many, relation
`groupBy` with sum/count, `sort`/`limit`, `extend(over(..), rank)`, post-project `filter`,
`distinct`, `let` statements, `->from(mapping, runtime)` and `->from(runtime)`.

**Gaps, each an upstream endpoint or shape, to close in `core/server` (G-series):**

Status 2026-09-30. "Thin" rows route an upstream address to what lite already does; "platform" rows
are engine analyses lite does not have -- new core features, NOT added on this line (the user's call).

| # | Gap | Upstream | Kind | Status |
|---|---|---|---|---|
| G1 | `execute` with `parameterValues` | ExecuteInput.parameterValues | thin | done (bound as `let`, engine-measured) |
| G2 | graphFetch results through `execute` | `{builder:{_type:'json'}, values}` | thin | done (engine-equal) |
| G3 | `compilation/lambdaReturnType` | `{model, lambda}` → `{returnType}` | thin | done |
| G4 | `compilation/compile` | model → ok / first error | thin | done |
| G5 | query store `/pure/v1/query` (+ `search`, `batch`, `history`, `patchQuery`) | engine query store | thin (storage) | done: `SavedQueries`, `--query-store DIR` |
| G6 | `analytics/mapping/modelCoverage` | mapped entities and properties | **platform** | proposed for core. A port of the engine's analysis was written and measured, then taken out; it is in git history (commit 8f2fc7e70, "analytics/mapping/modelCoverage") with its engine fixtures. A core version could read the compiled mapping's property functions instead of porting the engine's Pure. Meanwhile the app shows every property. |
| G7 | `server/v1/currentUser` | user id | thin | done (`anonymous`, as the engine without sign-in) |
| G8 | `executionManager/cancelUserExecution` | cancel | -- | client aborts the request |
| G9 | `analytics/dataSpace/render` | data space analytics | **platform** | proposed for core; not added. The app's data space page reads the model itself (display only). |
| G10 | `execute?serializationFormat=csv_transformed` | CSV export | -- | client writes CSV from the result |
| G11 | WASM twin of `grammarToJson/model` | -- (client plane) | thin | done (byte-identical) |

## 2. Architecture

```
query/src/
  wire/        upstream shapes: PMCD elements (class, enum, association, mapping, runtime,
               dataSpace, service, function, profile), Query store records, ExecuteInput,
               execution results. Types only.
  backend/     Engine (the /pure/v1 calls a query app makes), QueryStore, ModelSource.
               http-engine.ts (lite or engine), wasm-engine.ts (grammar/typing/plan in a
               worker), routed-engine.ts (per-operation routing from config, D3),
               local-query-store.ts (IndexedDB), http-query-store.ts.
  graph/       an index over PMCD: packages, classes with inherited + association
               properties, derived properties, enums, profiles/docs, mappings -> mapped
               classes and properties (coverage, G6), runtimes -> mappings, data spaces
               (analytics, G9), services, functions.
  builder/     the query as state: source (class+mapping+runtime | data space context |
               service | accessor), projection columns, filter tree, aggregation, window,
               post-filter, result modifiers, parameters, constants, milestoning,
               graph-fetch tree. build.ts: state -> lambda JSON (D4, D5).
               load.ts: lambda JSON -> state, or "unsupported" with the reason (text mode).
  ui/          DOM components; one file per panel.
  app.ts       the shell: routes (D9), header, panels, settings.
query/demo/    index.html, main.ts, config.json, models/*.pure, serve.mjs
query/test/    node:test + jsdom; builder round-trips against the WASM parser.
```

## 3. Milestones and the parity checklist

Status: `[ ]` todo, `[~]` partial, `[x]` done. Section numbers are the census's.

### M1 -- the core loop (a person can find data, build a query, run it, save it)

- [x] Package skeleton: Bazel (js_library, esbuild bundle, tsc test, node tests), demo site, `bazel run //query:serve`.
- [x] Config (`demo/config.json`): engine URL, planner plane, model sources, current user (G7).
- [x] Model source: `.pure` files -> PMCD (`grammarToJson/model`) -> graph index.
- [x] Landing (census §1, §2): the editor with a source picker -- data spaces, then classes (mapping query), services, recent queries; recently viewed data spaces and queries in localStorage.
- [x] Data space viewer (§3.3): header with Run Query, description (markdown), execution contexts, models documentation (searchable), quick start (executables with Open in Query and query text), support, info.
- [~] Setup panel (§3.4, §5.1): data space -> execution context -> class; class -> mapping -> runtime; service -> execution.
- [~] Explorer (§5.2): property tree with derived and subtype nodes, lazy children, mapped-only (coverage), row-explosion and derived badges, tooltips with docs, humanized names toggle, double-click/drag to add. *(v1: no mapped-only coverage -- G6.)*
- [x] Projection columns (§5.3): add (drag, double-click, menu), rename with validation, remove, reorder, clear.
- [x] Filter (§5.4): AND/OR tree, conditions from explorer drag, operators per type, value editors (string, number, boolean, enum, date with today/now/relative, lists with CSV paste), exists for to-many chains, variable values.
- [~] Parameters (§5.5): create/edit/delete, used-in-query guard, values prompt before run, `?p:` URL overrides.
- [x] Query options (§5.3): sort, distinct, limit, slice.
- [x] Run (§7): preview limit + overflow detection, row count and time, stop (abort), errors panel, executed SQL.
- [~] Result grid (§7): virtualized, column sort, copy cell/row/with headers, Filter By / Filter Out drill-through.
- [x] Text mode (§5.6): show/edit Pure, parse -> rebuild form, unsupported mode (still runs, saves, exports).
- [x] Save (§8): create / save / save as / rename / delete, query loader with search, mine-only and sort, `/edit/:id`, stored execution context and tagged values.
- [x] CSV export (§7, G10).

### M2 -- the builder complete

- [x] Aggregation (§5.3): count, distinct count, sum, avg, min, max, std dev, percentile, joinStrings, wavg.
- [x] Window / OLAP (§5.3): sum/count/min/max/avg, rank, dense rank, row number, percent rank; partition and sort.
- [x] Post-filter (§5.3): on projection, aggregate and window columns; column-to-column.
- [x] Derivation columns and constants (§5.3, §5.5): calculated columns; constants simple (typed values) and calculated (any expression), compiled before applied.
- [x] Derived-property arguments (§5.4): typed defaults when added, edited from the column or condition (literals, relative dates, parameters, constants).
- [~] Milestoning (§5.5): business/processing/bitemporal dates, all versions, propagation. *(Not: all versions in range -- both parsers refuse `allVersionsInRange` at 4.145.0; graph fetch through a milestoned property.)*
- [x] graphFetch + serialize (§5.3) with JSON result view -- needs G2.
- [x] Typeahead for string values (§5.4) and preview data on a property (§5.2).
- [~] Property search (§5.2) with type filters and "show in tree".
- [x] Undo/redo, change detection with navigation guard, query diff (§5.7).
- [~] Plan viewer (§7), relation (accessor) source with relation explorer (§6). *(v1: the executed SQL; no plan tree, no accessor source.)*
- [x] Revisions and history (§8), query info modal, version change.
- [x] Open the result in DataCube (§6) -- after DataCube's F6. *(v1: the Grid | DataCube switch.)*

### M3 -- backends and breadth

- [ ] Close G1-G11 in legend-lite's server and WASM.
- [ ] Depot model source (project/version pickers, GAV routes) and PMCD/pointer model contexts; verified against a real legend-engine.
- [ ] Watermark, calendar aggregation, check entitlements, lineage, functions explorer (§5).
- [ ] Data products / Lakehouse (§4) if wanted.

## 4. How we will know

- Every builder feature has a round-trip test: state -> JSON -> the WASM parser's text -> JSON
  is byte-identical, and load(build(state)) == state.
- Every run-path feature has an execution test against the demo model on the lite server.
- A browser harness drives the M1 loop end to end (find, build, run, save, reopen).
