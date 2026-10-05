# In flight

This file says who is working, on what files, in what order, and the rules between sessions; each line's status lives
in its own plan (plan rule 0b.16).

**Active lines and their order (the user, 2026-10-04).** Several sessions push to `main`; each announces a cross-area
edit here before landing it and lands with the full chain. The priorities are **Bazel, Studio, and the database owner
with the compiler's plan/execution split**, in this order:

1. **The Bazel program** (`docs/BAZEL_IMPLEMENTATION_PLAN.md`; log `docs/BAZEL_EXECUTION_LOG.md` on
   `docs/bazel-first-class-plan`): build files — `core/BUILD.bazel`, `tools/deps`, `MODULE.bazel` (splitting out a
   `release.MODULE.bazel`), `pct`, `spec`, `parser-equivalence`, the guards. Batch 8 lands first. It also carries the
   reference-lane fix the user decided ("fix rule to match pure": legend-pure's overload for `min`/`max` over a list
   literal), in `core/compiler/spec` — no one else edits those files until it lands.
2. **Studio** (`docs/STUDIO_FULL_PLAN_2026_10_04.md`; PR #24 `studio-m1`, then `query-by-name`): `studio/`,
   `legend-art/`, `query/`, `datacube/` (imports and labels), `site/`, a `@fontsource` block in `MODULE.bazel`. Nothing
   in `core/`. Lands on the user's merge; `MODULE.bazel` is shared with the Bazel program (the second to land rebases).
3. **The database owner** (the fourth line below; `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md`): C3c
   now (no shared files); **C1/C2, the plan/execution split, starts only after the Bazel program's batch 8 is on
   `main`** (both touch `core/BUILD.bazel` and `tools/deps`), announced to the other lines first.

**Parked:** the compiler rebuild (`docs/EXECUTION_PLAN_2026_09_26.md`; paused, coming back later — its open items C4,
B2/B3 and the W6.2 runner wait for it); DataCube + Python (two local commits in `legend-lite-dcsnap`: a GraalVM native
library with Python bindings and a `Typer` fix, which meets the Bazel program's overload fix on rebase); the server
program (`docs/SERVER_PROGRAM_2026_09_26.md`); NLQ (the untracked `nlq/` directory is not ours; leave it).

**Shared machine:** at most two heavy Bazel/JVM jobs at once; before a full `bazel test //...`, check that another
session is not running one (a browser test, `//studio:verify_test`, times out under two full chains).

## A second line, 2026-09-29: DataCube against the warehouse (the user's ask, rule 5)

In the worktree `legend-lite-dcsnap`, branch `datacube-live-snap`. **Owns:** `datacube/`. **Touches `warehouse/`** (one
line each, announced here before landing):
- `server/Identity.java`, `server/WarehouseServer.java`: `POST /sql/v1/token/refresh` (a valid token for a fresh one,
  never past the sign-in's session limit, 12h default); tokens carry their sign-in time (`principal|expiry|signedInAt`);
  `--token-key-file` (a key kept across restarts), `--token-minutes`, `--session-hours`; `Config.sessionLimit`.
- `server/Statements.java` (+ `Identity.hasUser`): a GRANT must name an object (or schema) that exists and a
  grantee (or role member) who is a user or role; REVOKE stays open. Test:
  `WarehouseEntitlementsTest.aGrantNamesSomethingThatIsThereForSomeoneWhoIs`.
- `sqlapi/SqlApiBinding.java`, `sqlapi/NativeBinding.java`: `refresh(token)`. Tests: `IdentityTest` (new),
  `WarehouseServerTest.aValidTokenRefreshesAndTheFreshOneWorks`.

**Touches CI** (`.github/workflows/gates-run.yml`, `gate.yml`): a `browser` lane, Linux only --
`//datacube:live_snap_test` and the `browser-ci` harnesses; the other lanes unchanged. `docs/GATES.md`: its row.

Nothing in `core/`, `spec/`, `tools/`. Runs a warehouse (`:9090`) and the DataCube site (`:8000`), idle: stop them before
a timing (rule 4).

## A third line, 2026-09-30: DataCube track A and the BI plan (the user's ask, rule 5)

`docs/BI_AND_ETL_PLAN_2026_09_29.md`; the user: "start track A now, and go in the suggested order for everything that does
not need any server side work". **Owns:** `datacube/` (and its CI lane). No edit in `core/`, `spec/`, `tools/` or
`warehouse/`; the plan's server-side items (F1 write, F2's resolver, F4, F5, unpivot) wait for the rebuild's say (§8).

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "announce it and do
it"). So a file opened in DataCube's tab has its model written WITHOUT the WebAssembly module, on any planner (legend-engine
included), and the writer cannot drift from the compiler's:
- STRUCTURED, no type string parsed (the user: "do the homework first of how to do this CORRECTLY in structured form"):
  `DuckDb.CATALOG_COLUMNS_SQL` reads a table's columns from `duckdb_columns()` joined on the type id to
  `duckdb_types()` -- each column's canonical type, and a DECIMAL's precision and scale as numbers. `catalogType` is
  data over that (`CATALOG_TYPES`, `CATALOG_ALIASES` for JSON, `CATALOG_REFUSED` with reasons); the regexes are gone.
  `CatalogModel.Column` CHANGED: `(name, dataType, logicalType, precision, scale)`. `CatalogModelTest` reads real
  DuckDB tables, and a completeness test fails on a canonical type with no decision.
- Callers moved with it: `wasm/.../Wasm.java` `databaseFromCatalogOrError` takes the structured column; the warehouse's
  `/sql/v1/catalogs/{c}/objects` adds `logicalType`, `precision`, `scale` to each column (`type` kept).
- `datacube/tools/catalogfacts/` generates `datacube/src/generated/catalog-facts.ts` (the tables and the catalog
  question) and `datacube/test/generated/catalog-corpus.ts` (what `CatalogModel.database` answers for real DuckDB
  tables); `datacube/src/catalog-model.ts` is DataCube's writer, tested against the corpus case for case.
  `WasmPlanner.databaseFromCatalog` is kept (its input is now the structured column); DataCube no longer calls it.

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "i think we should fix calc
column"). legend-engine refused 13 DataCube features that legend-lite passes; one cause is legend-lite's own leniency:
- `core/.../compiler/spec/Typer.java` `collection`: a literal of more than one value requires each element to be exactly
  `[1]`, as legend-engine (`ValueSpecificationBuilder.visit(Collection)`) and legend-pure (`InstanceValueValidator`) do.
  `$x.notional * 1.1` over a nullable column is then refused, as on engine (write `->toOne()`). This reverts the
  2026-09-11 loosening (`MULTIPLICITY_AUDIT_2026_08_20.md` §4a), whose premise "real pure accepts it" legend-pure's
  source contradicts; `MultiplicityStrictnessTest` flips back to expecting the refusal.
- NOT changed in `core/` (the user: engine's gaps are compensated in DataCube and recorded, not "fixed" in legend-lite):
  `over(String[*], SortInfo[*], Frame[0..1])` (in engine's over.pure, never registered by its Handlers.java) and BIT
  typed Boolean (engine's RelationalCompilerExtension says TinyInt; legend-pure says Boolean). Both get register rows.

**2026-10-01, announced before landing (rule 5): a cross-area edit in `core/`** (the user: "do them"). A column DuckDB's
catalog says is NOT NULL is declared `NOT NULL` in the written Database, so legend-lite and legend-engine type it `[1]`
and `$x.n * 1.1` compiles over it without `->toOne()`:
- `DuckDb.CATALOG_COLUMNS_SQL` reads `duckdb_columns().is_nullable`; `CatalogModel.Column` gains `notNull`; a line is
  `name TYPE NOT NULL` for such a column. Callers move with it: `Wasm.databaseFromCatalogOrError`, the warehouse's
  catalog listing (adds `notNull`), DataCube's generated writer and corpus.

## A fourth line, 2026-09-30: the Query app (the user's ask; design `docs/QUERY_APP_DESIGN_2026_09_30.md`)

**STATUS 2026-10-01: v1 LANDED on main** (the branch `query/app`, fast-forwarded after the full gate). Everything
below is on main; the line continues on the same branch -- next, upstream's look and layout, then the gaps in the
design doc's v1 status. A cross-area edit from here on is announced here first, as before.

In the worktree `legend-lite-query`, branch `query/app`. **Owns:** `query/` (new). **Touches, one line each:**
- `fixtures/saved-queries/` (new, 2026-10-01, asked by the DataCube line): saved queries exactly as
  `GET /api/pure/v1/query/{id}` answers -- explicit context, data-space context, defaultParameterValues, a graph
  fetch (not a cube source) -- made through the real API and each run on the demo model (`make.mjs`); README.md
  says how to read one. `js_library` `//fixtures/saved-queries:records`, visible to `//query` and `//datacube`:
  Query tests its reading against it (`query/test/saved-queries.test.ts`, through `persist.ts contextOf`); DataCube's
  saved-query source will test against the same files.
- `.github/workflows/gates-run.yml` (2026-10-01): the browser lane runs every `browser-ci` target in `//query` as
  well as `//datacube` -- `//query:verify` (the end-to-end steps, both planes), its server on Bazel's JDK.
- `core/src/main/java/com/legend/server/PureV1Api.java` + `LegendHttpServer.java` routes (+ `PureV1ApiTest`):
  the upstream `pure/v1` endpoints the Query app needs and lite lacks, each in legend-engine's shape, measured
  against 4.145.0 -- `execute` with `parameterValues`, `compilation/compile`, `compilation/lambdaReturnType`,
  graphFetch results, the query store. Each routes to existing lite functions; no new compiler logic.
- `core/src/main/java/com/legend/Compiler.java`: `executeWire(model, ValueSpecification, ...)` gains the graph-fetch
  branch its text twin already has (one `if`, no compiler logic) -- `execute` answers graphFetch as the engine does.
- `core/src/main/java/com/legend/server/SavedQueries.java` (new): the engine's query store, in a directory the
  server is started with (`--query-store DIR`).
- NOT on this line (2026-09-30, the user's call): `analytics/mapping/modelCoverage` and `analytics/dataSpace/render`
  are engine ANALYSES lite does not have -- new platform features, proposed for core (design doc §1, G6/G9), not
  added here. The Query app shows every property and lists a mapping's classes as the mapping declares them.
- Graph-fetch trees: ONE representation (2026-09-30, crosses parser/protocol/compiler -- announced here).
  `GraphFetchLiteral` is the tree only (class, property nodes, root subtype entries); the column-list desugaring
  it carried is gone. The parser reads a tree with its grammar only (the `IslandScan` character scanner's graph
  half is deleted; the tree re-lexes its slice at its real line/column for wire spans). `ProtocolReader` gains
  `rootGraphFetchTree` (the two print-and-parse workarounds in PureV1Api and Wasm are gone). `NameResolver` keeps
  the tree (class names resolve in place; call arguments are its children). `GraphFetchChecker`, `IsDistinctChecker`
  and lineage `ScanRelations` walk the tree; everything after the checker (`TypedGraphTree`) is unchanged.
  Graph-position argument spellings live only in `ProtocolEmitter.gftParam` and `ProtocolReader.graphArg`.
  Evidence: `//spec:corpus_duckdb` + `:corpus_h2` per-test pass/fail lists and judge ledgers identical to the
  pre-change baseline (trace ids masked); `//parser-equivalence:diagnostics`, `//core:core_tests` (4295),
  `//core:guardrails` (no pin raised), `//query:verify` green. Fixes W1.2's `graphFetchKeepsSubTypeTrees` known
  defect. Unchanged gaps, noted: a tree typed on its own still refuses (the engine's type is
  `RootGraphFetchTree<T>`); lineage does not trace inside subtype views; `prop()` vs `prop` is not on the wire.
- `wasm/src/main/java/planner/Wasm.java`: `modelJsonOrError` (E2's twin, byte-identical to the server's).
- `datacube/BUILD.bazel`: ONE line, `visibility = ["//query:__pkg__"]` on `:src` (2026-09-30), so the Query app runs
  its planned SQL on DataCube's engines (`engine.ts`, `duckdb.ts`, `warehouse.ts`). And (2026-10-01, user-approved) a
  `styles` filegroup (`src/**/*.css`, visible to `//query`): Query's results grid is a `CubeApp` over the query, so
  its page loads DataCube's stylesheets. Query uses `CubeApp`, `Planner`, `sourceColumns`,
  `RemoteRun`/`LegendEngineExecutor`, `relationColumns` as they are.
- `datacube/src` (2026-10-01, the user's ask, cleared with the DataCube line): a DataCube-owned "controls hidden" mode.
  `CubeApp` option `controlsHidden` + `setControlsHidden()`/`controlsHidden`; one root class
  (`dc-controls-hidden`, `app.css`) hides the title bar, drag zones, columns panel and status bar; the way back is the
  right-click menu's LAST entry, `view.controls` "Hide Controls"/"Show Controls" (`ui/menu.ts`; the order before it
  untouched; the user: "right click only"). View state: no query, no undo step, not saved. Tests: `menu.test.ts`,
  `app.test.ts`. Query opens its embedded cube with it.
- `datacube/src` (2026-10-01, the user's ask, cleared with the DataCube line): a DARK THEME, opt-in by the page --
  `<html data-dc-theme="dark">` (Query sets it from its own light/dark switch and the system setting). `theme.css`: the
  same tokens redefined under `:root[data-dc-theme='dark']` (palette roles kept: --tw-white the surface, --tw-black
  the ink, the neutral ramp reversed, tints dark; `color-scheme: dark`) and the chart panel's `--dc-chart-*`.
  `grid/screen-colours.ts` `onScreen`, at the grid's two paint paths only: a configured colour equal to its DEFAULT
  becomes `var(--dc-theme-*, <default>)`; a chosen colour is painted as chosen; exports never pass through it (they
  stay light). The last hard-coded colours became token references with their old value as fallback (grid.css total
  row, band, red-50 tint; app.css error ink). PROOF: 853 DataCube elements' computed colours (grid, menu, Properties)
  identical before/after in light. Tests: `screen-colours.test.ts`; `typed-values.ts` reads a cell's painted colour
  (jsdom does not resolve `var()`). Known gap: a heatmap's default low end (#ffffff) is interpolated in TS, still white.
- `MODULE.bazel`: a second `npm_translate_lock` (`npm_query`, `//query:pnpm-lock.yaml`), so `datacube/`'s lock
  is not touched.

Nothing in `datacube/`, the compiler packages, `warehouse/`.

**Found for the compiler's owners:** (1) FIXED on this line 2026-10-01 (the user: "def fix the lambda bug"):
`Compiler.resultType` on a lambda WITH parameters typed the lambda itself (`LambdaFunction<{Integer[1] -> Relation<...>}>`),
so `lambdaRelationType`/`lambdaReturnType` answered a function type where legend-engine types the result with the
parameters in scope. `SpecCompiler.typeQueryBody` now binds a lambda's declared parameters (`Typer.declaredType`, shared
with the annotated-lambda fold) and types the body; undeclared parameters stay refused. Test: `PureV1ApiTest`
`e5e6_aQueryWithParameters_typesAsItsBody`; the relational corpus (duckdb, h2) per-test lists identical with and
without the change. (2) NOT fixed here: a derived property `{$this.quantity *
$this.price}: Float[1]` (Integer * Float) compiles in lite; legend-engine refuses it ("'Number' is not a subtype of
'Float'").

**2026-10-01, the DataCube line: several sources on a page, and the source picker** (the user: "New is fine; Picker
should offer everything"). In `datacube/` (the Query line also edits it; ping before landing either way):
`src/app.ts`, `src/ui/menu.ts` (Insert becomes **New ▸ Source… / Grid / Chart**), `src/page/cube-page.ts` (a grid over
another source), `src/planner.ts` + `src/wasm-planner.ts` (`withModel`: a planner over another model sharing the same
worker or server), a new `src/ui/source-picker.ts` and `src/saved-queries.ts` (upstream `/pure/v1/query`, read only),
`demo/boot.ts` (the Data window becomes the picker). Nothing in `core/`.

**2026-10-01, the DataCube line: a saved query as a source** (the user: "lets work on saved queries now"). One line in
`query/`: `query/BUILD.bazel` exports `demo/models/*` to `//datacube` (read only), so DataCube's site serves the trading
project a saved query compiles against -- the same files, not a copy that could drift. In `datacube/`: `src/saved-queries.ts`,
`src/planner.ts`, `src/wasm-planner.ts`, `src/pure-v1.ts`, `src/planner-worker.ts` (`ModelOptions`: a mapping, read as
`->from(mapping, runtime)` outermost as Query runs it; the model's enumerations; `modelElements`), `src/relation-type.ts`,
`demo/boot.ts`, `demo/page-config.ts`, `demo/config.json`. Reads `fixtures/saved-queries/`, writes nothing there.

**2026-10-01, the DataCube and Query lines: one query store, in a package of its own** (the user: "the right
architecture", "i really do want the private browser sharing mode with no server", "a share link that encodes everything
you need about a Saved Query"). A new top-level package `query-store/`: upstream's `Query` records, ONE typed client
(`QueryStoreClient`, read-only `QueryReader` for DataCube) over a `fetch`, and an IN-PAGE server -- upstream's
`/api/pure/v1/query` answered from IndexedDB, ported from legend-lite's `SavedQueries.java` -- that the client is handed
in place of the network when no server is configured. One wire-level suite runs against both: the in-page server and
legend-lite (`//core:server`, `--query-store`). And a saved query's share link (`#q1.`). **Moves out of `query/`**:
`src/backend/local-store.ts` and the `Query` types of `src/backend/wire.ts` (re-exported there); `HttpEngine`'s store
calls go through the client; `query/BUILD.bazel`, `query/tsconfig.json`, `query/demo/main.ts`. In `datacube/`:
`src/saved-queries.ts` loses its own client. Then one site serving both apps from one origin, so the browser store is
shared. Nothing in `core/`.

## A fourth line, 2026-10-03: one owner for every database decision (the user's ask, rule 5)

`docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md` (homework done; its §4 the steps). The user: "make sure
there is really only one owner and one single place through the whole code that makes this decision". **This line
does ONLY B1 and C3** of that plan; everything that lives in the rebuild's areas is handed to the execution plan (see
below), not done here.

**Announced cross-area edits (each lands with the full chain):**
- B1, the guards: `core/BUILD.bazel` (`//core:guardrails` also loads `:duckdb_load`, so `exec/DuckDbAppenderLoad` is
  checked), `core/src/test/java/com/legend/ArchitectureTest.java` (`CORE_PROD_CLASSES` = exactly the product: test
  support `com.legend.testing..`/`com.legend.tools..` out; F1.3b's text says it measures JDBC API use).
- C3, the database registry: a new `com.legend.database` (one entry per `DatabaseType`, data and plan-side objects
  only, no `java.sql`), and its consumers, edited only where they decide by database: `Compiler.dialectFor` /
  `executesOn`; `StatementExecutor`'s engine-text renderer selection (`toSQLString`, `planDialect`, `H2_DDL`,
  `ENGINE_TEXT`) and `PlanAllocations`; `plan/InProtocol`; `SqlTextVerdicts`; `exec/SystemDatabase`;
  `exec/JdbcMetadata`; `server/ConnectionResolver`; `test/StorelessRuntime`; `sql/dialect/AnsiSqlRenderer` and its
  subclasses (the `jdbcProduct` constructor argument leaves); `DialectBoundaryTest`; `tools/deps/core-layers.txt`.
  C3a landed 2026-10-03: `//core:database` (`core/BUILD.bazel`, `tools/deps/BUILD.bazel`), `PlanEnvelope`,
  `ArchitectureTest`'s library map, and the `dialectFor` callers `wasm/.../Wasm.java` and
  `datacube/tools/catalogfacts/CatalogFacts.java` (now `Databases.dialect`).
  C3b announced 2026-10-03 (plan doc §4 C3b and its audit): the connecting side's one owner, a new `exec/Sessions`
  (`exec/JdbcMetadata` deleted into it); `Compiler` (`executesOn` returns the target, `dialectOf`'s session check, new
  execute/executeWire/executeStreaming entries taking a connection opener, `NO_RUNTIME` replaced); `CrossStoreGuard`
  (moved beside `executesOn`, silent returns deleted); `StatementExecutor` (its `CrossStoreGuard` and `SystemDatabase`
  calls only); `exec/SystemDatabase`; `server/ConnectionResolver` (`resolve` deleted; becomes the opener) and
  `server/QueryService`; `compiler/element/ModelContext` (`isModelConnection` default; a `databases()` view);
  `sql/dialect/SqlDialect` + `AnsiSqlRenderer` and subclasses (`jdbcProduct` deleted); `test/StorelessRuntime`;
  `pct/.../PctBackend`; `docs/SEMANTICS_REGISTER.md` S27; the tests that pin `NO_RUNTIME` and the server's resolver.
  Then (user, 2026-10-04) C1 and C2 on this line: `compiler/spec/StatementEffects` (new; the effect scan from
  `StatementExecutor` and `Compiler`), a new `//core:planner` library (the compile-once API replacing `Compiler`'s
  plan statics), a `com.legend.Execution` front door, and every `Compiler.*` caller switched (core, server, wasm, pct,
  spec, datacube tools); `core/BUILD.bazel`, `tools/deps`, `ArchitectureTest`, AGENTS.md's entry-point table.

**Handed to the rebuild (not touched here):** the effect scan's swallowed compile errors (`StatementExecutor
.containsEffect`; D10's demand-driven rule), the `ExecutionContext` reader's `"H2"` defaults (`ContextReading`), the
missing platform functions (W2.1 / W4.4a, decision D9), the plan/execution split (W6.2's first cut) — each with its
measurements in the plan document above.

## A fifth line, 2026-10-03: Studio, SDLC-lite and Depot-lite (the user's ask, rule 5)

On the remote branch `studio` (worktree `legend-lite-query`); everything lands there first, only this announcement on
`main`. Design `docs/STUDIO_DESIGN_2026_10_02.md` and census `studio/docs/` (both on `studio`). **Owns, all new
top-level modules:** `studio/` (the app), `sdlc-client/` and `depot-client/` (TypeScript clients), `sdlc-server/` and
`depot-server/` (Java: the model home's rules, written once and run both as servers and compiled to WebAssembly for
the page; they depend on `//core`'s public API, never the reverse), and the dogfood model.

**Spike landed (2026-10-04, on `studio`):** the SDLC's rules in Java compile to WebAssembly by `sdlc-server/`'s own
`teavm_wasm` target and pass the same conformance suite as the server over a real git repository. No edit in `core/`,
`json/` or `tools/`.

**2026-10-04, announced before landing: one edit in `wasm/`** for Studio's in-tab compile.
`wasm/src/main/java/planner/Wasm.java` gains one export, `compileOrError(model)`: exactly what the server's
`compilation/compile` does (`Compiler.compileModel` then `Compiler.compileAllBodies`), answered as `"OK\n" + [every
body error]` or the folded refusal, like the module's other exports. Nothing else in `wasm/` changes.

**2026-10-04, announced before landing: a cross-area edit in `datacube/` and `query/`** (the user: "let's also pull
out stuff that query is using from datacube"; "should we call it engine-client"). No behaviour changes, moves only:
- **New `engine-client/`** (TypeScript): where a query runs, shared by DataCube, Query and Studio. MOVED from
  `datacube/src/`: `engine.ts`, `relation-type.ts`, `result.ts`, `receipt.ts`, `values.ts`, `types.ts`, `pure-v1.ts`,
  `duckdb.ts`, `warehouse.ts`, `engine-remote.ts` (and whatever they alone import). Every importer in `datacube/`
  (about 50 files, one line each) and `query/` points at the new paths; BUILD and typecheck targets follow.
- **New `datacube/src/embed.ts`**: DataCube's one public entry for an app embedding the cube (`CubeApp`, `CubeView`,
  `RemoteRun`, the snapshot type, `sourceColumns`); Query imports only from it, no longer from DataCube's internals.
- Later in the same line: legend-art's tokens and the icon generator shared by Query, Studio and DataCube.
Gated by DataCube's own suites (its tests, `//wasm:all`, the browser lane) and Query's. A DataCube session working
on `datacube/` should rebase after this lands.

**2026-10-04, announced before landing: four one-line edits for the Bazel program's guards** (after merging `main`'s
P1-20/P1-25 into `studio`): `//sdlc-server` and `//depot-server` join the visibility lists of `//base:base`,
`//json:json` and (`//sdlc-server` only) `//core:core`; `tools/deps/pools.bzl` lets `sdlc-server` use `@maven_teavm`
(its page module, built like `//wasm:planner`), with the reason. The two modules use `legend_java_library`.
And `//engine-client` joins `//core:core`'s visibility: DataCube's type-facts generator (it runs legend-lite) moved
there with the type reader.

The compiler rebuild is paused by the user while this line works (design S11). Core pieces it will need later, each
announced here with its files before it lands: the model JSON reader and printer (opening existing upstream projects),
W1.2 diagnostics, and (v1) name-resolved entity JSON with the function-resolution fix
(`docs/function-resolution/README.md` on `studio`).

**2026-10-05, announced before landing: the reference-lane fix in `core/`** (the user: "fix rule to match pure for
sure"). The rule `c1f9bac5b` added (a list literal's elements are `[1]`) is legend-pure's and stays. The lane's two
new failures (`dataTypeToSqlTextH2`, `max([$MIN, min([$v.size, $MAX])])`) come from overload choice: legend-pure picks
`math::min(Integer[1..*]):Integer[1]`, which our catalog lacks, so we pick `min(Integer[*]):Integer[0..1]`. The fix adds
the six `[1..*]` overloads legend-engine registers (`math::min`/`max` over `Integer`, `Float`, `Number`) to
`native-membership.tsv` and `Pure.java` (their text and the `AT_MATH_MIN`/`AT_MATH_MAX` groups regenerated); lowering
already dispatches on the groups. Gate: the full chain plus `//spec:reference_lane` back at its golden.

## Rules between sessions

1. Never force-push; never bare `git stash` (the stash stack is shared by every worktree).
2. Before pushing, `git fetch origin`; `main` must fast-forward.
3. The gate chain is `bazel test //...` then `bazel test //tools/deps:all`; `//parser-equivalence:diagnostics` and
   `//parser-equivalence:diagnostics_reports` run only on their triggers (a pin bump, a parser/lexer/protocol change, a corpus manifest change).
4. A timing is a lane run alone with `--nocache_test_results`, load under 3 at the start, nothing else building.
5. If a second line of work starts again, this file becomes the handshake again: each side's owned area, cross-area edits
   announced one line each before they land, and dated status lines.
