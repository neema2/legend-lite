# In flight

This file says who is working, on what files, in what order, and the rules between sessions; each line's status lives
in its own plan (plan rule 0b.16).

**Active lines and their order (the user, 2026-10-04).** Several sessions push to `main`; each announces a cross-area
edit here before landing it and lands with the full chain. The priorities are **Bazel, Studio, and the database owner
with the compiler's plan/execution split**, in this order:

1. **The Bazel program** (`docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`, log `docs/BAZEL_EXECUTION_LOG.md`, both on
   main since 2026-10-07; its remaining work is the build rebuild's Phase 8): build files — `core/BUILD.bazel`, `tools/deps`, `MODULE.bazel` (splitting out a
   `release.MODULE.bazel`), `pct`, `spec`, `parser-equivalence`, the guards. Batch 8 landed (`053e15006`); the old
   plan's remaining work lives in the rebuild program below. It also carries the
   reference-lane fix the user decided ("fix rule to match pure": legend-pure's overload for `min`/`max` over a list
   literal), in `core/compiler/spec` — no one else edits those files until it lands. **Announced 2026-10-05:** two small core edits the
   guards found — `core/src/main/duckdb/com/legend/exec/DuckDbAppenderLoad.java` (its `finally` replaced an in-flight
   unchecked error with the staging drop's; found when P3-27 put `src/main/duckdb` under the guards), and the core
   test sources the guards and censuses read (declared file lists, P3-27/P3-05). F-L1 in `projects/FINDINGS.md` (a view
   inside a Schema is lifted twice; one line in `ModelBuilder`) is now the build rebuild's (Phase 3b item 1). **Announced 2026-10-05 (P4-18):** `core/.../server/LegendHttpServer.java` and the warehouse server gain
   `--exit-with-parent` (exit when stdin reaches EOF; only tests set it), so no test stops a server with `taskkill`.
   **Parked (2026-10-06)** with the rebuild program's Phase 8: not landing now.
   **Announced 2026-10-07 — the CI landing, the rebuild's L1 (plan §4; the lane set `docs/build-inventory/program/PHASE_8.md` §8), branch `build/ci-lanes`, before Phase 3 lands.**
   L1a, no source file: `.github/workflows/gate.yml` and `gates-run.yml` (the build lane builds the product; every
   lane a `//gates` suite; the lane guard; the cache reduced to the downloads; actionlint and the actions pinned);
   `gates/BUILD.bazel` (the suites, `all`, `heavy`, `local` as a suite of suites; its header's PR sentence goes); the
   root `BUILD.bazel` (`//:update_generated` manual); `spec/corpus.bzl` and `spec/BUILD.bazel` (the judge passes
   manual); `core/layers.bzl` (the layer queries manual: a Bazel file under `core/`, no Java); `tools/census`,
   `tools/junit`, `wasm` BUILD files (hand tools manual); `tools/guards/` (two new tests: every hand tool compiles,
   actionlint over the workflows); `MODULE.bazel` and `third_party/` (actionlint's archives); `docs/GATES.md`. L1b:
   `tools/browser/defs.bzl` (a Linux-only option), `datacube/BUILD.bazel`, `query/BUILD.bazel`, `site/BUILD.bazel`,
   `studio/BUILD.bazel` and `datacube/demo/*.mjs` (the harnesses as tests: `bazel/exec`'s P4-02, P4-03, P4-04 and
   P4-08 rebased) — Studio's line owns `query/`, `site/` (below; `datacube/` is the sixth line's since 2026-10-09): noted
   here once, proceeding.
   **L1a landed 2026-10-07 (`fd1b0ba77`); L1b landed 2026-10-07 (`d126b47e1`).** Its cache fix follows on
   `build/ci-cache` (`gates-run.yml` only).
   **Landed 2026-10-07 — L1c, the warehouse knows nothing about Bazel** (commit "Build rebuild L1c: the warehouse knows nothing about Bazel"; GATES entry "Build rebuild L1c"). The DataCube line's edits to `warehouse/` rebase onto it: `ServerRunfiles.java` is gone, `WarehouseServer.commandLine` resolves no path, `//warehouse:serve` and `//datacube:app` are `warehouse_folder` targets, `duckdb_extensions` is `postgres_extension`.
   **The build rebuild and the self-contained bump** (`docs/REBUILD_PROGRAM_2026_10_06.md`, on main since 2026-10-07 with its research and
   evidence; a session picking it up starts at `docs/build-inventory/program/START_HERE.md`. Phases 0 and 1 landed as PRs;
   since 2026-10-06 there are no PRs: one full CI run on the branch, then that commit pushed to main). **Announced 2026-10-06, replacing the 2026-10-05 note — PR 1, Phase 0:** the build targets
   (`//:java`, `//:web`, `//:wasm`, `//:native`, `//:sites`) and their compile-only guard, the product's jars on
   http_jar, `//:web` without Node, stamping off (`docs/BUILD_REBUILD_DESIGN_2026_10_05.md`, which the PR carries).
   Files: the root `BUILD.bazel`; `//:__pkg__` visibility on `//core:server`, `//sdlc-server:server`,
   `//sdlc-server:page` and `//site:dist`; **core's runtime drivers** in `core/BUILD.bazel`, now http_jars, with
   **Postgres JDBC 42.7.4 → 42.7.13 without checker-qual** (the server's Postgres arm); `warehouse/BUILD.bazel`'s
   DuckDB jar label; **Studio's** `datacube/`, `query/` and `studio/` BUILD files (each app's `:bundles`, built by
   `esbuild_bundle`) and their `package.json` and `pnpm-lock.yaml` (esbuild removed, relocked with the pinned pnpm);
   `MODULE.bazel` (the product-jar extension, esbuild's archives), `release.MODULE.bazel`, `.bazelrc`, `tools/deps`,
   `tools/guards`, `tools/js`, `tools/jars`, `pct/`, `spec/`, `docs/GATES.md`, and the CI cache key. No source
   edits; no `depot-server/` or warehouse visibility edit (the 2026-10-05 note was wrong there).
   **Phase 1 (generator hygiene, no behavior change), branch `build/phase1-generators`, stacked on PR 1; announced
   2026-10-06, the full scope:** core source: `CORE_IMPORTS` leaves `core/src/main/java/com/legend/compiler/NameResolver.java`
   for a new generated `CoreImports.java` (`SEQUENCE`), read by `NameResolver`, `BareNames` and `PreludeGenerator`
   (`DiagramService` a comment); core tests: two pins (`HarnessDisciplineTest`'s count for the trimmed eager-compile
   probe, `ParserBoundaryArchTest`'s entry for it); `core/BUILD.bazel`: visibility for the narrowed generators
   (`diagnostics`, `protocol`, `parser`, `sql_dialect`, `database`, `compiler_element_type`, `compiler`, `plan`,
   `planner`, `lowering`), a `stress_dense` group, `manual`/`testonly` tags and the generated-file map. **Studio's:**
   `datacube/` (the catalog generator split into `CatalogRulesFacts.java` + `CatalogFacts.java`, `catalog-facts.ts`'s
   header line regenerated, `manual`/`testonly` tags, narrowed deps) and `engine-client/BUILD.bazel`. Also: `wasm/`
   (`ZoneMain.java`, BUILD), `warehouse/BUILD.bazel` (the native image's metadata in its own library),
   `parser-equivalence/` (new programs `PmcdReachability`, `PmcdWorklist`, a committed `pmcd-reachable.tsv`; deleted
   `MigrationSizing`, `PmcdReachabilityCensus`), `spec/` (BUILD, `ImportsGenerator`, `PreludeGenerator`,
   `CoreImportsParityTest`, `EagerCorpusCompileProbe`), `scripts/corpus/`, `scripts/parser/` (`keywords.py`),
   `tools/` (`par`, `reference`, `engine-runner`, `java_run`, `generators`, `untangle`), `docs/BUILD.bazel`,
   `docs/GATES.md`. Every changed generator's output is byte-identical.
   **Phase 2 (announced 2026-10-06): the upstream tables joined when the classes load**, branch `build/phase2-tables`.
   core: `builtin/DynaFn.java` generated whole from upstream (members, dialects, inference); a new hand-written
   `builtin/DynaFnDecisions.java` holds the platform's resolutions, keyed by the generated members (an unlisted name is
   UNSUPPORTED); `builtin/EngineHandlers.java` joins upstream's (name, id) table with the platform's declarations by
   function id when it loads, and `engine-handlers.tsv` keeps upstream's two columns; `builtin/Pure.java` gains
   `liteSurfaceFunctions()` (the surface by function id); `native-claims.tsv` regenerated (its last column);
   comments in `normalizer/RelOpTranslator.java` and `core/BUILD.bazel`. core tests: `EngineHandlersTest`;
   `PlatformNamesGuardrailTest` (DynaFn.java's entry leaves) and `ParkedWorkLedgerTest` (PARK-3's anchor restated),
   each with a dated note; `docs/PARKED_WORK_LEDGER.md` follows. spec: BUILD, `DynaFnGenerator`,
   `EngineHandlersGenerator`, `DynaFnRegistryTest`, `SpecRatchets` and `ratchets.tsv` (the undeclared engine ids,
   shrink-only). `.github/workflows/gate.yml` runs nightly on main. `DynaFn`'s API is unchanged, so its users are
   not edited beyond that one comment.
   **Phase 3 (announced 2026-10-06): one table decides, by function id**, branch `build/phase3`. core:
   `compiler/spec/FunctionMatch.java` (new: legend-pure's overload ranking), `InferenceKernel.java`, `Overloads.java`;
   `compiler/element/FunctionCompiler.java` (candidates merged by function id; the PCT rule and the platform-owned list
   gone), `compiler/element/type/PlatformTypes.java` (the owned and assert-family lists gone), `Compiler.java`
   (`withoutPreludeShadows` by id), `builtin/Prelude.java`, `platform/ImplementationTable.java` and
   `Implementation.java` (a version of a function the platform implements with no row is refused),
   `compiler/StatementInline.java`; `builtin/Pure.java` and `native-membership.tsv` (27 versions get rows),
   `native-claims.tsv` regenerated, `lowering/Aggregates.java`; `compiler/ResolvedNames.java` and `platform/CoreFn.java`
   (a form by the names a call resolves to) with the 21 form-dispatch sites (`compiler/spec/` DeferredArgs,
   GraphFetchChecker, MatchChecker, Overloads, ProjectChecker, SortChecker, SourceSubst, TdsDesugars, Typer;
   `lineage/ScanRelations.java`, `normalizer/MappingNormalizer.java`), `builtin/TdsLegacy.java`,
   `compiler/spec/GroupByChecker.java`, `builtin/SystemMetamodel.java`, `builtin/NativeFn.java` (executeInDb's
   ConnectionStore version joins its family), and with the audit's fixes (2026-10-07) `compiler/spec/UserCallInliner.java`
   (a refused version reported as "no row for"). core tests: `InferenceKernelTest`, `CompileFunctionTest`,
   `PickByTableTest`, `DeclarationTableTest`, `IdentityGuardrailTest` (pins lowered, dated), `ParkedWorkLedgerTest`
   (PARK-5 to PARK-14 anchored), with `docs/PARKED_WORK_LEDGER.md`. spec:
   `ImplementationTableTest`, `SpecRatchets`, `DynaFnRegistryTest`, `SubsumedRegistryTest`, `ratchets.tsv`, the
   reference lane golden and its `reasons.tsv`. Also **Studio's** `datacube/src/generated/offer-facts.ts` (regenerated: one more offered
   function), `parser-equivalence`'s ratchets, `docs/GATES.md`, and `docs/EXECUTION_PLAN_2026_09_26.md`: W2.1's
   `ids`/`catalog` items now belong to this program (Phase 3), so nobody redoes them there; W1.1b stays with that plan
   (this program's Phase 3b was re-scoped on 2026-10-07 to the files Phases 4 and 6 load and what users meet).
2. **Studio** (`docs/STUDIO_FULL_PLAN_2026_10_04.md`; `studio-m1` landed as PR #24 on 2026-10-05): `studio/`,
   `legend-art/`, `query/`, `site/`, `engine-client/` (`datacube/` moved to the sixth line on 2026-10-09: the user),
   `sdlc-*`/`depot-*`; and the **protocol program** in core (`docs/PROTOCOL_PROGRAM_2026_10_05.md`: files in the fifth
   line's notes). **Resumed 2026-10-07 by session `neema-8f`**: see the fifth line's 2026-10-07 note.
3. **The database owner** (the fourth line below; `docs/PLAN_EXECUTION_SPLIT_AND_DATABASE_OWNER_2026_10_03.md`; since
   2026-10-05 also the execution plan boundary, `docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md`, which takes the rebuild's
   W6.1 and W6.2). Session "Plan Gen / Exec Split" (was `neema-20`), worktree `legend-lite-pgspike`. C3a–C3c, C1, C2a and C2b
   landed; the plan's step 1 (the plan records, `//core:execution_plan`) landed 2026-10-05; step 2's first piece,
   a connection's setup moved to the plan side (`//core:setup`), landed 2026-10-08 (`847b41df4`), and its landing 1 (the
   records, one parameter list) the same day (`f05fe7ada`). **Announced 2026-10-08: E, the dialects write through one
   writer** (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §10): `core/src/main/java/com/legend/sql/dialect/` — every
   renderer (`AnsiSqlRenderer`, `DuckDb`, `H2`, `H2Modern`, `Postgres`, `EngineStyleH2`, `EngineStyleDB2`,
   `EngineStyleComposite`, `DdlSpelling`) — a new `SqlWriter`, and AGENTS.md invariant 3; in stages, each
   byte-identical on the render census (its tool in `docs/execution-plan-boundary-2026-10-05/render-census/`). First a
   known fix, parked by the user to keep this line on plans and recorded as **PARK-16** (DDL and DML spell a table name
   raw where queries quote it — a reserved-word table in the default schema, found by the Studio line): the row and its
   anchor land with E-1, and E's DDL stage closes it. **Then** step 2's landing 2 and steps 3–4 — the planner makes an
   execution plan, a runner in `exec` runs it, the server's execute paths switch to it (files in the fourth line's
   2026-10-07 note; step 2's decisions, the user's, 2026-10-07/08, in the plan's §9).
4. **DataCube + Python** (resumed 2026-10-07 by the user; the sixth line below): worktree `legend-lite-dcsnap`, branch
   `datacube-pages`. `native/`, `python/` and, since 2026-10-09, all of `datacube/`. Landed: the compiler as a native
   library (`bc8107c4e`), `ll.show(df)` and the notebook cube on every platform, a real JupyterLab test and marimo
   (`169e0f062`). Now: DataCube pages, phase 1, the layout sprint (`docs/DATACUBE_PAGES_DESIGN_2026_10_09.md`).

**Parked:** the compiler rebuild (`docs/EXECUTION_PLAN_2026_09_26.md`; paused, coming back later — its open items C4,
B2/B3 and the W6.2 runner wait for it); the server
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
  (2026-10-08: the generators, `catalog-model.ts` and `WasmPlanner.databaseFromCatalog` are deleted; DataCube calls
  the compiler's writer, `tableModelOrError` -- the sixth line.)

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
  `datacube/tools/catalogfacts/CatalogFacts.java` (now `Databases.dialect`; that generator is deleted 2026-10-08).
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
- **2026-10-07, announced before the first edit: the execution plan, phase 1 steps 2–4**
  (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §9; the user's question: can lite run legend-engine's plans). What it
  does: the planner produces ONE plan (SQL rendered at plan time with typed bind slots, each statement's connection,
  its setup statements and in-memory identity); a new runner in `exec` validates the caller's parameter values against
  the plan, binds, runs and streams — with no compiled model. Then the server's execute paths run plans, and what they
  replace is deleted. Each step lands alone with the full chain; no behaviour change (the same answers on DuckDB, H2
  and Postgres, checked by a differential test until step 4). Files:
  - step 2: `TypedQuery.java` and the planner (a new `executionPlan`), `lowering/PlanParams`, `lowering/WireRender`,
    `sql/dialect/AnsiSqlRenderer` and its subclasses (`SqlExpr.PlanParam` rendered as a bind placeholder),
    `StatementExecutor` (its plan-text parameter builder moves to the one shared function), `setup/CsvSeed`
    (setup rendered at plan time; moved out of exec 2026-10-08, the running half `exec/SetupRunner`), `server/ConnectionResolver` (the in-memory identity computed planner-side),
    `executionplan/*`; `core/BUILD.bazel` (`:planner` depends on `:execution_plan`) and `tools/deps/core-layers.txt`
    — Bazel edits shown to the Bazel program session before landing;
  - step 3: `exec/PlanRunner` (new), `exec/Sessions` (`Source` no longer takes the model), a shrink-only guard on what
    `exec` reads from planning libraries (`ArchitectureTest`);
  - step 4: `server/PureV1Api` (`pure/v1/execution/execute` only: plan once, run with `parameterValues`;
    `boundParameters` deleted), `Execution` (`wireOn`/`streamOn` deleted), `server/QueryService`,
    `server/ConnectionResolver`, and their tests (`PureV1ApiTest`, `PureV1HttpTest`, the server and streaming tests).
  **Overlaps:** `PureV1Api.java` with the protocol program (its `grammar/*` routes; different methods — whoever
  lands second merges); `TypedQuery`/`Compiler` with the compiler design (`docs/COMPILER_RIGHT_DESIGN_2026_10_07.md`,
  no code yet — this work adds a method beside `plan`, it does not change typing or naming) and with the rebuild's
  Phase 3 reference branch (`Compiler.withoutPreludeShadows`, not touched here). Not touched: `generatePlan` (its
  legend-engine shape waits for the compatibility mode, phase 4), judging and tests (Studio's A4 stays parked).

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

**2026-10-05, announced before the first edit: the Studio plan's core work and the Snap move** (the user: "do the whole
thing on a branch"; `docs/STUDIO_FULL_PLAN_2026_10_04.md` A4, B1, A6). All of it on the branch `studio-engine`, landing
through its PR after `studio-m1` and `query-by-name`; nothing on `main` but this note.
- **A4, tests (the engine's testable framework)**, from `docs/DEFERRED_TEST_EXECUTION.md`. Files:
  `core/src/main/java/com/legend/test/` (the existing `ServiceTestRunner`/`TestAssertions` extended to mapping and
  function suites, and split into a planner half -- each test's query, data and assertion -- that runs without JDBC, so
  the tab can execute it on DuckDB, and the judgment, written once); the typed suite records in
  `core/src/main/java/com/legend/model/` (`MappingDefinition`, `ServiceDefinition`, `MappingFromProtocol`: the raw
  `testSuitesSource` replaced as the charter says); `core/src/main/java/com/legend/server/PureV1Api.java` and
  `LegendHttpServer.java` (`testable/runTests`); `wasm/src/main/java/planner/Wasm.java` (exports for the tab's runner).
- **B1, the model round trip**: `core/src/main/java/com/legend/protocol/` (a model composer beside `PureComposer`:
  model JSON to Pure text, element by element, upstream's grammar composer as the spec), `PureV1Api.java`
  (`grammar/jsonToGrammar/model`), `Wasm.java` (its export).
- **A6, Snap for every app**: `datacube/src/` (Snap's engine half -- freezing rows into the tab's DuckDB, the stamp,
  the refusals, receipts -- moved to `engine-client/`; DataCube keeps its cube behaviour on top) and DataCube's Snap
  tests, which move with it. Gated by DataCube's suites and browser lane.
Shared with the database owner's C2 (the execution front door): `core/.../test/` runs suites through
`Compiler.executeResolved`; whoever lands second merges.

**2026-10-05, later: A4 PARKED; B1 becomes the protocol program** (the user). **A4 (user tests) is parked** behind the
round trip as its own design program (tab and server must give the same verdict; judging's home is the database
owner's open question): nothing in `core/.../test/`, `com.legend.model` suite records, `testable/runTests` or `Wasm.java`
test exports is being changed; the provisional work sits unmerged on `studio-tests-parked`. **B1 is now the protocol
program** (`docs/PROTOCOL_PROGRAM_2026_10_05.md`, branch `protocol`): parse, emit, read and compose all on the typed
protocol records. Files: `core/src/main/java/com/legend/protocol/` (a model reader beside `ProtocolReader`, the
composers -- `PureComposer` and the element composers -- moved from JSON onto records, one public face),
`PureV1Api.java`'s `grammar/*` routes, `Wasm.java`'s grammar exports, and the oracles in `parser-equivalence/`. A6
proceeds as announced. **Correction, the reader leg (on `protocol`, not `main`):** the parser and the emitter changed
too, mechanically -- every `SourceInfo` in `Protocol`'s records is nullable (JSON without spans reads back), the
emitter omits an absent span as the engine's `NON_NULL` does, `PmcdParser.parseModel` returns the records
(`parseDocument` is its emit), and two shape flags (`EnumValue`, a connection's `element`) replace span-presence tests.
Gate 8 holds the emitter byte-identical with the engine through it. A session editing `Protocol.java`, the emitters or
the protocol parsers should expect to merge with `protocol` when it lands.

**2026-10-07: the Studio line resumes (session `neema-8f`, worktree `legend-lite-query`).** Nothing lands on `main`
today but this note. What it is doing now: bringing its three unlanded branches up to date with `main` (161 to 223
commits behind), locally first: `query-by-name` (Query and DataCube open Studio's projects by name; 7 commits),
`studio-engine` (running in the tab, the query builder in Studio, the editing features, the Snap move; 19 more) and
`protocol` (the protocol program: the model printer and the model reader, both proven exact over the corpus; 21
commits). The rebased branches are pushed under NEW names (rule 1: no force-push); the old ones stay as they are.
Done the same day: `query-by-name-1007`, `studio-engine-1007` (the first plus 20; its BUILD diff reviewed by the build
program's session) and `protocol-1007`, each on `c9a18b1ed`; the two touch disjoint files and merge cleanly.
**Landed 2026-10-08: `studio-engine` (with `query-by-name`), as 61ad2dbaa (GATES.md, "The Studio line"); `protocol`,
as e5640e35a (its core files: the list below); DataCube's busy signal, as 355a86035; Studio's setup lists and Query's
navigation, as 609fae95e; Studio's status bar, as 8ab4a5bdc; the tab's test data from the planner's statements, as
fb3acc2f1 (`sqlType`'s copy gone; `planner.Wasm.testDataSqlOrError`, `wasm/` one export).** Parked on the Studio
line (the user, 2026-10-08: the round trip first): its harnesses' sites off Node's file server; `verify_remote_test`'s
fetch of DuckDB-WASM's httpfs extension from the network, to be vendored as a pinned file.
**The protocol program's leg 2 is landed (2026-10-09)** (`docs/PROTOCOL_PROGRAM_2026_10_05.md` §4.2). Steps 1 and 2
landed as 447102e56 (run 37870936729): a table reference keeps how it was written (`AppliedFunction.island`), and the
reader reads the older JSON legend-engine 4.145.0 reads (`SEMANTICS_REGISTER` S29 to S34; S34 is the compiler line's).
Step 3 landed as a12a64d24 (run 37923006492; GATES 2026-10-09): every printer in
`core/src/main/java/com/legend/protocol/` prints the records, the parity counts held exactly, and the reader reads the
older or hand-written shapes the step's audits found refused (S35 to S37; `own_corpus.matched` 2734). **Leg 4 landed (2026-10-09)** as a0fdf4e6f (run 37947981268; GATES 2026-10-09): the model printer in PRETTY as
well as STANDARD (exact over the corpus in both), legend-engine's `jsonToGrammar/model` in `PureV1Api`, the tab on
`pure/v1`'s grammar, and one boundary for the embedded hosts with thin adapters (`planner.Boundary`, `Folded`,
`TabExports`; `native/` Python's adapter), leg 3 folded into it (invariant 5 revised with the user; legs 6 to 8 added).
**Next: leg 5, the round trip proven** (§4, item 5) over the corpus and the showcase projects, in the JVM and in the
tab, on a new branch from main; then legs 6 to 8. PARK-17 (Python's mixed refusal kind) closes in leg 6. Open, and not yet in the plan's legs: printing lite-only
mappings (a class mapping by function, a function association), which needs a design first; lite's grammar takes
no `doc` on a function test, though the engine's does; lite's persistence grammar accepts `];` after `tests`,
though the engine's does not.
**Planned landings, in this order** (each: the local gate, one CI run on the branch, then a fast-forward of `main`):
first `studio-engine-1007` (`query-by-name` inside it; CI lanes `ui`, `datacube`, `sdlc`), then `protocol-1007`
(engine code: `core/.../protocol/`, eleven files of `core/.../parser/`, `native-claims.tsv`; the engine's lanes). For
branches open elsewhere: `datacube-chart-spec` and `build/phase3` each raise `own_corpus.matched` in
`parser-equivalence/.../ratchets.tsv` from 2685, as `protocol` does (to 2716); whoever lands second rebases, reruns
`//parser-equivalence:parser_parity` and writes the number it measures. `protocol` adds two whole-corpus parity tests
to that target (about two minutes on a desk; well inside its limit).
Files the rebase touches beyond the line's own folders: the BUILD files of `query`, `sdlc-server`, `studio`,
`datacube` and `engine-client`, `gates/BUILD.bazel` and the CI lane lists (moving the line's tests into the new
`//gates` suites, the `ui` and `sdlc` lanes), and, for `protocol`, `core/src/main/java/com/legend/protocol/` (records,
emitters, readers, composers), `core/src/main/java/com/legend/parser/` (mechanical: `parseModel`, nullable spans),
`wasm/src/main/java/planner/Wasm.java` (one grammar export), `parser-equivalence/` (the oracles). Landing follows
the rebuild program's rule (branch rebased on `main`, an audit, the local gate, one CI run on the branch, then a
fast-forward), after the user decides the order. **Overlap to watch:** the compiler design's F1
(`docs/COMPILER_RIGHT_DESIGN_2026_10_07.md`, names resolved once) changes how call nodes (`protocol.spec`
`AppliedFunction`) are built, and the protocol program changes the same records (nullable spans, the `list` factory);
whichever lands second merges, and the two should agree before either starts on those files.

## A sixth line, 2026-10-07: DataCube + Python (the user's ask, rule 5)

Worktree `legend-lite-dcsnap`, branch `datacube-chart-spec`, rebased on `main` (`fd3d007e4`) on 2026-10-07. The goal:
DataCube on a Python dataframe, from a script and in a notebook, with Python running the SAME compiler the browser runs
as WebAssembly (one compiler source, so Python and DataCube share models and answers). **Owns** two new packages:
`native/` and `python/`, and, **since 2026-10-09, all of `datacube/`** (the user: "This session should own all the
datacube stuff"): the DataCube app, its page layout and page save, its notebook cube. Another line editing `datacube/`
asks this line first, as it asked the Studio line before.

**Landed 2026-10-07 (`bc8107c4e`; GATES entry "The compiler as a native library"): the compiler as a native library, with
Python bindings (L1).**
`//native:compiler`: `//wasm:boundary` (`planner.Wasm` over `//core`) built by GraalVM native-image as a SHARED library, with the warehouse image's C
toolchain (the host's checked, zlib from source, lld on Linux); Linux and macOS for now. `python/legend_lite`:
bindings on the standard library alone (ctypes). `//python:bindings_test`, a `py_test` on the repository's Python
3.12: the bindings' suite and the planner differential corpus, 69 of 69 answers identical to the JVM's. Cross-area:
`MODULE.bazel` and a new `maven_native_install.json` (a compile-only pool, `@maven_native`: GraalVM's `nativeimage`
and `word` API 25.0.2, in `@maven_teavm`'s shape), `tools/deps/pools.bzl` (its one user, `//native`),
`gates/BUILD.bazel` (`//python:bindings_test` in the `warehouse` lane, which builds native images already),
`wasm/BUILD.bazel` (visibility only). Nothing in `core/`. Also CI's product step (`gates-run.yml`: `//native:compiler` named
on its own, with `--skip_incompatible_explicit_targets`) and `tools/guards` (the compile-only guard walks it in the
native tier, except on Windows). **The Bazel program session reviewed and accepted the Bazel edits (2026-10-07).**

**Moved to the compiler line (2026-10-07, the user):** the typing fix this line found while testing the native library
(an erased `TDSRow` trusted any column name, so `#>{db.T}#->filter(x|$x.nope == 1)` typed; legend-engine refuses it at
typing). It is branch `compiler/tdsrow-erased-row` (one commit on `main`: `Type.java`, `PlatformTypes.java`, `Typer.java`,
`TdsRowReceiverTest`, `own_corpus.matched` +1), handed to the Compiler Rewrite session with its diagnosis and
measurements; it is W3.1's territory (`docs/EXECUTION_PLAN_2026_09_26.md`). That line decides whether and when it lands.

**Then, in this order** (each on the branch, each announced here with its files before it lands):
- **Frames in duckdb-python, and ONE model writer for a table (announced 2026-10-07, before the first edit; the
  user: "make sure Datacube actually does move to this exact same code").** **Landed 2026-10-08 (`0a20eb895`, with
  `live_snap_test` moved into the browser; GATES entries "Frames" and "DataCube writes every table's model").** Two
  commits, in order:
  1. **The writer moves into the compiler's boundary; Python uses it.** `wasm/src/main/java/planner/Wasm.java` gains
     `tableModelOrError` -- a table's catalog rows in; the whole model out (the Database, its connection and runtime,
     and the snap runtime when asked), the relation that reads it, the conversions as data AND as the converting
     select list in the dialect's own quoting, the columns left out, the BIT columns -- what DataCube's `infer.ts`
     writes by hand today -- and `catalogColumnsSqlOrError(schema, table)`, the catalog question
     (`DuckDb.CATALOG_COLUMNS_SQL`, filled). Then `native/` (two entry points), `python/legend_lite` (`register(name,
     frame, mode)`, Live the default: the frame re-read as Arrow at each query; Snapped: copied into DuckDB once;
     `execute(query)` -> an Arrow table), `python/BUILD.bazel`, `tools/python/requirements.in` and its lock (duckdb,
     pandas and polars for the tests; pyarrow is already pinned). In `core/`, one additive method:
     `core/.../sql/dialect/CatalogModel.java`, `Database.copySelectList()` -- the select list a copy applies, beside the
     writer's own identifier quoting -- with its test in `CatalogModelTest`.
  2. **DataCube moves to the same code** (after Studio's `studio-engine` lands; files agreed with the Studio line
     first): `datacube/src/infer.ts` (`inferModel` becomes the planner's `tableModel`), `upload.ts`, `catalog-model.ts`
     (its TypeScript copy of the writer deleted), `generated/catalog-facts.ts` and its generator where nothing else
     reads them, `wasm-planner.ts` and `planner-worker.ts` (the two calls), `demo/boot.ts` (its callers), and the tests
     that call `inferModel` or the TypeScript writer. **Settled 2026-10-08 (the user):** the writer is legend-lite's
     module whichever planner the page chose (`Engine.tables`: the planner in the tab; beside a server's planner, the
     module loaded in a worker on first use), so the TypeScript writer and both generators (`datacube/tools/
     catalogfacts`, their `datacube/BUILD.bazel` rules) go; also `demo/planners.ts`, `demo/stress.ts`, and comments
     only in `core/.../sql/dialect/CatalogRules.java` and `DuckDb.java` and `engine-client/src/snap.ts` and
     `warehouse.ts` (they named the deleted files). Diffs of `boot.ts` and `live-snap.ts` sent to the Studio line (no
     collision); the Bazel edits reviewed by the Bazel program session (accepted; with them `tools/deps/jars_table.bzl`
     drops `datacube` from `duckdb_jdbc_warehouse`'s users, and `docs/GENERATORS.md` loses the two generators).
- **`datacube.show(df)` -- steps 1 and 2 LANDED 2026-10-08 (`f49033d8f`, run 37857474050); steps 3, 5 and 6 LANDED
  2026-10-09 (`2b32f6d25`, run 37872741038; GATES entry "DataCube on a Python dataframe, steps 3, 5 and 6"): DataCube's
  page of one cube on an engine, `ll.show(df)`, the wheel per platform, LICENSE and NOTICE, CI's kept wheels. Steps 7
  and 8 LANDED 2026-10-09 (`ead4a7857`, run 37915130012; GATES entry "steps 7 and 8"): the cube under a notebook's cell
  and Windows. The real JupyterLab test and marimo first class LANDED 2026-10-09 (`169e0f062`, run 37926766513; GATES
  entry "a real JupyterLab, and marimo").** (Announced 2026-10-08, before the first edit; the design, agreed with the user:
  `docs/DATACUBE_PYTHON_SHOW_DESIGN_2026_10_08.md`).** DataCube as the UI in its remote-run mode; Python a small Legend
  engine answering upstream's `pure/v1` slice (parse, print, a query's types, execute with Arrow results) through the
  native library and duckdb-python. Files, in order: `wasm/src/main/java/planner/Wasm.java` (the refusal answer and the
  execute metadata, beside the boundary's functions), `native/` (their entry points), `python/` (the server, `show()`,
  its tests); `engine-client/src/engine-remote.ts` and `pure-v1.ts` (Arrow as well as JSON, each engine's format
  declared); DataCube's start for an engine at an address (`datacube/demo/boot.ts`, `planners.ts` -- the Studio line's,
  agreed with it before the first edit); a browser test in `live_snap_test`'s shape; `gates/BUILD.bazel` (its lane).
  **Amended 2026-10-08, before the first core edit:** Python's answers come from legend-lite's own server code, not a
  second copy in the boundary. `core/src/main/java/com/legend/server/PureV1Api.java` keeps its package and name and
  moves into a plan-side library of its own, `//core:pure_v1` (as `:planner` sits beside `:driver`), so the boundary
  can call it. Its one database call, execute's run, is handed in by the caller (`PureV1Api.Runner`; legend-lite's
  server passes `QueryService.executeUpstream`, unchanged). The path-to-endpoint switch moves from
  `LegendHttpServer.PureV1Handler` into `PureV1Api.route`, so the two servers share one routing table. New in it:
  execute's Arrow half (`?serializationFormat=ARROW_IPC`): the SQL to run and the Arrow schema metadata, in the layout
  measured against legend-engine 4.145.0. Files: `PureV1Api.java`, `LegendHttpServer.java` (the switch only),
  `core/BUILD.bazel` (the library; the plan side and `:server_lib` reach it), `tools/deps/core-layers.txt`,
  `ArchitectureTest` (the library's sample class), the tests that call `execute` (`PureV1ApiTest`,
  `ConnectionLeaseTest`). **Overlaps:** the protocol program's `grammar/*` routes and the execution-plan line's step 4
  (`execute`) are in the same file, in other methods: whoever lands second merges, and step 4's "plan once, then run"
  fits the runner. The Bazel edits go to the Bazel program session before landing.
- **DataCube pages (announced 2026-10-09, before the first edit; the design, agreed with the user:
  `docs/DATACUBE_PAGES_DESIGN_2026_10_09.md`).** Every tile equal, the page owning the board; a page as bands (scrolling
  or fitting the window), the layout picker, drop zones, dividers, maximise, smart placement, edit and view, a narrow
  page stacked; then (phase 2) the whole page saved with several sources, (3) tabs, (4) Python pages. On branch
  `datacube-pages`. Phase 1's files, all in `datacube/`: `src/layout/bands.ts`, `src/layout/band-board.ts`,
  `src/ui/layout-picker.ts` (new), `src/page/cube-page.ts`, `src/app.ts`, `src/page-document.ts`, `src/export-model.ts`,
  `src/app.css`, `demo/boot.ts`, their tests, and a browser test of a session (`datacube/BUILD.bazel`,
  `gates/BUILD.bazel`: its lane; the Bazel edits to the Bazel program session first).
- **The notebook widget**, `DataCube(df)`: the same two calls over the notebook's widget channel.
- Later: model handles and `execute` from Python, typed Pythonic queries, the shared warehouse from Python.

## Rules between sessions

1. Never force-push; never bare `git stash` (the stash stack is shared by every worktree).
2. Before pushing, `git fetch origin`; `main` must fast-forward.
3. The gate chain is `bazel test //...` then `bazel test //tools/deps:all`; `//parser-equivalence:diagnostics` and
   `//parser-equivalence:diagnostics_reports` run only on their triggers (a pin bump, a parser/lexer/protocol change, a corpus manifest change).
4. A timing is a lane run alone with `--nocache_test_results`, load under 3 at the start, nothing else building.
5. If a second line of work starts again, this file becomes the handshake again: each side's owned area, cross-area edits
   announced one line each before they land, and dated status lines.

7. **The compiler line (session "Compiler Rewrite"), resumed 2026-10-07 (the compiler plan's D25; the user: "let's do the evidence way").** **Landed 2026-10-07** (GATES entry "The erased TDS row is read only through its accessors"; run 37699620156): `compiler/tdsrow-erased-row` from the DataCube line (`core/src/main/java/com/legend/compiler/element/type/Type.java`, `PlatformTypes.java`, `compiler/spec/Typer.java`, a new core test, `parser-equivalence`'s `ratchets.tsv` own_corpus.matched 2685 → 2686; whoever lands second against the Studio line's protocol-1007 reruns `//parser-equivalence:update_ratchets`). Next pushes, each announced here before its core edit: W1.0b (the stress tool `StressSuites` gains per-phase timers; `tools/metrics`), W0.8 (**landed 2026-10-08, d069cc5c9, GATES "W0.8"; the files:** `core/src/main/java/com/legend/lexer/Lexer.java` and `TokenStream.java` (a range lexed in place, `TokenStream.lexRange`), `parser/MappingProtocolParser.java` (`readIsland`) and `parser/SpecParser.java` (`parseGraphFetchTree`); `ServiceStubDataParser.java` and `RelationIslands.java` only call `readIsland` and do not change; the aggregate-lambda and merge-validation padded re-lexes in MappingProtocolParser stay, they emulate engine span shifts; branch `compiler/w0.8-islands`), then the D24 cleanup in `compiler/spec` and the resolver. **Landed 2026-10-08 (608b5d221, GATES "W1.5 (a slice)"):** the determinism fix handed over by the database-owner line (every JVM-salted copy in core/src/main, 87 sites in 50 files; the table reference's columns in declaration order; two tests; the census's compare.py; parser-equivalence's ratchet measured). **Announced 2026-10-08 (evening) — the D24 cleanup's first slice, one substitution engine over the typed tree:** `core/src/main/java/com/legend/compiler/spec/UserCallInliner.java` (substitute once through `TypedSubst` at each β site, then reduce with no environment; its own capture machinery deleted), possibly `compiler/spec/typed/TypedSubst.java` (an entry for a body with its lets) and `compiler/spec/typed/FreeVars.java`; the design is `docs/plan-audit-2026-09-26/d24-one-substitution-engine-2026-10-08.md` §3b; the judge is the render census before and after, ids checked; branch `compiler/d24-one-subst`. Status lives in the plan's Now line (rule 0b.16). **Landed 2026-10-08 (fa4160740; GATES "D24 (1)"; run 37938204427) — D24's first slice, branch `compiler/d24-one-subst`:** `core/src/main/java/com/legend/compiler/spec/UserCallInliner.java` (substitutes through `TypedSubst`, capture machinery deleted), `compiler/spec/typed/TypedSubst.java`, `compiler/spec/ExecuteChainAssembly.java`, `builtin/NativeFn.java`, comments in `compiler/spec/typed/FoldStrategy.java` and `compiler/spec/ResultEnvelopeSplice.java`, three tests, the DuckDB fail roster (one row, its reason in GATES). Next, announced here first: the syntax-level policies (the homework's second slice). **Landed 2026-10-08 (6be695aed; GATES "W1.0b, the baseline") — W1.0b, the measurement (D25: the stress corpus and the eager probe, nothing new built to measure; the user, 2026-10-08: "do the measurements"):** a compile-only mode of the stress tool (`core/src/test/java/com/legend/integration/StressTool.java`, a new `CompileLatency.java` beside it: per-test compile latency over the stress corpus and over `wasm/corpus/queries.tsv`, no database), `tools/metrics/` (new: `BUILD.bazel`, `baseline.py`, the receipt: product lines per package, the probe's numbers, the latencies, the reference-lane buckets from the golden, the rosters, the planner's bytes), `core/BUILD.bazel` only if a target is needed. No `core/src/main` edit.
