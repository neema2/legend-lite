# Studio, the full experience: run in the browser, then a drop-in replacement for upstream Studio (2026-10-04)

The ask (the user, 2026-10-04): "a comprehensive plan … for full experience — execute against duckdb wasm in
browser, then full interop with real engine/depot/sdlc so it can really be drop in replacement for current
studio". This plan builds on `docs/STUDIO_DESIGN_2026_10_02.md` (decisions S1–S22, departures §4) and the
censuses in `studio/docs/` (behaviour: `UPSTREAM_STUDIO_CENSUS.md`; look: `UPSTREAM_STUDIO_LOOK.md`; wire
contracts: `SDLC_CONTRACT_SLICE1.md`, `SDLC_CONTRACT_SLICE2.md`, `DEPOT_CONTRACT.md`).

---

## 0. What "drop-in replacement" means — the acceptance tests

**Scope (the user, 2026-10-04): OUR Studio replaces upstream Studio.** "I don't care about upstream studio running
against our stuff really at all — I want ours to be full drop in replacement." So the work goes into our Studio doing
everything upstream Studio does, against our servers AND against real Legend deployments. Upstream clients against our
servers (upstream Studio on SDLC-lite, the upstream engine reading Depot-lite) are **out of scope**; what they would
have needed is listed in §7 in case that changes.

Our Studio is a drop-in replacement when both work, each proven by a test that runs in CI:

| # | Direction | Acceptance test |
|---|---|---|
| **D1** | **Our Studio, our stack** (in the page, or our model home and lite's engine) | In Chromium: write a model with a mapping and test data, **run a function, a service and a mapping query on DuckDB in the tab**, see the rows in the grid, run the model's tests, build queries, save, review, release — at level 0 with no server process anywhere, and against our servers. |
| **D2** | **Our Studio → a real Legend deployment** (legend-sdlc, legend-depot, legend-engine 4.145.0) | Opens an existing upstream project (the Legend showcase projects), shows every element as text, edits one, saves, reviews, commits, releases; compiles with real Depot dependencies; runs functions, services, mapping queries and tests on the real engine; every untouched element byte-identical. |

D1 is the **full experience**; D2 is **drop-in**. Plus **feature parity**: everything upstream Studio offers a user
(census Parts A–B) has a counterpart in ours — §3 B5 lists the gaps by feature, not by wire route.

---

## 1. Where we are (2026-10-04, branch `studio`)

Works, tested at both levels (page alone; model home over git), `//studio:verify_test` in Chromium:
- **write** any element lite compiles, as text, one per file, comments kept (no imports: v0);
- **compile** live in the tab (WebAssembly), with dependencies' released text from Depot;
- **save** with upstream's revision lock; **review → merge commit** (workspace closes); **release** (compile-gated);
  **dependencies** picked from Depot (upstream's nearest-wins).

Does not work yet: **running anything** (functions, services, mappings, tests), any **upstream server** (text routes
are lite's own), and most of upstream Studio's features beyond text editing and the SDLC loop (§3 B5). And it does
not **look** like upstream Studio (re-skin pending, look census done).

What exists to build on:
- **Query's in-tab engine** (`query/src/backend/browser-engine.ts`): planner (WebAssembly) → SQL → DuckDB-WASM or the
  warehouse, answered in the engine's own `execute` shapes (TDS, graph fetch JSON). Refuses what only a server answers.
- **`engine-client/`** (2026-10-04): `QueryEngine`, results, receipts, DuckDB in the tab, the warehouse, legend-engine's
  `pure/v1` client — shared by DataCube, Query, Studio.
- **DataCube's grid**, embeddable (`datacube/src/embed.ts`), already Query's results view; **Snap** (freeze rows in the tab).
- **lite's engine server** (`core/.../server`) answers: grammar (model/lambda both ways for lambdas), `compile`,
  `lambdaReturnType`, `lambdaRelationType`, `generatePlan`, `execute`, saved queries.
- **The protocol layer is fully typed** for every test-suite and data shape (W8/W9 parity, 26,168 PMCD files);
  `docs/DEFERRED_TEST_EXECUTION.md` is the charter for running tests (deleted runners were inventions).

---

## 2. Track A — the full experience in the browser (D1)

### A0. The shared look first (S2 look; prerequisite for every screen below)
- One shared package of legend-art tokens (`default-dark` + Studio's extras + `default-light`) and the icon generator
  (react-icons 5.5.0 paths, upstream's `Icon.ts` names), used by Query, Studio and later DataCube.
- Re-skin Studio from `UPSTREAM_STUDIO_LOOK.md`: setup page, activity bar, explorer per-type icons/colours, tabs,
  Monaco's Pure theme (upstream's token rules), Problems, status bar (22px, `#007acc`), dialogs, toasts, react-select
  look. Screenshot per screen in `//studio:verify_test`.
- **Done when:** side-by-side screenshots against the census show no unexplained difference.

### A0½. Query and DataCube open models by name (design Phase 3; added 2026-10-05)
Left out of this plan when it was written; the user ruled it next after A0 (2026-10-05).
- Query and DataCube load a model from Depot by coordinates instead of the demo's `.pure` config: a project, a
  version, then a data space or class (upstream Query's pickers), the version's text and its dependency closure from
  Depot (its nearest-wins resolution, already Studio's) handed to the planner. In the page, Depot is the same
  WebAssembly module Studio runs, on the same origin (`//site`), so a project Studio publishes is one Query opens.
- **The project line first** (the user: "start with non-releases first"): Depot's `master-SNAPSHOT` -- the line's
  head, read live, upstream Query's HEAD -- is the default; releases are the same picker with a fixed version. A saved
  query pins the version it was built on (a snapshot follows the line, a release does not), with the upgrade path.
- A workspace's unmerged edits are not Depot's: upstream reads those from inside Studio ("Query..." on a class), so
  they come with A5.
- **Done when:** `//site:verify` creates a project in Studio, commits a class, and Query opens it at HEAD and runs a
  query; then releases it and opens 1.0.0.
- **STATUS 2026-10-05: done** (branch `query-by-name`). `//site:verify` walks it: Studio publishes the demo projects;
  Query's start page lists them by name unloaded (`depot.projects()`), "Open at HEAD" loads one; Query opens
  `party:master-SNAPSHOT` and runs to the seeded rows; the Version field offers HEAD and the releases, 1.0.0 opens by
  name (loaded the first time, `AppContext.ensure`) and runs; a query saved on 1.0.0 reopens on 1.0.0; DataCube opens
  that saved query by name (its `depot` config, the model fetched only then, under the bundle budget). The model text
  is one Depot call (`depot-client/src/model-text.ts`); the party rows are one fixture (`//fixtures/demo-data`).
  **Deferred, deliberately:** retiring Query's bundled trading demo -- the saved-query fixtures, DataCube's demo and
  tests and both harnesses name `demo:trading:0.0.0` as served files, and a bundled model is a fair way for a
  deployment to ship one; it retires when the demo itself is published (A2's seeding of Depot on first visit).
  Next for fidelity: Depot-lite's classifier routes (S8) and upstream's data-space search across every project.

**STATUS 2026-10-05, branch `studio-engine` (stacked on `query-by-name`):**
- **A1 done.** Query's in-tab engine is engine-client's (`engine-client/src/legend/`): one planner worker for both apps
  (Studio's compile included), the BrowserEngine, the WasmGrammar, HttpEngine. Studio's Compiler asks the session's
  engine -- in-tab by default, or a legend server's pure/v1 (`StudioConfig.engine`). Pending: lite's own server in
  that test (needs //core:server visible to //studio, a core edit announced on main first).
- **A2 step 1 done.** A model's own test data (relational Data elements) is loaded into the tab's DuckDB
  (`engine-client/src/model-data.ts`; types as SQL, DuckDB judges them; DuckDB parses the CSV) by Studio before each
  run, by Query when it opens a project by name, by DataCube when a saved query does. The demo's party rows are its
  Data element; the separate seed file is gone.
- **A2 step 2 done.** Studio's Data panel lists every table the model's Databases declare, where its rows in the tab
  are from (the model's test data, a person's file, or nothing) and how many. Upload fills a table from a .csv (header
  row, read with the declared types) or .parquet (each declared column by name, cast to its type) -- DuckDB refuses a
  file that does not fit -- and the file's rows are kept over the test data on every run until Reset
  (`engine-client/src/tab-data.ts`, TabTables). Next: generated samples (lite's testdatagen, a core WASM export:
  announced first), Snap (A6).
- **A7 started.** Ctrl+P element search (by path, names first, arrows and Enter); a local change's diff (Monaco's diff
  editor, the revision's text beside today's); a delete undone before the save (click the removed element); and
  upstream's workspace update (`POST …/workspaces/{w}/update` in sdlc-server: the workspace's commits replayed onto the
  line's head, author and message kept; NO_OP / UPDATED; a CONFLICT -- a file both changed, differently -- leaves the
  workspace as it was and names the files, a departure from upstream's conflict-resolution workspace), offered in
  Local Changes when the line has moved. The SDLC conformance suite covers update on the page's SDLC and the server.
  Then: rename/move (one dialog, the full path; the path rewritten in the declaration and every reference by full path
  in the workspace, not in comments or strings -- `studio/src/model/rename.ts`); the review's changes (BASE against the
  workspace's head, each opening its diff); and a History tab (the workspace's revisions, each one's changes against
  the revision listed before it, with diffs). Then: discard per local change; approvals on the review (approve /
  revoke); whole-project text mode (F8: the workspace as one text, split back into one file per element on leaving --
  `studio/src/model/split.ts`, every file read by the compiler first, nothing applied unless each holds one element);
  the model importer (F2: Pure text pasted, each element added or replacing its path); upstream's project viewer
  (`#/view/<project>[/<version>]`: a released version, or the line's head, read-only -- explore, compile, run, query;
  from the setup page and the Project view's Versions). Next: conflict resolution, group workspaces; definition, hover
  and completion from the compiler (core, announced first).
- **A3 mostly done.** Run (F5) a function -- its parameters asked for as Pure, read by the compiler -- or a
  single-execution service, in the tab; RESULTS shows the rows, count, time and SQL. The SQL playground runs SQL on the
  tab's DuckDB with the model's rows. `//studio:verify_test` runs a function, a service, a parameterised function and
  the playground at both levels. Left for A5: the results in DataCube's grid, and mapping execution (the query builder).
- **A5 step 1 done.** Query's builder is embedded in Studio (`query/src/embed.ts`, as upstream Studio embeds
  legend-query-builder): "Edit Query" on a service opens its query in the form (full screen, over the workspace's
  model, on the session's engine -- in the tab, the model's own rows); Save Query writes the query's Pure text into the
  service's text (`studio/src/model/service-query.ts`: only the lambda after `query:` replaced, then read back by the
  grammar and refused if it differs), an undoable edit pushed with the workspace. "Query…" on a class opens a new query
  on it, and "Execute…" on a mapping a query on the first class it maps (upstream's mapping execution); both run and
  are kept nowhere. The results are Query's: its grid and DataCube's. `//studio:verify_test` builds a column in the
  form, saves it into a service and runs the service's text, and executes party's mapping (5 rows). Next: a
  function's body in the builder, and mapping tests (with A4).

### A1. One in-tab engine for every app
- Move Query's `BrowserEngine` (planner → SQL → DuckDB/warehouse, engine-shaped answers) into `engine-client/` as **the
  in-tab legend engine**: `execute`, `generatePlan` (SQL shown), `lambdaRelationType`, `lambdaReturnType`, `compile`
  (now possible: `compileOrError`). Query and Studio both use it; Query's tests move with it.
- An **`Engine` choice per Studio session**: in-tab (default), lite's server (`pure/v1`), or upstream legend-engine —
  one interface (`pure/v1` shapes), so every run feature below works on all three.
- **Done when:** Query's suites pass on the moved engine; Studio compiles through the chosen engine.

### A2. Where the rows come from (the hard question for a browser)
A model's mapping points at a database the browser cannot reach. Rows come, in order of what to build:
1. **The model's own test data** — `Data` elements (`relationalCSVData`, `ExternalFormat`) and mapping/service test
   suites' embedded data: loaded into DuckDB tables named by the model's `Database` (schema/table/columns from the
   store definition). Upstream runs mapping tests on engine-side H2 (census B §8 #17); lite runs them on DuckDB in the
   tab — a recorded departure.
2. **CSV / Parquet dropped in by the user** for a table of the model's `Database` (DataCube's upload code exists).
3. **Generated sample rows** — lite already has `testdatagen/TestDataGenerator` (upstream's "Generate Sample Data…");
   expose it as an in-tab action.
4. **Snap from a live plane** — rows pulled from the warehouse or a lite server into the tab, stamped with time and row
   count (Snap made general: §A6).
- A **Data panel** shows each table's source (test data / upload / generated / snap), row count, and lets the user
  reset it. The runtime is the model's own with its connection redirected to the in-tab DuckDB (a session override,
  never written into the model).
- **Done when:** the S18 demo's party mapping, with a `Data` element of parties, answers a query in the tab.

### A3. Running things (upstream census B §6)
- **Run function** (F5 / button): parameter dialog (types from the signature), result in DataCube's grid
  (`embed.ts`), the SQL shown (generatePlan), cancel.
- **Service execution**: the service's query with its mapping/runtime (multi-execution: pick the key), path
  parameters as query variables (bound, never string-substituted — the deleted invention).
- **Mapping execution**: a query on a mapped class (the embedded query builder, §A5), against the runtime of choice.
- **Execute SQL** on a connection (upstream's SQL playground) — in-tab DuckDB only.
- **Done when:** `//studio:verify_test` runs a function, a service and a mapping query on the demo model and checks rows.

### A4. Tests (upstream's testable framework)
- **Core work** (announced on `main`; the charter `docs/DEFERRED_TEST_EXECUTION.md`): `testable/runTests` with the
  engine's assertion semantics (`equalTo`, `equalToJson`, `equalToRelation`, embedded data resolution, test suites per
  mapping/service/function), a WASM export, and lite's server route. Spec by upstream's PCT/engine tests, never by
  porting the deleted runner.
- Studio: per-element Test tab (suites, atomic tests, assertions; run one/suite/all/failing), the global Test Runner
  side panel, failure diff viewer.
- **Done when:** the upstream showcase projects' mapping and service tests pass in the tab, with the engine's results as
  the oracle.

### A5. The query builder inside Studio
- Query's builder (fetch structure, filter, parameters, milestoning, text mode) embedded for "Query…" on a class,
  service query editing, mapping execution and tests — the same component Query uses, over the in-tab engine.
- **Done when:** a service's query is built in the builder, saved into the service's text, and runs.

### A6. Snap, for every app
- Snap's engine half (freeze source rows into the tab's DuckDB, the stamp, refuse what cannot run locally, receipts)
  moves to `engine-client/`; the Live/Snap control and stamp badge into the shared look package; DataCube keeps its
  cube behaviour on top. Query and Studio get the same control. DataCube's Snap tests move with it and run per app.

### A7. Editing comfort Studio needs for real use
- Ctrl+P element search, rename/move element (one dialog, full path), delete with undo before save, local-changes
  **diff** (Monaco's diff editor), review **diff** view, whole-project text mode, workspace **update** (rebase) and
  conflict resolution (SDLC rules: `update`, `conflictResolution/*`), group workspaces, revision history browser.
- Definition/hover/completion from the in-tab compiler (design §3 h); diagnostics with positions inside bodies (W1.2,
  core — announced before it lands).

---

## 3. Track B — our Studio on a real Legend deployment (D2), and feature parity

The common thread: **the full model round trip** — upstream servers speak entity JSON; our Studio edits text. B2–B4
depend on B1.

### B1. The model round trip (core; S19, S20 — design §3 pieces e, f)
- **Model printer** (`jsonToGrammar/model`, engine-exact, byte parity with 4.145.0 goldens as for lambdas) and **model
  JSON reader** (PMCD `data` contexts → protocol records → compiler), both in core, both WASM exports, both on lite's
  server.
- **The exact-round-trip rule** (S5): JSON → text → JSON must give equal protocol records, proven over the whole
  corpus (5,259 sources) and the showcase projects; an element that fails opens read-only as JSON and is preserved
  byte for byte (S19 point 2).
- **Done when:** the corpus and the showcase projects round-trip with equal records, in the tab (WASM) and on lite's
  server.

### B2. Our Studio → real legend-sdlc (D2)
- `sdlc-client` detects the server: text routes present (ours) or not (upstream). Upstream: read entities JSON → print
  to text in the tab (B1, WASM); save by parsing text → entity JSON in the tab → `entityChanges` (same lock).
- Project structure: upstream projects have `pom.xml`s and `src/main/legend` JSON files — the real SDLC handles its own
  layout; we only speak its API. Snapshot dependencies and patches: follow upstream's rules (refusals shown).
- Auth: upstream SDLC's GitLab OAuth (`/auth/authorized`, `/auth/authorize` redirect, terms-of-service), as upstream
  Studio does (census A §9) — the page follows the redirect flow.
- **Done when:** D2's SDLC half passes against a real legend-sdlc with a GitLab (or its FS backend for CI).

### B3. Our Studio → real legend-depot (D2)
- Dependencies: `dependenciesFromArtifactDependencies` entities → print to text in the tab (B1) for the in-tab compile,
  or compile from JSON (the reader). Version lists, project configurations, classifiers: already upstream shapes.
- **Done when:** a project depending on a real Depot version compiles in the tab.

### B4. Our Studio → real legend-engine (D2)
- Compile, execute, generatePlan, runTests, lambda types through the engine's `pure/v1` (the A1 engine choice).
- **Dialect parity**: lite accepts code the engine refuses (function resolution J/L/P/Q/R, recorded in
  `docs/function-resolution/README.md`, and other lenient spots). Fix: the v1 function-resolution work (Steps 0–5) and
  an **engine-dialect compile in the tab** (lite's `Dialect.LEGEND_ENGINE`) when the session targets a real engine, so
  live errors match what the engine will say.
- Errors: the engine answers the first error with `sourceInformation` — mapped to file and line like ours.
- **Done when:** D2's engine half: the showcase projects compile, and a query runs, on 4.145.0 from our Studio.

### B5. Feature parity: everything upstream Studio offers a user (census Parts A–B)
Measured by feature, against both stacks (ours and a real deployment). **Must-have** for drop-in; **later** after D2.

| Feature (upstream) | Ours today | To build | |
|---|---|---|---|
| Workspace setup, projects, user workspaces | yes | group workspaces; patches (follow upstream's rules) | must |
| Text editing, live compile, Problems | yes (text per element) | whole-project text mode; definition/hover/completion; positions in bodies (W1.2) | must |
| Push (save) with the lock, local changes | yes | local-changes **diff** (Monaco diff editor); discard per change | must |
| Workspace update (rebase) and conflict resolution | no | `update`, `conflictResolution/*` in SDLC-lite; a 3-way merge editor | must |
| Reviews: create, approve, commit, close, reopen | create/commit/close | the review page: diffs per element and configuration, approvals | must |
| Versions / release, project overview | yes | release notes view, versions viewer (read a version's elements) | must |
| Project configuration: dependencies, platform config, structure version | dependencies | the rest of the configuration editor; dependency report (conflicts) | must |
| Run function / service / mapping query, generate plan | no | A3 | must |
| Tests: per element, global test runner | no | A4 | must |
| Query builder (Class "Query…", services, mapping tests) | no | A5 | must |
| Element forms: class, enumeration, association, profile, function | no | forms that edit text surgically (design §3 d) | must |
| Element forms: mapping, runtime, connection, database, service, data space, diagram | no | the same, larger editors; database view (schema tree) | later |
| Element search (Ctrl+P), rename/move, new element per type | new element | search, rename/move | must |
| Model importer (paste JSON/grammar, F2) | no | import entities or text into a workspace (B1 printer for JSON) | must |
| Viewing a project or a version read-only (`/view`, GAV viewer) | no | read-only editor over a version (Depot) | must |
| File generation (F10), code/schema generation, external formats & bindings | no | engine routes on lite's server + views | later |
| SQL playground, sample data generation | no | A3, A2 | later |
| Function activators, lakehouse/data products, data quality | no | extensions; follow demand | later |
| Light theme | no | A0 (`default-light`) | must |

### B6. What our own servers need for D1
- **lite's engine server** (when the session targets it instead of the tab): `testable/runTests` (A4),
  `jsonToGrammar/model` and `data` contexts (B1), `executeRawSQL` (SQL playground), generation routes (later).
- **SDLC-lite**: group workspaces, `update` and `conflictResolution/*`, review comparison routes, revision lists by
  entity (history view) — the routes OUR Studio's features above call, in upstream's shapes, so the same Studio code
  drives a real legend-sdlc (D2).
- **Depot-lite**: the dependency report, version viewing. Our stack resolves Depot in process (S9) where the engine
  needs it.

### B7. Teams and production
- The GitHub backend (S16: `createCommitOnBranch` with `expectedHeadOid`, PR reviews, Actions as the gate, the merge
  queue or the server as the queue), identity from the user's own token (S10), Depot rebuilt from tags.
- Imports in files (v1, the function-resolution record) — then "imports kept" (S13) is true.

---

## 4. Order and milestones

| Milestone | Contents | Proves | Core / `main` coordination |
|---|---|---|---|
| **M0** | Land `studio` on `main` (full gate, PR, review) -- **done 2026-10-05** (PR #23, 6f86c86dd, `main` green on every lane) | — | the line's announced edits |
| **M1** | A0 look + **A0½ Query and DataCube by name (the snapshot first)** + A1 in-tab engine + A2 test data + A3 run function/service/mapping | **D1 minus tests**: write → run on DuckDB in the tab → see rows; what Studio publishes, Query opens | none beyond A1 moves |
| **M2** | A4 tests (runTests in core + WASM + UI) + A5 query builder in Studio + A6 Snap | **D1 complete** | runTests in core: announce |
| **M3** | B1 round trip (printer + reader, corpus and showcase parity) | the hinge for D2 | core: announce |
| **M4** | B2 + B3 + B4: our Studio on real sdlc/depot/engine; v1 function resolution; engine-dialect compile | **D2** | function-resolution fix in core: announce |
| **M5** | B5 must-haves not yet done (diffs, update/conflicts, review page, forms for the core kinds, importer, viewer, search) + B6 | **drop-in replacement** | SDLC-lite routes |
| **M6** | B5 later items, A7, B7 teams (GitHub backend, imports) | daily use by a team | — |

M1 first: it is what makes Studio *do* something, and it needs no core change. M3 can run in parallel with M2 (core
protocol vs. Studio/engine-client). M5's features are built once and work on both stacks, because every one goes through
`sdlc-client`, `depot-client` and the engine choice in upstream's shapes.

---

## 5. Decisions

1. **Tests run anywhere; the browser first** (ruled 2026-10-04, the user: "yes on tests running in browser as v1, but
   they really should be able to be run anywhere"). The test runner -- the engine's assertion and embedded-data
   semantics -- is written ONCE in core, like the SDLC rules, and runs in the tab (WebAssembly, rows in DuckDB-WASM),
   on lite's server (`testable/runTests`), and Studio can send the same tests to a real engine's `runTests`. The browser
   is v1; the server route follows in the same milestone. Upstream runs mapping tests on H2 engine-side; lite runs them on
   DuckDB -- a recorded departure, with the real engine as the oracle on the showcase projects.
2. **Engine rules by default; a "full Pure" mode as a per-project choice** (the user: "engine rules in the tab for sure as
   default, but maybe a special tab to run in full pure mode as well"). lite compiles two dialects: `LEGEND_ENGINE` (what
   legend-engine accepts) and `LEGEND_LITE` (more of legend-pure's language). Recommended refinement: the mode belongs to
   the PROJECT (in its configuration), not to an editor tab -- whether saved text is valid must not depend on which tab it
   was typed in, and the release gate must use the same rules as the editor. Engine mode is the default and is forced
   when the project lives on a real deployment (D2); full-Pure projects show a badge, and their releases say so.
3. **One WebAssembly module per app** (the user: "def a single wasm for studio, maybe stripped for datacube and query").
   TeaVM compiles only what an entry class reaches, so each app gets its own build from one source: Studio's (compiler,
   planner, SDLC and Depot rules, test runner), Query's and DataCube's (planner and grammar only, as today). At M1.
4. **Forms edit the text, never reprint it** -- see the recommendation in the answer of 2026-10-04: start with
   position-based splices (the parser's source positions, which exist today), move to a lossless syntax tree (design §3 d)
   when forms need it.

## 6. Risks

- **Test semantics** (A4): the engine's assertion and embedded-data rules are broad; mitigate by the charter (spec by
  upstream tests, the engine as oracle), not by approximation.
- **Exact round trip** (B1): an element we cannot print exactly cannot be edited as text when it comes from a real
  SDLC; mitigated by S19's read-only-and-preserved rule and showcase/corpus parity before turning it on.
- **Dialect drift** (B4): lite's leniencies make code that works in the tab fail on the engine; mitigated by the
  engine-dialect compile and the v1 resolution fix, measured on the corpus (as the function-resolution record did).
- **Browser data scale** (A2): DuckDB-WASM holds what fits in a tab; large data stays live (warehouse, server) with Snap
  for slices.
- **Real deployments' auth** (B2): GitLab OAuth through the SDLC, engine and Depot auth headers differ per deployment;
  mitigated by following upstream Studio's flows exactly (census A §8–9) and testing against a real deployment early
  (M4 starts with a read-only connection).

## 7. Out of scope (the user, 2026-10-04) — kept for reference

Upstream clients against our servers. If that changes, the work is:
- **Upstream Studio → our servers**: `entityChanges` saves (B1 printer), every SDLC/engine route upstream Studio calls at
  init and in its editors (census A §7, B §7: `getClassifierPathMap`, `getSubtypeInfo`, generation lists, external
  formats, function activators, `currentUser`, relational-operation grammar, `modelCoverage`), upstream Depot's
  `/classifiers/*` gap, and a pinned local build of legend-studio as the oracle.
- **The upstream engine → our Depot**: `pureModelContextData` exactly as `DEPOT_CONTRACT.md` §6.4 requires, without
  upstream's collapse bug (§6.2 Q2).
