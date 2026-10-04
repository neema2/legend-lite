# Studio, the full experience: run in the browser, then a drop-in replacement for upstream Studio (2026-10-04)

The ask (the user, 2026-10-04): "a comprehensive plan … for full experience — execute against duckdb wasm in
browser, then full interop with real engine/depot/sdlc so it can really be drop in replacement for current
studio". This plan builds on `docs/STUDIO_DESIGN_2026_10_02.md` (decisions S1–S22, departures §4) and the
censuses in `studio/docs/` (behaviour: `UPSTREAM_STUDIO_CENSUS.md`; look: `UPSTREAM_STUDIO_LOOK.md`; wire
contracts: `SDLC_CONTRACT_SLICE1.md`, `SDLC_CONTRACT_SLICE2.md`, `DEPOT_CONTRACT.md`).

---

## 0. What "drop-in replacement" means — the acceptance tests

Studio-lite is a drop-in replacement when all four directions work, each proven by a test that runs in CI:

| # | Direction | Acceptance test |
|---|---|---|
| **D1** | **Our Studio, nothing else** (level 0) | In Chromium: write a model with a mapping and test data, **run a function, a service and a mapping query on DuckDB in the tab**, see the rows in the grid, run the model's tests, save, review, release — no server process anywhere. |
| **D2** | **Our Studio → upstream servers** | Our Studio, configured with a real legend-sdlc, legend-depot and legend-engine (4.145.0), opens an existing upstream project (the Legend showcase projects), shows every element as text, edits one, saves, reviews, commits, releases; runs a query through the real engine; every untouched element byte-identical. |
| **D3** | **Upstream Studio → our servers** | Real legend-studio (built locally), configured with our model home and lite's engine server, does its whole loop: open, edit in forms and text, compile (F9), run a function, run tests, push, review, commit, release, browse dependencies. |
| **D4** | **Upstream engine → our Depot** | legend-engine 4.145.0 with Depot-lite as its `alloy` metadata server executes a service by pointer against a version our Studio released. |

D1 is the **full experience**; D2–D4 together are **drop-in**. Each track below names which it serves.

---

## 1. Where we are (2026-10-04, branch `studio`)

Works, tested at both levels (page alone; model home over git), `//studio:verify` in Chromium:
- **write** any element lite compiles, as text, one per file, comments kept (no imports: v0);
- **compile** live in the tab (WebAssembly), with dependencies' released text from Depot;
- **save** with upstream's revision lock; **review → merge commit** (workspace closes); **release** (compile-gated);
  **dependencies** picked from Depot (upstream's nearest-wins).

Does not work yet: **running anything** (functions, services, mappings, tests), any **upstream server** (text routes
are lite's own), **upstream Studio against us** (no JSON saves, missing init routes), the engine's **pointer** to Depot.
And it does not **look** like upstream Studio (re-skin pending, look census done).

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
  look. Screenshot per screen in `//studio:verify`.
- **Done when:** side-by-side screenshots against the census show no unexplained difference.

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
- **Done when:** `//studio:verify` runs a function, a service and a mapping query on the demo model and checks rows.

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

## 3. Track B — interop with upstream (D2, D3, D4)

The common thread: **the full model round trip** — upstream speaks entity JSON; we keep text. Everything in this track
depends on B1.

### B1. The model round trip (core; S19, S20 — design §3 pieces e, f)
- **Model printer** (`jsonToGrammar/model`, engine-exact, byte parity with 4.145.0 goldens as for lambdas) and **model
  JSON reader** (PMCD `data` contexts → protocol records → compiler), both in core, both WASM exports, both on lite's
  server.
- **The exact-round-trip rule** (S5): JSON → text → JSON must give equal protocol records, proven over the whole
  corpus (5,259 sources) and the showcase projects; an element that fails opens read-only as JSON and is preserved
  byte for byte (S19 point 2).
- **Done when:** the corpus round-trips with equal records, and `entityChanges` on our SDLC stores upstream clients'
  saves as text (the 501 retires).

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

### B5. Upstream Studio → our servers (D3)
- **SDLC-lite**: every route upstream Studio calls (census A §7, slice 1–2 contracts): `entityChanges` (B1),
  `auth/*`, `server/platforms`, `configuration/*`, group workspaces, `update` and `conflictResolution/*`, review
  comparison routes (`comparison`, `from|to/entities|configuration`), revision lists, `pureModelContextData` (SDLC
  side), patches (501 is acceptable: Studio catches it).
- **Depot-lite**: `analyzeDependencyTreeFromArtifactDependencies`, `/classifiers/*` (Studio and Query call them; upstream
  Depot lacks them — we may answer, recorded), the GAV viewer routes.
- **lite's engine server**: Studio's init calls (`server/v1/currentUser`, `protocol/pure/getClassifierPathMap`,
  `getSubtypeInfo`, code/schema generation lists, external formats, function activators, supported auth flows),
  `data` model contexts (B1's reader), `testable/runTests` (A4), `jsonToGrammar/model` (B1), relational-operation
  grammar for mapping forms, `analytics/mapping/modelCoverage`, `executeRawSQL`.
- **The oracle**: build legend-studio 821c74c locally, point it at our servers, script its loop in Playwright (S12c).
- **Done when:** D3 passes, scripted.

### B6. Upstream engine → our Depot (D4, design S9)
- Depot-lite serves `GET /projects/{g}/{a}/versions/{v}/pureModelContextData?convertToNewProtocol=false&clientVersion=`
  exactly as the engine requires (DEPOT_CONTRACT §6.4: serializer, origin with `version: "none"`, dependencies
  included, every pointer path present) — **without** upstream's collapse bug (§6.2, quirk Q2), recorded.
- lite's own server resolves `pointer`/`combination` contexts against Depot-lite in process (S9).
- **Done when:** D4 passes on legend-engine 4.145.0.

### B7. Teams and production
- The GitHub backend (S16: `createCommitOnBranch` with `expectedHeadOid`, PR reviews, Actions as the gate, the merge
  queue or the server as the queue), identity from the user's own token (S10), Depot rebuilt from tags.
- Imports in files (v1, the function-resolution record) — then "imports kept" (S13) is true and §4 row 2 applies.
- Form editors (Phase 4–5), editing text surgically (a parser keeping trivia, design §3 d).

---

## 4. Order and milestones

| Milestone | Contents | Proves | Core / `main` coordination |
|---|---|---|---|
| **M0** | Land `studio` on `main` (full gate, PR, review) | — | the line's announced edits |
| **M1** | A0 look + A1 in-tab engine + A2 test data + A3 run function/service/mapping | **D1 minus tests**: write → run on DuckDB in the tab → see rows | none beyond A1 moves |
| **M2** | A4 tests (runTests in core + WASM + UI) + A5 query builder in Studio + A6 Snap | **D1 complete** | runTests in core: announce |
| **M3** | B1 round trip (printer + reader, corpus parity) | the hinge of interop; `entityChanges` works | core: announce |
| **M4** | B2 + B3 + B4: our Studio on real sdlc/depot/engine; v1 function resolution; engine-dialect compile | **D2** | function-resolution fix in core: announce |
| **M5** | B5 + B6: upstream Studio on our servers; engine pointers on our Depot | **D3, D4** | lite server routes: announce |
| **M6** | A7 comfort, B7 teams (GitHub backend, imports, forms) | daily use by a team | — |

M1 first: it is what makes Studio *do* something, and it needs no core change. M3 is the hinge for all interop and can
run in parallel with M2 (different code: core protocol vs. Studio/engine-client).

---

## 5. Decisions to make (recommendations given)

1. **Where tests' rows live in the browser** — upstream runs mapping tests on H2 engine-side; we run them on DuckDB in
   the tab. Recommend: DuckDB, recorded as a departure, with the engine as the oracle on the showcase projects (A4).
2. **Engine-dialect compile in the tab** when targeting a real engine (B4) — recommend yes: live errors must match the
   engine the user deploys to.
3. **Our Depot answering `/classifiers/*`** (routes upstream Studio and Query call but upstream Depot lacks) —
   recommend yes, recorded; they make upstream Studio and Query work fully against us.
4. **Building upstream legend-studio as the D3 oracle** — recommend yes (a pinned local build in CI); without it D3 is
   unprovable.
5. **Merging the page modules** (planner + SDLC/Depot rules in one WebAssembly module, so the grammar is downloaded
   once) — recommend at M1, when the in-tab engine joins Studio and size starts to matter.

## 6. Risks

- **Test semantics** (A4): the engine's assertion and embedded-data rules are broad; mitigate by the charter (spec by
  upstream tests, the engine as oracle), not by approximation.
- **Exact round trip** (B1): an element type we cannot print exactly would block saves from upstream clients;
  mitigated by S19's read-only-and-preserved rule and corpus-wide parity before turning it on.
- **Dialect drift** (B4): lite's leniencies make code that works in the tab fail on the engine; mitigated by the
  engine-dialect compile and the v1 resolution fix, measured on the corpus (as the function-resolution record did).
- **Browser data scale** (A2): DuckDB-WASM holds what fits in a tab; large data stays live (warehouse, server) with Snap
  for slices.
- **Upstream Studio's breadth** (B5): it calls ~60 engine routes; many are extensions (lakehouse, activators) — answer
  them with upstream's 501/empty lists where Studio tolerates it, recorded, and prove the core loop (D3) first.
