# Studio, SDLC-lite and Depot-lite — design and plan (2026-10-02)

**What:** a lite Legend Studio (`studio/`) that **writes** models, and the model home it writes into:
an SDLC-lite (projects, workspaces, revisions, reviews, versions) and a Depot-lite (published versions,
readable by `groupId:artifactId:version`). Query, DataCube and the engine then read models **by name**
instead of carrying `.pure` text.

**Why now, and why together (the user, 2026-10-02):** "write the depot and lite projects/versions/model
home as part of studio so we have a real writer of models instead of only readers". Every lite client
today is a reader carrying its own model text (census F §0); a model home designed from the reader
side would guess what a project, a workspace and a version are. Studio is the thing that creates them.

**Source of truth:** `studio/docs/UPSTREAM_STUDIO_CENSUS.md` (legend-studio `821c74c`, legend-sdlc
`1021fda`, legend-depot `9c0a809`, legend-engine `230c159`). Section references below (A §4.3, C §5.3,
…) are to its parts. Rulings that bind from earlier work: upstream APIs and shapes exactly; a local
implementation of the same contract where no server exists; sources point to models, never copy them;
saved artefacts pin model versions with an easy upgrade path; everything through Bazel; no Java
dependencies; identity always (census F §7, C1–C21).

---

## 1. What the homework established

1. **Upstream Studio is a thick client over three thin servers.** It builds the whole typed graph in the
   browser, computes change detection itself (entity hashes), and calls SDLC for storage, Depot for
   dependencies, and the engine only for grammar, compile, execute and tests (A §0, B §0). The servers'
   job is storage and compilation, not editing logic.
2. **SDLC already defines a "minimal backend"** (C §5.3, `legend-sdlc/docs/re-architecture.md:133-145`):
   storage (files at revisions, atomic submit, history) plus project and workspace lifecycle are
   required; entities, configuration, dependencies and comparison are generic on top; reviews,
   versions, patches, workflows, builds, backup, issues and conflict resolution are optional
   capabilities (501 when absent). That is the line between a first SDLC-lite and later ones.
3. **Publishing is nobody's API upstream.** A version is a git tag in SDLC; CI builds Maven artifacts and
   calls Depot's `/queue`; neither repo contains that wiring (C §4, D §1). A lite model home owns
   publishing by necessity, as MODEL_HOME D-C already said.
4. **Depot's read side is small and exact.** Query and DataCube need project, versions, a version's
   entities, its transitive dependency entities and `latest` (D §4.2a); Studio needs
   `dependenciesFromArtifactDependencies` plus pickers (D §4.2b); the engine needs exactly one route,
   `…/versions/{v}/pureModelContextData`, whose `origin`/`serializer` shape it asserts (E §2.3, §6.1).
5. **The engine side has two hard prerequisites lite lacks** (B §7.1, F §8.1):
   - a **model JSON reader** (PMCD → compiler): Studio compiles by sending a `data` context, and Depot
     answers are model JSON;
   - a **model printer** (`jsonToGrammar/model`): Studio's text mode and every "Grammar" view.
   And one strong want: `compilation/compile` returning `sourceInformation` (Studio places its error
   marker from it; lite puts positions only in message text today).
6. **lite has the pieces upstream lacks:** a multi-file, tolerant, all-errors compiler
   (`parseSources`, `buildModule`, `compileAllBodies` — F §3.2) and an in-tab WASM planner. Upstream
   Studio compiles only on F9, returns one error, and has no client-side type checking (B §8.6-8.9).
   lite can give live, multi-error diagnostics in the tab — the place lite can be better, not just equal.

---

## 2. Decisions

Status: **settled** (follows from the homework and standing rulings) or **proposed** (needs your call;
a recommendation is given and the alternatives named).

| # | Decision | Status |
|---|---|---|
| S1 | **Three servers' contracts, upstream shapes exactly, one lite process.** SDLC-lite serves legend-sdlc's REST shapes, Depot-lite serves legend-depot's read API, the engine stays `pure/v1`. Each has its own base URL in the Studio/Query config (as upstream: `sdlc.url`, `depot.url`, `engine.url`, A §8.1), mounted in one lite server under distinct prefixes (`/sdlc/api`, `/depot/api`, `/api`). Pointing a client at real upstream servers stays a configuration change. | settled (C1, C2, MODEL_HOME D-B) |
| S2 | **Our own Studio app, upstream's wire.** `studio/` is a lite rebuild like Query (plain TypeScript + DOM, esbuild, node tests, Playwright verify), with upstream's look and layout. It is not legend-studio running against lite. | **proposed** — recommended. Alternative: run upstream legend-studio itself against lite (needs its whole React/MobX/legend-graph stack and its yarn build; lite would be only the servers). I recommend using upstream Studio instead as a **compatibility oracle** in tests (S12), not as the product. |
| S3 | **A code editor dependency.** Studio's text mode needs a real editor (Monaco upstream). Query/DataCube have no editor dependency (D6). | **proposed** — options: **Monaco** (what upstream uses; matches look and behaviour exactly; ~3 MB, worker setup) or **CodeMirror 6** (modular, ~300 KB, easier with plain DOM). Recommendation: **Monaco**, for "exactly upstream" and because Pure tokenizing rules already exist for it (legend-code-editor, studio-lite). |
| S4 | **Compile in the tab, all errors, live.** The WASM planner gains a compile entry (multi-file `parseSources` + tolerant `buildModule` + `compileAllBodies`) returning structured diagnostics with spans. The editor shows every error as you type (debounced), F9 still exists, and the server's `compilation/compile` answers the same diagnostics for upstream clients (first error with `sourceInformation`, warnings as `defects`). | **proposed** — recommended; it is lite's advantage and what Studio exists to show. It depends on W1.2 (diagnostics with spans), owned by the compiler rebuild (C20), so the order is negotiated, not assumed (S11). |
| S5 | **Storage is a revision store of entity JSON, written by lite.** No JGit (C6). A small content-addressed store: blobs (one per entity, the exact JSON received), trees (path → blob), revisions (tree + parent(s) + author + time + message), refs (project line, one per workspace, one tag per version). Layout on disk follows upstream project structure **v0** (`/project.json` + `/entities/<pkg>/<Name>.json`, a real upstream layout, C §3.3) inside each tree. Storing the bytes Studio sent means a read returns them unchanged, which is what Studio's hash-based change detection needs (A §3.3: otherwise phantom changes). | **proposed** — recommended. Alternatives: (a) one `.pure` file per element (upstream v11+ prefers it; readable diffs) — needs the model printer on every write and a byte-stable JSON→text→JSON round trip, or Studio sees phantom changes; (b) write real git objects (zlib + SHA-1 in plain Java) so the store is a git repository — attractive, more work; can follow later without changing the API. |
| S6 | **SDLC-lite scope, in capability terms (C §5.3).** First: projects, user and group workspaces, revisions (with `BASE`/`CURRENT`/`HEAD` aliases and history), entities (all filters), entity changes with the 409 lock, project configuration (incl. dependencies), comparison, **reviews** (create, list, get, commit, close; approval optional) and **versions**. Later: workspace update (rebase) + conflict resolution, patches, backup. Never (lite): workflows, builds, issues — answered as upstream's 501 "capability not supported". | **proposed** — recommended; reviews are kept because they are Studio's only path from a workspace to the project line (C §2.3). Alternative: a "workspace commits straight to the project line" mode (simpler, but not upstream's flow). |
| S7 | **Publishing = cutting a version.** `POST /projects/{p}/versions` compiles the project line at that revision with its dependencies (compile-gated: refuses on any error), tags it, and writes an **immutable** Depot record (entities + direct and transitive dependencies). A review commit also republishes `master-SNAPSHOT` (what upstream CI does for the default branch, D §1.4). | **proposed** — recommended (MODEL_HOME D-C, "ours by necessity"). Open sub-question: also expose Depot's own Maven-free ingest shape (`PUT /queue/rest/metadata`, D §1.3) for loading the 59 `projects/` as seed data? Recommended yes, owner-only. |
| S8 | **Depot-lite serves the read API in full for Query, DataCube, Studio and the engine**, including the routes upstream's OSS Depot lacks but its clients call (`GET /classifiers/{path}` and `/classifiers/{path}/entities`, D §7.1). Aliases `latest` and `head` (and `HEAD`/`master-SNAPSHOT` as Studio sends them); releases immutable; missing versions are **404** (upstream's 500 is a defect, D §7.4 — a SEMANTICS_REGISTER row). Dependency resolution: one algorithm, nearest-wins with exclusions, computed at publish and at request (upstream has two that can disagree, D §7.11). Accept `version` and `versionId` in dependency bodies. | settled (D, E, C1) — the departures from upstream's defects are listed for your review in §4. |
| S9 | **The engine resolves pointers in-process.** `pure/v1` accepts `pointer` (alloy) and `combination` beside `text`, and `data` once the model reader lands; an alloy pointer resolves against Depot-lite inside the same process, cached per immutable release (snapshot and alias never cached — upstream caches `latest`, a defect, E §6.5). | settled (MODEL_HOME D-A); caching follows the measurement owed (C12). |
| S10 | **Identity.** Studio needs authorship, owners and review authors. Upstream's SDLC is the identity source (`/auth/authorized`, `/currentUser`, A §8.3). | **proposed** — first slice: a configured single local user (as SDLC's FS backend does), with every record carrying a real author field; then sign-in through the warehouse's identity (SERVER_PROGRAM E1) so authorship is the person's own. Never a shared service account (C7). |
| S11 | **Ownership of core work.** The model JSON reader, model printer, structured diagnostics with spans, WASM compile entry and pointer contexts are `core/` and `wasm/` work, which the compiler rebuild program owns (C20, EXECUTION_PLAN W1.2). | **proposed** — these are announced in `docs/IN_FLIGHT.md` and sequenced with that program before Studio depends on them; the Studio line builds the servers' storage and the app, and does the core pieces only where the program agrees. |
| S12 | **Verification, as Query.** (a) Contract tests per endpoint, shapes from the census; (b) `//studio:verify`, Playwright end to end: create project → workspace → write classes → compile → save → review → commit → publish → open it in Query by GAV → run a query; (c) **upstream as oracle**: legend-engine 4.145.0 configured with Depot-lite as its `alloy` metadata server must execute an alloy pointer against a lite-published version; and, if the build is practical, upstream legend-studio pointed at lite's SDLC/Depot/engine must open, save and publish. | settled (method); (c)'s second half depends on building legend-studio locally. |
| S13 | **Editing model: text-first, then forms.** Upstream is form-first with a whole-project text mode (B §3). | **proposed** — first slice: explorer + per-element and whole-project text editing with live diagnostics, plus the read-only JSON/Grammar views; then form editors for class, enumeration, association, profile and diagram; then mapping, runtime, connection, service, data space. Alternative: form-first from the start (upstream's default experience, much more UI before the loop closes). |

---

## 3. Plan

Each phase closes something end to end and is verified before the next starts. Phases 0–2 are the
loop; 3+ deepen it.

**Phase 0 — engine prerequisites** (core/wasm; S11 coordination first)
- Model JSON reader: PMCD JSON → protocol records → compiler (the mirror of `ProtocolEmitter`, F §2.3); `data` and `combination` contexts on `pure/v1`. Proof: every corpus model round-trips text → JSON → compile identically (byte-exact grammarToJson already holds on 5,259 sources).
- Model printer: `jsonToGrammar/model` byte-equal to legend-engine 4.145.0 (goldens captured from the engine, as for lambdas).
- Diagnostics with spans (W1.2 a–b) reaching `compilation/compile` (`sourceInformation`, `defects`) and a WASM `compile` export.
- Pointer contexts: alloy resolution through a Depot interface (Depot-lite in Phase 1).

**Phase 1 — the model home** (server; Java, no dependencies)
- Revision store (S5), SDLC-lite routes for S6's first scope, Depot-lite read API (S8), publish on version (S7), Maven-free seed ingest of `projects/`.
- Contract tests per route; the engine-oracle test (S12c first half).

**Phase 2 — Studio, the loop** (`studio/`)
- Shell and setup (projects, workspaces, create both), explorer (own, dependency and system trees), Monaco text editing per element and whole-project, live diagnostics (S4), Problems panel, status bar.
- Save (`entityChanges`, the revision lock, out-of-sync handling), review create/commit, Project overview: versions and publish.
- `//studio:verify` through the whole loop, including reading the published version from Query.

**Phase 3 — readers by name**
- Query and DataCube load models from Depot-lite by GAV (data space listing via `/classifiers`); saved queries and cubes pin the version, with the upgrade path (MODEL_HOME D-G); the demo's `.pure` config retires.

**Phase 4 — forms and project configuration**
- Class, enumeration, association, profile and diagram editors; project configuration (dependencies picked from Depot-lite, nearest-wins report); workspace update and conflict resolution.

**Phase 5 — the rest of the core editors and running**
- Mapping, runtime, connection, service and data space editors; run function and mapping execution (an ad-hoc DuckDB in place of upstream's H2, B §8.17); tests (`testable/runTests`).

---

## 4. Departures from upstream to rule on (each a SEMANTICS_REGISTER row if kept)

| Upstream behaviour (census) | lite proposal |
|---|---|
| Depot returns HTTP 500 for a missing, excluded or evicted version (D §7.4) | 404 for missing; excluded/evicted not modelled at first |
| Depot's project id must match `^PROD-\d+$` (D §7.2) | any SDLC project id |
| Two dependency algorithms (stored closure + root override vs request-time Aether) that can disagree (D §7.11) | one: nearest-wins with exclusions |
| The engine caches `latest`/`head` for up to 30 min (E §6.5) | aliases resolved per request; only exact releases cached |
| `compilation/compile` returns the first error only (B §8.7) | first error (upstream shape) on the wire for upstream clients; lite's own Studio gets every error from the tab |
| Text mode loses comments and imports (B §8.2) | same at first (storage is JSON); a later `.pure`-per-element store (S5 alternative) could keep them |
| SDLC FS backend defects (C §5.2) | not inherited — lite's SDLC is its own implementation of the contract |

---

## 5. Decisions needed from you before Phase 0/1

S2 (own Studio, upstream as oracle), S3 (Monaco or CodeMirror), S5 (JSON revision store, v0 layout),
S6 (reviews in the first scope), S7 (publish on version + snapshot on commit), S10 (single local user
first), S11 (sequencing the core pieces with the compiler program), S13 (text-first), and the §4
departures.
