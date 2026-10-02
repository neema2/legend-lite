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
| S2 | **Our own Studio app, upstream's wire.** `studio/` is a lite rebuild like Query (plain TypeScript + DOM, esbuild, node tests, Playwright verify), with upstream's look and layout. It is not legend-studio running against lite. | **ruled 2026-10-02** (the user: "build our own exactly like Query that can also run lite WASM embedded"): our own app, with the in-tab WASM plane as Query has; upstream Studio only as a compatibility oracle in tests (S12). |
| S3 | **A code editor dependency.** Studio's text mode needs a real editor (Monaco upstream). Query/DataCube have no editor dependency (D6). | **ruled 2026-10-02: Monaco** — upstream's editor (exact look and behaviour, built-in diff editor for local changes, reviews and merges); the language intelligence is ours either way, from the in-tab WASM compiler. |
| S4 | **Compile in the tab, all errors, live.** The WASM planner gains a compile entry (multi-file `parseSources` + tolerant `buildModule` + `compileAllBodies`) returning structured diagnostics with spans. The editor shows every error as you type (debounced), F9 still exists, and the server's `compilation/compile` answers the same diagnostics for upstream clients (first error with `sourceInformation`, warnings as `defects`). | **proposed** — recommended; it is lite's advantage and what Studio exists to show. It depends on W1.2 (diagnostics with spans), owned by the compiler rebuild (C20), so the order is negotiated, not assumed (S11). |
| S5 | **Storage: one `.pure` file per element, the authored text, is the only truth.** Each file holds one element with **its own imports and comments, stored exactly as typed** (upstream's SDLC refuses imports in `.pure` files, `PureEntitySerializer.java:244-260`, and its JSON has no comments — so upstream loses both; lite keeps them). Entity JSON is **never stored**: it is derived on read by parsing the file and resolving its names to full paths (S14), which is the fully-qualified shape upstream Studio saves. No second copy, so nothing to drift. Files live in a revision store written by lite (no JGit, C6): revisions (tree + parent(s) + author + time + message), refs (project line, one per workspace, one tag per version), and every revision can be written out as a plain directory tree (`/project.json` + `<pkg>/<Name>.pure`) — what per-element Bazel targets (S7's later gate) build from. Why text and not JSON: JSON storage (upstream structure v0) can hold neither comments nor imports and gives noisy diffs. **Phantom changes** (A §3.3): Studio compares hashes of what it holds with what the server returns; if text → JSON were not deterministic, unchanged elements would show as changed — guarded by a corpus test that the derived JSON is stable, and by published versions keeping the JSON derived when they were cut. | **ruled 2026-10-02** (the user: "one pure file per element because I want bazel dependencies per element file"); text as the only truth, recommended 2026-10-02 — awaiting the user's OK. Open: whether the store's on-disk format is lite's own or real git objects (zlib + SHA-1 in plain Java, so the store *is* a git repository); either keeps the same API. |
| S6 | **SDLC-lite scope, in capability terms (C §5.3).** First: projects, user and group workspaces, revisions (with `BASE`/`CURRENT`/`HEAD` aliases and history), entities (all filters), entity changes with the 409 lock, project configuration (incl. dependencies), comparison, **reviews** (create, list, get, commit, close; approval optional) and **versions**. Later: workspace update (rebase) + conflict resolution, patches, backup. Never (lite): workflows, builds, issues — answered as upstream's 501 "capability not supported". | **ruled 2026-10-02**: as proposed, reviews in the first scope (they are Studio's only path from a workspace to the project line, C §2.3). |
| S7 | **Publishing: a version is an immutable tag on a revision that passed the gate; Depot-lite is a read-only view over those tags** (option D of 2026-10-02). `POST /projects/{p}/versions` runs the gate at that revision, then tags it; Depot-lite serves tags (entities derived per S5/S14, direct and transitive dependencies from `project.json`), `head`/`master-SNAPSHOT` = the project line's latest revision. Nothing is copied, so nothing drifts. **The gate grows:** first lite's compiler in-process (compile-gated: refuses on any error); then **Bazel** (option C): the revision is written out as files with per-element targets generated from resolved references, built and tested, and tagged only if green. D is the storage and read model; C is the gate — they compose. Seed data (the 59 `projects/`) enters as SDLC projects with versions, not through a side door. Alternatives weighed: A (copy into a separate Depot store at version time — two copies that can drift), B (tag only, publish as a separate step — upstream's split, two user steps). | **proposed** — D with the compiler gate now, the Bazel gate (C) next; awaiting the user's OK. |
| S8 | **Depot-lite serves the read API in full for Query, DataCube, Studio and the engine**, including the routes upstream's OSS Depot lacks but its clients call (`GET /classifiers/{path}` and `/classifiers/{path}/entities`, D §7.1). Aliases `latest` and `head` (and `HEAD`/`master-SNAPSHOT` as Studio sends them); releases immutable; missing versions are **404** (upstream's 500 is a defect, D §7.4 — a SEMANTICS_REGISTER row). Dependency resolution: one algorithm, nearest-wins with exclusions, computed at publish and at request (upstream has two that can disagree, D §7.11). Accept `version` and `versionId` in dependency bodies. | settled (D, E, C1) — the departures from upstream's defects are listed for your review in §4. |
| S9 | **The engine resolves pointers in-process.** `pure/v1` accepts `pointer` (alloy) and `combination` beside `text`, and `data` once the model reader lands; an alloy pointer resolves against Depot-lite inside the same process, cached per immutable release (snapshot and alias never cached — upstream caches `latest`, a defect, E §6.5). | settled (MODEL_HOME D-A); caching follows the measurement owed (C12). |
| S10 | **Identity.** Studio needs authorship, owners and review authors. Upstream's SDLC is the identity source (`/auth/authorized`, `/currentUser`, A §8.3). | **ruled 2026-10-02**: first slice a configured single local user (as SDLC's FS backend does), with every record carrying a real author field; then sign-in through the warehouse's identity (SERVER_PROGRAM E1) so authorship is the person's own. Never a shared service account (C7). |
| S11 | **Ownership of core work.** The model JSON reader, model printer, structured diagnostics with spans, WASM compile entry and pointer contexts are `core/` and `wasm/` work, which the compiler rebuild program owns (C20, EXECUTION_PLAN W1.2). | **proposed** — these are announced in `docs/IN_FLIGHT.md` and sequenced with that program before Studio depends on them; the Studio line builds the servers' storage and the app, and does the core pieces only where the program agrees. |
| S12 | **Verification, as Query.** (a) Contract tests per endpoint, shapes from the census; (b) `//studio:verify`, Playwright end to end: create project → workspace → write classes → compile → save → review → commit → publish → open it in Query by GAV → run a query; (c) **upstream as oracle**: legend-engine 4.145.0 configured with Depot-lite as its `alloy` metadata server must execute an alloy pointer against a lite-published version; and, if the build is practical, upstream legend-studio pointed at lite's SDLC/Depot/engine must open, save and publish. | settled (method); (c)'s second half depends on building legend-studio locally. |
| S13 | **Editing model: text-first, then forms.** Upstream is form-first with a whole-project text mode (B §3) that loses comments and imports, so everything must be written fully qualified. | **ruled 2026-10-02** (the user: "start text but make sure we can actually use imports and comments that dont get lost in roundtrip"): text-first — explorer + per-element and whole-project text editing with live diagnostics; imports and comments survive because the text itself is stored (S5). Form editors later (class, enumeration, association, profile, diagram; then mapping, runtime, connection, service, data space) must edit the text surgically, not reprint it, so comments in an edited element survive too — that needs a parser that keeps comments and whitespace as data (core piece d, §3). |
| S14 | **Entity JSON from text: resolve, then emit (core piece a).** The engine's `grammarToJson` records names as written (`Customer`) and keeps imports in a separate `SectionIndex`; resolution happens later, in its compiler — lite matches this byte for byte today and keeps doing so. Fully-qualified entity JSON is what upstream **Studio** writes when it saves (B §8.2). lite's equivalent: parse the file to protocol records, apply `NameResolver`'s rules (imports → full paths, which today run on the compiler's model, downstream) to the records, emit with the existing `ProtocolEmitter`. Oracle: an element written with imports and short names, through this step, must equal byte for byte the engine's `grammarToJson` of the same element written fully qualified without imports (minus `SectionIndex` and source positions). | settled (follows from S5); the oracle test is mechanical. |
| S15 | **Text on the wire: an optional `pureCode` on entity changes.** Upstream's `entityChanges` carry JSON only. lite adds an optional `pureCode` (the element's authored text) per CREATE/MODIFY; upstream clients ignore it. The server stores the text; if `content` is also sent it must equal the JSON derived from `pureCode` (else 400), so a client cannot save an inconsistent pair. A change with only `content` (an upstream client) is printed to text — needs the model printer (core piece f), and loses only what that JSON never had. | **proposed** — awaiting the user's OK; a SEMANTICS_REGISTER row (additive). |

---

## 3. Plan

Each phase closes something end to end and is verified before the next starts. Phases 0–2 are the
loop; 3+ deepen it.

**Phase 0 — engine prerequisites** (core/wasm; S11 coordination first). What is missing in core, and when it is needed:

| # | Missing piece | Needed for | Needed for the loop (Phases 1–2)? |
|---|---|---|---|
| a | Name-resolved entity JSON per element (S14) | every JSON reader: Depot entities, `pureModelContextData`, Query's graph, Studio's explorer | **yes** — the one hard prerequisite |
| b | Diagnostics with spans, every error (W1.2 a–b) | live squiggles and a Problems list; `compile`'s `sourceInformation` | **yes** for a good editor |
| c | A WASM compile entry (files → diagnostics) | live diagnostics in the tab | **yes** (small, after b) |
| d | A parser that keeps comments and whitespace as data | form edits that keep comments (S13) | no — Phase 4 |
| e | Model JSON reader (`data` contexts) | upstream clients sending JSON; upstream Studio as oracle | no — our Studio compiles text (one `###Pure` section per file, each with its imports) |
| f | Model printer (`jsonToGrammar/model`) | upstream clients' JSON-only changes (S15); form mode | no — Phase 4 / oracle |
| g | Pointer and combination contexts | Query and DataCube executing on the server by GAV | no — Phase 3 |
| h | Language features (go-to-definition, hover, completion) | a good editor | partly: definition and hover early in Phase 2, completion after |
| i | Test running, relational-operation grammar, classifier lists | tests and mapping forms | no — Phase 5 |
| j | Incremental, memoized compile | big models | no — measure first (C12) |

Phase 0 is therefore **a, b, c**; the rest arrive with the phase that needs them.

**Phase 1 — the model home** (server; Java, no dependencies)
- Revision store of `.pure` files (S5), SDLC-lite routes for S6's first scope (with S15's `pureCode`), Depot-lite read API (S8) as a view over version tags (S7), the compiler gate on version cut, the 59 `projects/` loaded as SDLC projects with versions.
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

**Kind 1 — on the wire** (each a SEMANTICS_REGISTER row if kept):

| # | Upstream (census) | lite |
|---|---|---|
| 1 | `entityChanges` carry JSON only | optional `pureCode` text (S15), additive |
| 2 | `.pure` files refuse imports (`PureEntitySerializer.java:244-260`) | imports allowed, per file (S5) |
| 3 | Depot returns 500 for a missing, excluded or evicted version (D §7.4) | 404 for missing; excluded/evicted not modelled at first |
| 4 | Depot project id must match `^PROD-\d+$` (D §7.2) | any SDLC project id |
| 5 | two dependency algorithms that can disagree (D §7.11) | one: nearest-wins with exclusions |
| 6 | aliases matched lowercase only (`latest`, `head`; D §5) | `HEAD`/`LATEST` accepted in any case |
| 7 | dependency bodies: JSON creator names the key `version`, Studio sends `versionId` | both accepted |
| 8 | the engine caches `latest`/`head` up to 30 min (E §6.5) | aliases resolved per request; only exact releases cached |
| 9 | a pointer without `serializer` crashes `compile`, `lambdaReturnType`, `runTests` (E §6.6) | defaults to production |
| 10 | a combination containing a pointer fails everywhere but `compile` (E §3) | works |

**Kind 2 — upstream defects not copied** (no wire change): Depot's sub-package filter over-matching (`a::b` matches `a::bc`, D §7.6); the classifier search skipping one project per 100 (D §7.8); the unsorted versions list (D §7.19); a transitive closure frozen at ingest (D §7.10); excluding a version wiping its dependencies (D §7.9); a state-changing GET `/queue` (D §7.15); the SDLC file-system backend's defects (C §5.2).

**Kind 3 — our Studio behaving better** (app only): live, every-error diagnostics instead of F9 and one error; comments and imports kept; a real commit message on save; project-configuration saves without a full reload; errors shown in place instead of a forced switch to whole-project text mode; DuckDB in the connection editor; upstream's approve-button and close-vs-reject client bugs not copied.

**Kept on purpose, though odd:** classifier-filtered Depot routes return wrapped `DepotEntity` objects (DataCube relies on it); workspace type inferred from `userId`; the wire typo `auhorizedProjectAction`; a review commit deletes the workspace; `compilation/compile` answers one error on the wire (upstream's shape) — lite's own Studio gets every error from the tab.

---

## 5. Decisions needed from you before Phase 0/1

Ruled 2026-10-02: S2 (own Studio), S3 (Monaco), S5 (one `.pure` file per element), S6 (reviews in
the first scope), S10 (single local user first), S13 (text-first, imports and comments kept).

Awaiting: S5's "text is the only truth" and the store's on-disk format (lite's own or git objects);
S7 (D with the compiler gate now, Bazel gate next); S15 (`pureCode`); S11 (sequencing core pieces
a, b, c with the compiler program); the §4 Kind-1 departures (Kinds 2 and 3 are fixes and
improvements only).
