# The rest of the program: twin hosts, one implementation, and legend-lite as a drop-in legend-engine

Agreed 2026-10-12. Rewritten 2026-10-13 as the runbook a fresh session builds the whole program from, end to end.

The user, 2026-10-12 and 2026-10-13:
- "lets plot out the whole rest of the program completely so we can see every step";
- "we should try as hard as we can to use the same interfaces inside them - with only the 'storage' part different";
- "really why we want legacy mode is actually so that legend-lite can be a drop in replacement for legend-engine - and
  so legacy mode really has two parts the UI talking to real legend-engine and the UIs talking to a real legend-lite
  server over the EXACT same protocols/APIs";
- "the tenet is that typescript and python are twins - so both columns should be the same";
- "please write it in detail so that we can have a fresh session implement the whole thing end to end please expertly
  from the docs".

## 0. How to use this document

Read all of it before writing code, then the designs it cites for the step in hand:
- `docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md`: the one app, its services, R1 to R6 (§13), and PlanAndRun (§15);
- `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md`: classes, associations, data spaces, dashboards on the model (§13, §15);
- `docs/DATACUBE_HEADLESS_PYTHON_DESIGN_2026_10_11.md`: DataCube's code in V8 inside Python;
- `AGENTS.md`, `docs/IN_FLIGHT.md` and `docs/GATES.md` on main: the architectural invariants, what other lines are
  doing, and how a landing is recorded.

Each step below has:
- **its goal**;
- **what is decided** (never reopened without the user);
- **the design**;
- **the files**;
- **its proof**;
- **its reviews**;
- **what to measure or ask first**.

Where a step says "ask the user", stop and ask, with a recommendation. Everywhere else, build.

## 1. The product's rules (the user's, each settled)

1. **Twin hosts.** The browser and Python give the same capabilities.
   - A feature's code comes with the app; a host gives only services (one-app design §12: "on Python, the work is
     services, never features"). A check in the app for one host's own pieces, rather than for a service, is a bug.
   - TypeScript and Python are twin client languages. A per-language library is designed once: the same functions,
     the same parameters, the same choice of backends (the host's own, in process, or a remote server). One shared test
     corpus holds the two alike. A two-column comparison whose columns differ in what is possible is a gap to close,
     not a design.
   - **A host plans in process** (the user, 2026-10-15). Python as a host -- its frames, its pages, DataCube on
     Python -- runs legend-lite's compiler in its own process. A remote engine is a client feature over the remote
     side's own models and data (DataCube's REST client in the browser app; a future Python client on `warehouse.py`'s
     pattern, a candidate, not built), never a mode of the host. So a Python host has one plane, and the remote
     planners G1 built are removed (G1 below).
2. **One implementation, storage per host.** Logic that must run on more than one host exists once; only where it keeps
   its records and how a call reaches it differ.
   - Logic a server needs too is Java, built three ways: the JVM, the browser's WebAssembly module (TeaVM), and
     Python's native library (GraalVM). Examples: the compiler, the SDLC, the Depot, saved queries, the warehouse's
     binding.
   - Client glue over services is two twin libraries, TypeScript and Python, held by one corpus. Example: creating a
     class in a project.
   - DataCube's own semantics are DataCube's TypeScript. Python's pages run it headless, never a Python copy. Examples:
     what a grid's relation is, how a page exports.
   - A remote git (GitHub) is a push and pull of a local git store, never a `Storage` of its own.
3. **legend-lite is a drop-in legend-engine.**
   - Studio, Query and DataCube speak legend-engine's APIs exactly, and work against a real legend-engine and against
     a legend-lite server with the same answers.
   - A feature that needs legend-lite's own routes (Create a Class…) is offered only where those routes answer, as
     every action is (one-app design §3.4).
4. **Python's DuckDB reads only what it was given (R4).** The page sends Pure, never SQL.
   - Python's own planner, in process, writes every line of SQL Python runs (§1.1: a host plans in process; the user's
     ruling of 2026-10-15, which replaced the server planners of 2026-10-13).
   - Python plans only over a model every table of whose Databases is one Python serves: its frames, a page's files,
     and the page's projects over them (the user's option (c), `Frames.model_for`).
   - A remote file is read only from a remote URL, matched exactly against `<scheme>://`. An Iceberg table's data files
     must be remote too.
   - A query's own `sourceUrl('file:...')` is accepted (the user: "your python could just read that file anyway").
5. **Every grid is one PlanAndRun** (one-app design §15): the chosen planner's plan, the connection's runner, and the
   rows typed by the plan by the executor of the language they become (§15.3a). A Variant is its JSON text.
6. **One session on every host.** Each host runs the compiler's session setup (`Boundary.sessionSetup`: DuckDB in UTC)
   on its DuckDB as it sets it up.
7. **No hacks.** Find the cause and agree a design before code. A shortcut becomes an anchored row of
   `docs/PARKED_WORK_LEDGER.md`, never a quiet fix.
8. **The simplest right thing, done in our own pull request.** A fix that touches another line's files is built here and
   reviewed by that line, never handed off to wait behind its pull request.

## 2. How work is done here

**The worktree.**
- Work in the worktree, never `cd` elsewhere; temporary files go in its `runs/`.
- Never a bare `git stash`: the stash stack is shared across worktrees and sessions.
- Never switch branches or edit sources while a gate runs.
- The shell is zsh: `for x in $list` does not split a string, so use an array or `while read`.

**A step's life.**
1. **Branch** from main: one branch per pull request, its steps separate commits.
2. **Announce** a cross-area edit (core, or another line's files) in `docs/IN_FLIGHT.md` on main before building it: the
   files, and why. When it lands, record "files touched beyond the announced list".
3. **Build and prove locally:**
   - `bazel test --lockfile_mode=error //gates:local //datacube/... //python/...`, plus every package the change
     touches;
   - the lane query, which must be empty: `bazel query 'tests(//...) except tests(//gates:lanes) except
     tests(//gates:heavy)'`.
4. **Review:**
   - an independent audit agent (`auditor`), on the commits; every blocker fixed and every should-fix answered before
     the pull request;
   - Bazel changes by the Compiler Rewrite session;
   - a planner-boundary change (`Boundary`, `TabExports`, the native library's entry points, a plan's shape) by the
     Plan Gen / Exec Split session as well;
   - Studio's files (`studio/`, `query/`, `site/`, `sdlc-*`, `depot-*`, `query-store/`) by the Studio / SDLC / Depot
     session.
5. **Pull request** with the description's sections: what it is; What this closes; Proof; Windows; Independent review.
6. **Merge:** the full CI run must be green on every platform.
   - A job failed by a runner's filesystem error (a Node module tree, lost remote-cache inputs) is re-run once.
   - Merge is an admin merge, with the user's approval each time.
   - Main then runs its own CI. If the merged commits carry `[skip ci]`, GitHub skips the push run, so dispatch
     `gate.yml` on main by hand.
7. **After landing:** a GATES entry and the IN_FLIGHT "landed" line, a docs-only commit on main (no CI runs for
   Markdown).

**Never:**
- push code to main without asking;
- merge without the user's approval;
- reopen a decision of §1 or §7.

**Tests.**
- **Every test target is in a lane** of `gates/BUILD.bazel`.
- **A Python test never touches a real home:** `python/tests/main.py` points `LEGEND_LITE_HOME` at the test's folder.
- **The headless lane** (`//datacube:headless_test`) runs every DataCube test file in V8. One that needs the page's own
  APIs is left out with a reason (`_HEADLESS_LEFT_OUT`).
- **Each test's import list** (`datacube/test_imports.bzl`) is regenerated by `bazel run //datacube:update_generated`.
- **Pure text added to a test** needs `//parser-equivalence/...`.
- **A typing change** needs `//spec:reference_lane`.
- **A bundle that grows** raises its budget in `datacube/test/bundle-budget.test.ts`, with the measured size before and
  after and the reason.

**Every commit ends with:**

```
Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>
Claude-Session: <the session's link>
```

**Explaining to the user.**
- Use plain words.
- Give one settled recommendation, not a menu.
- Ask before a decision of the user's; never decide it quietly.

## 3. Where things stand (2026-10-13)

**Landed on main:**
- **PR #31** (36b72ee13): DataCube is one app on every host, steps 1 and 2. Every grid is one PlanAndRun:
  - Python's results are typed by the plan;
  - a page's file goes to Python a part at a time, any size;
  - the tab's DuckDB runs the compiler's session setup.
- **The Compiler Rewrite line's star egress** (47349d82a): a stored Variant leaves every query shape as JSON.
- **PR #36** (8bd1eaf9e), B1 to B3:
  - the model actions over the host's services;
  - the page's settings kept by Python;
  - Python's model home, the SDLC and Depot rules built a third time into the native library, with option (c);
  - `bazel run //python:repl` carrying everything the wheel does.

**Elsewhere:** phase L, the legacy-mode differential, is the Studio / SDLC / Depot session's. Its first check is the
saved-query API, which B5's acceptance test takes.

**To try things by hand:**
- the site (Query, DataCube, Studio) is served from this worktree on 127.0.0.1:8200 and :8210 (`runs/serve-site.sh`,
  made by `bazel run --script_path=runs/serve-site.sh //site:serve`);
- a Python with the wheel installed is `runs/ll-venv/bin/python` (rebuild with `bazel build //python:wheel`, then
  `pip install --force-reinstall --no-deps` the wheel).

## 4. Logic that exists twice today (measured 2026-10-12 and 2026-10-13)

| Logic | Where it is now | The move | Step |
|---|---|---|---|
| Saved-query rules | Java `core/.../server/SavedQueries.java` (files written directly, no storage interface); a TypeScript port `query-store/src/local-server.ts` ("rule for rule, word for word") over IndexedDB | The Java over a records interface, answered by the module, the native library and the server; the port deleted | B5 |
| legend-engine's execute API in the tab | `engine-client/src/legend/browser-engine.ts` binds parameters, finds a graph fetch by its `serialize` call, writes the TDS answer. It already differs from Java's `PureV1Api`: its "Missing external parameter(s)" names no types, its builder columns carry no `relationalType`, its activity carries no `comment` | The tab asks the module's Java for the API part; its TypeScript executor still runs the SQL and types the rows | C1 |
| The warehouse's SQL API client | TypeScript `engine-client/src/warehouse.ts`; the Java binding `warehouse/.../sqlapi/SqlApiBinding.java` (no I/O, meant to be the only one, in neither the module nor the native library) | The binding in both; TypeScript and Python only make the HTTP calls it describes | C3 |
| Saved-page rules | TypeScript `datacube/src/cube-store.ts` `RuleStore`, records per host | Java beside saved queries | B6 |
| Two planner protocols | DataCube's (ten kinds: `datacube/src/planner-protocol.ts`, Python's `_planner.py`) and Query/Studio's (four: `engine-client/src/legend/planner-answer.ts`). Their `plan` kinds clash (held and Pure text, against unheld and lambda JSON) | One protocol | C2 |
| Small copies | the site's file types (`python/legend_lite/engine.py` `_SITE_TYPES`, `WarehouseServer.java` `SITE_TYPES`); a saved query's context (DataCube's `saved-queries.ts` `contextOf`, Query's `persist.ts` `contextOf`) | One each | C4 |
| Into a project (Create a Class… and the rest) | `datacube/src/page-model.ts` (`PageModel.ensure/state/save`, `saveFresh`, `projectText`): generic SDLC glue living in DataCube | Twin libraries, TypeScript and Python | B4 |

## 5. The phases

### L. legend-lite as a drop-in legend-engine (the Studio / SDLC / Depot session's)

**Goal:** each UI against a real legend-engine and against a legend-lite server over the same APIs, the same answers.

**The steps:**
1. **The check.** Each UI's flows run against legend-engine 4.145.0 (the pinned reference, with legend-sdlc and the
   Depot for Studio) and against a legend-lite server.
   - The flows: Query (a data space, then a query built, run and saved), Studio (a project opened, edited, compiled,
     tested, reviewed) and DataCube (a source on the engine plane).
   - Every request and answer is recorded, and each call is the same, different, or missing on one side.
   - The saved-query API comes first.
2. **The guard.** In legacy mode a UI sends nothing legend-engine does not answer, and answers nothing in the tab that
   legend-engine would.
   - Measured 2026-10-13: Query's server mode keeps grammar and a query's types in the tab (`RoutedEngine`,
     `engine-client/src/legend/engine.ts`), so that is a known gap.
3. **The fixes.** Each difference is fixed on legend-lite's side: its compatibility plan mode, and the answer's exact
   value and text (PARK-24's rule). The differential becomes a test in a lane.
4. **The rest of the API surface**, beside the compiler line's parity work (PCT, the corpora).

**DataCube's slice is this line's.** The Studio session will ask for its hooks: likely a page started against a given
engine URL, and a flow run through the page's own actions rather than clicks.

### B. One implementation per store, storage per host

**B1 to B3: built and landed (PR #36).**
- **B1, the model actions over the host's services.** `ModelWriting` (`datacube/src/app/services.ts`), given by both
  hosts: the place, the writer, a project's rows. A grid's rows are read through its PlanAndRun (`rowsOf`).
- **B2, the page's settings kept by Python.** `settings.json`, in `ll.home()/settings`. Chosen over a fixed port: two
  notebooks at once would still have two addresses.
- **B3, Python's model home.** `native/src/main/java/com/legend/nativelib/ModelHome.java` (`lite_sdlc_handle`,
  `lite_depot_handle`) over `GitStorage` in `ll.home()/projects`.
  - Python serves `/sdlc/api` and `/depot/api` under its token and a lock held across processes.
  - Python's pages get a model home service, with no Studio link until E.
  - `Frames.model_for` holds option (c).
  - Proof: the SDLC's conformance suite, 28 tests, passes against Python (`//datacube:python_model_home_test`), and
    Create a Class… runs on Python's page (`python_engine_test`).

#### B4. Into a project: twin libraries, TypeScript and Python

**Goal.** Creating a class, an association or a data space in any project is a generic Legend capability, a library in
each client language. DataCube's windows are one caller; Python's `grid.create_class()`, `page.create_association()`
and `page.create_data_space()` another; Query, Studio and scripts more.

**Decided** (the user, 2026-10-13): "this is not datacube code its generic legend capability"; the TypeScript and
Python libraries are twins, the same in every column.

**The design, three layers:**
1. **The writer, Java, shared already:** legend-lite's `createClass`, `createAssociation` and `createDataSpace` routes
   (`core/.../server/ModelChanges.java`; one-app design §15.6a). Given a project's text and a request, they answer
   every file to write. The browser's module answers them (`pureV1`), as do Python's native library (`lite_pure_v1`)
   and legend-lite's server.
2. **Into a project: the twin libraries.** Each has the same three functions and the same parameters:
   - `createClass({ sdlc, writer, project, workspace, package, request })`, and its association and data-space twins.
   - `project` is a project id, or a name to make one (its artifact id by the SDLC's rule, a taken name numbered).
   - **The SDLC:** the host's own model home (the browser's in-page SDLC; Python's in process) or any SDLC server, a
     real legend-sdlc included.
   - **The writer:** the host's own (the browser's module; Python's native library, in process) or a legend-lite
     server.
   - **What each does:** make the project and the workspace when they are not yet; read the project's files and join
     them as one model text (`projectText`'s rule); call the writer; save its changes as one revision; made again once
     over a newer revision (`saveFresh`'s rule). It answers the revision, the classes made, and the writer's notes.
   - **Where:** the TypeScript library is a package of its own beside `sdlc-client` and `depot-client` (a js_library;
     the Studio line's to review); the Python one is `legend_lite/model.py`.
   - **One corpus holds them alike:** a folder of cases (a request, the project's files before, the changes and files
     after), run by a TypeScript test and a Python test against the host's own SDLC and writer.
3. **DataCube's own part:** a grid as the request (its finest-level relation `classLambda`, its sample rows read
   through its PlanAndRun, its table `storeTableOf`), the windows, and the page's record of which grid is which class.
   - `PageModelRef` keeps `project`, `workspace`, `package`, `tables` and `classes`. **Owed:** the revision the page is
     pinned to (model design §6).
   - `page-model.ts` keeps only that record; its project and workspace handling, `saveFresh` and `projectText` move
     to the TypeScript library.

**Python's calls:**
- `grid.create_class(name=None, package=None, columns=None)`:
  - `columns` maps a column to `'property'`, `'key'` or `'omit'`, as the window's choices do;
  - its request is built as the window builds it: the relation from DataCube's headless `gridRelation`, the sample
    through Python's executor;
  - it records the class on the page (`page.py`'s model record, the twin of `PageModelRef`).
- `page.create_association(...)` and `page.create_data_space(...)` take the window's parameters.
- The parked branch `datacube/python-model-parked` (606298656) has their first shapes and 13 tests. Its held model and
  page document version 5 are withdrawn; its tests of a page "whose classes are in a model home" invert.

**Built 2026-10-13 (branch `datacube/python-parity`):**
- `model-changes/`, the TypeScript library: `createClass`, `createAssociation`, `createDataSpace` (`{ sdlc, writer,
  project, workspace, package, ... }`, `project` an id or a `NewProject` to make, `made` told its id at once), the
  project's handling (`makeProject`, `ensureWorkspace`, `projectState`, `saveFresh`), the writer's wire moved out of
  DataCube (`wire.ts`, `writerBodies`: each route's body as every writer sends it), and `serverWriter` for a legend-lite
  server. `python/legend_lite/model.py` is its twin: the same functions, `local_sdlc`/`server_sdlc`,
  `local_writer`/`server_writer`.
- **Held alike by transcripts:** `model-changes/test/cases` records every call a create makes, to the SDLC and to the
  writer, with its exact body and answer, recorded by Python against the native library from a real page's grids
  (`MODEL_CHANGES_RECORD=1 bazel run //python:model_test`); `//python:model_test` and `//model-changes:replay_test`
  each replay them, byte for byte, including a stale save made again once (a real 409).
- DataCube: `page-model.ts` keeps only the page's record (`PageModel.into`, `keepTable`, `keepClass`); the windows call
  the library. Its headless code gives Python a grid as a class (`gridClass`: the relation, the table, the sample's
  query, the names the window offers) and the duplicate-key query (`gridDuplicate`).
- Python: `grid.create_class(name, package=, columns=, documentation=)`, `page.create_association(a, b, name=)` (an
  end's how-many preset from whether its key is unique over its grid's rows, as the window presets it),
  `page.create_data_space(name, title=, description=, associations=)`, and `page.model`.
- Still owed: the revision the page is pinned to (model design §6).
- **Reviewed** (the audit, the Studio / SDLC / Depot line, the Compiler Rewrite line):
  - the SDLC is one with lite's text routes (the page's, the model home, sdlc-server). **A real legend-sdlc is owed:**
    it answers neither `…/pure` nor `…/pureChanges`, its project ids are its own, and it may answer a stale save 500;
    reading `…/revisions/{r}/entities` and saving with `entityChanges` waits on the Studio line's leg 8 (the JSON hop);
    **decided 2026-10-15 (the user):** the two orchestrations, model-changes (TypeScript) and model.py (Python), become
    one Java operation in the SDLC module, "edit a project" -- approved on the condition that it works with a real
    legend-sdlc too (its entities read and `entityChanges` saved, a project found by its coordinates, a stale save
    answered as legend-sdlc answers it), with lite's text routes kept on lite's own SDLC; the Studio / SDLC line's PR,
    after neema2/legend-lite#47, done when the same cases pass on lite's SDLC and a real legend-sdlc's file backend;
  - a workspace whose making is refused is read again (lite answers 500 "Branch already exists" once it has commits),
    the refusal said only when it is still not there; a save that changes nothing (the SDLC's 204) keeps the revision
    read, sdlc-client's `performPureChanges` and `performChanges` answering `Revision | undefined`;
  - `projectText` is sdlc-client's (`src/project-text.ts`), the one rule the Studio line moves its own readers onto;
  - Python asks DataCube for the page's names and project (headless `pageModelNames`), a grid that cannot be a class
    refused in DataCube's words, an association's how-many read as the window reads it (the class's relation function
    over the project: headless `classDuplicate`), its samples written as the window writes them, a Live frame read
    once; `project=` names the project a page's first class makes.
- **How the two are held alike, as built:** recorded transcripts, replayed against a stub SDLC and a stub writer over
  `writerBodies` (the function the module's writer and a server's both use); the race and 204 answers the real SDLC
  cannot record are unit tests in both languages. The in-page SDLC and the module's writer are not run against the
  cases (changed from this plan's first draft: a request, the files before and after, run against each host's own);
  DataCube's own windows are proved by `create_class_test` and `python_engine_test`.

**Files.** The new TypeScript package and its BUILD, `datacube/src/{create-class,model-actions,page-model}.ts`,
`python/legend_lite/{model.py,page.py}`, the corpus folder and its two tests, and `gates/BUILD.bazel` for the tests'
lanes.

**Proof:**
- the corpus in both languages;
- `create_class_test`;
- a `python_engine_test` check that a class made from Python appears on the page;
- `headless_test`.

**Reviews:**
- the Studio line, for the new package beside its clients;
- Compiler Rewrite, for the BUILD files;
- an audit.

#### B5. Saved queries, one implementation

**Goal.** One set of saved-query rules: Java `SavedQueries` everywhere, only the storage per host. The TypeScript port
goes, and Python's pages get saved queries.

**The design:**
1. **A records interface** under `SavedQueries.java`, shaped as `query-store/src/records.ts` is (a query's versions;
   put a query's versions; all). Today's one-file-per-query layout is its file implementation.
2. **One router.** Move the `/api/pure/v1/query` routes out of `LegendHttpServer.java` into `PureV1Api.route`, so the
   module and the native library answer them as the server does.
3. **The browser:** the module answers them over records the page keeps in IndexedDB. Write behind, as the SDLC does
   (`sdlc-client/src/wasm-server.ts`): the Java keeps them in memory, the page persists the changes. Delete
   `query-store/src/local-server.ts`.
4. **Python:** Python's engine serves the routes over the file layout in `ll.home()/queries`, under the folder's lock.
   - `_API`'s prefix sends every `/api/pure/v1/*` path to `compiler.pure_v1`, so the saved-query routes must reach the
     store with its records.
   - `python-host.ts` gives a `queries` service.
   - DataCube's saved-query source on Python needs what it takes from `services.engine.local` today (`local.elements`,
     `local.another`) as services. That is B1's sweep, finished here.

**Proof:**
- `query-store/test/conformance.ts` against the module, the native library and the server;
- **the acceptance test is L1's saved-query differential** (legend-engine against legend-lite).

**Reviews.** Announce in IN_FLIGHT: a core edit (`SavedQueries.java`, `PureV1Api`), and `query-store/`, Studio's. Then
the Studio line and Compiler Rewrite, and an audit.

**Built 2026-10-13 (branch `datacube/python-parity`; announced in IN_FLIGHT on main, 89b25e667):**
- The rules moved to `core/src/main/java/com/legend/queries/` (`//core:saved_queries`, over `//base` and `//json`
  only) over `Records`: `FileRecords` (a directory, one JSON file per query, the server's layout) and `MemoryRecords`
  (the page's records handed in). Changed from this plan's first draft: the routes stay `SavedQueries.answer`'s (the
  server's, the module's and the native library's one entry), not `PureV1Api.route`'s, since they take a method and a
  user the pure/v1 router does not; and the module keeps nothing between calls (each call is handed every record),
  so two tabs see each other's queries without the SDLC's stamps.
- The page: a TeaVM module of its own, `//query-store:page` (230 KB; `query-store/java/.../QueriesPage.java`, one
  export), served by DataCube and Query at `vendor/queries/`; `query-store/src/wasm-server.ts` replaces the
  TypeScript port (`local-server.ts`, deleted).
- Python: the native library's `lite_saved_queries` (`native/.../SavedQueriesHome.java`) over `ll.home()/queries`,
  served at `/api/pure/v1/query` under the folder's lock (GET, POST, PUT, DELETE); `services.json` says where.
- Also changed from the draft: `python-host.ts` gives no `queries` service. What reads it opens a saved query with the
  page's own compiler and database (PARK-33), so Python's page offers no Saved queries section yet; the Goal is met as
  far as Python serving the store.
- Reviewed (Studio, Compiler Rewrite, an audit), and fixed on the branch:
  - A body or records of the wrong shape: the rules check each field's kind before reading it (the engine's 500
    `IllegalArgumentException: ...` on every host). In the page's WebAssembly a failed cast stops the module, where
    the JVM throws; the adapter also answers anything that escapes the module as that 500.
  - One order on every host: `Records.all()` is in id order (`FileRecords` had listed by file name, so `q-2` came
    before `q` on the server and Python, after it in the page). Search order among queries made in the same
    millisecond is now by id on the server too (it was by file name: benign, now the page's order).
  - The page keeps each query's versions as the JSON text the rules wrote (`records.ts` layout 3, layouts 1 and 2
    moved over; `//query:verify_test` checks both), so a number keeps its spelling as in the server's file; and each
    call is one IndexedDB transaction, so two tabs never interleave a read and a write.
  - The module is fetched at the store's first call in both apps (Query had loaded it at boot).
- Measured: one page call over 1,000 queries of 5 versions each (3.2 MB of records) takes about 55 ms in the module
  (median; IndexedDB's read not included; `docs/one-platform/evidence/b5-page-call.mjs`). It grows with versions, since
  every update keeps one; fine for one browser's store.
- Proof: the one suite (`query-store/test/conformance.ts`) passes on the module (`//query-store:wasm_test`), the server
  (`:lite_test`) and Python (`//datacube:python_saved_queries_test`, new).
- Owed: a saved query opened as a grid on Python's page (PARK-33: a mapping planned on Python, a project's data made
  without the page's SQL), with E. **Acceptance is pending L1** (the saved-query differential, legend-engine against
  legend-lite: the Studio line's, waiting on the user's OK for phase L and on MongoDB).

#### B6. Saved pages, one implementation

`cube-store.ts`'s `RuleStore` rules move to Java beside `SavedQueries`, records per host as now (IndexedDB in the tab,
`ll.home()/pages` on Python).
- **Why:** a server can then keep pages too, under the same rules. Today `SavedQueries.java` answers `/dataCube` 404.
- **Measure first:** the rules trust a configured user name, so a shared server needs sign-in first (G10). Build the
  rules to take the user from the host.

**Built 2026-10-13 (branch `datacube/python-parity`; announced in IN_FLIGHT on main, 5aa3dd817):**
- The rules are `core/src/main/java/com/legend/queries/SavedCubes.java`, in `//core:saved_queries` beside the saved
  queries': RuleStore's (the client's id, the owner forced, `lastOpenAt` stamped, a search's light records -- the
  caller's first, then the sort, then the limit), with the user the host's to say: the page's "this browser", Python's
  "this user", the server's anonymous caller. Each call takes the request's query string, as the saved queries' do.
- Kept as rows, the content its exact JSON text, as before: `FileCubeRecords` (one `<id>.json` each: Python's
  `ll.home()/pages`, the layout its pages were already in, so they open as they were; the server's
  `--query-store DIR/dataCube`) and `MemoryCubeRecords` (the page's IndexedDB rows, handed in). A row keeps upstream's
  `DataCubeQuery` fields only (id, name, description, content, owner, the three times), so every host keeps a page
  exactly (the page's rows are structured clones: a number past 2^53 outside the content would not be kept so). A row
  that is not a page -- a file cut short, another id than its name, a row of another shape -- is left out on every
  host: not listed, not found, every other page opening, and a page saved under its id replacing it.
- Where the rules differ from RuleStore's, each on every host (reviewed: an audit of bf0efe899):
  - a page's id is one a page makes (letters, digits, "-", "_"; no Windows device name), refused 400 where only
    Python's folder refused it; an id or a name that is not a string is refused 400 (RuleStore took any truthy value);
  - a content of `""`, `0` or `false` is kept (RuleStore refused a falsy content; only a missing or null one is now:
    "Query content is missing");
  - a limit below 1 answers none (RuleStore's `slice(0, -1)` dropped the last), one above 100 the cap of 100;
  - a body or rows of the wrong shape are the engine's 500, checked, not cast (B5's lesson: the page's WebAssembly has
    no ClassCastException); its fields in the engine's order on every host;
  - the record's fields are in the order the client wrote them (its protocol JSON sorts the keys), the stamps last.
- Not served yet, answered as paths the store has not (the Studio line's review): upstream's `dataCube/batch`,
  `dataCube/events` and `dataCube/stats`, the routes that take a query string. L1's differential lists them expected.
- The page: `QueriesPage.cubes`, an export of the saved queries' module (one module for both stores, fetched once, at
  the first call of either); `datacube/src/cube-store.ts` is now the one client (`CubeStoreClient`, the content read
  exactly, the times plain numbers), the page's adapter (`wasmCubeServer`) and its rows, each call one IndexedDB
  transaction (as B5's). `RuleStore` and `HostRecords` are gone; the IndexedDB layout is unchanged.
- Python: `/api/pure/v1/query/dataCube` answered by the native library's `lite_saved_cubes` under the folder's lock,
  behind the engine's token (`/saved-pages.json`, the records' four calls, gone); `page.save()` (D) keeps a page there.
  It differs from the tab's Save in two ways, recorded: saved over a copy deleted meanwhile, it saves it again (the
  tab's Save answers the store's 404); and the page object knows only its own saves, so a page saved in the tab and
  then with `page.save()` is two saved pages.
- Proof: one suite (`datacube/test/saved-pages/conformance.ts`) on the page (`//datacube:saved_pages_page_test`), the
  server (`:saved_pages_server_test`) and Python (`:python_saved_pages_test`, the warehouse lane); the page's own cases
  (two users over one store, mine first under a sort, a row that is not a page, a field not upstream's, the module
  failing); Python's folder's own cases (`python/tests/test_engine.py` SavedPagesKept: a page kept before opens as it
  was); `python_engine_test` saves on Python's page and opens it again, and opens a page saved from Python. Measured
  (the audit): a call over 200 pages of 100 kB (20 MB of rows) takes about 210 ms in the module, 50 of 10 kB 6 ms.

#### B7. A remote git

A project's git store pushed to and pulled from a remote (GitHub, a team's), as git does: the local store stays the
`Storage`. It is what F4's promotion builds on.

### C. One engine path

#### C1. The tab's execute API from the module's Java

**Goal.** The tab answers legend-engine's execute API with the Java Python and the server use. Its TypeScript executor
still runs the SQL and types the rows.

**The design:**
1. **A `TabExports.executePlan` export,** and a planner kind for it. Today only the native library exports
   `Boundary.executePlan` (`lite_execute_plan`).
2. **The Java's graph-fetch plan half:** `PureV1Api.arrowPlan` refuses a graph fetch today.
3. **`BrowserEngine.execute`:** asks for the plan and its answer's shape, runs the SQL on the tab's DuckDB, and fills
   the answer in. The TypeScript `bindParameters`, graph-fetch detection and TDS shaping go.
4. **`WasmGrammar.returnType` and `generatePlan`** over its `#pureV1`. The module already routes `lambdaReturnType` and
   `generatePlan` (`PureV1Api.route`). Query's SQL button (`query/src/app/run.ts:76-83`) then works with no server.

**Proof.** Query's tests, and a differential of the tab's answers against Python's for the same executes (builder
columns with `relationalType`, the activity's `comment`, the "Missing external parameter(s)" message).

**Reviews.** Plan Gen (the boundary), Compiler Rewrite, Studio (Query), an audit.

**Built 2026-10-13 (branch `datacube/python-parity`; announced in IN_FLIGHT on main, 42ea629f0):**
- The plan half: `PureV1Api.jsonPlan(body, models)`, beside `arrowPlan` and planning as it does (one step: the request
  read, its parameters bound by `boundParameters`, planned over the host's models only), answers `run`, the SQL the
  host runs -- a relation's JSON wire (`TypedQuery.wireSql`: the rows as legend-lite's server's database writes them,
  `Execution.executeWire`'s SQL, one value), a graph fetch's own SQL (its objects' array) -- and the JSON answer's every
  other part: the plan's `sql` (the activity's), the builder, the activities and the columns, as `execute` writes them.
  `PureV1Api.jsonAnswer(plan, wire)` then arranges the one value the host's database wrote with `execute`'s own
  writers (`tdsText`, `graphResult`, shared with the server's answer), so the tab's answer is the server's text: the
  database renders every value, Java arranges them, the tab writes neither. (Changed from this plan's first build,
  60afa7006, at the Plan Gen line's review: there the tab's executor typed and wrote the rows, a second owner of the
  answer's text beside PARK-24's output layer.) `Boundary.executeJsonPlan`/`executeJsonAnswer`, the exports
  `TabExports.executeJsonPlanOrError`/`executeJsonAnswerOrError` (Folded.http), the planner kinds `executePlan` and
  `executeAnswer` (engine-client's planner-answer.ts).
- The tab: `BrowserEngine.execute` asks the plan, runs its `run` SQL on its engine, and hands the one value back to be
  arranged -- its own `bindParameters`, graph-fetch detection, row typing, TDS shaping and enumeration list gone (the
  constructor takes no `isEnumeration`: Query and Studio no longer build one). The tab passes the input's own model as
  the one it runs (its data is the page's), so the planner's model check is met there; a model not given as text is
  the planner's to refuse, in the server's words. `WasmGrammar.returnType` and `generatePlan` are the planner's
  pure/v1, as the server routes them, so the tab answers both (Query's SQL button already showed the tab's plan
  through its planner; unchanged).
- The session the SQL assumes (the Plan Gen, Compiler Rewrite and Studio lines' reviews of bf058c233 and b44780c8e):
  every tab's DuckDB is set up by one function, engine-client's `setUpSession(engine, setup)` -- the dialect's own setup
  (`Boundary.sessionSetup`: a DuckDB session in UTC, as the server sets up every session it opens). `startDuckDbInTab`
  runs it before it hands the connection out (Query's and Studio's: their test data, their grids' SQL and their
  executes all after it). DataCube's browser services run it before any planner SQL -- after its demo's own rows (the
  generated sample, a mounted remote view, written while the planner loads, neither of them zoned: a view's zone is
  applied when it is queried). An engine on a server (the warehouse) runs on that server's sessions. Left alone, DuckDB-WASM takes the browser's zone, and a value of the
  session's zone (`now()->hour()`) answered otherwise than the server.
- A value's answer (a scalar or a list: neither a relation nor a graph fetch) is refused by both plan halves, as the
  server's execute refuses it ("a SCALAR result's serialization is unprobed"; the audit of b44780c8e).
- Python answers legend-engine's JSON execute too (it answered ARROW_IPC only): the same two halves as the tab
  (`compiler.execute_json_plan`, `execute_json_answer`; its DuckDB runs `run`), so a JSON client -- Query served from
  Python (E) -- reads the server's answer text.
- What the tab answers differently now, as the server does (recorded, the audit of 60afa7006): a query that does not
  compile or parse, or names no runtime, is refused 500 with errorType COMPILATION (it was 400 through the planner's
  own plan); and an execute over a column whose relational spelling was never measured against legend-engine (a
  Variant's) is refused, where the tab's own typing ran it -- PARK-34.
- Proof: `PureV1ApiTest.e8_jsonPlanThenJsonAnswer_*` -- the plan's `run` SQL on the server's own test database, its one
  value arranged by `jsonAnswer`, is the server's `execute` answer byte for byte (each answer's trace id its own), and a
  graph fetch's plan and answer (one object bare). `//datacube:python_execute_test` (the warehouse lane) -- an execute
  over Python's frame answered in the tab (the planner in-process, DuckDB-WASM in Node, the same rows, its session
  started in Kathmandu's zone, +05:45, as a browser there would start it) and by Python's engine: the answer's TEXT
  byte for byte against Python's JSON answer (each trace id its own), its columns a whole Float, a Decimal, a DateTime
  and `now()->hour()` (the session's zone, UTC's on both; the test fails with the setup left out, measured); the same
  builder as Python's ARROW_IPC answer; a parameter bound alike; the missing parameter refused in the same words, as
  Python's JSON refuses it; the return type and the execution plan the same. (The frame's own columns are strings,
  integers and floats: an enumeration's builder type is the server's by construction, the same `tdsBuilder`.)
  Query's `verify_test` (in the browser: a run, a filter, a parameter, a graph fetch) and Studio's tests pass.
- Changed from this plan: the differential is the tab against Python, as written; legend-lite's server would run the
  same Java but seeds only a connection's declared test data, not a page's Data, so it cannot hold the same rows (the
  answer's text is held to the server's by the Java test instead).

#### C2. One planner protocol

DataCube's ten kinds are the superset, but for Query's `warm` and its unheld `plan` (model text, lambda JSON:
`planJsonOrError`).
- One protocol in `engine-client`, its kinds named so that none clashes: DataCube's held `plan` against Query's unheld
  one.
- Answered by one worker in the tab and by `legend_lite/_planner.py`.
- DataCube, Query, Studio and Python use it.

**Built 2026-10-13 (branch `datacube/python-parity`):**
- The protocol is `engine-client/src/legend/planner-protocol.ts` (moved from DataCube's `planner-protocol.ts`, deleted):
  fourteen kinds -- DataCube's ten, Query's unheld plan as `planModel` (its old `plan`, which clashed with DataCube's held
  one), `warm`, and C1's `executePlan` and `executeAnswer` -- with `REQUEST_KINDS`, `Request`, `Response` and
  `PlannerWorker`. One answer (`planner-answer.ts`: each kind the module's export of the same name) and one worker
  (`planner-worker.ts`, engine-client's bundle): DataCube's own worker (`datacube/src/planner-worker.ts`) is gone, and
  DataCube serves engine-client's bundle as its `./planner-worker.js`, as Query and Studio do. DataCube's
  `LitePlanner`, its headless host and Python's page host, and Query's and Studio's `WasmGrammar` send it.
- Python answers every kind (`legend_lite/_planner.py` KINDS, the start-up check `_headless.check_interface` holding
  the two lists alike): the native library gains `lite_execute_json_plan`, `lite_execute_json_answer` and
  `lite_warm_model`, each delegating to the Boundary function the tab's export delegates to. (A page on Python asked
  `planner.json` the kinds its planner sent, `engine.py` `_PAGE_KINDS` -- retired 2026-10-15, PR neema2/legend-lite#47:
  a page asks Python on legend-engine's own routes, `pure/v1`, and the host's compute, `plan-and-run.json` and its dry
  run `plan.json`, over the models Python serves only; the one-app design's §15.10.)
- Every kind answers folded alike on both hosts: the tab's `release` and `warm` through new exports
  (`TabExports.releaseModelOrError`, `warmModelOrError`, as the native library's entries fold them; the Plan Gen line's
  review). And one mapping of kinds to exports wherever the module runs: DataCube's LitePlanner, with no worker,
  answers in-thread through the same `planner-answer.ts` (its own copy of the mapping gone).
- Proof: every app's suites on the one worker (DataCube's, Query's and Studio's browser tests included), the headless
  lane (its start-up check), `python_execute_test`.

#### C3. Warehouse tables on Python's pages

**Goal.** A warehouse table opens on Python's page as in the tab. Live runs on the warehouse, and Snap lands in
Python's DuckDB.

**The design:**
1. **One binding in every build.** `warehouse/.../sqlapi/SqlApiBinding.java` (no I/O: it builds the HTTP calls and
   reads their results; //base and //json only; it already compiles to WebAssembly in `//warehouse:sqlapi_wasm`) goes
   into the tab's module and the native library.
   - The TypeScript client (`engine-client/src/warehouse.ts`) and a Python twin (`legend_lite`, over `urllib`: no new
     dependency) only make the calls it describes: sign-in by password or launch key, objects, statements, chunks,
     cancel, refresh, history.
   - One shared corpus of recorded calls holds the two alike.
2. **Python plans the warehouse's table** (its model by `lite_table_model`) and runs Live through the binding, the rows
   coming back as Arrow chunks. The page sends Pure, never SQL. The warehouse's credentials are those of the person
   running Python (the user, 2026-10-12).
3. **Snap** loads the chunks into the frames' DuckDB. The table is then served like a page's file.
4. **The page:** `pythonServices` gives a `warehouse` service, and `datacube/src/app/sources.ts` stops gating the
   warehouse on `services.engine.local`.

**Reviews.** The warehouse's BUILD and the native library's are Bazel (Compiler Rewrite); `engine-client/` is shared;
an audit.

**The steps, settled 2026-10-13 (by this line, the user away: listed for them at landing).**
1. **The binding's one surface.** `warehouse/.../sqlapi/SqlApiCalls.java`, beside `NativeBinding` and over it, on
   //base and //json only (the warehouse's closure: Compiler Rewrite):
   - `call(op, args)`: the next HTTP call, as `{method, path, headers, body}`, for `login`, `loginKey`, `refresh`,
     `submit`, `fetchChunk`, `cancel`, `closeStatement`, `history` and `objects` (every catalog's).
   - `read(op, status, body)`: what an answer says. A token; the objects; the history; and for `submit` (and each
     poll), the next step: poll (its call), done (the result's counts) or failed. A refusal is `{"error": {code,
     message}}`, marked `expired` when a call that carried a token was answered 401: what the drivers raise as an
     expired session, so no driver reads a status itself.
   - What stays the driver's: the HTTP, the Arrow bytes (a chunk's body is handed back as it came; only a failed
     chunk's answer is read), the clock, aborting.
   - Moved into it from the TypeScript: the key sign-in (`ApiJson.loginKey`), the catalog's structured columns
     (`CatalogColumn`'s logical type, precision, scale and not-null, which the server writes and the Java reader
     dropped), an interactive statement's settings (a 10-second poll, 100,000 rows a chunk, 5 minutes), and each
     read's checks by `instanceof` (WebAssembly traps on a failed cast).
2. **In every build.** The native library: `lite_warehouse_call`, `lite_warehouse_read` (`native/.../WarehouseCalls`).
   The tab: `//warehouse:sqlapi_page`, its own small module (as the saved queries' is), at `vendor/warehouse/` in
   DataCube's and Query's sites; `//warehouse:sqlapi_wasm`'s compile check stays.
3. **One corpus.** `warehouse/src/test/resources/sqlapi-corpus.json`: recorded exchanges (sign-in by password, by
   key, refused; the objects; a statement polled, its Arrow chunks read, closed; a failed one; a token expired
   mid-statement; a refresh; the history). The Java test holds the binding to it. Each driver replays it, the
   corpus answering each call, and must make exactly the recorded calls.
4. **The drivers.** `engine-client/src/warehouse.ts` keeps its API (`signIn`, `signInWithKey`, `listObjects`,
   `connect`, `WarehouseEngine`), each now taking the loaded binding; its hand spelling of the API goes.
   `legend_lite/warehouse.py` is its twin over `urllib` and the library.
5. **Python's page.**
   - `pythonServices` gives a `warehouse` service: Python signs in (the person running Python is the person at the
     page) and lists the objects; the token never reaches the page.
   - Opening a table: Python writes its model (`lite_table_model`, the catalog's database type and DuckDB's for
     the snap) and holds it. A plan over it runs where its runtime says: the live runtime's on the warehouse,
     through the binding, as Python's session, its Arrow chunks the rows; the snap runtime's on Python's DuckDB.
   - Snap: the page sends the pull query (Pure); Python plans it with the live runtime, runs it on the warehouse,
     and loads the chunks into its DuckDB as the table. Release drops it. The cube's Snap gains a host's kind beside
     the page's own (`SnapManager` over the page's engines): the same states, the copy planned and run by Python.
   - `sources.ts` opens a warehouse table on Python's page through that service, not through `services.engine.local`.

**Built 2026-10-13, steps 1 to 4 (branch `datacube/python-parity`):**
- **The surface.** `SqlApiCalls` (`call`, `read`) over `NativeBinding`, which gains `loginWithKey` and `poll`.
  `ApiJson`'s answer readers check each value's kind (`notTheApis`): an answer that is not the API's is a refusal,
  `INTERNAL`, never a trap. `CatalogColumn` carries the catalog's structured fields, and `ApiJson.objects` writes
  them. `SqlApiCallsTest` holds the binding to the corpus, and to its refusals (wrong arguments, values of the wrong
  kind, a proxy's page, a 401 that is an expired session only where a token was carried).
- **Changed from ruling 7's words** (docs/SERVER_PROGRAM_2026_09_26.md §2c: "into the WebAssembly module beside the
  planner"): the binding is its own module, `//warehouse:sqlapi_page` (110 KB, its runtime 34 KB), at
  `vendor/warehouse/` in DataCube's and Query's sites. The planner runs in a worker, so every HTTP step of a statement
  would be a round trip to it, and a page reading a warehouse would need the planner started. The ruling's point --
  the same Java in the tab and on the server -- stands.
- **The native library:** `WarehouseCalls` (`lite_warehouse_call`, `lite_warehouse_read`), on `//warehouse:sqlapi`.
  The warehouse's closure is unchanged: the native library reaches the binding, never the reverse.
- **The corpus:** thirteen scenarios (`//warehouse:sqlapi_corpus`, a copy in the output tree, as a node test takes
  another package's file only from there), the audit's four added: a statement with nothing to read, a proxy's page in
  place of a poll's answer, a chunk refused, and a read that stops after its first chunk; the expired read records the
  cancel it sends, and the refresh the next call carrying the new token. `//engine-client:warehouse_corpus_test` replays it through the tab's driver over the
  module, and `python/tests/test_warehouse.py` (in `//python:engine_test`) through Python's over the native library;
  each makes exactly the recorded calls.
- **The drivers.** `engine-client/src/warehouse.ts`: `WarehouseBinding`, `loadWarehouseBinding`; `signIn`,
  `signInWithKey`, `listObjects` and `connect` take the binding first, and `WarehouseEngine` takes it in its
  constructor. Its hand spelling of the API (paths, bodies, the poll's and the chunk's settings, the reading of a
  status) is gone. DataCube's browser services load the binding at the first sign-in; Query's warehouse plane
  (`query/demo/main.ts`) before its sign-in. `legend_lite/warehouse.py` is the twin (`sign_in`, `sign_in_with_key`,
  `list_objects`, `connect`, `Warehouse`), refreshing its token in its last fifth at the next call (the tab refreshes
  at 80% on a timer: one rule, each host's clock).
- `datacube/test/warehouse-session.test.ts`, which runs headless too (no WebAssembly), hands the engine a binding it
  never calls; the HTTP half loads the module.
- **What the audit changed:**
  - A read that ends early leaves its statement as the tab's rule says, in both drivers: a statement still running is
    cancelled, a result done is closed. A refused close or cancel is ignored: the rows were read, or the read failed.
  - One rule for a refusal's words (`NativeBinding.failure`, which `SqlApiCalls` uses): a non-API body is cut at 200
    characters.
  - Python's driver follows no redirect, which would carry the token elsewhere. A broken connection is said as one.
  - A failed step is an answer, not a refusal: it carries an `error` and a `step`.

**Built 2026-10-13, step 5 Live (branch `datacube/python-parity`):**
- **Python.** `services.json` offers `warehouse`. `POST /warehouse.json`:
  - `connect` signs in as the person running Python (`warehouse.connect`) and answers the session with `token: ''`,
    with what they may read. The token stays in Python. A table already open from that warehouse, as the same person,
    goes on with the new token.
  - `table` reads the objects again, so the columns are as the catalog has them now and the table is one the person
    may read. It writes the model with the tab's own writer (`table_model`: read-only, its copy planned for DuckDB),
    holds it, and answers it as that writer does.
- **Refused at opening:** a table whose model text a frame or file of this Python, or a table from another warehouse
  or catalog, already holds. A plan names its table by the model's text alone.
- **Plan-and-run over that model** runs on the caller's thread, not the compiler pool. Planning and reading the plan
  go to the compiler pool, as Python's own planner's do. The SQL runs on the
  warehouse, through the binding, as the signed-in person; the server's Arrow chunks are the rows, typed by the plan
  as Python's own are. The binding's calls run on the compiler pool (`warehouse.set_library_thread`): a thread that
  calls the library is attached to it for good. A page's dry run (`plan.json`, 2026-10-15) plans over the table's
  model too.
- **The page.** `WarehouseService` gains `table`, the host's own reading; `live` is the tab's. `pythonServices` gives
  the warehouse when Python offers it. `sources.ts` opens a table through `table` when the service has it
  (`openedHostTable`): its rows the host's runner, as a frame's are. Opening needs no `services.engine.local`. A page on
  Python signs in by user and password: the launch key is the single-user warehouse's own page.
- **Measured.** `//datacube:python_engine_test` starts the native warehouse, seeds it as its owner, and checks, in
  Chromium on Python's page:
  - a wrong password refused in the warehouse's words;
  - a session without its token;
  - only the table rita may read listed;
  - the columns typed by Python's model;
  - rows read Live through Python;
  - a table not listed refused.

**Built 2026-10-13, step 5 Snap (branch `datacube/python-parity`):**
- **The cube.** `engine-client/src/snap.ts` names what a cube asks of its snaps (`Snaps`). `SnapManager` keeps the page's
  copy in its own store; `HostSnaps`, beside it, keeps the same states for a copy its host keeps (`HostSnapTarget`:
  `snap(pull)`, `release()`).
  - `CubeControllerOptions.hostSnap` is for a cube read through a runner: the copy's runner is the host's, and
    `snap()` sends the pull, the source's every column, as Pure.
  - The plane button offers Snap there and says the copy is in Python's database (`describePlane`'s `store`).
  - `hostSnap` travels where `snapTarget` does: `GridPlace`, `GridSource`, the app's options, a spawned or copied grid.
- **Python.** `POST /warehouse.json` gains two calls.
  - `snap` plans the pull with the live runtime. It counts the pull on the warehouse against the tab's limit
    (`_MAX_SNAP_ROWS`, `MAX_SNAP_ROWS`'s twin, with its words), pulls the chunks, and copies them into the frames'
    DuckDB under the table's own name, applying the model's conversions as the tab's copy does. It answers the count
    and the pull's receipt.
  - `release` drops the copy.
  - A plan-and-run against the snap runtime runs on the frames' DuckDB while the copy is there, and is refused
    otherwise. A copy never replaces a frame's, a file's, or any table this engine did not make.
- **The page.** `WarehouseService` gains `snap` and `release`. `openedHostTable` gives the grid its `hostSnap`: the
  copy's runner is the host's over the snap runtime.
- **Measured.** `datacube/test/plane.test.ts` checks a cube with a host's snap:
  - the pull goes to the host as Pure, with every column;
  - the copy's runner answers while snapped;
  - the copy is kept while live does not answer, and let go once it has.

  `//datacube:python_engine_test` checks against the native warehouse:
  - the copy refused before the snap;
  - the snap's count (3) and its receipt (as rita, 3 rows on the server);
  - the copy's rows read in Python's DuckDB;
  - refused again once released.
- **In a browser, as a person does it** (added 2026-10-13, `//datacube:python_engine_test`):
  - Python's page signs in through the Database section and opens the table.
  - It reads Live: "Live", the warehouse's receipt as rita, Snap offered into Python's database.
  - The grid's plane button snaps it: "Snapped", "a copy in Python's database", the receipt "Python's copy".
  - Clicked again, it is Live once more.

  Not driven in a browser: `signInAgain` on an expired session, held by the typecheck and the Python tests.
- **Changed from the design:** the copy is read only through the warehouse model's snap runtime, not served like a
  page's file. The refusal of a table whose model a frame or file holds cannot fire today: a frame's model is written
  under its own package with no snap runtime, so the two texts differ. It stays as the guard of that rule.

**The step 5 audit's changes (2026-10-13):**
- **No wait on the compiler's threads under the warehouse lock**, which those threads also take (`_planner`): opening a
  table holds its model before taking the lock, and lets a spare go after. A binding call already on a compiler thread
  runs in place (`_on_compiler_threads`), never queued behind itself.
- **Every `/warehouse.json` call is answered** (`_answered`), and a copy that DuckDB refuses is 422. The copy is named
  whole (`database.schema.table`), so a schema the warehouse names (`temp`, an attached database's) reaches no other
  catalog. The schema the copy made is dropped with it.
- **An expired session is 401 on every path**, and on the page the tab's own `SessionExpired`. The cube offers to sign
  in again there too: a host's table gives its grid `signInAgain` (Python signs in as the same person, and the open
  tables go on with the new token). Python's sign-in is a reader of its own (`warehouse.Warehouse`), refreshed as its
  token nears its end, for the next table opened.
- **One person per open table:** a table open as one person is refused to another, until Python restarts.
- **The copy:** one per table, as the tab's is one per tab, held by the grids that snapped it (`holders`) and dropped
  when the last lets go.
  - A grid that snaps it while it is there holds the same copy, as it is: nothing is pulled again, so the first grid's
    rows never change under it (the re-check).
  - A grid that closes lets go of it, and of one a snap in flight makes, once it is made (`HostSnaps.close`).
  - A release while a snap copies waits for it. A copy that fails leaves no table and no schema behind.
  - Not cleared: a page closed without closing its grids leaves the copy until Python exits; in Python's own DuckDB
    (in memory) it goes with the process.
- **Where the rows are, said:** a Live read's receipt is the warehouse's (`legend.receipt`: where, as whom, its
  statement and count); a read of the copy says Python's copy, and that nothing went to the warehouse. The picker
  says Snap copies into Python's database.
- **Measured:** `python/tests/test_engine.py` `WarehouseOnPython`:
  - the shapes refused, and a table only once signed in;
  - the session without its token, and a table not listed refused;
  - Live rows and their receipt;
  - one person per table;
  - the copy held by two grids and dropped by the last, with its schema;
  - the snap limit in the tab's words;
  - an expired session as 401 on plan-and-run, snap and table.

**The full local run's fixes (`//gates:local` and `//gates:warehouse` on the stack merged with main, 2026-10-13: 375
tests, 369 passed, 4 failed, and 2 skipped -- `//tools/guards:classpath_test` and `markdown_inputs_test`, incompatible
with macOS):**
- The grid page was 521 bytes over its budget: the warehouse's driver is now fetched at the first sign-in
  (`services/browser.ts`), and the expired-session error is a module of its own (`engine-client/src/
  warehouse-session.ts`) that the app reads without the driver.
- DataCube's built folder (`demo/make-dist.mjs`) now carries `vendor/queries`, `vendor/sdlc` and `vendor/warehouse`:
  the warehouse's single-user app signs in through the binding (`verify_app_test`, `live_snap_test`), and the tab's
  saved stores fetch `queries/`.
- `:planner_worker`, a copy of engine-client's worker since C2, left DataCube's `:bundles` (`//:web`), as Studio's
  did (`//tools/guards:compile_only_test`).

#### C5. A project's data where its queries run (PARK-35, PARK-33), with D5 (no SQL text in TypeScript)

Added 2026-10-13 (the user: "make sure park35 and park33 are on the list to do"). Designed with the user the same day;
D5 claimed from the dialect program (`docs/DIALECT_LEAK_CENSUS_AND_PLAN_2026_10_12.md` Phase 7, decision D5) through
`docs/IN_FLIGHT.md` (its section, items 2 and 3).

**Goal.** A project's queries run on the project's own data on every host, not only in a browser tab.
- Query and Studio on Python, and on legend-lite's server, then run a project's query. Today Python refuses, and the
  server runs on an empty in-memory DuckDB: PARK-35.
- A saved query opens as a grid on Python's page: PARK-33.

**The design (the user's, 2026-10-13).**
- **A project's own database, in the host's database.** Each project version's `###Data` rows go in a database of
  their own inside the host's database: the tab's DuckDB, Python's DuckDB, or the server's. Its schemas and tables are
  made exactly as the model declares them, so the compiler's SQL runs unchanged. On DuckDB that is an attached
  in-memory database (`ATTACH ':memory:'`), with a connection of its own pointed at it (`USE`). That is the corpus's
  `DuckWorkspaces` shape (session-topology ruling 2026-08-14: about 10% faster than a database per session).
- **Named by what it holds, and kept** (the user, 2026-10-13: "do we need any delete strategy at all?" -- no). Its
  name is the hash of the model's Databases and Data rows (`ProjectData.contentKey`), never a caller's label: an edited
  row or table is a fresh database, a class edited beside unchanged rows keeps the one it has, and a SNAPSHOT
  republished with new rows under the same coordinates is never answered with the old (Plan Gen's review). Each is kept
  for the life of the page or process, as legend-lite's server keeps its own: a tab mostly shows one project, and a new
  database is only another project's or an edited row's. So nothing a screen holds goes stale. A cap comes back only if
  a measured page or process holds too much (the case to watch: a long Studio session editing large Data rows).
- **Never beside a person's own tables.** Python's dataframes and the tab's files stay in the host's own database.
  No name can clash, and dropping a project's database is one statement.
- **Not tied to DuckDB.** "A project's own database" is the platform's concept, in Java. How a database makes one is
  that database's: its dialect's statements, in `sql/dialect/HostOperations.java`. DuckDB has them; every other
  database refuses by name until it is built (Postgres would `CREATE DATABASE`). A database that cannot hold a
  model's own schemas without renaming them (ClickHouse, SQLite) refuses. A query mixing a project's rows with a
  person's own tables is a DuckDB capability, not part of the promise, and not built here.
- **Asked for by the host's database type**, as `sessionSetup(databaseType)` is today. The tab and Python run what
  Java answers and spell no SQL themselves (D5).

**D5, built with it (claimed from the dialect program).** Every statement the tab, and Python's host, still spells
becomes a named operation answered in Java for the host's database type, refused by name by every database without
it:
- a project's database (above);
- reading a file (`upload.ts`, `model-data.ts`);
- an extension and a secret (`remote.ts`);
- the extension folder (`duckdb-tab.ts`);
- a copy's local tables and its count (`snap.ts`, `duckdb.ts`, `tab-data.ts`; the count on a warehouse catalog that
  may be Postgres, written in that catalog's own SQL);
- Python's same operations (`engine.py`, `frames.py`, `_headless.py`), the twins' rule.

Then a guard: the tab's engine runs only statements the planner produced (a type the TypeScript compiler checks), and
a test finds no SQL text left in TypeScript or in Python's host. The operations live in new files in `sql/dialect/`,
not in `DuckDb.java`, which the dialect program is restructuring; it folds them in when it consolidates. It keeps
D4 (the warehouse) and D6 (the dead code).

**The steps** (one branch, `datacube/c5-project-data`, a commit each, pushed once):
1. **The Java.** `sql/dialect/HostOperations.java` (the operations, DuckDB's, the refusal) and `setup/ProjectData.java`
   (for a compiled model and a database type: its database's name, the statements that make it, point a connection
   at it, fill it from every relational Data element through that type's dialect, and drop it). The compiled model
   lists its Data elements (`ModelContext.dataElements()`). Unit tests: DuckDB's statements, each refusal, a
   project's rows read back in a DuckDB.
2. **The planner's question.** `Boundary.projectData(model, key, databaseType)`, and the native library's twin.
   `Boundary.testDataSql` takes its dialect from the type instead of always DuckDB's.
3. **The tab.** engine-client opens a project's database on a connection of its own (the extension folder and the
   session set up on it too), and `TestData`'s swapping goes. Query, Studio and DataCube use it; two grids on two
   versions of one project stop swapping. Studio's files for a table go into the workspace's database.
4. **D5's other operations**, the Python twins, and the guard.
5. **Python.** `PureV1Api.hosted` admits a project's model; Python makes its database on its DuckDB, runs the query on
   a cursor of its own pointed at it, and drops it when idle.
6. **The server.** The in-memory database it already keeps per project connection is filled from the model's Data
   when made. Moving the server onto per-project databases is the dialect program's Phase 5 (connectors), not this.
7. **PARK-33:** a saved query opened on Python's page goes through Python's project database.

**Proof.**
- PARK-35's acceptance: a query built in Query on Python over a published demo project answers the rows the tab
  answers (`query/demo/verify.mjs`'s run, against Python), and the same on legend-lite's server.
- PARK-33's: a query saved in the tab opens from Saved queries on Python's page, its rows as the tab shows them.
- D5's guard, red on a statement spelled in TypeScript or Python's host.
- Measured: a demo project's fill, the first and the repeated query on Python and in the tab, and the server with
  several projects open. (The attach itself: 0.3 ms on Python's DuckDB 1.5.5, 0.4 ms on DuckDB-WASM 1.5.4; a query in
  an attached database as fast as in the main one. A quick loop, 2026-10-13, not a benchmark.)

**Not in C5:** the corpus onto the same operation (the dialect program's Phase 6, `DuckDbBackend.openWorkspace()`,
judged by interleaved before-and-after corpus runs); Postgres's and the other databases' versions; a query mixing a
project's rows with a person's own tables.

**Reviews:** the Compiler Rewrite line (core, BUILD files), Plan Gen (the planner's new question), the Studio line
(Query, Studio, engine-client), the dialect program through IN_FLIGHT (the new `sql/dialect/` files, announced before
they land), an audit.

**Built 2026-10-13 (branch `datacube/c5-project-data`), C5's steps; D5's sweep (step 4) next:**
- **1, the Java:** `setup/ProjectData.java` (name, open, use, fill, close; `rowsKey` and `fill` for a host keeping a
  database of its own), `sql/dialect/HostOperations.java` with `DuckDbHostOperations.java` (ATTACH, USE, DETACH; every
  other database refused by name, `DialectCapability`), `ModelContext.dataElements()`. `HostOperationsTest`, in the
  dialect package's tests.
- **2, the planner's question:** `Boundary.projectData`, the planner-protocol kind `projectData` (the tab's module and
  Python's native library, `lite_project_data`). `Boundary.testDataSql` keeps DuckDB's dialect until its callers move
  in step 4.
- **3, the tab:** engine-client's `ProjectDatabases` (made once, kept for the page; a model with no Data rows runs on
  the tab's own connection; `made` runs as it is made, before any query reaches it) and `ProjectRunner`;
  `duckdb-tab.ts` `connect` sets every connection up alike. DataCube's saved queries and Query's projects by name use
  it. Studio's tables hold ONE database at a time, the workspace's own (`own`, even with no Data rows): whenever what
  fills it changes (a Data row or a table edited, an undo, a file reset) it is dropped and made again from the model
  and the person's files as they are now (the user: "blow away and recreate"; nothing about earlier databases is
  tracked), the screens pointed at the new one before the old is dropped; a class edited keeps it. `TestData` and
  `withTestData` deleted.
- **5, Python:** `legend_lite/projects.py`, the twin (made once under the frames' lock, kept; one use at a time per
  project, a DuckDB connection being unsafe across threads; a model that does not read as a model is no project's, any
  other refusal said as it is). The engine's execute runs a project's model on its project's cursor.
- **6, the server:** `ConnectionResolver` makes its kept in-memory database holding the model's Data rows, keyed by
  them too, opened outside the store's lock (`getOrOpenSlowly`, as `PlanSessions` opens its held ones), the database
  let go when a fill is refused. Two changes of behaviour: a model whose Data element is refused (a table no Database
  declares) has every query refused by name, where before it ran on an empty database; and a model text with Data rows
  and one without, over the same stores, no longer share the database a caller seeded (ConnectionIsolationTest's
  feature holds for models without Data rows). The server keeps every database for its life, as it always has
  (`HandleStore`, no eviction): an edited row adds one. PARK-35 closed.
- **7, PARK-33:** saved queries on every host, read by the host's compiler (`EngineService.reading`) and opened over
  the host's rows; Python's plan-and-run and held planner requests answer over a project's model held beside its
  database. PARK-33 closed.
- **Measured:**
  - in a browser, on Python: Studio on Python loads its demo projects; Query on Python runs the party project by name
    (5 parties); the query saved there opens from Saved queries on Python's DataCube page (5 parties)
    (`//datacube:python_engine_test`);
  - the tab: two versions answered at once, each its own rows (`//engine-client:model_data_test`); Query on a project
    by name at HEAD, then 1.0.0 (`//site:verify_test`);
  - the server: `ProjectRowsTest`, through `QueryService.execute` -- not Query's run in a browser against the server,
    PARK-35's acceptance as written: the browser tests' runs on legend-lite's server use the demo's seeded project,
    with no Data rows;
  - all of `//gates:datacube`, `//gates:ui` and `//gates:sdlc` after step 3 (182 tests), `//core:core_tests`,
    `//core:guardrails` and both stress suites after step 6.
- **Not measured yet:** the fill and query timings this section asks for; the corpus onto the same operation (the
  dialect program's Phase 6).

#### D5. No SQL text in TypeScript or Python's host (claimed from the dialect program; C5's step 4)

Branch `datacube/d5-host-operations`, stacked on C5. The dialect program's decision D5 (the user's, 2026-10-13): all
client SQL goes through the platform; the tab's DuckDB-only operations become named operations of the DuckDB dialect,
refused by every other; a guard bans SQL text. Claimed with Python's twins (IN_FLIGHT, the dialect program's section,
item 3).

**The design.**
- **One question, `hostOperations`.** A planner-protocol kind `{kind: 'hostOperations', databaseType, operations}`,
  `operations` a JSON list of named operations; answered `[[statement, ...], ...]`, one list per operation, by
  `Boundary.hostOperations` (the tab's module) and its native twin (Python). One round trip for a host's whole step (a
  file read: the read, the copy, the count). A database without an operation refuses it by name (`DialectCapability`).
- **The operations** (in `sql/dialect/HostOperations.java`, DuckDB's in `DuckDbHostOperations.java`, beside C5's
  three; names and values are data, quoted and written by the dialect, never spliced by a host):
  - a relation made: a table or a view, replacing one or not, its select list the planner's copy list or all, from a
    source -- a relation, a file read (DuckDB's readers: CSV sniffed with a header, CSV as text for a declared table's
    file, Parquet, JSON with its records' keys as columns), a remote file (CSV, Parquet, Iceberg), or a query a
    planner wrote; with columns replaced by conversions (Snap's copy);
  - rows put into a declared table from a source, each declared column from the file's column of its name;
  - a relation dropped; a schema made;
  - rows counted, of a relation or a query (DuckDB as it is; Postgres, a warehouse's catalog, as Postgres reads it,
    the subquery aliased: Snap's preflight);
  - a secret (S3's keys, scoped to a source's own path); extensions installed and loaded;
  - the tab's extension folder (C12's two settings); Python's: its extension directory, no autoinstall, a page's
    file search path;
  - a transaction begun, committed, rolled back;
  - the catalog questions a host asks: whether a name is taken in its own database (and its temporary one), how many
    relations a schema holds, Iceberg's metadata (a table's manifest lists, their files, its data files).
- **The demos' seeds become their models' Data elements.** Query's demo and DataCube's configured project load their
  rows by running a shipped SQL file (`trading-seed.sql`): SQL text the page runs. Their models carry the rows as
  `###Data` instead, filled by C5's project database; the configs' `seed` lists and the seeded-project path go (and
  with it the refusal of such a project on Python). The warehouse's seed (`config-warehouse.json`, a real server's
  data) is not the page's: it stays the warehouse's fixture.
- **The guard** is a test, not a type: a branded statement type would run through every engine and every test that
  runs SQL, where a test reading the product sources finds the same. It reads every product `.ts` (DataCube, engine-
  client, Query, Studio) and `python/legend_lite/*.py`, and fails on SQL text in a string (a statement's leading
  keyword). Kept sites are named with their reason: a person's own SQL (Studio's SQL editor) is the person's text,
  carried, never spelled.
- **Not in D5:** the warehouse's own SQL (D4, the dialect program's); `TestDataGenerator`-style hand SQL in core
  (the dialect program's Phase 3).

**Proof.** The guard, red on a statement spelled in TypeScript or Python's host. Each operation's DuckDB statements and
refusals in `HostOperationsTest`; every app's browser tests, Python's engine and headless tests unchanged in what they
see; Snap's preflight against a Postgres catalog (the warehouse's lane).

**Built 2026-10-13 (branch `datacube/d5-host-operations`, on C5):**
- **1, the operations:** `sql/dialect/HostOperation.java` (sealed records: a relation made, rows put in, a drop, a
  schema, a count, a secret, extensions, the extension folder or directory, the file search path, a transaction step,
  the catalog questions, Iceberg's metadata), `DuckDbHostOperations.statements` (one exhaustive switch, each statement
  the one the host spelled before), `PostgresHostOperations.java` (the count; every other refused by name).
- **2, the question:** `Boundary.hostOperations` (`planner/HostOperationsJson.java` reads the operations), the
  planner-protocol kind `hostOperations`, the native library's `lite_host_operations`; engine-client's
  `host-operations.ts` (`runHostOperations`). The tab's DuckDB start asks it for the extension folder; DataCube's
  remote files (`remote.ts`: a view, its secret, its extensions) and uploads (`upload.ts`: the read, the conversion, the
  count, the drop) go through it.
- **3, Studio and Snap:** a file put into a declared table (the transaction and the insert, its columns the planner's
  seed's), the tables' counts, Snap's preflight (counted by the engine's own database type: the warehouse's when its
  catalog is Postgres), its copy, its landing drop and discard (`snap.ts`, the `prepare` statements of an Arrow load).
  A warehouse engine takes its catalog's database type, never a default: DataCube's from the catalog it opens, Query's
  from the warehouse's listing (its own catalog, `main`, empty before the fixture loads, is its DuckDB).
- **4, Python:** `legend_lite/_host.py` (`statements`, `run`, `ask`, `count`), the twin; a frame's relations and drops,
  its name checks, Snap's copy on Python, Iceberg's checks and the extension settings through it.
- **5, the demos' rows:** the trading demo's rows are its Data (`query/demo/models/trading-data.pure`), filled by C5's
  project database in Query's tab and DataCube's; Query asks the keeper for every project (its `ownData` flag gone),
  DataCube's seeded-project path and its refusal on Python gone. `trading-seed.sql` stays the warehouse's fixture
  (`config-warehouse.json`). The rows' three copies -- the Data, the warehouse's fixture, the H2 connection's setup --
  are held equal by `//datacube:saved_queries_test` (each into a database of its own, every table compared row for row
  both ways, NULLs and column types included; red on one changed cell).
- **6, the demo's trades:** DataCube's 200,000 sample trades are made in the tab as Arrow columns
  (`datacube/src/sample-trades.ts`, over engine-client's `arrow-ipc.ts` `arrowIpcStream`) and put in by DuckDB's Arrow
  reader, where one `CREATE TABLE ... AS SELECT` over `range()` made them: `//datacube:sample_trades_test` holds every
  row and column type to that statement; made in about 32 ms in node (measured once); the startup bundle 831 bytes
  gzipped more (budget 395,000, dated).
- **7, the guard:** one rule (`tools/guards/host_sql.txt`), two halves: `//tools/guards:host_sql_test` (every `.ts`
  of DataCube, engine-client, Query and Studio's `src/` and `demo/` and of the libraries they bundle, each string
  literal and each part of a template's text read by a lexer that skips comments and regular expressions --
  TypeScript 7's own API is an unstable bridge to its binary, the reason dated in the test) and `//python:host_sql_test`
  (legend_lite's modules on Python's `ast`, every part of an f-string, docstrings skipped). A literal fails that begins
  with an upper-case statement keyword and a space, or with a lower-case statement's shape (select ... from, insert
  into, create and drop of a relation, attach, detach, install, load, set, pragma, use, begin transaction). Three
  sites kept, each by its literal's exact text and reason: Studio's SQL editor's placeholder, Query's `formatSql` (the
  plan's SQL shown for reading), the remote test page's stand-in query. Each half red on the code before D5
  (DataCube's generator; `frames.py`'s drop). What it cannot see, written in the rule: a statement built from parts
  none of which begins one, and a regular expression right after a closing bracket; the three operation fields that
  carry SQL (a source query, a select list, a conversion) carry the planner's text, traced at each caller.
- **Measured:** `HostOperationsTest` (each operation's statements, run on DuckDB as they read; each refusal);
  `//core:host_operations_postgres_test` (the count, on the embedded Postgres 16); every app's browser tests, the
  Python engine, headless, notebook and wheel tests as before. The Postgres count is right by construction, not a fix
  of a wrong answer anyone met: Postgres before 16 requires a subquery's alias, but the warehouse refuses a Postgres
  older than 16 (`warehouse/.../server/Postgres.java`, `MIN_SERVER_VERSION_NUM`), and 16 accepts the old spelling.

#### C4. The small copies

- One table of the site's file types, which Python reads.
- One saved-query context reader, shared by DataCube and Query.

**Built 2026-10-13 (branch `datacube/python-parity`):**
- The site's media types are `site/src/main/java/com/legend/site/SiteTypes.java` (`//site:types`, no dependencies):
  the warehouse's server answers with it (its `SITE_TYPES` copy gone), and Python's engine reads it through the native
  library (`lite_site_types`; `engine.py` `_site_types`, its `_SITE_TYPES` copy gone). The development servers' and
  test harnesses' own tables (`demo/*.mjs`, `site/serve.mjs`: `bazel run` tools, not a product's host) stay theirs.
- A saved query's context is `query-store/src/context.ts` (`contextOf`, `projectOf`): an explicit mapping and runtime,
  or a data space's execution context, with the data space it came from. Query's `persist.ts` and DataCube's
  `saved-queries.ts` call it, each handing in how it finds a data space (Query's model graph, DataCube's elements);
  their own two readers gone. DataCube's now also answers the data space a context came from, as Query's did.

### D. Headless pages

**Goal.** Every window's action has a core of its own, and the window only asks the person (one-app design §8). Headless
and Python call the cores.

**The steps:**
1. **Save…** becomes `savePage(name)`, and **export** becomes `exportPage(format)` (the formats the page's export
   menu offers).
2. **The model actions' cores** are B4's libraries.
3. **Python:**
   - `page.export(format)`, run by DataCube's headless code (DataCube's own semantics, §1.2);
   - `page.save(name)` into the page store Python keeps (`ll.home()/pages`).

**Proof.** `headless_test`, and a `python_engine_test` check that a page saved from Python opens in the tab.

**Built 2026-10-13 (branch `datacube/python-parity`), and what moved:**
- The export's core is `datacube/src/export-render.ts` (`renderExport`, `columnFormats`, `measureHeatmaps`,
  `heatmapColour`, `heatmapSetting`), moved out of `CubeApp` unchanged; the screen's menu calls it, and so does the
  headless `gridExport`. Python: `grid.export(kind, to=None)`, kind `csv`, `excel`, `html`, `text` or `pdf`.
- A page's board with its charts is not exported headless: PARK-32 (charts drawn headless).
- **`page.save()` landed with B6**, calling the Java rules in the native library: with no path it keeps the page in
  this user's saved pages (`ll.home()/pages`) as DataCube's Save does -- over the copy it was last saved as, else as a
  new one, named `name=` or the page's own name -- and answers its id; `python_engine_test` opens it from Open…. Changed
  from this plan: `page.save(path)` still writes the page file (the one-app design keeps it, §8), so the store is the
  call without a path, not `page.save(name)`.
- The model actions' cores are B4's libraries (done).

### E. Studio and Query from Python

**Goal.** `ll.studio()` and `ll.query()` open those apps served by Python, on Python's SDLC, Depot, saved queries and
engine, as `ll.show()` opens DataCube.

**First: G11.** Move the model home's lock into the repository itself, before a second kind of process opens it.

**What Python must give** (measured 2026-10-13):
1. **The two sites.** Ship `//query:site` and `//studio:site` in the wheel; today `:wheel_site` carries only DataCube's
   page.
2. **A `config.json` for each,** pointing at Python's own addresses.
   - Studio's: `sdlc`, `depot`, `vendor`, `worker`, `engine` (`studio/src/backend/sdlc.ts`).
   - Query's: `execution: server`, `planner`, `projects`, `depot` (`query/src/app/context.ts`).
3. **The token.**
   - Python serves every private path under a Bearer token, and DataCube carries it in `#token=`. Query and Studio
     route by `location.hash`, and their clients (`SdlcClient`, `DepotClient`, `HttpEngine`, `QueryStoreClient`) take a
     fetcher but send no token.
   - **Ask the user,** with a recommendation: Python exchanges the link's token for a same-site, HttpOnly cookie on
     the first load, and every call of the page's carries it.
4. **The routes Studio and Query call that Python does not answer yet:**
   - `/api/server/v1/currentUser`;
   - `/api/lite/v1/*` over HTTP;
   - execute as JSON. Python's execute answers Arrow only, and `HttpEngine.execute` sends JSON. C1's Java gives the
     JSON answer's shape.
   - saved queries (B5).
5. **The calls themselves:** `ll.studio()` and `ll.query()` (`legend_lite/datacube.py`'s `show`, beside it).

**Reviews.** The Studio line (its apps' configuration), Compiler Rewrite (the wheel), an audit.

**Built 2026-10-13 (branch `datacube/python-parity`):**
- **The sites.** The wheel carries `//query:site` and `//studio:site` (`//python:wheel_query_site`,
  `:wheel_studio_site`, at `legend_lite/_apps/<app>`, each tree as built), without DuckDB-WASM's builds (Python's
  DuckDB runs the queries: about 75 MB left out) and the demo's own configuration and models. Studio's demo projects
  stay (84 KB). The wheel is 52 MB. `LEGEND_LITE_QUERY_SITE` and `LEGEND_LITE_STUDIO_SITE` point elsewhere, as
  `LEGEND_LITE_SITE` does for DataCube's.
- **The configs.** `engine.py` writes `/<app>/demo/config.json` itself, every address at the origin the page was loaded
  from: Query's `execution: server` at `/api`, no projects, its Depot at `/sdlc/api` and `/depot/api`; Studio's
  `sdlc`, `depot`, `engine`, `vendor`, `worker`. Query has no `planner`: Python's pure/v1 answers the grammar, so there
  is one planner, Python's.
- **The token (the runbook's recommendation, taken without asking: asked at landing).** `ll.query()` and `ll.studio()`
  open `/<app>/demo/index.html?token=<the engine's token>`. That first load answers 303 to the same address without
  the token, setting a cookie `legend-lite-<port>` (HttpOnly, SameSite=Strict, Path=/); every private path admits the
  cookie as it admits the Bearer header. The cookie is named by the port because a browser sends a host's cookies to
  every port: two Python engines would otherwise replace each other's. Every `/api/` path is private now, not only
  pure/v1's.
- **What the cookie costs, and its guard (E's audit).** SameSite is by host, not by port, so a page of any other server
  on 127.0.0.1 sends the cookie too. The cookie therefore admits a call only when the browser marks it the page's own
  (`Sec-Fetch-Site: same-origin`), or, a browser that marks none, when its `Origin` is the engine's. A form or a
  `no-cors` fetch from another port is refused (`AppsServed`; in Chromium, the evidence walk's last
  check: 401). The header is read by hand: it carries every local
  server's cookies, and `http.cookies` drops the whole header at the first value it cannot read. What stays: while the
  browser runs, the token goes in that header to every server on 127.0.0.1 the browser visits, and any of them can
  overwrite the cookie (the app then answers 401 until the link is opened again). It also passes once in an address,
  into the browser's history. It dies with the process. A token the apps' own clients carry, as DataCube's does,
  would end this; it goes to the user with E's landing.
- **Where the link is clicked.** Strict holds where it is clicked on another site (Jupyter's output, at
  `localhost:8888`; the Studio line's question, measured): the cookie is stored from the cross-site load, the page's
  files need none, and the page's own calls are same-site, so they carry it.
- **Studio's link from DataCube on Python** (added 2026-10-13): `services.json` names the apps Python serves (`apps`),
  and Python's model home then gives Studio's link to a project. That link is the editor's address with the engine's
  token, which its first load trades for Studio's cookie. A notebook's cube, whose channel carries no token, gives none.
  `//datacube:python_engine_test` serves Studio as the wheel ships it (`//python:wheel_studio_site`): Create a Class…
  on Python's page offers "Open it in Studio", and the link opens Studio on the workspace with the class.
- **The session keeps the web server for an app.** An app's tab says nothing when it closes, so once `ll.query()` or
  `ll.studio()` is called, the web server stays while the process lives (closing the last cube no longer stops it),
  and a plain script's end waits for the app as for a cube (a link only printed, `browser=False`, ends with it).
- **The routes.** `/api/server/v1/currentUser` answers this user (`"this user"`, as the stores name it), and POST
  `/api/lite/v1/*` goes to the planner's pure/v1, which answers it as legend-lite's server does (`PureV1Api.route`).
  Execute as JSON landed with C1 and C2; saved queries with B5.
- **The calls.** `ll.query(browser=True)` and `ll.studio(browser=True)` (`datacube.py`, beside `show`) print the link,
  open it, and return it.
- **Measured.** `python/tests/test_engine.py` `AppsServed` (each app's files, the trade, the configs, the cookie's
  checks, lite's routes); `//python:wheel_test` (from the installed wheel: both apps served, traded, configured,
  answering with the cookie alone; Studio's demo projects shipped; no DuckDB-WASM). And by hand in Chromium, from the
  installed wheel on a fresh home (`docs/one-platform/evidence/e-apps-on-python.mjs`), the link clicked on a page of
  another site: Studio's "Load demo projects" publishes the four projects into Python's repository (293 ms); a
  published project opens and compiles, 0 errors; Query lists the four from Python's Depot, opens `trading` at HEAD,
  and builds a query on `Party`; another port's POST is refused. No page error, no failed call but the run's.
- **The one call that fails: running a project's query (PARK-35).** A project's rows are its `###Data` elements', which
  only a browser tab loads, into its own DuckDB. Python refuses a model its frames did not write (500, "a project's
  queries run only in a browser tab for now"). legend-lite's server has the same gap: it opens the runtime's own
  connection, an empty in-memory DuckDB. The fix is one design for both server hosts, with PARK-33's, and goes to the
  user: a database per project and version, filled from its Data by the planner's statements (Pure, not the page's
  SQL), and when it closes.

### F. The model plan's remaining steps

From `docs/DATACUBE_MODEL_DESIGN_2026_10_10.md` §13. Each lands once, in the one app, so on both hosts.

1. **Dashboards on classes:** page filters and a chart's clicks by a class's property. This rebuilds charts batch 1's
   page filters (`docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md`, built on its branch, not landed).
2. **Combining in prep:** the join dialog in every form legend-lite has (any condition, as-of, lateral and Expand,
   with or without a match, append), its match summary; choose and rename, remove duplicates (model design §3.4).
3. **The model view:** classes as boxes and associations as lines; related properties walked in the column chooser;
   filters through named ends, both ways a page's choice; the one-connection refusal (§4).
4. **Promote and publish:**
   - a page's project promoted to a team's SDLC, or pushed to a remote git (B7);
   - export as Pure;
   - publishing only on a released model, its data where the published place can read it (§6, §7).
5. **Measures in the model:** a measure and a dimension declared on a class and offered by name
   (`docs/BI_AND_ETL_PLAN_2026_09_29.md` B4).

### G. Follow-ups, taken where they block something

1. **Remote planners on Python's pages -- removed 2026-10-15** (the user: "the whole point of DataCube on Python is
   that Python is the host"; §1.1, a host plans in process). `planner=`, `server=`, `ll.planner()` and
   `legend_lite/upstream.py` are gone with the server paths of the engine, the frames and the page, and the Python
   host's server branch (`python-host.ts` `planning`); the browser app keeps its own `UpstreamPlanner` and its corpus
   (`datacube:upstream_plans_test`). PARK-30 and PARK-31 closed with it. What G1 was, below, is kept as history.
   Python plans through legend-lite's server or legend-engine and still runs the
   SQL on its own DuckDB. R3: the plane is chosen once, shown, never substituted.
   - **The twin of the tab's `UpstreamPlanner`.** `datacube/src/planner.ts` asks a server's pure/v1 `generatePlan`
     (its runtime added by `fromRuntime`) and reads the first SQL node and `tdsColumns`; also `lambdaRelationType` and
     the grammar calls.
     - Python gets the same, a `RemotePlanner` beside `NativePlanner` (`python/legend_lite/engine.py`, the `Planner`
       protocol), over `urllib`.
     - One corpus of queries holds the two: the same plans from the same server.
   - **The branch points that bypass the planner today:**
     - `_plan_and_run`, which calls `lite_plan_json_held` directly;
     - `_planner` (the planner protocol), `Frames.plan`, and headless.
     - Each must ask the engine's chosen planner. A server's plan (its `tdsColumns`) reaches the page in the plan shape
       the page reads.
   - **The page:**
     - `pythonPlanes` stops refusing Python's other planes;
     - `ll.show(planner=...)` chooses, read once;
     - the token and the server's sign-in follow C3's twin-client pattern.
   - **Decided 2026-10-13 (the user):** the words are the tab's, `ll.show(df, planner='local' | 'remote' | 'engine',
     server='http://...')`, or `ll.planner(...)` before the first cube; a server's sign-in is skipped for now ("dont
     stress too much about auth right now"; the user's ideal, SSO as the person running Python, is G10, for both hosts
     at once: the tab sends no sign-in to a server today either).
   - **Built 2026-10-13 (branch `datacube/python-parity`; removed 2026-10-15, above):** `legend_lite/upstream.py` (`RemotePlanner`, the twin of
     `UpstreamPlanner`; the requests and the plan's reading held alike by `datacube/test/fixtures/upstream-plans.json`,
     read by `datacube:upstream_plans_test` and `python:upstream_test`); the engine's `Planner` gains `plan`, `read`
     and `refused_plan`, and plan-and-run, the pure/v1 calls and `Table.plan`/`execute` ask it; a session chooses once
     (`_Session.choose_planner`); the page on a server's plane plans with the tab's `UpstreamPlanner` at Python's
     address (`python-host.ts` `planning`) and reads the plan by `planner.ts` `readServerPlan`. A headless grid still
     plans with the native library: PARK-30. Execute through a server's plan on Python is refused in words (C1).
   - **Reviewed:** the readers accept one shape, a `relationalTdsInstantiation` over one `sql` node written for the
     database it runs on, with no `${` placeholder, and refuse the rest by name (the Plan Gen line); both are INTERIM,
     until core reads a legend-engine plan (`docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md` §8, phase 2), the fixture then
     its cases. Python runs a server's plan as one SELECT, awaits it holding no lock of the frames', and keeps each
     column's SQL type (`Column.sql_type`, for G9); what remains is PARK-31.
2. **Streaming results back from Python.**
   - Python writes record batches, each typed by the plan; the plan stays in the schema, which is sent first.
   - The page reads the body with a `RecordBatchReader`, chunk by chunk, as the tab's DuckDB streams.
   - Today `Frames.run`, `_plan_and_run` and `pythonRunner` each take the result whole.
   - **Built 2026-10-13, then dropped the same day (the user: "is this chunking thing worth it ... ended up in bit of
     hack solution instead of true streaming"; "drop it"):**
     - it read a frame's result whole anyway, so it saved no memory;
     - it passed only a warehouse's chunks through;
     - a stream cut short looked complete, since HTTP/1.0's body ends at the connection's close. That was fixed only by
       counting rows, a patch over a transport that says no end.

     True streaming is a design of its own: PARK-36.
3. **One DuckDB 1.5 release everywhere,** the corpus tests' included. Today the tab's engine is v1.5.4 and Python's
   1.5.5.
4. **Text encodings:** UTF-16, gzipped files, and Latin-1 read as Latin-1. Today a text file is decoded as UTF-8 in
   the page on both hosts (`upload.ts` `fileParts`).
   - **Built 2026-10-13 (branch `datacube/python-parity-2`):** `fileParts`, the one reader of a picked file on both
     hosts, reads a file by its first bytes:
     - gzip's are unpacked first (`DecompressionStream`), then read as what is inside;
     - UTF-16 is read by its byte-order mark, either order;
     - UTF-8 when its first 64 KB are UTF-8, or hold one UTF-8 character at least (a stray byte then replaced, as a
       File's `text()` replaces it: a UTF-8 file is never read as Latin-1 for one bad byte, the audit's catch);
     - else Latin-1 (the web's windows-1252): bytes above ASCII and no UTF-8 character among them.

     Every host's DuckDB is given the text as UTF-8, as before. `formatOf` and `tableNameOf` read a gzipped file by
     its name within (`trades.csv.gz` is a CSV, table `trades`), and every file chooser takes `.gz`.
   - **The rule's one limit:** the choice is the first 64 KB's. A Latin-1 byte past them, in a file UTF-8 until there,
     is replaced, as a File's `text()` replaces it.
   - **Measured:** `datacube/test/upload.test.ts`:
     - a Latin-1 CSV opens as José and München;
     - a UTF-8 file with one stray byte stays UTF-8, and a character cut at the 64 KB edge is not a fault;
     - UTF-16 LE and BE, a gzipped CSV (UTF-8 and UTF-16 inside) and a gzipped JSON read alike;
     - the 64 KB rule;
     - the `.gz` names.
5. **Iceberg files on Python. Decided 2026-10-12 (the user): ship the extension.**
   - **Pins.** DuckDB 1.5.5's `iceberg`, and the `avro` it reads manifests with, pinned in `MODULE.bazel` as httpfs is
     (`http_file` per platform; their integrity measured 2026-10-13):

     | | iceberg | avro |
     |---|---|---|
     | osx_arm64 | sha256-ub3asCJoQ03N70nxa719eNOjW+rnTLecTs27FD4QbIo= | sha256-VTHiQY1VOwab9Mw25t3vrf/QHxeZFbEAfOXXeLu64iA= |
     | osx_amd64 | sha256-PGfkenjIfRCAYnw3vHkplga9ed1K2pqTE0XK3Tf9w6o= | sha256-O4zm/TMb/9ZkIEieHc1TK3S6Sl0V09u6hQD0ixvC1Qc= |
     | linux_amd64 | sha256-JYjLAEbbDvFfDxjHhDQXkGLDckgyqwDctDVnvigw7dQ= | sha256-tnx7j1Q+G4JBZ/dI5XpistYZoDx0x2gXrnM0DrwukGg= |
     | linux_arm64 | sha256-l0XQGON2T9nurkwGKXX7X17VhAhdhfKX1Utdc4xa3ks= | sha256-72CG6pbmogs5bgrs6I4R/WoKMt7Qp1E02zDM2mtAmD8= |
     | windows_amd64 | sha256-XxSrB2XOqirZAyV/GxpYNYLkRGgz2dPl677ux1MHh/E= | sha256-SYdNsYedcdkvdvaByHZaF/bxl5jYYfA15fLUoU7udTs= |

     They are 9 to 18 MB (iceberg) and 2 to 8 MB (avro) gzipped per platform.
   - **Build.** Gunzipped by `//tools/gunzip` as httpfs is (`java_run`, once per platform; `DUCKDB_PLATFORMS` in
     `tools/platforms/defs.bzl`), and laid out in `//python:wheel_duckdb` beside httpfs.
   - **The rule, R4.** An Iceberg table's metadata names its data files by path, so a remote table could name a file on
     this machine.
     - **Built 2026-10-13 (branch `datacube/python-parity`):** before Python mounts an Iceberg table,
       `Engine._iceberg_refusal` reads the manifest lists its snapshots name, the manifests those name, and the files
       its current snapshot reads, each level only once the level naming it is checked, and refuses the table, naming
       the path, unless every one is a remote URL by `remote_url`'s rule. The shared mount gives the extensions and keys
       first (`remote.ts` `accessRemote`; headless `remoteAccess`), then the view.
     - **Not pinned** (changed from this plan's first draft, measured): DuckDB reads the metadata's current snapshot
       on every scan and finds the current metadata file by its own rule, so pinning a snapshot closes nothing a
       changing server could not reopen. What the check cannot close is PARK-29, with its fix (DuckDB refusing this
       machine's paths on legend-lite's connection, with G6).
     - **Measured:** loading iceberg loads avro from the extension folder with installing off; `iceberg_scan` and
       `iceberg_metadata` take `snapshot_from_id`; `iceberg_scan` ends the process when a manifest cannot be read
       (`iceberg_metadata` and `read_avro` report it), so no view is made before every manifest was read through those.
   - **Proof (built):** `python/tests/iceberg_table.py` writes a small Iceberg v2 table named by the URLs a test serves
     it at; `headless_test` reads a served one and refuses tables naming a local data file, manifest or manifest list;
     `python_engine_test` opens one from the app's Remote section on Python's page; `wheel_test` reads one from an
     installed wheel.
6. **A connection per page on Python** (optional): today one connection, shared, a feature.
7. **A page plan's `sourceUrl('file:...')` refused** (optional hardening, §1.4).
8. **The upload's SQL into the dialect:** D9, under the dialect-leak plan's D5, the "cloud backend setup and testing"
   session's.
9. **One result-typing table** for the Java, TypeScript and Python executors: the Plan Gen line's phase 3,
   `ExecutionPlan.TdsResult`'s `TdsColumn(name, type, sqlType)`.
   - Measured 2026-10-12: Java's `ExecutionResult` matches columns by position, checking only the count, and has no
     StrictTime case.
10. **Sign-in where a store is shared:** the in-page stores trust a configured user name.
11. **The model home's lock in the repository itself** (the Studio line's review of B3).
    - Python holds a lock across its processes around each SDLC call, but `GitStorage` and `sdlc-server` take none.
    - Before a second kind of process opens the same repository (E), the lock moves into the repository.
    - **Built 2026-10-13 (branch `datacube/python-parity`):** `GitStorage.locked(call)` makes each call of the rules
      under the repository's own lock -- a lock per repository for this process's threads, and the operating system's
      exclusive lock on `<repository>/legend/lock` for every other process -- and both hosts call through it: the model
      home's server (`SdlcServer`, which had serialized only its own threads) and Python's native library
      (`ModelHome`, its SDLC and its Depot). Python keeps its own lock across its processes, taken first on the
      caller's thread, so a call waiting on another Python process still holds no compiler thread; the repository's
      lock is the one every kind takes. Proof: `python/tests/test_model.py` TheRepositorysLock -- another process holds
      the repository's lock, and the library's call waits for it -- and `sdlc-server` `GitRepositoryTest` (a call inside
      a call, a second thread's wait, and a second `GitStorage` over the same folder sharing the lock). The
      cross-process proof is POSIX's (Linux and macOS: the other process holds the lock with `fcntl`, and the test skips
      on Windows); on Windows the JVM's lock is `LockFileEx` on the same file, the same mechanism, not measured.

12. **The tab's DuckDB extensions from the site, and one start of it** (added 2026-10-13, the Compiler Rewrite line's
    finding).
    - **The problem.** As each page opened, DuckDB-WASM downloaded extensions from extensions.duckdb.org:
      - ICU, when the session's time zone was set (`setUpSession`, every tab's DuckDB since C1);
      - JSON for a JSON file;
      - Parquet for a Parquet file;
      - httpfs for a remote file.

      The apps, and a desk's browser tests, needed that site up; behind a firewall, a page could not open. CI passed
      only because its runners reach it.
    - **Built (branch `datacube/python-parity-2`):** the four extensions are pinned for both browser builds in
      `MODULE.bazel`, by content, as Python's are, and served by each site at `vendor/duckdb-extensions/`
      (`//engine-client:duckdb_extensions`; Studio's beside its DuckDB at `vendor/duckdb/duckdb-extensions/`; DataCube's
      built folder too; not in the wheel, whose apps run no DuckDB-WASM). The tab's DuckDB is pointed at them
      (`custom_extension_repository`) before anything loads one.
    - **One start of a tab's DuckDB:** engine-client's `startDuckDbInTab`, which now returns the database too. DataCube's
      boot, and the stress, remote and Python-engine test pages, started theirs with copies of their own; each now calls
      it. Its session is set up there, and `browserServices` no longer sets it up again.
    - **What that changed in DataCube's start:** the session's setup is the planner's, so the database is ready once
      the planner is too. DuckDB's start still overlaps the planner's, but the sample's rows and a `?remote=` mount now
      come after both instead of beside the planner's load: before a value is written, the session is set up (C1's
      rule, which the old order broke). The mark is `dc:database-ready`.
    - **The guard:** every browser test makes extensions.duckdb.org unresolvable. `tools/browser/pinned-chromium.mjs`
      wraps both of Playwright's launches for the Playwright the test's entry script resolves beside itself;
      `//site:verify_test`, which loads DataCube's, calls `hermetic` on it by hand. A Chromium flag, not Bazel's
      block-network tag, which works only under a sandbox (Windows has none). A page that needs another extension fails
      there, as it would behind a firewall.
    - **Measured:**
      - The guard bites: with the tab's `ownExtensions` taken out, `//studio:verify_test` failed in 4 s on "Failed to
        load 'https://extensions.duckdb.org/v1.5.4/wasm_eh/icu.duckdb_extension.wasm'", and `//query:verify_test`
        failed every step. `//datacube:browser_guard_test` keeps it so: a page under either launch must meet an unknown
        host.
      - All 19 browser tests pass under it. They ran with the lanes `//gates:datacube`, `//gates:ui` and `//gates:sdlc`,
        plus the warehouse lane's browser tests: 186 tests in all.
      - Only the `wasm_eh` files are loaded by a test: headless Chromium has exception handling, so DuckDB-WASM always
        picks that build. (2026-10-15: the apps ship that build alone -- `duckdb-tab.ts` refuses a browser without
        exception handling, in words -- so the `wasm_mvp` files are no longer pinned; PR neema2/legend-lite#42.)
    - **On a DuckDB-WASM bump:** three places move together: `engine-client/package.json`, `_DUCKDB_VERSION` in
      `engine-client/BUILD.bazel`, and the URLs in `MODULE.bazel`. A stale one fails loudly: the tab asks for another
      folder, ICU does not load, and every page's start fails.

### H. Publishing

PyPI; the npm packages' third-party notices; the FINOS name; the copyright-holder sweep. Each is the user's to decide.

## 6. The order: the pull requests

| # | Pull request | Steps | What you get |
|---|---|---|---|
| 1 | Python parity | G1 + G5 | Remote planners on Python's pages (removed 2026-10-15: a host plans in process, §1.1); Iceberg files on Python, every data file remote |
| 2 | The model actions as a generic Legend capability, and headless pages | B4 + D | Twin libraries, TypeScript and Python, for a class, an association or a data space in any project; Python's `grid.create_class()`, `page.create_association()`, `page.create_data_space()`, `page.export()` and `page.save()` |
| 3 | Stores on one implementation | B5 + B6 | Saved queries and saved pages each one set of Java rules in the browser, Python and the server; Python's pages get saved queries; L1's saved-query differential the acceptance test |
| 4 | One engine path | C1 + C2 + C4 | The tab answers legend-engine's execute API with Python's Java; Query's SQL button with no server; one planner protocol |
| 5 | Warehouse on Python | C3 | Warehouse tables on Python's pages, Live on the warehouse, Snap into Python's DuckDB |
| 6 | Studio and Query from Python | G11 + E | `ll.studio()` and `ll.query()` on Python's own SDLC, saved queries and engine |
| 7 | A project's data where its queries run, and no SQL text in TypeScript | C5 (PARK-35 + PARK-33) + D5 | Query and Studio on Python, and on legend-lite's server, run a project's queries on its own data; a saved query opens as a grid on Python's page; the tab and Python's host spell no SQL |
| 8+ | The model plan | F1 to F5 (B7 with F4) | Dashboards on classes, joins in prep, the model view, promote and publish, measures |

- Rows 1 to 3 are independent of each other, so each lands as soon as it is green.
- Row 6 needs rows 2 and 3.
- G2, G3, G4, G6, G7, G9 and G10 are taken where they block a row. L runs beside all of them.
- H comes last.

## 7. Decided: not to be reopened without the user

- One DataCube app on every host. Python's pages give services; Python's model home is an SDLC (one-app design R1 to
  R6).
- PlanAndRun for every grid. Each host's executor types its results by the plan.
- R4 as §1.4 states it, including option (c), the accepted `sourceUrl('file:...')`, remote-only files and Iceberg
  data files.
- Python plans in process, always (the user, 2026-10-15: DataCube on Python has no remote engines, and Python as a host
  is no client of one; §1.1). Running queries on a server's own models and data, if wanted, is a separate client on
  `warehouse.py`'s pattern, recorded as a candidate.
- The two DuckDB versions, accepted until G3. Python's one shared connection, a feature.
- Python's model home:
  - the place `Python` (the tab's `Browser`);
  - the SDLC user `local` / "Local User", as the browser's;
  - no Studio link until E.
- Row order and float last digits without a sort are not guaranteed: "users have to add their own sort if they want
  that guarantee like database query".
- The page's settings are kept by Python (B2). Each host's DuckDB runs the compiler's session setup.
- Iceberg on Python ships, with its data files remote (G5).
- The model actions are twin libraries, not DataCube code (B4).
- legend-lite is a drop-in legend-engine over the exact same APIs (L).
