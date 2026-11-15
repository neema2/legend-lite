# C5-T: TypeScript's plan/exec split, its record (2026-11-15)

PR #105 (branch `claude/total-wall-b3`). This file holds what the PR first wrote into the shared documents: its
`docs/GATES.md` entry, its hole check and its lines in the start-here document's C5 list. No PR edits the shared
documents (the user's process, 2026-11-15: they made each landing conflict with the next), so whoever updates them
after #105 lands takes the text from here. Until then, `docs/GATES.md`'s "Its limits" lines on TypeScript's run wall,
and the start-here document's line "TypeScript's is C5-T's" under run frames, describe the tree before #105.

The witness-cost homework is the PR's own, in `docs/witness-cost-homework/` (README, `results/ts.txt`,
`scripts/node3.sh.txt`). The independent re-check's fixes are in `docs/records/c5-t-recheck-2026-11-15.md`.

## What TypeScript's split covers (the GATES entry)

TypeScript's wall, `datacube/test/run-wall.test.ts`, is deleted (C5-T, 2026-11-15). Its lists went with it: 17
`COMPOSER_MODULES`, 22 `COMPOSER_SIGNATURES` and 2 `COMPUTED_DOORS`. The audit's fix round of the same day restored
what the deletion had dropped. The browser and the headless V8 keep no frame, so there the run-time witness is blind
and the build holds the line.

TypeScript's finish line now covers:
- **The run layers, `//engine-client:run` and `//datacube:run`.**
  - They reach the records and two pinned npm packages alone (`tools/deps/ts-layers.txt`,
    `//tools/deps:ts_layering_test`, which pins every label it trusts).
  - Each is typechecked alone by a strict tsc over its closure, with no `@ts-nocheck`, `@ts-ignore` or
    `@ts-expect-error` (`//tools/deps:layers_alone`).
  - They open no import door chosen at run time but the two vendor loads named, and no literal worker, URL, require or
    importScripts specifier names a module they may not reach.
  - No function, member or interface of theirs takes anything that answers a planner's statement, plan or planned
    record.
  - No module of theirs, nor of the doors', writes an explicit `any` (the last round's T2).
- **The engine's API, `//engine-client:engine_api`** (the final audit's MUST 1).
  - It holds the engines' interfaces, whose methods send, and the run frame, which opens a run, out of the records.
  - The run layers, the doors and the apps reach it; it reaches the records alone, and is typechecked alone.
  - The planner's check of the frame is a record (`plan-guard.ts`), which cannot open a run: it reads the run through
    the reader the frame installs once.
- **The planner layers, `//engine-client:planner` and `//datacube:planner`, and the server layer,
  `//engine-client:server`** (legend-engine's clients). They reach the records (the server also the planner and its
  npm package) and no engine's API, run layer or door, and each is typechecked alone.
- **The doors, `//engine-client:doors` and `//datacube:doors`.** Each asks the planner, then runs its answer in a run
  frame; their deps are pinned, and each is typechecked alone (the last round's T2).
- **The apps: DataCube's app, Query, Studio** (the last round's T1).
  - Their targets depend on no run layer, nor do the libraries they depend on (`//tools/deps:ts_layering_test`: an app
    reaches the run side through the doors alone).
  - No app module, nor any page of theirs (`demo/`), imports a run module (`//tools/deps:app_runs_test`). That holds
    for any import, a `.js` spelling or none read as the module it loads. It holds through a re-export, followed through
    every package an app imports (an import exported again too; a module the walk cannot read is named, never skipped).
    The exception is three named page boots, shrink-only: DataCube's demo sample, and Query's warehouse seed and
    sign-in (the backlog below).
  - Every plan-and-run site outside the doors and the run side is a shrink-only census: 38 sites in 19 files. It
    covers the apps, their pages, and engine-client's records, planner and server, whose types are structural, so no
    import shows a run there. The held connection's calls (`exclusive(`, `.rows(`) and a database's receivers are
    counted too.
  - Each site is a call of a door or one of those page boots:
    - DataCube's runner (cube, plan, treeview);
    - its services' rows (a planner and the runner);
    - an Engine's execute (a server's or BrowserEngine's; legend-engine's own, passed on by the server layer);
    - an engine a door handed (the stress and remote harness pages, the headless grid's table output).
- **The workspace's tables** (Studio's Data panel, `tab-data.ts`). Load, put, reset and tables run one at a time, so a
  Reset never plans without a file whose put then lands (the final audit's MUST 2). Run, the SQL playground and the
  query builder's run load the tables first, so they wait for an upload or a reset in flight.
- **Every module a browser bundle reads is one it declares** (`//tools/deps:bundle_inputs_test`). esbuild looks for a
  sandboxed input's relative imports in the real output tree. So `//engine-client:planner_worker`, which named three
  files by hand, built only when another action had copied the fourth, `plan-guard.ts`; it now takes the planner layer
  whole.
  - Each bundle writes esbuild's metafile and the files it declares, its npm packages' directories included
    (`tools/js/esbuild.bzl`).
  - They are collected from every package (guards_package's `guard_bundles`), so a new bundle is checked with no list.
  - A bundle declared after its package's `guards_package()` is refused (`//tools/js:late_bundle_test`).
- **Every worker the product starts** is pinned by call site, through any receiver (`new globalThis.Worker(`), as each
  send, connect, fetch and submit is (`PlannedSqlTest.WORKERS`, the last round's T4 and the final audit's item 7): the
  planner's, DuckDB's own, and Monaco's.
- **On Node, every send is refused outside a run** at the send primitives: DuckDB's `#whole`, `#batches` and `#bound`,
  the held connection's sends, and the warehouse's submit. The streaming path, Node's real one, is tested by name (the
  final audit's item 5). Every node_test runs with `--experimental-async-context-frame`.

It does not cover:
- a run-time witness in the browser or the headless V8 (none is kept there);
- a send on a receiver the census's pattern does not name: it reads names, so it catches drift, not an adversary;
- the multi-step doors (the snap door, `writeTable`, the upload and catalog flows, `TabTables`), which are not yet one
  keyed plan each: moved to the DataCube stream's one-intent API (the backlog below, the user's word of 2026-11-15);
- the planner's server clients' protected generic POST ("no execute" by the method's name alone, an adversarial
  route), and the headless bridge's connection, which calls no `sending()` (the V8 keeps no frame): the backlog below;
- Python's half, which is C5-P's (`docs/GATES.md`'s C5-P entry: `//python:run_closure_test`).

## The hole check (the last round's T4)

A fixed list of 15 routes by which a TypeScript run could plan, or an app could run. Three real holes were closed; the
other twelve are named by their existing guard.

| # | Route | Answer |
|---|---|---|
| R1 | an app imports a run layer | closed for `src/` (T1). **Fixed: the pages (`demo/`)**, which no rule read: now read, three boots pinned |
| R2 | an aliased or renamed import | closed (`runReaches` goes by module path). **Fixed: a `.js` spelling**, which typechecked, bundled and passed |
| R3 | a re-export through a module no rule reads | closed: re-exports are followed through every module read; the apps' libraries reach no run layer |
| R4 | an `any` | closed: `layers_alone` (T2), `PlannedSqlTest.noAnyInTheProduct` |
| R5 | a computed import or an injected loader | closed: computed specifiers refused by default in run layers; the loader removed (T3) |
| R6 | a send in the browser or headless V8 outside a door | closed by the choke-point censuses and the host's answered set. **Fixed: a worker**, the one transport no census pinned |
| R7 | a DuckDB send not through the six send points | closed: `DATABASE_SENDS`, `CONNECTS`, `run_frame_test` |
| R8 | a planner reached from a run layer through records | closed: `ts_layering_test`, `layers_alone`, the mint and cast censuses |
| R9 | a test-only escape reached from product code | closed: `noProductSourceReachesTheTestsOwnStatement` |
| R10 | the headless page's global host connection | closed: the host refuses any statement its planner did not answer (`//datacube:headless_test`) |
| R11 | a remote plan-and-run from an app | closed: `FETCHES` pins it; Python plans and runs, and refuses unplanned SQL |
| R12 | a compiler suppression hiding an import | closed: `layers_alone`, `PlannedSqlTest`'s suppression count |
| R13 | an npm package that sends | closed: no app pins `@duckdb/duckdb-wasm`; `CONNECTS`, `DATABASE_SENDS` |
| R14 | an engine a door hands an app | closed: `app_runs_test` `CENSUS`, shrink-only |
| R15 | a non-`.ts` source | closed: app and page targets take `.ts` only; the choke-point census reads `.js`/`.mjs` |

## C5-T's lines for the start-here document's C5 list

- **The structural split: TypeScript's half is done** (C5-T, 2026-11-15, branch `claude/total-wall-b3`).
  - Its run code is the `run` targets of `//engine-client` and `//datacube`, whose deps `tools/deps/ts-layers.txt`
    pins (records and npm only; `//tools/deps:ts_layering_test`).
  - Each is type-checked alone by a strict tsc that sees nothing else (`//tools/deps:layers_alone`, with a planted
    violator's bite).
  - Every send on Node is refused outside a run at its choke point (`run-frame.ts` `sending`: DuckDB's connection, the
    warehouse's submit).
  - `datacube/test/run-wall.test.ts` is deleted.
  - The audit's fix round of the same day restored what the deletion dropped: DataCube's doors a target of their own
    (`//datacube:doors`), the apps' own plan-and-run sites a shrink-only census (`//tools/deps:app_runs_test`), and the
    wall's signature, suppression and literal-specifier rules on each run layer (`//tools/deps:layers_alone`). The
    section above says what it covers.
- **Run frames around every send site: TypeScript's is done** with the structural split's TypeScript half.
- **Node's `--experimental-async-context-frame`: done.** It was measured against Node 24 and preferred: both remove the
  promise tax, and a run costs less with the flag. It is set for every `node_test` in `tools/js/defs.bzl` and pinned by
  `//engine-client:run_frame_test` (`docs/witness-cost-homework/README.md`).

Backlog:
- **Each multi-step TS door made one keyed plan**, as Java's Keyed pattern does (a step's rows fill the next
  statement), so a door is one planning call and one run call. The orchestrator's note 3 on C5-T accepted the snap
  door, `writeTable`, the upload and catalog flows, and `TabTables` as multi-step doors until then.
  - MOVED (2026-11-15, the user: "Yes add multi step to datacube") to the DataCube stream's one-intent API
    (`claude/datacube-engine`, its contract's R2).
  - Its finish line: each intent is one planning step and one run, a keyed plan where one step's rows shape the next.
    The truly two-phase intents (upload, describe) are named exceptions. A test counts the planning and run calls of
    each intent.
- **The C5-T audit's backlog: FIXED** in the last round's T3: `upload.ts`'s `?? 0` (:360), `tab-data.ts` `tables()`'s
  catch-all, `PlannedSqlTest.UNPLANNED_IMPORT`'s quote style, `infer.ts`'s stale reference (:152), `warehouse.ts`
  `loadWarehouseBinding`'s load callback.
- **The pages' boots re-pointed to doors** (the last round's T4: the pages, `demo/`, are app code no rule read until
  then): `datacube/demo/boot.ts`'s demo sample (`runLoad` of the synthesize plan), and `query/demo/main.ts`'s warehouse
  seed (`runLoad`) and warehouse sign-in by password (`warehouse.ts`'s driver). Pinned meanwhile, shrink-only, by
  `//tools/deps:app_runs_test` (`PAGE_REACHES`, and their sites in `CENSUS`).
- **The final audit's backlog** (recorded, not fixed):
  - The planner's server clients keep a protected generic POST (`legend/planning-client.ts` `json()`,
    `pure-v1-planning.ts` `request()`), so "no execute" holds by the method's name alone: an adversarial route only.
  - The headless bridge's connection (`datacube/src/headless/connection.ts`, reached by `hostConnection`) does not call
    `sending()`. That is right in the V8, which keeps no frame, but a Node test of `hostDatabase` would send outside a
    run.
- **The bundle-inputs round's** (recorded, not fixed): esbuild also searches upward for `tsconfig.json` from each
  input's REAL path. A bundle could read a tsconfig another action copied into bazel-bin, and its compile settings would
  depend on build order. No such file is there today (engine-client has none; DataCube, Query and Studio declare theirs
  in `:src`), but nothing guards it, and the metafile (`//tools/deps:bundle_inputs_test`) does not list a tsconfig read.
- **The independent re-check's** (recorded, not fixed): its backlog is in `docs/records/c5-t-recheck-2026-11-15.md`.
