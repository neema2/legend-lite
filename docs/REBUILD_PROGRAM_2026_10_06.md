# The program: an amazing build with a self-contained upgrade bump (2026-10-06)

Status: the user, 2026-10-06: "we should do the whole thing here, in phases that make sense - let's get it fully fully
done". Not one PR: each phase lands as its own PR (or a few), proven locally first, with throwaway CI on the lanes it
touches (a full run when MODULE, toolchains, `.bazelrc` or workflows change).

This file is the program's one plan. The decisions and evidence behind it:
- `docs/BUILD_REBUILD_DESIGN_2026_10_05.md`: the build (decisions D1 to D15);
- `docs/GENERATORS.md`: every generator, its axes, the bump's records and the seal;
- `docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md`: each upstream generator's inputs and its upstream-only design;
- `docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md` and `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`: the default world,
  measured.

## 1. The north star

**Three kinds of files, never mixed:**

| Kind | What it is | Who changes it |
|---|---|---|
| Upstream | the pinned legend-engine and legend-pure archives: every declaration and upstream body | only the bump |
| Ours | hand-written decisions: the implementation table (what we implement and how, keyed by function id), the module choice for the default world (the core and relational compiler modules), legend-lite's own declarations (`meta::legend::lite`), the boot registration (the types our Java needs at boot) | people, when they decide something |
| Generated | made from upstream alone, only by the bump, committed and sealed: the default world (what `prelude.pure` becomes) and the other upstream tables (DynaFn, handlers, core imports, the bump's records and reports) | only the bump |

**How a program gets its world:**
- **User programs** (Studio, DataCube, Query, the server) boot on the default world. The implementation table joins
  it at boot by function id: a row says "ours" (an intrinsic or a language form), no row means upstream's body runs.
- **Test programs** (the corpus, PCT) load their manifest from the pinned archive on top: each element once, the
  implementation table applied, platform-namespace functions from the default world only. Test input, never product.

**The bump:** move the pins, regenerate from upstream alone, write the seal, run the tests, judge the diffs. If
upstream renamed something we implement, a row stops matching and a test says so; a person updates the row.

**Never:** a generator reading our code, a hand edit of a generated file, a signature we typed ourselves.

**The build:** "build" means compile only, guarded (`compile_only_test`); every generator, test and check runs on its
true trigger; no Node anywhere; one shape per app.

## 2. The phases

Every core-changing phase is announced in `docs/IN_FLIGHT.md` on main first (standing authorization). The checks
named "the experiment harness" are `docs/build-inventory/manifest-world/experiments/`, promoted to tests where a
phase changes the world: the user side (56 projects as one graph, 3 demos, every body type-checked), the six corpus
passes (every roster, register, ledger and verdict) and every PCT case.

### Phase 0: land the build foundations (done locally on `build/rebuild`)
- What: the build targets (`//:java`, `//:web`, `//:wasm`, `//:native`, `//:sites`) and the compile-only guard;
  product jars through `http_jar` (Postgres 42.7.13 without checker-qual); `//:web` without Node (native esbuild);
  stamping off.
- Check: rebased on main; `//gates:local`; a full throwaway CI (MODULE and `.bazelrc` changed); the Bazel review the
  standing rule asks for.

### Phase 1: generator hygiene (no behavior change)
- Delete the dead generators; mark on-demand tools and reports `manual`; build outputs `testonly`; the PCT adapter's
  constant jar timestamps; narrow every generator to what it reads; remove self-inputs (`GENERATORS.md` section 6,
  steps 1 to 3).
- The small upstream generators upstream-only: fixtures, manifest, vocab, `ref_imports` (a committed report, proven
  deterministic), the reachability census split (upstream half committed, worklist on demand), CORE_IMPORTS in its
  own file generated from `CompileContext` alone.
- Check: every diff test and `//:generated`; two runs give the same bytes; the everyday gate.

### Phase 2: upstream tables joined at class init
- DynaFn.java generated whole from upstream (with its `Dialect` enum); a hand `DynaFnDecisions.java` holds the
  resolutions.
- `engine-handlers.tsv` emitted whole from upstream; `EngineHandlers` keeps a row's FQN at class init only when the
  platform declares it, then appends the Lite surface.
- Check: `DynaFn.values()` and `fqnsOf` identical before and after, for every name.

### Phase 3: the implementation table switched on (core compiler)
- `ImplementationTable` (over `DeclarationTable` and `Registrations`) becomes the one authority at boot and at module
  load, keyed by `FunctionId`. The FQN-level suppressions go (the claims-based drops, the platform-owned list,
  function shadowing by name).
- Folds in what the experiments found: `CoreFn`'s bare-name forms become rows; `TdsLegacy`'s Java-implemented
  functions become rows; the system metamodel's own versions become rows; forms recognize their helpers
  (`agg`, `col`) by resolved id, never by spelling.
- Check: today's prelude is unchanged in this phase, so the experiment harness must be identical, and the table's
  shadow diff zero. The riskiest phase.

### Phase 4: the default world from upstream (replaces `PreludeGenerator`)
- The generator: upstream core whole (legend-pure `platform*` and engine `core_functions_*`, tests stripped by
  upstream's own markers), plus upstream's query surface (the handler registrations of the core compiler and the
  relational extension, and the classes those modules instantiate), closed through declarations and the bodies of
  runnable functions; the built-in m3 section; the boot registration.
- Its inputs: the pinned archives and the module choice. No Java scan, claims, hand enums, path lists or exclusions.
- Decision inside the phase: the browser start-up (+0.6 s measured). Accept it, or add Phase 4b: pre-build the boot
  layer at build time (resolve, normalize and index are 55% of the browser's first answer).
- Check: the experiment harness: projects' body walls 146 to 0 (`orElse`), corpus and PCT identical; boot times
  recorded.

### Phase 5: Pure.java as rows keyed by function id (the catalog goes)
- No signature text: every row names an upstream function id and its implementation. Declarations come from the
  world (the default world for users, the program's own files for the corpus and PCT).
- Retires: the membership list and its draft, `native-claims.tsv` with `core_next` and `gen_claims` (D2), the
  natives generator.
- A test checks every row against the pinned archive: a row matching nothing fails and lists that name's real ids.
- Check: the experiment harness identical.

### Phase 6: the corpus on its real manifest
- The runner loads its manifest's repositories (the relational tree's 9 repositories and their closure, 38) with the
  loading rule, replacing `LIBRARY_FILES`, `SHAPE_FILES` and the folder lists. The H2 register gains its one entry.
- The compiler gaps the real manifest exposed: the parser (`;` as a property-mapping separator, `->` where we reject
  it), units of measure (a `Measure` as a type), `routeFunction`'s resolution, duplicate view functions (which also
  removes the projects' 4 build walls).
- Check: the six corpus passes identical apart from recorded improvements; the projects' build walls 4 to 0.

### Phase 7: the self-contained bump
- `//:update_upstream` (the upstream records only, checked to be a fixed point); the reports committed; the seal and
  its everyday test; `//:update_generated` becomes ours only and `manual`; `bazel run //tools/bump -- <release>` runs
  decide, move, regenerate, reports, seal, test (nothing re-blessed first), and leaves the judging to a person.
- Acceptance: a real bump to the next legend-engine release, end to end, with no hand edit to any generated file.

### Phase 8: the rest of the build
- Tests and checks by their true trigger (checks as one aspect, not per-package reports); narrow dependencies.
- D8 (delete `ide` and `probe`), D9 (warehouse server paths as flags; runfiles jars out of the product), D10 (one
  shape per app), D5 (DataCube fixtures out of the site), D13 (the warehouse client), D15; the TS typecheck cleanup.
- Node out of the tests: a CDP client driving pinned Chromium.
- CI lanes from `//gates`, and caching (D14).
- Reconcile the held `bazel/exec` work (the Phase 4 commits, P3-25, the `datacube:dist` fix).
- Last: the renames (D11: core, db, sdlc, depot; D12: depot its own server).

## 3. Order and what can move

Phase 0 first. Phases 1 to 7 are a chain: 3 needs 2's tables, 4 needs 3's filter, 5 needs 4's world, 6 needs 3 and
5, 7 needs all. Phase 8's items are independent of that chain and can interleave where they do not touch the same
files.
