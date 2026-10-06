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
| Ours | hand-written decisions and code: the implementation table (what we implement and how, keyed by function id), legend-lite's own declarations (`meta::legend::lite`), the system metamodel (its core views load always; its harness views join a program whose world has their classes) | people, when they decide something |
| Generated | made from upstream and one setting, only by the bump, committed and sealed: the default world (what `prelude.pure` becomes) and the other upstream tables (DynaFn, handlers, core imports, the bump's records and reports). The one setting is the module choice (the core and relational compiler modules count), kept in the bump's configuration beside the pins | only the bump |

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

## 2. Decisions (the user, 2026-10-06)

1. **The system metamodel splits (option B):** its harness views (execution-plan nodes, plan connections, execution
   activities, lineage results, and the functions over engine machinery) join a program's layer only when that
   program's world declares their classes; the core views load always. No list of ours feeds the default-world
   generator. Those views write no data at boot or ever: their rows ride each query (`PlanRows`, `LineageRows`).
2. **Boot speed: profile and optimize the boot first**; the pre-built boot layer (Phase 4b, the parked compiler plan's
   W2.1 "generated at build time, not parsed at class load") is decided with measured numbers.
3. **The product ships a generated copy of upstream bodies** (the default world). AGENTS.md and TENET_CHARTER C6.3
   are amended in Phase 4 to match WORLD_MAP rule 2 (amended 2026-09-08).
4. **Bazel changes are reviewed by an audit agent** before a push or PR (this session is the Bazel program).
5. **D2: native-claims.tsv, `core_next` and `gen_claims` retire** (Phase 5).

## 3. The phases

Every core-changing phase is announced in `docs/IN_FLIGHT.md` on main first (standing authorization). The checks
named "the experiment harness" are `docs/build-inventory/manifest-world/experiments/`, promoted to tests where a
phase changes the world: the user side (56 projects as one graph, 3 demos, every body type-checked), the six corpus
passes (every roster, register, ledger and verdict) and every PCT case.

### Phase 0: land the build foundations (done locally on `build/rebuild`)
- What: the build targets (`//:java`, `//:web`, `//:wasm`, `//:native`, `//:sites`) and the compile-only guard;
  product jars through `http_jar` (Postgres 42.7.13 without checker-qual); `//:web` without Node (native esbuild);
  stamping off.
- First: reconcile the held `bazel/exec` work (its unpushed Phase 4 commits, P3-25, the `datacube:dist` fix
  `dab833263`) and the old Bazel plan's "batch 8", which IN_FLIGHT says the database owner and DataCube+Python lines
  wait on. They touch the same BUILD files as Phases 0 and 1.
- Check: rebased on main; `//gates:local`; a full throwaway CI (MODULE and `.bazelrc` changed); an audit agent's
  review.

### Phase 1: generator hygiene (no behavior change)
- Delete the dead generators; mark on-demand tools and reports `manual`; build outputs `testonly`; the PCT adapter's
  constant jar timestamps; narrow every generator to what it reads; remove self-inputs (`GENERATORS.md` section 6,
  steps 1 to 3).
- The small upstream generators upstream-only: fixtures, manifest, vocab, `ref_imports` (a committed report, proven
  deterministic), the reachability census split (upstream half committed, worklist on demand), CORE_IMPORTS in its
  own file generated from `CompileContext` alone.
- Check: every diff test and `//:generated`; two runs give the same bytes, on macOS, Linux and Windows; the everyday
  gate.

### Phase 2: upstream tables joined at class init
- DynaFn.java generated whole from upstream (with its `Dialect` enum); a hand `DynaFnDecisions.java` holds the
  resolutions.
- `engine-handlers.tsv` emitted whole from upstream; `EngineHandlers` keeps a row's FQN at class init only when the
  platform declares it, then appends the Lite surface.
- Check: `DynaFn.values()` and `fqnsOf` identical before and after, for every name.

### Phase 2b: the end-state experiment (before Phase 3 commits to its design)
- Every experiment so far ran the new world with Pure.java's catalog and without upstream's declarations of the
  functions we implement. The end state is the opposite: every upstream declaration in the world, no catalog, the
  implementation table deciding. Upstream has 184 overloads at names we implement that we do not, so overload
  resolution can change (a call binding to an overload that is not implemented).
- Run that configuration through the experiment harness (emulated, as before) and record every change it causes.

### Phase 3: the implementation table switched on (core compiler)
- `ImplementationTable` (over `DeclarationTable` and `Registrations`) becomes the one authority at boot and at module
  load, keyed by `FunctionId`. The compiler's name-based suppressions go (function shadowing by name, the
  platform-owned checks); the generator's drops go in Phase 4.
- This takes over W2.1's `ids` and `catalog` items from the parked compiler plan (`docs/EXECUTION_PLAN_2026_09_26.md`):
  said so in IN_FLIGHT and in that plan, so nobody redoes them.
- Folds in what the experiments found: `CoreFn`'s bare-name forms become rows; `TdsLegacy`'s Java-implemented
  functions become rows; the system metamodel's own versions become rows; forms recognize their helpers
  (`agg`, `col`) by resolved id, never by spelling.
- Check: today's prelude is unchanged in this phase, so the experiment harness must be identical, and the table's
  shadow diff zero. The riskiest phase; several PRs.

### Phase 4: the default world from upstream (replaces `PreludeGenerator`)
- The generator: upstream core whole (legend-pure `platform*` and engine `core_functions_*`, tests stripped by
  upstream's own markers), plus upstream's query surface (the handler registrations of the core compiler and the
  relational extension, and the classes those modules instantiate), closed through declarations and the bodies of
  runnable functions; the m3 metamodel printed from upstream's m3 graph, as today's generator does. (Upstream marks
  tests with stereotypes; the `::tests::` package rule is its naming convention, used alongside.)
- Its inputs: the pinned archives and the module choice. No Java scan, claims, hand enums, path lists or exclusions.
- The system metamodel splits (decision 1): its harness views join a program's layer when that world declares their
  classes. It needs Phase 3: the corpus may then load `scanRelations.pure`, which declares `RelationTree` and which
  the runner excludes today only because nothing owns functions by id.
- AGENTS.md and TENET_CHARTER C6.3 amended (decision 3).
- Boot speed (decision 2): profile and optimize the boot first; Phase 4b, the pre-built boot layer, decided with the
  measured numbers (+0.6 s in the browser before any optimization).
- Check: the experiment harness: projects' body walls 146 to 0 (`orElse`), corpus and PCT identical, the demos'
  queries executed, not only type-checked; boot times recorded.

### Phase 5: Pure.java as rows keyed by function id (the catalog goes)
- No signature text: every row names an upstream function id and its implementation. Declarations come from the
  world (the default world for users, the program's own files for the corpus and PCT).
- Retires: the membership list and its draft, `native-claims.tsv` with `core_next` and `gen_claims` (D2), the
  natives generator.
- A test checks every row against the pinned archive: a row matching nothing fails and lists that name's real ids.
- Check: the experiment harness identical. Several PRs (837 signatures, and every registry that names them).

### Phase 6: the corpus on its real manifest
- The runner loads its manifest's repositories (the relational tree's 9 repositories and their closure, 38) with the
  loading rule, replacing `LIBRARY_FILES`, `SHAPE_FILES` and the folder lists. The H2 register gains its one entry.
- The compiler gaps the real manifest exposed: the parser (`;` as a property-mapping separator, `->` where we reject
  it), units of measure (a `Measure` as a type), `routeFunction`'s resolution, duplicate view functions (F-L1 in
  `projects/FINDINGS.md`, "a view inside a Schema is lifted twice", which also removes the projects' 4 build walls).
- PCT's own file composition reviewed the same way (it passed unchanged, but was never examined).
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
- The measurement group, set aside on 2026-10-05: the corpus judges as cached build actions, the ratchets, the
  ladder. Decide what each is and carve it by its true trigger.
- Last: the renames (D11: core, db, sdlc, depot; D12: depot its own server).

## 4. Order and what can move

Phase 0 first. Phases 1 to 7 are a chain: 2b informs 3, 3 needs 2's tables, 4 needs 3's filter, 5 needs 4's world, 6
needs 3 and 5, 7 needs all. Phase 8's items are independent of that chain and can interleave where they do not touch the same
files.
