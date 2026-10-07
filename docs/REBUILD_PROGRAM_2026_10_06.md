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

1. **The system metamodel stays in Pure** (the user: its model, mapping and store are the platform building on
   itself). What changes is where its **result views** load. Those are the parts added on 2026-09-03 (harness
   burn-down batches 18 and 20, under the 2026-09-02 ruling that the database, not Java, answers what a test reads):
   the tables, mapping and small classes that let Pure read the results of `executionPlan`, `execute`,
   `scanRelations` and `scanColumns` (plan nodes and connections, activities, lineage trees), and our versions of
   three upstream functions over those rows (`allNodes`, `routerExtensions`, `relationTreeAsString`).
   - **Everything they describe is declared upstream in two engine modules**, `core` and `core_relational`. So they
     **load in a program whose manifest includes those two modules** (the corpus's does) and never at boot. A user's
     program has no manifest and gets the default world, which holds none of those functions, so its world never needs
     the engine classes those views name, and no list of ours feeds the default-world generator.
   - **The classes stay upstream's**, used as they are; the views only say where their instances' data lives. They
     write no data at boot or ever: the rows ride each query (`PlanRows`, `LineageRows`, the activity rows).
   - **The three functions** exist upstream too. Which version runs is decided by the implementation table, by
     function id: each has a row saying "the platform's version", here our Pure over the rows. That needs one more
     kind of row, **the platform's own Pure**, which the system metamodel's other upstream-named functions
     (`classMappingById`, `mainTable`, …) also use; today a name-based rule does this, and Phase 3 replaces it.
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
- Only `build/rebuild`'s 8 commits (decided 2026-10-06: keep the critical path short). The old Bazel plan's held
  work stays parked, untouched, until Phase 8: `bazel/exec`'s 14 Phase 4 commits (DataCube's browser checks as Bazel
  tests, servers exiting with their parent, `//datacube:dist` complete, and the rest), its small uncommitted harness
  fixes, and the unfinished P3-25 (the stress corpus through legend-engine, the wrong-rows tool).
- IN_FLIGHT on main: the old plan's batch 8 is already on main (`053e15006`), so nobody waits on it; its remaining
  work lives in this program; the P4-18 announcement (servers exit with their parent) is parked with Phase 8.
- Check: rebased on main; `bazel build --nobuild --config=bazel10 //...`; `//gates:local` with
  `--lockfile_mode=error`; the compile-only guard; the clean `//:java` timing; a full throwaway CI (MODULE and
  `.bazelrc` changed); an audit agent's review. Then the PR, with the user's go.

### Phase 1: generator hygiene (no behavior change)
- Delete the dead generators; mark on-demand tools and reports `manual`; build outputs `testonly`; the PCT adapter's
  constant jar timestamps; narrow every generator to what it reads; remove self-inputs (`GENERATORS.md` section 6,
  steps 1 to 3).
- The small upstream generators upstream-only: fixtures, manifest, vocab, `ref_imports` (a committed report, proven
  deterministic), the reachability census split (upstream half committed, worklist on demand), CORE_IMPORTS in its
  own file generated from `CompileContext` alone.
- Check: every diff test and `//:generated`; two runs give the same bytes, on macOS, Linux and Windows; the everyday
  gate.
- **Status (2026-10-06): done locally on `build/phase1-generators` (14 commits, stacked on PR #25).** Every changed
  generator's output is byte-identical; `//gates:local` and `//:generated` (290) and `bazel build //...` pass on
  macOS; the PAR and `pmcd-reachable.tsv` are the same bytes across two runs. Decided on the way (the user):
  `ref_imports` deleted, not committed (a one-time measurement, its finding recorded); the catalog generator split
  (option B, 3 libraries). Corrections to GENERATORS.md: `//datacube:offer_queries` is the input of the committed
  offer-facts.ts generator, not test data (left non-testonly); `//datacube:catalog_corpus` is DataCube's test
  expectations (real DuckDB's answers), not a measurement; `test_imports` had no npm inputs to narrow. Deferred:
  engine-tree subset filegroups (sandbox inputs only), the parity tests' unread data (Phase 8), the PAR's permanent
  entry-time test; the offer-facts chain still reads its own committed output (emit_offer_queries' `:src`; early
  cutoff keeps it to one Node action with the same bytes); `//spec:eager_corpus_compile` still declares the pure
  tree it no longer reads (a set-aside measurement, narrowed with that group); the link-dictionary tool's runtime
  check (a small `makeDictionary()` test, now that the build no longer runs it). The audit (2026-10-06): ready
  after fixes, all made.

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
- **Done (2026-10-06):** `docs/build-inventory/manifest-world/experiments/phase2b/README.md`. Upstream core adds 45
  extra overloads (6 native); one, `collection::get(T[*], String)`, causes every change (corpus, PCT), because our
  compiler sums overload scores where legend-pure ranks parameter by parameter. Without it, all six corpus passes
  and all PCT suites are identical. Phase 3 ranks overloads as legend-pure does. (This note first said the reference
  lane's 745 `OVERLOAD` rows share that root; the Phase 3 pre-experiment showed they do not, below.)

### Phase 3: one table decides, by exact function id (core compiler)
Agreed with the user 2026-10-06. Today the compiler collects a call's candidates by name, drops some by name (the PCT
rule and the platform-owned list in `FunctionCompiler`), ranks the rest by adding per-parameter scores, then asks
`ImplementationTable` how the winner runs; language forms, `TdsLegacy` and `groupBy`'s `agg` are recognized by spelling.
After: candidates and implementations come from the table by `FunctionId`, and the ranking is legend-pure's. One batch
on a branch, one CI run, then pushed to main.
1. **Ranking as legend-pure's `FunctionMatch`.** The parameters' type matches left to right, then their multiplicity
   matches; the first difference decides. Per parameter, legend-pure's measure: an exact type, then the nearer parent
   class (hierarchy distance), then a type parameter; `Any` is a concrete class; multiplicity by upper-bound distance,
   then lower. `InferenceKernelTest.overload_incomparableSignaturesAreAmbiguous` changes to legend-pure's answer.
   Target: the reference lane's 79 class-hierarchy rows.
2. **Candidates by id.** Every function with the call's name is a candidate; both by-name drops are deleted. A built-in
   with exactly an upstream function's id is that function: ours runs, upstream's copy is not a second candidate. Every
   other upstream version of a name we implement needs its own row, usually "runs as built-in X"; a version without one
   is refused (a call to it fails before anything runs, naming it), never run from upstream's Pure body: for a function
   the platform implements, upstream's body is the spec (the PCT rule's own lesson: upstream's `or`/`and`/`max` bodies
   produced wrong SQL). Names the platform does not implement keep running their bodies. W2.1's id checks come along:
   two upstream declarations with one id are an error, and a collision guard on the id (it uses short type names).
   Rows added now: the versions the corpus, PCT and the reference lane call (the reference lane's 617 need 14). A
   ratchet counts the upstream versions of our names without a row (184 at most) and only goes down.
3. **Forms, TDS functions, the boot layer's versions and helpers, by id.** A call is a form only when its name resolves
   (through the imports) to a full name the form owns (`CoreFn`'s ownership list); a short name that could mean a
   form's function and another function is refused, naming both; `^Class(...)` stays syntax. `TdsLegacy`'s 17
   functions get rows by id. The boot layer's own versions of 29 upstream names get rows (ours runs). `GroupByChecker`
   recognizes `agg` by resolved id.
4. **Check and land.** IN_FLIGHT on main first, every core file listed. During the work only the touched targets; at
   the end, once: the full local gate, the six corpus passes, all PCT suites, the reference lane report. Bar: corpus and
   PCT identical (a difference fixed, or explained to the user, before landing); the reference lane's `AGREE` up and the
   new report committed. Auditor, fixes, one CI run, push to main.
- Not here: the 49 typing rows (Phase 3b); the default world (Phase 4, before which the rest of the 184 rows come);
  `Pure.java`'s signature text (Phase 5).
- This takes over W2.1's `ids` and `catalog` items from the parked compiler plan (`docs/EXECUTION_PLAN_2026_09_26.md`):
  said so in IN_FLIGHT and in that plan, so nobody redoes them.
- **Pre-experiment (2026-10-06):** `docs/build-inventory/manifest-world/experiments/phase3-ranking/README.md`.
  legend-pure's ranking rule changes nothing on today's world (local gate 289 of 290, the one failure a unit test
  asserting the old rule's tie; corpus identical; PCT 17 of 17; reference lane byte-identical) and fixes Phase 2b's
  `get`. The 745 `OVERLOAD` rows are not the ranking rule: 617 are calls whose legend-pure overload our compiler
  drops by name (this phase's suppression removal brings them back), 79 are class-hierarchy choices (legend-pure
  measures hierarchy distance), 49 are numbers and optional values, likely argument typing.

### Phase 3b: match legend-pure on upstream's own code (core compiler)
Agreed with the user 2026-10-06: after Phase 3, before Phase 6. Every function legend-pure types, we type, and every
call picks the same function with the same type as legend-pure, whether the fix is in overload logic or in typing. That
includes engine code the platform never runs (protocol translation, the engine's SQL generator): a body we cannot type
is a compiler gap that user code can hit too. Running that code stays refused, as today.
- Where it starts (today's reference lane, core_relational): 830 calls disagree (745 other version, 14 other package,
  32 drift, 39 property read against call); 68,643 positions the lane cannot match (a form, a property read or a
  rewrite where legend-pure has a call); 15,938 calls of ours with no counterpart; 1,508 bodies we fail to type, 1,335
  of them typed by legend-pure, whose calls are not compared (649 are the protocol translation layers, about 40
  functions in each of 16 protocol versions; JSON format 78; model-to-model 71; the engine's SQL generator 72); 32
  files dropped; types not compared at all.
1. Complete the measurement: legend-pure's type at every call, compared with ours (the parked plan's W1.1b); form
   nodes joined to the functions they stand for (Phase 3 gives every form its ids); why each body fails and why each
   file is dropped.
2. The causes, with counts, shown to the user before any fix.
3. Fix by cause, biggest first, each measured by the lane.
4. Done: every call the same function and type as legend-pure; every body legend-pure types, typed; every file loaded;
   anything left written down with its reason and approved by the user; corpus and PCT identical or better at every
   step.
- Takes over the parked compiler plan's items it covers (W1.1b and the typing work): said so in IN_FLIGHT and in that
  plan. The biggest phase; step 2 sizes it.

### Phase 4: the default world from upstream (replaces `PreludeGenerator`)
- The generator: upstream core whole (legend-pure `platform*` and engine `core_functions_*`, tests stripped by
  upstream's own markers), plus upstream's query surface (the handler registrations of the core compiler and the
  relational extension, and the classes those modules instantiate), closed through declarations and the bodies of
  runnable functions; the m3 metamodel printed from upstream's m3 graph, as today's generator does. (Upstream marks
  tests with stereotypes; the `::tests::` package rule is its naming convention, used alongside.)
- Its inputs: the pinned archives and the module choice. No Java scan, claims, hand enums, path lists or exclusions.
- The result views move off startup (decision 1): they load with the `core` and `core_relational` modules, which the
  corpus's manifest already includes (Phase 6 comes first for that reason).
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

### Phase 6: the corpus on its real manifest (runs before Phase 4)
- First: re-run experiment 8 against today's prelude (it ran on the new default world), since this phase now lands
  before Phase 4.
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
- The old Bazel plan's parked work: `bazel/exec`'s Phase 4 commits and fixes, and P3-25.
- The measurement group, set aside on 2026-10-05: the corpus judges as cached build actions, the ratchets, the
  ladder. Decide what each is and carve it by its true trigger.
- Last: the renames (D11: core, db, sdlc, depot; D12: depot its own server).

## 4. Order and what can move

**Order: 0, 1, 2, 2b, 3, 3b, 6, 4, 5, 7**, with Phase 8's items interleaved where they do not touch the same files.
- 2b informs 3's design; 3 needs 2's tables.
- 3b needs 3 (forms carry ids, so the lane can join them) and runs before 6 and 4, so the real manifest and the
  upstream world arrive on a compiler that matches legend-pure.
- 6 needs 3 (the table owns what the manifest's files redefine) and runs before 4, so the result views can move off
  startup at the moment the default world changes, with nothing temporary in between.
- 4 needs 3's ownership and 6's manifest; 5 needs 4's world; 7 needs all.

## 5. Working on this program (for any session that picks it up)

**Process (the user's rules):**
- Plan each phase and get the user's agreement before writing code; explain plainly, without jargon; never invent a
  new mechanism when an existing one (the implementation table, modules, manifests) does the job.
- Prove locally first; throwaway CI only on the lanes a change touches; one PR per phase (or a few).
- Core edits: announce in `docs/IN_FLIGHT.md` on main first. Bazel changes: an audit agent reviews before a push or
  PR.
- Never write bare "native": "upstream native" (upstream's keyword) or "platform-lowered" (our Pure.java).
- No local paths (home directories, temp directories) in anything committed; scrub evidence copied from scratch.

**The experiment harness** (`docs/build-inventory/manifest-world/experiments/`, README): how to synthesize a world,
swap `prelude.pure` without code changes (first on the classpath; `-Xbootclasspath/a` through `JAVA_TOOL_OPTIONS` for
Bazel tests), rerun each corpus pass's exact Bazel command by hand (`e6_lanes.py`), compile the user side
(`UserSideProbe`), and time the browser (`bazel run //wasm:startup`).

**Pitfalls already hit (each cost a rerun):**
- Segmenting upstream Pure files: a doc string belongs to the element below it; keywords at a line start inside a doc
  string or block comment are prose; names come after every `<<stereotype>>` and `{tagged value}`, however long
  (`docstart.py` does all three).
- Upstream's test markers are the test stereotypes and `::tests::` packages; `::test::` packages hold upstream's test
  infrastructure, which ordinary code references.
- An ownership filter has to cover the functions Pure.java implements, `CoreFn`'s forms (registered by bare name),
  the system metamodel's own versions, `TdsLegacy`'s Java-implemented functions, and the helpers forms recognize by
  spelling (`agg`, `col`); missing any one breaks hundreds of tests.
- Running a hand-made corpus command after another Bazel command: re-run a cached `bazel build` of the corpus
  targets first, or the execution root lacks the upstream trees ("legend-engine checkout not present").
- In zsh, `echo ====` fails (`=` expansion), and unquoted `$VAR` holding several paths is one word.
