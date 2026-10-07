# The program: an amazing build with a self-contained upgrade bump (2026-10-06)

Status: the user, 2026-10-06: "we should do the whole thing here, in phases that make sense - let's get it fully fully
done". Each phase is proven locally first and lands on main from its own branch. **No PRs since 2026-10-06** (the
user: "I think we should not do PRs anymore so you can automatically push to main without my manual step"): an
audit, the local gate, one full CI run on the branch, then that exact commit pushed to main (Phases 0 and 1 landed as
PRs #25 and #26, before the change).

**A session picking this program up starts at `docs/build-inventory/program/START_HERE.md`**: where everything is,
the state, the working rules, how to run every check. That folder holds one execution brief per remaining phase
(files, steps, checks, traps, open decisions); this file stays the plan.

This file is the program's one plan. The decisions and evidence behind it:
- `docs/BUILD_REBUILD_DESIGN_2026_10_05.md`: the build (decisions D1 to D15);
- `docs/GENERATORS.md`: every generator, its axes, the bump's records and the seal;
- `docs/UPSTREAM_ONLY_HOMEWORK_2026_10_05.md`: each upstream generator's inputs and its upstream-only design;
- `docs/MANIFEST_WORLD_HOMEWORK_2026_10_05.md` and `docs/MANIFEST_WORLD_EXPERIMENTS_2026_10_06.md`: the default world,
  measured.

## 0. What the whole program is for

Written 2026-10-07 at the user's request ("add the actual overall goal of this program not just phase 3 ... so we have
holistic view of what we are trying to accomplish"). Each part quotes the user's own words, with the date they were
said. The program has four parts, in this order, each built on the one before; after the bump lands come the
compiler's debts (0.5).

### 0.1 A first-class Bazel build, starting with a fast compile of only the product (the base; first deliverable done)

**The goal.** An expert, principled Bazel build. Bazel knows every step: what it reads and what it makes, so Bazel
alone decides, deterministically, what runs and when. Nothing outside Bazel drives the work: no shell script, no Java
or Python program, no Node. As hermetic as Bazel allows: every tool and every piece of data pinned, nothing read from
the machine (no host JDK, no hard-coded paths, not even PATH), the same bytes on Linux, macOS and Windows. Its base is
a very fast compile of only the real product.
- The user, 2026-10-03: "the build needs to be principled and bazel first class - not a hodge podge of random scripts";
  "there are java code orchestrators like Repo.java that run outside of bazel?"; "at end of whole plan will we have
  deleted all non-bazel orchestration like calling out to shell scripts or java?"; and Windows must not break for the
  people who use it.
- The user, 2026-10-05: "our java build used to take 30 seconds for clean build now saying 5-10 min - we first have to
  fix the basic basic stuff - a single java build that makes sense and is fast"; "building all of our java stuff should
  be around 30 seconds so how do we carve up all the rest of everything correctly in the right way to get to a sane
  build? I don't want any guessing or sampling".

**What "the product" is.** The servers: core (the compiler and the execution pipeline), the warehouse, and the SDLC
server, which carries Depot's rules until Depot becomes its own server (design D12; D11 renames them core, db, sdlc,
depot). Then the web bundles (DataCube, Query, Studio), the planner compiled to WebAssembly (TeaVM), and the
warehouse's native image. Generators, tests, checks and measurements are not the product build.

**How the first deliverable was built (Phase 0, PR #25, merged 2026-10-06 as `ab4723ba2`).**
- The inventory first, no sampling: all 1,162 targets from Bazel's own graph, each with one verdict (compile, tool,
  generator, check, test by kind, wiring, dead), every claim citing a file and line (`docs/build-inventory/`, design
  §2).
- Named compile targets (design §4.1, §5a): `//:java` (every runtime jar of the servers, computed from their
  dependencies), `//:web` (8 esbuild bundles; no Node: Bazel calls the esbuild program directly), `//:wasm`,
  `//:native`, and `//:sites` (packaging).
- A guard keeps them honest: `//tools/guards:compile_only_test` fails if anything but compiles and file plumbing enters
  them (proven to fail on a generator put into `//:native`).
- The slow clean build's cause, found by measuring: the Maven plugin stamped the DuckDB jar's manifest and copied it
  (17.4 of the 19.7 s critical path). The product's jars moved to Bazel's own `http_jar` (pinned by URL and sha256),
  and stamping went off.
- Result (design §5a-§5c; macOS, clean, three runs): **a clean `//:java` in 12.4 s** (22.6-23.1 s before; the
  critical path is now our own code, the compiler's jar); a one-line comment edit to `Compiler.java` rebuilds in 0.44 s
  (one library: its interface did not change, so nothing above it recompiles); a clean `//:web` in 4.8 s.

**What is left of this part (Phase 8; `docs/build-inventory/program/PHASE_8.md`).** "Build means compile" holds for
the compile targets, not yet for CI: CI's build lane runs `bazel build //...`, which also runs the corpus passes and
the 2 GB `//pct:ratchets`. Tests and some generators still run on Node. Scripts outside Bazel still drive work (the
SQL census's `lanes.sh` and `render.sh`, P3-25's corpus runner, scripts that read a host JDK: the brief's Short-22,
Short-25, Exec-2). Checks run as reports in every package. CI's cache barely works. The build's own carried shortcuts
are listed in the brief's §2.8.

### 0.2 The bump as a standalone piece (Phases 1 to 7: the main body)

**The goal.** Moving to a new upstream release (legend-engine and legend-pure) is one self-contained job: it runs only
when we move the pins, reads only upstream's files, regenerates everything made from upstream, seals it and runs the
tests; a person judges what changed. Nothing else ever regenerates an upstream-derived file, and no upstream
generator reads our code.
- The user, 2026-10-05: "we def need to keep the generated code only generated on upstream bump".
- The user, 2026-10-06: "I want the bump to be a self contained thing that only runs when need to bump"; "So how do
  we actually get to self contained bump for real?"; "We still need the java override list? And the generators still
  need our list as input"; "let's get it fully fully done so we have an amazing build with self contained upgrade
  bump".
- The user, 2026-10-07: "really want to get this bump self contained program done".

**Why it takes more than build work.** The upstream generators took our own lists as input (Pure.java's signature
catalog, the "override list"; the prelude generator's hand lists; `native-claims.tsv` with `core_next`), and the
compiler decided some calls by name, so the bump could not run from upstream alone. Getting there restructures how
the product boots and what it reads from generated upstream code, and fixes the compiler where it differs from
legend-pure:
- Phase 1 (done, PR #26, `96bf6ae5d`): generator hygiene; the small upstream generators made upstream-only.
- Phase 2 (done, `ff70aef01`): DynaFn and the engine handlers generated whole from upstream; our decisions joined when
  the classes load.
- Phase 2b (done): the end-state experiment (every upstream declaration in the world, no catalog).
- Phase 3 (on branch `build/phase3`, not landed): one table decides, by exact function id; overloads ranked as
  legend-pure ranks them.
- Phase 3b: the compiler fixes the bump and users need (the boot layer's twin files, the mapping files,
  `routeFunction`, F-L1).
- Phase 6: the corpus loads its real manifest instead of our hand lists of files.
- Phase 4: the default world (what users boot on) generated from upstream alone, replacing `PreludeGenerator`; the
  system metamodel's result views leave startup.
- Phase 5: Pure.java becomes rows keyed by function id; the catalog, the membership list, `native-claims.tsv`,
  `core_next` and `gen_claims` go.
- Phase 7: `bazel run //tools/bump -- <release>` decides, moves the pins, regenerates, writes the reports and the
  seal, and runs the tests. **Acceptance: a real bump to the next legend-engine release, end to end, with no hand edit
  to any generated file.**

The target shape is §1, the north star (three kinds of files, never mixed). One open point bounds "only upstream's
files": a generator that reads upstream through our parser (Pure.java's generator today; the default world's
generator, the Phase 4 brief's D4-5) makes a parser change a real trigger too (design §4.2 group B), and the seal has
to say so.

### 0.3 Our own generators, separate from the bump (Phases 1 and 7, then Phase 8)

**The goal.** Generators that have nothing to do with the bump (made from our own code or from pinned data) run only
when what they read changes, as Bazel actions with exact inputs. Things that only look like generators, measurements
of our own engine (corpus results, ratchets, the ladder), become tests.
- The user, 2026-10-05: "we need to split the generation work into 'generators that are needed for version bump' vs
  'our generators that have nothing to do with version bump at all' vs 'tests that look like generators'"; "Why is
  relational corpus in generators at all?" and then "relational corpus should be tests"; "I don't think we should make
  update generated manual until we go through in detail exactly what all the generators are doing".

**Where.** `docs/GENERATORS.md` places each of the 57 generators on two axes (what it is, when it runs), with a
dossier each in `docs/build-inventory/generators/`; design §4.2 groups them A to F by what truly changes them. Phase 1
narrowed every one of ours to what it reads (done). Phase 7 makes `//:update_generated` ours only and `manual`, with
a guard that every writer and diff test belongs to a group. The rest is the Phase 8 brief: §2.2 (Gen-1 to Gen-9: the
JavaScript generators off Node, the offer-facts tool reading its own output, drafts `manual`) and §2.9 (the
measurement group, each carved by its true trigger).

### 0.4 The tests, untangled (after the bump)

**The goal.** Each family of tests depends only on the code it exercises, so it reruns only when that code changes.
Each CI lane is a Bazel suite in `//gates`, and a guard holds that every test is in some lane. No Node: the JavaScript
tests and the browser checks run in the pinned Chromium through a small client for Chrome's remote-control protocol
(CDP). Comparing a run with a previous run, or with a sibling run, is done the Bazel way, not by scripts.
- The user, 2026-10-07: "various different tests like core, rcorpus, stress corpus, different flavors of PCT, parser
  equivalence, UI/playwrite, etc etc etc that we also want to untangle after".
- The user, 2026-10-05: "what tests do we have that should run when"; "what is the bazel first class native way to do
  run by run test compares either to a previous run or to sibling runs? What would expert do".

**The families today** (the lanes as `docs/GATES.md` on main lists them), and what tangles each (the Phase 8 brief's
item):

| Family | Targets | Tangled by |
|---|---|---|
| Core | `//core:core_tests` (gate 1), `//core:corpus_differential_test` | one `core_tests_lib` carrying the stress corpus and the linked projects (Test-1); heavy classes skipped by name, not by tag (Test-10) |
| Spec parity | `//spec:spec_tests` (gate 3) | one `spec_tests_lib` (Test-2) |
| The relational corpus ("rcorpus") | six passes, a host judge and a database judge on each of DuckDB (`//spec:corpus_duckdb`, gates 4 and 11), H2 (`//spec:corpus_h2`, gate 5) and the warehouse (`//spec:corpus_warehouse`, manual); the reference lane (`//spec:reference_lane`, manual) | the passes are build actions, so `bazel build //...` runs them (CI's build lane up to twice per platform); hand lists of files until Phase 6; the manual ones in no lane (Test-11) |
| The stress corpus | `//core:stress_suites`, `//core:stress_suites_h2` (gate 10); its generators `//scripts/corpus:gen_*` | one Python library for five gates (Gen-7); the engine-side runner P3-25 unfinished (Exec-2) |
| PCT, four flavors | `//pct:pct_duckdb` (five suites, gate 6), `//pct:pct_h2` (gate 7), `//pct:pct_postgres` (five suites, gate 7P), `//pct:pct_channel_b` (gate 9); `//pct:pct_discipline`; `//pct:ratchets` (manual, 2 GB) | Channel B builds the adapter archive it does not use (Test-4); CI's build lane builds `//pct:ratchets` |
| Parser equivalence | `//parser-equivalence:parser_parity` (gate 8); `:diagnostics` (manual) | `pe_tests_lib` depends on all of core (Test-3); test data no test reads (Short-6) |
| The model projects | 56 compile tests (`tools/legend`) | 56 JVMs on all of core (Test-5) |
| The app, WebAssembly, the warehouse | the app lane (`//datacube:tests`, `//datacube:verify_app_test`, `//wasm:all`, `//warehouse:tests`, `//query-store:lite_test`); the native lane (`//warehouse:tests_native`, `//warehouse:launcher_test`) | the native image inside `//gates:local` (Test-9); `postgres_live` in no lane (Test-8) |
| The UI and the browser | `//datacube:live_snap_test`, `//datacube:verify_smoke_test`; the Playwright harnesses (tagged `browser-ci`, run by `bazel run`, Linux only) | Node and Playwright throughout (Node-1 to Node-9); CI installs Playwright's own Chromium (Short-18) |
| Checks | `//:generated` and the guards (the checks lane) | a report in every package (Check-1 to Check-7) |

**Where and when.** The Phase 8 brief: §2.3 (Test-1 to Test-13), §2.5 (no Node), §2.6 (CI and caching), §2.9 (the
measurement group). It comes after the bump because the bump's phases change these tests: Test-1 waits for Phases 3b,
4 and 5 (they add core tests), Test-2 for Phases 6 and 5 (Phase 6 rewrites the corpus runner). **Not designed yet:**
the Bazel way to compare a run with a previous run or a sibling run. Today the corpus compares against committed
results through diff tests, while the SQL census (`tools/census/`) and the landing checks (START_HERE §5) compare two
commits by script and by hand. It needs a design agreed with the user.

### 0.5 After the bump lands: the compiler's debts

The shortcuts this program carries in the compiler are `docs/PARKED_WORK_LEDGER.md` rows PARK-5 to PARK-14 (§5). The
user, 2026-10-07: "we need a ledger of hacks that we need to come back a fix", a list "to fix correctly after we land
the bump". Two close inside the program (PARK-11 in Phase 4, PARK-12 in Phase 3b); PARK-5's timing is the user's open
decision.

### 0.6 Done means

- `bazel build //:java //:web` compiles and nothing else (the guard); a clean `//:java` in about 12 s; an edit that
  leaves a library's interface unchanged rebuilds that library alone. CI's build lane builds the compile targets, not
  `//...` (design §4.1).
- No work driven from outside Bazel: every generator, test and check is a Bazel target with exactly its inputs; each
  remaining script is a `bazel run` tool or gone; `bazel query` finds no Node toolchain.
- Nothing read from the machine; the same bytes on Linux, macOS and Windows.
- The bump: `bazel run //tools/bump -- <release>` from upstream alone; a real bump with no hand edit; the seal's test
  in every lane.
- Our generators run when our code changes; `//:update_generated` is ours only and `manual`; no "update everything"
  command re-blesses a measurement.
- Every test in a `//gates` lane, each depending on what it exercises, with a guard.
- The ledger's rows closed by agreed designs, their anchors gone.

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
     (`classMappingById`, `mainTable`, …) also use. (2026-10-07: Phase 3 replaced the name rule with hiding by function id, `SystemMetamodel.shadows`; the
     row kind itself is not built yet. Whether Phase 3b item 1 builds it, so that its acceptance can delete `shadows`
     (PARKED_WORK_LEDGER PARK-12), is the 3b brief's open decision 3b-O1, its one owner; Phase 4 needs it when the
     result views move.)
2. **Boot speed: profile and optimize the boot first**; the pre-built boot layer (Phase 4b, the parked compiler plan's
   W2.1 "generated at build time, not parsed at class load") is decided with measured numbers.
3. **The product ships a generated copy of upstream bodies** (the default world). AGENTS.md and TENET_CHARTER C6.3
   are amended in Phase 4 to match WORLD_MAP rule 2 (amended 2026-09-08); so are WORLD_MAP's own §3 ("declarations only
   ... no bodies"), rule 1 ("never loaded") and rule 2's first text and 2026-09-04 amendment, which still contradict it
   (`docs/build-inventory/program/PHASES_4_5_7.md` §2.9), and AGENTS.md's guard name (`Runner.registerLibrarySource`
   is now `MinimalCorpus.refusePlatformNamespace`).
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
   form's function and another function is refused, naming both; `^Class(...)` stays syntax. `TdsLegacy`'s 17 (18 in
   the code) functions get rows by id. The boot layer's own versions of 29 upstream names get rows (ours runs). `GroupByChecker`
   recognizes `agg` by resolved id.
4. **Check and land.** IN_FLIGHT on main first, every core file listed. During the work only the touched targets; at
   the end, once: the full local gate, the six corpus passes, all PCT suites, the reference lane report. Bar: corpus and
   PCT identical (a difference fixed, or explained to the user, before landing); the reference lane's `AGREE` up and the
   new report committed. Auditor, fixes, one CI run, push to main.
- Not here: the 49 typing rows (Phase 3b); the default world (Phase 4, before which the rest of the 184 rows come:
  109 remain after Phase 3, `implementation.unrowed`; each gets a row or a recorded refusal before Phase 4 lands, the
  Phase 4 brief's D4-12); `Pure.java`'s signature text (Phase 5).
- This takes over W2.1's `ids` and `catalog` items from the parked compiler plan (`docs/EXECUTION_PLAN_2026_09_26.md`):
  said so in IN_FLIGHT and in that plan, so nobody redoes them.
- **Pre-experiment (2026-10-06):** `docs/build-inventory/manifest-world/experiments/phase3-ranking/README.md`.
  legend-pure's ranking rule changes nothing on today's world (local gate 289 of 290, the one failure a unit test
  asserting the old rule's tie; corpus identical; PCT 17 of 17; reference lane byte-identical) and fixes Phase 2b's
  `get`. The 745 `OVERLOAD` rows are not the ranking rule: 617 are calls whose legend-pure overload our compiler
  drops by name (this phase's suppression removal brings them back), 79 are class-hierarchy choices (legend-pure
  measures hierarchy distance), 49 are numbers and optional values, likely argument typing.

- **Status (2026-10-07): built and audited on `build/phase3`, not landed** (the commits and their state:
  `docs/build-inventory/program/START_HERE.md` §3; GATES entry "Build rebuild Phase 3"; what is left: `docs/build-inventory/program/PHASE_3_LANDING.md`).
  Reference lane: AGREE 73,103 -> 74,586, OVERLOAD 745 -> 58, DRIFT 32 -> 0, PROPERTY_AS_CALL 39 -> 1, bodies we fail
  to type 1,508 -> 1,469 (51 newly typed, 12 newly failing). The six corpus passes: four tests the step-2 change broke
  were fixed by the fourth commit (dot calls property-first, the statement inliner deferring to overload resolution,
  `executeInDb`'s ConnectionStore row); every result file identical to main's. PCT 17 of 17. The audit (2026-10-07,
  maximum effort): ready after fixes; its blocker (a function-typed parameter ranked after a type parameter) is fixed
  with three tests, and nothing on any lane moved. Its other findings are corrected or recorded as
  `docs/PARKED_WORK_LEDGER.md` PARK-5 to PARK-14; whether PARK-5 (typing +18% on the compile probe) is fixed before
  landing is the user's decision. Adjustments, each measured: `Any` ranks with the type parameters (legend-pure
  matches the calls in a lambda before their arguments are typed; run on legend-pure itself), m3's literal order settling
  a remaining tie; a fit only a platform rule accepts ranks after every real parent. A short name that could mean a form's
  function or another function stays decided by the argument types (`ReceiverOwnedFunctions`, as legend-pure decides),
  not refused as first planned. About 12 upstream versions at the boot layer's 29 names (14 by a text count; `resolvePrimaryKey`,
  `propertyMappingsByPropertyName`, `inferRelationalType`, ...) still run upstream's body: PARKED_WORK_LEDGER PARK-12,
  closed by Phase 3b item 1.
  About 480 places still branch on a resolved callee's full name (the identity guard's shrink-only counts); Phase 3 did
  not take those. Phase 3b, re-scoped 2026-10-07, takes over neither W1.1b nor the typing work at large.
- **Correction (2026-10-07): item 3's rows for the legacy TDS functions move to Phase 4.** A row is keyed by a function
  id, and an id needs a declaration; the platform's own world declares none of `TdsLegacy`'s 18 functions (not the
  catalog, not the prelude), so in a user program `restrict(...)` resolves to nothing and only its spelling is left.
  Phase 3 built recognition by the names a call resolves to, falling back to the spelling when nothing resolves
  (`TdsLegacy.matches`, `GroupByChecker.isAgg`); `docs/PARKED_WORK_LEDGER.md` PARK-11 anchors it. Adding them to the
  catalog would grow what Phase 5 deletes; 14 of the 18 are upstream query handlers, so Phase 4's default world
  declares them and the rows come with them. The other 4 are not functions the default world brings (the Phase 4
  bullet below).

### Phase 3b: what the bump and users need from the compiler (core compiler)
Re-scoped with the user 2026-10-07, after the census (`docs/build-inventory/manifest-world/experiments/phase3b-census/`):
of the 1,469 bodies we fail to type, 931 (364 functions) are engine machinery the platform never runs, 460 are
upstream's own tests, 52 are library functions user code could call (mostly legacy TDS functions the forms handle, or
reflection not run here), 26 other. The six corpus passes and the PCT suites measure what users get; the reference lane
stays a guard that must not get worse, not a target. Small: days (the Phase 3b brief, 2026-10-07, finds items 1b and
5b larger than that: size it with the user). **The execution brief is `docs/build-inventory/program/PHASES_3B_6.md`.**
1. **The boot layer's twins merge by function id, and the view lifted twice (F-L1) is fixed.** Today 5 upstream
   files drop as twins of the boot layer's versions ("defined more than once": 3 in `platform_dsl_mapping`, 1 in
   `platform_store_relational`, the engine's `scanRelations.pure`), and 2 more for F-L1; Phases 4 and 6 load them. Found by the brief (inferred from the
   code): the twins fail because `SystemMetamodel.shadows` compares type spellings the two sides write differently
   (bare `String` against `meta::pure::metamodel::type::String`; `EnumerationMapping` against `EnumerationMapping<T>`)
   while their function ids are equal; once the files load, `superMapping` has two versions with identical parameters;
   the other upstream versions at the boot layer's 29 names are 14 by a text count (to verify by id). The boot layer's
   versions should win by implementation rows, not hiding (PARKED_WORK_LEDGER PARK-12). F-L1's fix is in
   `compiler/ModelBuilder.java` and holds only while the flat and per-schema view lists share objects, which the name
   resolver can break. This item owns F-L1; Phase 6 relies on it.
2. **No change for the 18 files whose mappings use an unsupported feature** (decided with the user 2026-10-07). The
   reference lane stays strict: it builds with `Compiler.buildModel`, drops a file that fails and pins the list, so
   every model-level gap with legend-pure stays loud. A tolerant lane would turn those failures into unpinned walls and
   raise its counts with nothing fixed. Phase 6 already has the files: the corpus builds with the tolerant
   `Compiler.buildModule`. The features themselves (set-routed bindings, enum transformers, explosions) are a recorded
   product gap, outside this program.
3. **The reference lane's 58 OVERLOAD and 14 PACKAGE rows, reviewed for user impact:** fix those that change results or a
   type users see; record the rest as type-only differences.
4. **A dot call finds a qualified property when a plain property shares its name** (`Extension.serializerExtension(
   version)`, about 391 bodies; a model of the user's can have the same shape). Two `Typer` routes need it, and it also
   explains the reference lane's last PROPERTY_AS_CALL row (`RoutingStrategy`'s plain `toString` and qualified
   `toString()`). PARK-6's and PARK-14's anchors sit in that code: restate or close them in the same commit.
5. **Two bugs the census found that nothing else schedules:** (a) 9 bodies crash typing (an index out of bounds)
   inside the ambiguity error's own message, instead of failing with that error (the ambiguity comes from our
   own-package rule, the parked plan's W2.3b; this item fixes the crash, not the rule). (b) `Runtime` and `Mapping`
   "not found as type names" (the service's `from`, the router's `routeFunction`) is one general bug, confirmed in the
   code: a file's imports are recorded per element full name (`Compiler.java`, `elementImports.put(fqn, ...)`;
   `NameResolver.java`, `elementImports().get(el.qualifiedName())`), so overloads of one function in different files
   are all resolved with the imports of the last file read. Users with multi-file projects meet it; Phase 6 needs it
   fixed (this item owns `routeFunction`'s case; Phase 6 only checks it). The census blamed the wrong files for these
   rows because the element-to-file map is keyed the same way.
- Conditions: the reference lane and the six corpus passes (against a fresh baseline) run before every compiler change
  lands; Phase 4 opens by re-measuring the library bodies upstream core brings in (each types or is refused with a
  reason).
- Not doing (decided 2026-10-07, reopenable with the census): typing the engine machinery (931 bodies) and upstream's
  unrun tests (460); the lane instrumentation (forms compared as calls, our inserted calls marked, types per call: the
  parked plan's W1.1b stays there). Known soft spot: Phase 3's adjustments (`Any` with the type parameters, the four kept
  tie-breaks) are not checked against legend-pure's types; the first place to look if a wrong-version bug appears.

### Phase 4: the default world from upstream (replaces `PreludeGenerator`)
- The generator: upstream core whole (legend-pure `platform*` and engine `core_functions_*`, tests stripped by
  upstream's own markers), plus upstream's query surface (the handler registrations of the core compiler and the
  relational extension, and the classes those modules instantiate), closed through declarations and the bodies of
  runnable functions; the m3 metamodel printed from upstream's m3 graph, as today's generator does. (Upstream marks
  tests with stereotypes; the `::tests::` package rule is its naming convention, used alongside.)
- Its inputs: the pinned archives and the module choice. No Java scan, claims, hand enums, path lists or exclusions.
  **Open decision (found 2026-10-07, the Phase 4 brief's D4-1):** "the bodies of runnable functions" was measured by
  reading our own code to know which functions are lowered, forms or walled (`closure.py:61-77`), which a generator may
  not do; following every body is upstream-only (+80 names, +74 KB at the measured size) but never yet run through the
  corpus, PCT and the user side. Also open: which parser the generator uses (D4-5: ours makes the default world
  "upstream plus our parser", which the seal cannot see). No experiment measured the real Phase 4 world: every measured
  world still dropped upstream functions by name, the rule Phase 3 deleted (U4-1), so the recorded boot costs are a
  floor.
- The result views move off startup (decision 1): they load with the `core` and `core_relational` modules, which the
  corpus's manifest already includes (Phase 6 comes first for that reason).
- **The legacy TDS functions by id (moved from Phase 3, 2026-10-07; closes PARKED_WORK_LEDGER PARK-11).** `TdsLegacy`'s
  18 members (`meta::pure::tds::` agg, col, func, window, columnByName, columnValues, olapGroupBy, project,
  projectWithColumnSubset, renameColumn, renameColumns, restrict, restrictDistinct, tdsRows, and
  `meta::pure::functions::math::olap::` rank, denseRank, rowNumber, averageRank). **14 are upstream query handlers**
  (`engine-handlers.tsv`), so the query surface brings their declarations into the default world: each id gets an
  implementation-table row ("the platform's desugar", a form row), recognition reads the row through the names a call
  resolves to (the resolver resolves `restrict` through the core import `meta::pure::tds`), calls the typer builds
  name the full name, and `TdsLegacy.matches`' spelling fallback and `GroupByChecker.isAgg`'s name test are deleted.
  **The other 4 (checked 2026-10-07, `docs/build-inventory/program/PHASES_4_5_7.md` D4-2):** `columnByName` is not a
  function but a qualified property of `TabularDataSet` (`tds.pure:21`): it comes with the class and is read as the
  class member. `columnValues` (2 versions), `renameColumn` and `window` (`tds.pure:516-533`, `:748`) are upstream
  functions that no handler registers and only upstream's own tests call; no closure brings them, and legend-engine
  does not let a user query call them (code reading, to confirm with a run: U4-5). **Open decision for the user:**
  leave them out of the default world (faithful to legend-engine; the corpus gets them from its own files after Phase
  6), or another rule (D4-2). The row kind a TDS function takes (a `CoreFn` form, or a `Form` row that accepts both)
  is D4-3.
- AGENTS.md and TENET_CHARTER C6.3 amended (decision 3).
- Boot speed (decision 2): profile and optimize the boot first; Phase 4b, the pre-built boot layer, decided with the
  measured numbers (+0.6 s in the browser before any optimization).
- Check: the experiment harness: projects' body walls 146 to 0 (`orElse`), corpus and PCT identical, the demos'
  queries executed, not only type-checked; boot times recorded; the 14 legacy TDS query handlers declared by the
  default world with rows by id, `columnByName` read as the class member, the other three as decided, and no spelling
  test left (PARK-11's anchor gone, its row deleted).

### Phase 5: Pure.java as rows keyed by function id (the catalog goes)
- No signature text: every row names an upstream function id and its implementation. Declarations come from the
  world (the default world for users, the program's own files for the corpus and PCT).
- Retires: the membership list and its draft, `native-claims.tsv` with `core_next` and `gen_claims` (D2), the
  natives generator.
- A test checks every row against the pinned archive: a row matching nothing fails and lists that name's real ids.
- Check: the experiment harness identical. Several landings (837 signatures, and every registry that names them; no
  PRs, see the header). Before coding: 70 of the 821 rows (45 names) are not in the measured default world, mostly
  corpus helpers but also `fromEpochValue`, `sortByReversed` and `sqlNull`, which the platform's own code calls in every
  world (`docs/build-inventory/program/PHASES_4_5_7.md` §3, D5-4 and D5-5).

### Phase 6: the corpus on its real manifest (runs before Phase 4)
- First: re-run experiment 8 against today's prelude (it ran on the new default world), since this phase now lands
  before Phase 4. **What experiment 8 did not test** (the Phase 6 brief, `docs/build-inventory/program/PHASES_3B_6.md`):
  it ADDED the rest of the manifest to today's `LIBRARY_FILES`/`SHAPE_FILES`, it did not replace them (one SHAPE file,
  `core_relational_duckdb/relational/connection/metamodel.pure`, is in neither candidate manifest); its "38
  repositories" came from a path-prefix choice (the corpus's own `core_relational` closure is 27); it ran every pass
  with a 4 GB heap (the DuckDB and warehouse passes run with 1 GB); and it removed platform-namespace functions in
  Python with pre-Phase-3 ownership lists, where the runner's own guard throws. Each is a decision or a measurement
  before the runner changes (the brief's §6.8, §6.10).
- The runner loads its manifest's repositories (the relational tree's 9 repositories and their closure, 38: a path-prefix choice; the corpus's own
  `core_relational` closure is 27, the brief's 6-O1) with the
  loading rule, replacing `LIBRARY_FILES`, `SHAPE_FILES` and the folder lists (`PreludeGenerator` and
  `FeatureFlagParityTest` read the same lists until Phase 4). The H2 register gains its one entry: it is the
  host-compared register, empty since 2026-09-21 and meant to stay at zero, so adding a row is the user's decision.
- The compiler gaps the real manifest exposed: the parser (`;` between property mappings, which legend-pure does
  not accept either: its mapping rule has no end anchor, so it silently ignores the rest, whole property mappings
  included; how we treat those files is a decision, the brief's §6.8; `->` where we reject it; `m3.pure`), units of
  measure (a `Measure` as a type), `routeFunction`'s resolution (fixed by Phase 3b item 5b), duplicate view functions (F-L1 in
  `projects/FINDINGS.md`, "a view inside a Schema is lifted twice", which also removes the projects' 4 build walls:
  fixed by Phase 3b item 1, which runs first; Phase 6 relies on it).
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
- CI lanes from `//gates`, and caching: **pulled forward as L1, the first landing (§4), agreed 2026-10-07**; the lane
  set is the Phase 8 brief's §8. The cache is the downloads only, per platform (GitHub's 10 GB cap rules out output
  caches); a remote cache stays an open decision (OD-5) for L10, with L1's measured hit rates.
- The old Bazel plan's parked work: `bazel/exec`'s Phase 4 commits and fixes, and P3-25.
- The build's own carried shortcuts (listed 2026-10-07, the user: fixed correctly, not worked around), complete in
  the Phase 8 brief's §2.8 (26 rows): of the Windows PR's (#14) four follow-ups, two are already fixed on main (the
  gzip `run_shell`, `4f7448989`; `LauncherTest`'s JDK text, `067973962`) and two remain (the hard-coded Windows bash
  path in `.bazelrc`; `warehouse_run` not taking visibility, tags or `target_compatible_with`); Phase 1's six deferrals
  (above); and 14 the first list missed (the `C:/bzl` output root, Windows runfiles trees, the rules_graalvm patch,
  libxml2 by `apt-get`, actionlint by `curl`, `taskkill` in two tests, 22 libraries without NullAway, never-run
  `postgres_live` tests, and more); and two found while writing §0 (the SQL census's shell scripts driving Bazel from
  outside it, one with a hard-coded macOS JDK path; the CI watcher on `curl`).
- The design's open decisions with no other home: design D1 (one Error Prone policy), D4 (`postgres_live`: a lane or
  delete), D6 (the CDP client and its transport: a spike first; the pipe transport may not work from Java), D7 (the
  probe scripts, which contradicts the old plan's D18); and the 17 open decisions the Phase 8 brief lists (§3.4).
- **The Phase 8 brief, `docs/build-inventory/program/PHASE_8.md`, is this phase's plan in detail**: about 80 items in
  10 themes, both decision series with their status, the old workplan's items mapped so nothing is lost or done
  twice, the stale statements, the homework, and a proposed order.
- The measurement group, set aside on 2026-10-05: the corpus judges as cached build actions, the ratchets, the
  ladder. Decide what each is and carve it by its true trigger.
- Last: the renames (D11: core, db, sdlc, depot; D12: depot its own server).

## 4. Order and what can move (agreed with the user 2026-10-07; the live state is START_HERE §3)

The remaining work is a numbered list of **landings** (one branch, one audit, one full CI run, one push to main).
**The CI landing comes first** and **the bump is done at L8**: the user, 2026-10-07, "we do all the L2 stuff first
right now, make our build super amazingly awesome, and then we go back to all the code including phase three and the
rest of the bump program in order; then pushes will be much faster and we move fast". Measured reason (the Phase 8
brief's §8 and `docs/build-inventory/program/evidence/phase8/CI_LANES_2026_10_07.md`): a full run took 40 to 57
minutes because the build lane built everything (`//...`, the corpus passes and the 2 GB ratchets included), the
browser lane ran its harnesses one at a time by a shell loop, and Linux and Windows fetched everything cold on every
run (GitHub's 10 GB cache cap had evicted every cache but macOS's). After L1 a run is about 15 minutes, bounded by
GitHub's five-at-a-time macOS queue; every later landing pays that instead of an hour.

| # | Landing | Contains | Needs |
|---|---|---|---|
| **L1** | **The CI landing** (Phase 8's CI work, pulled forward; the lane set is the Phase 8 brief's §8) | (a) every lane a `//gates` suite with a guard that every test is in one; the build lane builds the product (`//:java //:web //:wasm //:native //:sites //datacube:app`) plus the two analysis checks; `manual` on the six judge passes, `//:update_generated`, the hand tools and the layer queries; actionlint as a Bazel test and the actions pinned by commit; the cache reduced to the downloads, one per platform. (b) the browser harnesses as Bazel tests (the parked `bazel/exec` commits P4-02, P4-03, P4-04, P4-08 rebased), the Linux-only ones by `target_compatible_with`, no install step, no loop | nothing; Phase 3's branch stays untouched meanwhile and rebases after (L1 touches no file Phase 3 touched) |
\1 **Landed 2026-10-07** (commit \"Build rebuild L1c: the warehouse knows nothing about Bazel\"; run 37668591690; the one change from the agreed design: `--open` stays a flag of its own, so the app folder is judged by a test without a browser; `//warehouse:serve` is a folder too, since `//:native` must stay a compile).| **L1d** | **L1c's follow-ups** (the shortcuts and unknowns L1c named on landing, written here before anything else moves: the user, 2026-10-07) | In order: (1) the downloads cache's size: Bazel 9's repo contents cache (the unpacked repositories) defaults to `{--repository_cache}/contents`, inside the path CI caches; L1c's run measured it (evidence §10) -- turn it off on CI (`common:ci --repo_contents_cache=`) or leave `contents/` out of the saved path, then take the two measuring `du` lines out of the product job; (2) a listing check of `//datacube:app_package` (its four entries, no runfiles tree: `include_runfiles = False` is what keeps it from doubling, and nothing judges it); (3) `//datacube:verify_app_test` on Windows stops the server with a hard kill, which runs no cleanup, so its temporary data folder leaks per run: stop it in a way that runs the shutdown hook, or give it `--data` under `TEST_TMPDIR`; (4) a Windows desk check that Ctrl+C removes the temporary folder (verified on macOS for SIGINT and SIGTERM, 2026-10-07; a native image installs no handlers unless asked, and here it evidently has them); (5) optional: a `.zip` package, and `datacube-app/` paths in the archive (a Starlark mtree writer: tar.bzl's `mutate` runs gawk under a shell, bsdtar's `-s` leaves the empty parent entry). Two decisions for the user, on this run's numbers: OD-5, a remote build cache -- every lane rebuilds the native image (4-6 minutes) and the WebAssembly planner on a fresh runner, and the `datacube` lane is ten minutes of that plus, on macOS, eighteen Chromium sessions one at a time (evidence §9, §10); and whether the harness tests run on macOS and Windows by default (+7 minutes of wall clock, all macOS). | L1c landed; its run's `du` |
\3 **Phase 3 lands** | the five commits as they are, rebased onto main after L1; PARK-5 recorded (option (a) below), its full fix scheduled as L7; the ledger rows PARK-13 and PARK-14 get their "fixed in 3b" line in the same rebase | L1; one CI run on the fast CI |
| **L3** | **Phase 3b** | its five items (3b.5), with: 3b-O1 (b) the "platform's own Pure" row kind (PARK-12 closed, `shadows` deleted); 3b-O2 (a) import scopes per element; **PARK-14 decided in item 4's code and PARK-13's trace deleted** | L2; the 3b brief's homework H1 to H7; its open decisions |
| **L4** | **Phase 6** | the corpus on its manifest, as the 6 brief describes; 6-H3 decides whether PARK-11's rows must come here instead of Phase 4 | L3; homework 6-H1 to H9 (experiment 8 rerun at the real heap) |
| **L5** | **Phase 4** | the default world from upstream, the result views off startup through L4's loader, the legacy TDS rows by id (PARK-11), the 109 unrowed versions each rowed or refused, the rule texts, the boot profiled, 4b decided on numbers | L3, L4; U4-1 (the real world measured: run right after L2, it needs no 3b or 6 code) and U4-2 to U4-9 |
| **L6** | **Phase 5** | Pure.java as rows by id; the catalog, the membership list, the claims, `core_next`, `gen_claims`, `gen_natives`, `native_declarations` retired | L5 |
| **L7** | **PARK-5's full fix** (moved into the program) | the resolver records every platform call's names once; calls built after it are built resolved; `BareNames.catalog` goes; typing on the eager probe at or below main's | L6: Phases 4 and 5 touch the same ~200 call sites and change where declarations come from; the fix is done once, on the final shape |
| **L8** | **Phase 7: the bump** | `//:update_upstream`, the seal and its test, the measurements' writers out of `//:update_generated` (D7-4: here, not Phase 8), `Bump.java`, one real bump to the release after 4.145.0 | L6 (L7 helps). **The bump is done here.** |
| **L9** | **The typer's order** (PARK-6, 7, 8, 9, 10 as one design) | arguments typed once, the function chosen on typed arguments, legend-pure's acceptance test, the tie-breaks checked against legend-pure | L8; earlier only if U4-1 shows wrong picks in the real world |
| **L10** | **Phase 8, the build part** (several landings) | D8, D9 (OD-1: runfiles trees stay on Windows), D10/D5, the Node-independent parts of `bazel/exec` (OD-3 (b) for the rest), the small items, the remote cache if the numbers say so | L1 |
| **L11** | **Phase 8, no Node** (several landings) | the CDP spike, the driver, the tests, the harnesses, the servers, the JS generators, TypeScript as a build action, Node removed, then the Windows bash path and runfiles | L5: the boot measure `//wasm:startup` is a Node program Phase 4 needs |
| **L12** | **Phase 8, the tests untangled and the measurement group** | the corpus passes narrowed, the ledgers committed as goldens (the run-against-run comparison, the Bazel way), `spec_tests_lib` split, the ladder and ratchets carved, the core test libraries per package, the H2 stress suite split per suite, the weekly heavy suite, the run-vs-run comparison design | L4, L6 |
| **L13** | **The rest** | PARK-1 to PARK-4 (product debts, not this program's: one select-merge pass closes PARK-2 and PARK-4), the script review and documents, NullAway and Error Prone, the remaining guards, the renames (D11, D12), the final audit | everything |

**Why this order.** 2b informed 3's design; 3 needs 2's tables. 3b needs 3 and runs before 6 and 4: it loads the files
those phases need (the boot layer's twins, the mapping files) and fixes what users would meet. 6 needs 3 (the table
owns what the manifest's files redefine) and runs before 4: Phase 4's default world lacks the ten classes the result
views name, so the views must leave startup the moment the world changes, and only Phase 6's loader gives them a
home. 4 needs 3's ownership and 6's manifest; 5 needs 4's world; 7 needs all. Phase 8 waits for the bump except L1,
which pays for itself at once.

**L1's second run (2026-10-07) found the cache's design wrong**: the runner image's version in the key (GitHub rotates
images run to run), a prefix fallback that matched another platform, and a 19 GB Linux cache (the product job saved
after analysing everything) against GitHub's 10 GB cap. Fixed as L1b's follow-up (key without the image, no prefix
overlap, the save right after the product build); if the downloads still do not fit, GitHub's cache goes and OD-5 is a
remote cache (`PHASE_8.md` §8, the evidence file §8).

**The debts, placed.** PARK-5: L7 (the user chose option (a), 2026-10-07: land Phase 3 with it recorded; the fix inside
the program, after Phase 5, not after the program). PARK-6 to PARK-10: L9. PARK-11: L5 (or L4 if 6-H3 says so).
PARK-12, PARK-13, PARK-14: L3. PARK-1 to PARK-4: L13.

## 5. Working on this program (for any session that picks it up)

**The program's debts:** `docs/PARKED_WORK_LEDGER.md` rows PARK-5 to PARK-14 (2026-10-07): the shortcuts and
simplifications this program carries in the compiler (a platform call never resolved once, arguments typed more than
once, the ranking's adjustments and unported parts, legacy TDS functions by name, the boot layer's versions, a debug
trace, the dot-call fallback), each with its cost, what closes it and an anchor test that goes red when the code
changes. The user, 2026-10-07: they are fixed correctly, with a design agreed first; none is worked around in the
meantime, and a new shortcut is a new row, not a quiet one. Each row's landing is in §4 ("The debts, placed"): PARK-11
in Phase 4, PARK-12 to PARK-14 in Phase 3b, PARK-5 as L7 right after Phase 5 (decided 2026-10-07: the fix is inside the
program, once, on the final shape), PARK-6 to PARK-10 as L9 after the bump.

**Process (the user's rules):**
- Plan each phase and get the user's agreement before writing code; explain plainly, without jargon; never invent a
  new mechanism when an existing one (the implementation table, modules, manifests) does the job.
- Prove locally first; throwaway CI only on the lanes a change touches during the work; to land, one full CI run on
  the branch and a push of that commit to main (no PRs; `START_HERE.md` section 4 has the exact commands).
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
