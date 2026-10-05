# Rebuilding the build: inventory, causes, and the design (2026-10-05)

Status: DRAFT for the user's review. Nothing in the repo changes until this is agreed.

Evidence: `docs/build-inventory/`.
- `BRIEF.md`: the audit brief.
- `inventory.tsv`: all 1,162 targets from Bazel's own graph; `build_inventory.py` regenerates it.
- `area1_report.md` … `area8_report.md`: one row per target. Every claim cites a file and line, a query, or a commit.

The root causes in section 3 were checked again by hand after the audits. The check is noted against each one.

## 1. What we are building toward

**Goal.** Compiling all the Java we ship takes about 30 seconds from clean. Everything else the repo does is carved
into kinds of work, and each kind runs only when something it truly depends on changes.

The principles below decide every item in this document.

1. **"Build" means compile, nothing else.** The everyday build compiles what we ship: Java, the web bundles, and later
   the wasm and native images. It runs no generator, no corpus, no measurement and no check.
2. **Every step declares exactly what it reads, no more.** Bazel already reruns a step only when its declared inputs
   change. Most of the mess comes from steps that declare far more than they read, usually "all of core". Once the
   declarations are exact, *when* a step runs is correct automatically, and a cached step costs nothing in any lane.
3. **Committed generated files are grouped by what truly changes them.**
   - An upstream bump: run only by the bump procedure.
   - Our own source text: core's Java, our parser, the stress corpus.
   - Pinned data: icons, DuckDB.

   Drafts and reports are run by hand (`manual`). An "update everything" command never re-blesses a test's expected
   results.
4. **Anything that pins engine behaviour is a test, not a generator.** Examples: SQL goldens, corpus rosters, the
   saved-query records the server writes. These run in test lanes on engine changes.
5. **Tests depend on the slice of code they exercise.** They are grouped by kind (unit, integration, corpus, browser,
   stress). Each CI lane is a test suite in `//gates`, the one place that lists it.
6. **Checks are cheap.** A structural guard runs where the graph is already analysed (the compile lane). The light
   local gate never has to analyse the whole repo.
7. **No Node.** esbuild and TypeScript 7 are native programs, so Bazel calls them directly. JS tests run inside the
   pinned Chromium, driven by a small CDP client. Dev servers are our Java servers.

## 2. The inventory in one table

There are 1,162 real targets. The 1,176 npm package-store targets and 588 path-only file groups are excluded: they
do no work. Each target has exactly one verdict.

| Verdict | Count | What it means |
|---|---|---|
| COMPILE | 59 | product code we ship or run: Java libraries and servers, TS bundles and type checks, 3 TeaVM, 1 native image |
| TOOL | 99 | programs that only run inside another step, or by hand |
| GEN-COMMITTED | 84 | generators whose output is committed, plus their writers |
| GEN-BUILD | 17 | generators whose output the build consumes directly |
| CHECK-DIFF | 60 | diff tests: a committed file vs its generator |
| CHECK-GUARD | 172 | structure checks: 108 per-package guard reports, 30 layer queries, closures, locks, inventory |
| TEST-UNIT / -INTEGRATION / -CORPUS / -STRESS / -BROWSER | 106 / 56 / 89 / 3 / 13 | tests by kind (CORPUS includes the 57 model-project compile tests) |
| WIRING | 353 | file groups, aliases, suites, launchers |
| DEAD | 51 | proven unused; 45 of them are unused per-project file groups |

By area (each report's Part A has the rows):

| Area | Targets | Report |
|---|---|---|
| 1. Core Java, base, json, testing | 133 | area1 |
| 2. Spec and its generator chain, docs | 79 | area2 |
| 3. Parser-equivalence, PCT, reference compiler, engine-runner, bump | 103 | area3 |
| 4. Corpus and stress data, saved-query fixtures | 95 | area4 |
| 5. DataCube | 239 | area5 |
| 6. Query, Studio, site, engine-client, clients, tools/js, tools/browser | 109 | area6 |
| 7. wasm, warehouse, sdlc/depot servers, TeaVM, GraalVM | 74 | area7 |
| 8. Model projects, guards, gates, rule tooling, plus the whole-repo lane map | 330 | area8 |

What we actually ship, and so what "the build" should be:
- **Java:** `//base`, `//json`, core's 30 layer libraries, `//core:duckdb_load` and `//core:server`. `//core:server` is the
  smallest target that compiles all of core. `//core:core` only re-exports the layers and compiles nothing itself
  (checked with aquery).
- **Other Java products:** `//warehouse:{sqlapi,client,server_lib,server}`, `//sdlc-server:{rules,server_lib,server}` and
  `//depot-server:rules`.
- **Web:**
  - DataCube app and planner worker, plus page and dist;
  - Query app and worker;
  - Studio app, editor worker and planner worker.
- **WebAssembly (TeaVM):**
  - `//wasm:planner`;
  - `//sdlc-server:page`;
  - `//warehouse:sqlapi_wasm`, which is a portability check only; nothing ships it.
- **Native:** `//warehouse:server_native`.

## 3. Why the build is the way it is

Ten causes explain almost every problem the audits found.

**R1. A generator's step takes its whole dependency tree as input, and most generators depend on all of core.**
- `java_run` puts every runtime jar of its dependencies into the action's inputs (`tools/java_run/defs.bzl:46,110`;
  re-checked).
- Generators depend on the `//core` umbrella: 30 layers, plus the server, execution and test libraries.
- So any core edit reruns 42 of the 70 programs that run at build time.

**R2. Steps depend on "all of core" when they use a small slice.** All of these were re-checked:

| Step | What it actually uses |
|---|---|
| `spec:gen_dynafn`, `gen_imports` | no core class at all (0 `com.legend` imports) |
| `engine-client:lite_facts` | one small library, `compiler_element_type` |
| the 57 model-project tests | `Compiler.compileModel` (`//core:planner`, 21 libraries); today they reach all 31 (`somepath` reaches `//core:exec`) |
| `//sdlc-server:rules`, which feeds the 2 GB Studio page TeaVM compile | 3 classes |
| `pe_tests_lib` | 6 libraries |
| DataCube's three facts generators | the plan side only |

**R3. Tests and checks hidden inside build steps.**
- the corpus passes `judge_host_*` and `judge_database_*` (DuckDB, H2);
- `core:ladder_report`, which fails its build step when a rung stops passing;
- `fixtures/saved-queries:gen`, which starts the server and checks row counts;
- `datacube:catalog_corpus`, which runs DuckDB;
- the wasm JVM answer files, which are not testonly.

**R4. `bazel build //...` builds everything that isn't tagged manual.**
- That includes every generator, draft and report.
- The root `//:update_generated` is not manual, and it pulls in the manual 2 GB `//pct:ratchets` (re-checked with
  `somepath`). It also builds every generator a second time.

**R5. Giant shared libraries.** Each one makes any edit inside it rerun everything that uses it:
- `core_tests_lib` carries the 13 MB stress corpus and 12 projects, and has 36 users;
- `spec_tests_lib`;
- `pe_tests_lib`;
- `pct_tests_lib`: Channel B builds the 4 GB adapter PAR it never uses, and that PAR differs between runs;
- the 29-module `scripts/corpus` Python library.

**R6. Whole-tree text inputs with no early cutoff.**
- `gen_prelude` and `gen_claims` scan every core `.java` file, so a comment edit reruns them.
- `//core:srcs` (all of `src/**`) is declared where only test snippets are read.
- `inventory_test` reads all 3,867 files to check that their paths exist.

**R7. `core_next` recompiles 749 core files on every edit for a file nobody ships.** That file is `native-claims.tsv`,
which core's BUILD file already marks as retiring.

**R8. Lanes are defined twice, and the light gate isn't light.**
- `gates-run.yml` and `gates/BUILD.bazel` each list the lanes.
- The guard tests make `//gates:local` analyse every target in the repo.
- The native image is built in the local gate.
- 10 tests run in no lane; `warehouse:postgres_live` and `postgres_live_native` have never run.

**R9. Error Prone coverage is upside down.** In NullAway libraries, `-XepDisableAllChecks` turns off every default check
(`tools/nullaway/defs.bzl:12`; re-checked). So product code runs 3 checks while test and tool code runs the full set.

**R10. Node sits in the loop.**
- `bazel build //...` never type-checks TypeScript; the only type checks are two tests.
- CI runs the Query and site browser checks with `bazel run`, using Playwright's own Chromium, not the pinned one.

## 4. The design

### 4.1 Compile tiers

There are four named compile targets. Each is a `filegroup` or a suite naming exactly what ships.

| Tier | Contents | Rebuilds when |
|---|---|---|
| `//:java` | every runtime jar of the three servers (core, warehouse, SDLC with Depot's rules), computed from their dependencies, so it shrinks when D8/D9 land | their Java changes |
| `//:web` | the 8 bundles plus TypeScript type checks as build actions (native tsc, `--noEmit`); a type error fails the build | TS changes, or the wasm planner |
| `//:wasm` | the 3 TeaVM compiles | plan-side core changes; sdlc/depot rules after R2 is fixed |
| `//:native` | the warehouse native image and its launchers | warehouse server changes only (it uses no core) |

- **The everyday build is `//:java` plus `//:web`.** `//:java` is measured first, against the 30-second goal.
- **CI's build lane builds the tiers, not `//...`.** `bazel build --nobuild //...` stays, as the cheap analysis-only check
  for Bazel 10 compatibility.
- **A new guard keeps compile honest.** The compile tiers may contain only compile kinds of action: Javac, header jars,
  esbuild, tsc, TeaVM, native-image and file copies. The guard checks the action kinds with `aquery`, so a generator
  can never slip back in.

### 4.2 Generators, by what truly changes them

Every generator and the change it needs. Evidence: the "true trigger" column of each area report.

**A. Upstream bump only.** Run by a new `//:update_upstream`, which `tools/bump` runs. A guard proves these steps have no
path to core's execution libraries.

| Generator | Change needed |
|---|---|
| `spec:gen_dynafn`, `spec:gen_imports` | move into a generator library with no core dependency |
| `parser-equivalence:gen_fixtures` | depend on the harvest code only (not `pe_tests_lib`, not core) |
| `parser-equivalence:gen_manifest` | `//core:diagnostics` plus `//testing` only |
| `tools/engine-runner:vocab` | split TokenDump out of the runner, which depends on all of core |
| `scripts/parser:keyword_coverage` | drop vocab from its tool's inputs (never read) |
| `tools/reference:ref_dump`, `ref_imports` | already right |
| `pct:adapter_par` | trigger already right; make its output reproducible so it stops invalidating PCT tests |
| `query:icons_gen`, `warehouse:duckdb_library`, `warehouse:duckdb_extensions` | already right: their pins |
| `spec:native_declarations` | `manual` (by hand) |

**B. Upstream plus our parser or our committed registries.** A parser change is a real trigger here.

| Generator | Change needed |
|---|---|
| `spec:gen_natives` → `Pure.java` | depend on the parser and resolver only (needs `Compiler.parseSources` below `:planner`). Option to evaluate: parse with upstream's own parser, which would make it group A |
| `spec:gen_engine_handlers` | `//core:builtin` plus `//core:model`; read the chain's outputs, not the committed copies, so one update run is enough |
| `parser-equivalence:ratchets` | parser and protocol, plus test sources only |
| `parser-equivalence:gen_roster` | uses no lite parser: `//testing` plus diagnostics, and test sources only |
| `spec:ratchets` | own small library; triggers are an upstream bump, `DynaFn.java`, or the catalog |
| `pct:ratchets` | stays manual; the bump runs it; it leaves the non-manual root writer |

**C. Derived from core's own source text.**

| Generator | Change needed |
|---|---|
| `spec:gen_prelude` | one small extraction step reduces core's sources to the names it reads, so a comment edit stops at the extraction; narrow its dependencies |
| `spec:gen_claims` with `core_next` | retire `native-claims.tsv`, or make it a build output read by its one test; delete `core_next` |

**D. Data that doesn't touch the engine.** These are already correct apart from small over-declarations.

| Generator | Change needed |
|---|---|
| `scripts/corpus:gen_dense`, `gen_stress`, `gen_differential` | split the Python library by import; drop `queries.pure` from `gen_dense` |
| `core:stress_layout`, `stress_index` | right |
| `datacube:test_imports`, `link_dictionary_next`, `offer_queries`, `cube_queries` | right triggers; ported off Node in section 4.6 |

**E. Engine behaviour, which becomes tests** (principle 4).

| Today | Becomes |
|---|---|
| `core:ladder_report` | a ladder test in the core suite, on a small library; re-pinned only explicitly, never by `//:update_generated` |
| `fixtures/saved-queries:gen` | record generation on a narrow server library, plus a separate server test for the row counts |
| `engine-client:lite_facts`, `datacube:offer_facts`, `catalog_rules` | stay generators, but depend on exactly the library they read (one, or the plan side) |
| `datacube:catalog_corpus` | its DuckDB run becomes a test |
| `wasm:jvm_answers`, `wasm:zone_jvm`, `datacube:cube_jvm_answers` | testonly outputs of their differential tests; `zone_jvm` narrowed to `LiteralSpelling` |
| `spec:judge_*`, `spec:eager_corpus_compile*` | corpus-lane steps tagged `corpus`, outside the compile tiers |
| `warehouse:reachability_metadata` | own small testonly library on `:server_lib`, not on the whole warehouse test library |

**F. Drafts and reports: `manual`.** These are `native_membership_draft` (and `core:draft_native_membership`),
`docs:draft_own_corpus_ledger` (and `gen_own_corpus_draft`), `tools/census:render_census` and `lanes_diff`, and the new
`scripts/corpus:run`.

**Mechanics:**
- `//:update_generated` becomes manual.
- Writers are split by group: `//:update_upstream` (A and B), `//:update_generated` (C and D) and explicit test re-pins (E).
- Diff tests are grouped by trigger in `//gates:generated_upstream` and `//gates:generated_source`.
- A guard checks that every diff test belongs to a group.

### 4.3 Tests

**Split the giant libraries by trigger:**
- `core_tests_lib`, one per package (P3-06): the stress corpus goes only to the 7 classes that read it, and the 12
  projects only to their readers;
- `spec_tests_lib`, into corpus, parity, reference and ratchets;
- `pe_tests_lib`, onto its 6 libraries;
- `pct_tests_lib`: Channel B stops building the PAR it doesn't use.

**Projects:** one JVM compiles all 56 projects, each separately, and depends on `//core:planner`. That is 2 JVMs on a
compiler edit instead of 57. The 45 dead file groups are deleted.

**Lanes and placement:**
- Every CI lane is a `//gates` suite. `gates-run.yml` names only `//gates:<lane>`, and `//gates:local` is a suite of
  suites.
- A guard checks that every test is in some lane or on a reviewed allowlist, with the reason. That would have caught
  `postgres_live`.
- The native image leaves `//gates:local`. `live_snap_test` moves from `//datacube:tests` to the native and browser lanes.
- The heavy benchmarks are excluded by their `@Tag("heavy")`, not by class-name luck.

### 4.4 Checks

- **Guard reports:** an aspect or validation actions in the compile lane replace the 108 per-package reports, of which 57
  are always empty and 2 unused. `classpath_test` and `markdown_inputs_test` leave the light gate.
- **`inventory_test`:** compares file names, not contents, so it reruns only when a file is added, removed or renamed.
- **The 30 layer queries:** stay, but only the layering test builds them.

### 4.5 Java compile settings

- **Error Prone:** choose one policy for all first-party code, product and test alike (decision D1).
- **The DuckDB jar manifest rewrite** (`--@rules_jvm_external//settings:stamp_manifest=False`): decide after the clean
  measurement in step 1. It saves one rewrite of a 70 MB jar, once per cache.

### 4.6 No Node

**Compile:**
- Small rules call the esbuild and tsc binaries for each platform directly.
- Type checking is a build action, so `//:web` fails on a type error.
- The npm packages (DuckDB-WASM, Monaco, fonts, type definitions) are fetched straight from the pnpm lock with their
  checksums. Whether rules_js is kept for fetching only is decided in step 7.

**Tests:**
- Each JS test is bundled into a test page and runs in the pinned headless Chromium, reporting back through the driver.
- 1,405 of DataCube's 1,878 Playwright calls are DOM reads, waits and value setting, and move into the page.

**The driver:** a small Java CDP client over `--remote-debugging-pipe` (decision D6).
- What it must provide is listed in area5 and area6, Part E:
  - contexts and navigation;
  - in-page evaluation with callbacks and init scripts;
  - trusted mouse and keyboard input, including hover, drag, double-click and context-click;
  - file input and downloads;
  - blocking network requests;
  - viewport and colour scheme;
  - screenshots and page errors;
  - clipboard permission;
  - starting servers.
- Open question: which UI handlers need trusted events rather than synthetic ones.

**Servers and tools:**
- The Node `serve` scripts are replaced by the Java servers or a tiny Java static server.
- The JS generators (icons, sample data, link dictionary, test imports) are ported to Java `java_run`s, or run in the page.

## 5. The order of work

Each step leaves the repo green and ends with a check made by a query or a guard, not by a timing. The exception is
step 1, whose purpose is the measurement.

0. **Hold.** Feature work on the build stops.
   - The 14 unpushed Phase 4 commits and the uncommitted P3-25 work stay on `bazel/exec`.
   - They are re-checked against this design before anything lands. Known issues: P3-25's `scripts/corpus:run` must be
     manual, and `engine_stress` needs `shard_count`.
1. **The build targets.** Define `//:java`, `//:web`, `//:wasm`, `//:native` and the packaging target `//:sites` (D10), plus the guard that the compile tiers contain only compile actions. Then measure a clean build of `//:java`, and record what a one-line `Compiler.java` edit invalidates (by query).
   - Method: a fresh output base, no disk cache, shared download cache, on a machine with no other Bazel build running,
     plus an execution log.
   - Record what is on the critical path.
   - Decide D1 (Error Prone) and the manifest rewrite.
   - Exit: the number, and the list of what's in it.
2. **Build means compile.**
   - Define the other three tiers.
   - Mark `//:update_generated` and the drafts manual.
   - Tag the corpus targets `corpus`; make the answer files testonly.
   - Switch the CI build lane to the tiers.
   - Add the guard that the tiers contain only compile actions.
   - Exit: that guard passes.
3. **Narrow every dependency on "all of core"** (R1, R2).
   - Make the needed core slices visible: `plan_side`, `planner`, `parser`, `compiler_element_type`, `diagnostics`.
   - Switch each consumer named in section 4.2 and in sdlc-server.
   - Exit, measured by query, not timing: the number of targets that depend on `//core:exec`, and on any core file
     through a generator, before and after.
4. **Generators by trigger** (section 4.2).
   - Split the generator libraries; add the extraction step; retire `core_next` and `native-claims.tsv` (D2).
   - Add `//:update_upstream` and wire it into the bump; close the chain so one run is enough; turn group E into tests.
   - Exit: the "no path to execution" guard and the diff-test grouping guard pass, and a bump dry-run shows one update
     run reaches a fixed point.
5. **Tests by trigger** (section 4.3).
   - Split the libraries; run the projects in one JVM; make the lanes `//gates` suites; add the every-test-has-a-lane
     guard; take the native image out of the local gate.
   - Exit: those guards pass, and CI runs from the suites.
6. **Checks** (section 4.4). Exit: the light gate no longer analyses the whole repo (checked by query).
7. **No Node** (section 4.6).
   - The native compiler rules come first.
   - Then the CDP driver and the in-page test runner, proven on one harness on all three operating systems.
   - Then port the tests and harnesses, replace the servers, and remove the Node toolchain.
   - Exit: `bazel query` finds no Node toolchain dependency.
8. **CI.** Lanes come from `//gates`; one cache per platform (then a remote cache, Phase 5).
9. **Last: the product naming (D11), and Depot as its own server (D12)**, each its own announced change.

Every step that touches Bazel setup is reviewed by the Bazel session before it is pushed, and goes through the
throwaway CI run on all platforms.

## 5a. Step 1 results (2026-10-05, branch `build/rebuild`, commit `1479486dd`)

**The targets** (root `BUILD.bazel`):

| Target | What it builds |
|---|---|
| `//:java` | `java_runtime_jars` over `//core:server`, `//sdlc-server:server` and `//warehouse:server`: each binary's own runtime classpath (`JavaRuntimeClasspathInfo`), so it follows their dependencies. 46 jars: 39 of ours, plus DuckDB JDBC 1.4.4, H2, Postgres, SQLite, checker-qual and the 2 runfiles jars (gone with D9). |
| `//:web` | `//datacube:bundles`, `//query:bundles`, `//studio:bundles`: 8 esbuild bundles |
| `//:wasm` | `//wasm:planner`, `//sdlc-server:page` |
| `//:native` | `//warehouse:server_native` |
| `//:sites` | `//site:dist` (packaging) |

**The guard:** `//tools/guards:compile_only_test`, run in `//gates:local` and the checks lane.
- `compile_only.bzl` lists every action kind the four compile targets' closures register, at analysis time. It
  skips the exec configuration and a java_binary's non-classpath edges.
- The test holds every kind to an allowlist of compiles and file plumbing, each entry with its reason.
- Proven to fail: with `//warehouse:duckdb_extensions` (a generator) put into `//:native`, it fails naming
  `GunzipDuckdbExtension` and the target.

**B1: a clean build of `//:java`.**
- Method: a fresh output base, downloads already fetched, no disk cache, and the Bazel server and javac workers
  restarted before every run; macOS arm64, 10 cores, no other build running.
- Three runs: **22.6 s, 23.1 s, 22.9 s** (critical path 19.7 s).
- **The critical path is the DuckDB jar:** rules_jvm_external stamps its manifest (10.4 s), then makes its
  compile-only copy (7.0 s). That is 17.4 of the 19.7 s, before `//core:duckdb_load` (2 files) can compile.
- Our own code is 64 s of javac work summed over 46 compiles, run in parallel, and finishes before the DuckDB chain.
- So the fix that lowers the clean build is the DuckDB jar handling, not our code. Candidates, each to be
  measured:
  - `--@rules_jvm_external//settings:stamp_manifest=False`;
  - a cheaper compile-only copy;
  - one DuckDB version for core and the warehouse.

**After a one-line comment edit to `Compiler.java`: 0.44 s.** Only `//core:planner` recompiles. Its header jar is
unchanged, so nothing above it recompiles.

**B2: what a one-line `Compiler.java` edit invalidates, by query.**
- The query: `rdeps(<all packages>, //core:src/main/java/com/legend/Compiler.java)`, minus npm package plumbing.
- **495 targets:**
  - 31 Java libraries, 15 Java binaries and 2 TeaVM compiles;
  - **40 build-time programs**, 30 of them not manual, so `bazel build //...` reruns them;
  - 168 tests (122 Java, 46 JS);
  - 45 diff tests and 55 writers of committed files;
  - 41 guard reports and layer queries;
  - the rest wiring.
- The 40 programs are:
  - spec: `gen_*` (6), `judge_*` (6), `eager_corpus_compile*`, `ratchets`, `native_*`, `reference_lane_report`;
  - parser-equivalence: 9;
  - `pct:ratchets`;
  - `tools/engine-runner:vocab`, `scripts/parser:keyword_coverage`;
  - datacube: `catalog_*`, `offer_facts`, `cube_jvm_answers`, `dist`;
  - `engine-client:lite_facts`, `fixtures/saved-queries:gen`, `core:ladder_report`;
  - `wasm:jvm_answers`, `wasm:zone_jvm`.
- Steps 3 and 4 are measured against these numbers.

## 6. Decisions for the user

- **D1. Error Prone.**
  - Option (a): the full default checks for all first-party code.
  - Option (b): an explicit list shared by product and test code.
  - Either way the inversion ends.
- **D2. `native-claims.tsv`.** Retire it, or keep it as a build output read by its one test. Either way `core_next` is
  deleted.
- **D3. `gen_natives`.** Investigate parsing upstream with upstream's own parser, which would make it bump-only.
- **D4. `warehouse:postgres_live` and `postgres_live_native`.** Put them in a lane, or delete them.
- **D5. DataCube's fixture bundles** (`remote_bundle`, `stress`) ship in the public site. Take them out?
- **D6. The CDP client.**
  - Recommendation: a small Java client in the repo. It adds no new toolchain, and the driver starts Java servers anyway.
  - The alternative is Go's chromedp, which brings a whole Go toolchain into Bazel.
- **D7. The 16 broken `scripts/corpus/probe_*.py` scripts.** They are unrunnable since 2026-09-23. Delete them. Also say
  how `docs/FUNCTIONS_EXECUTED.tsv` is regenerated, since the functions gate's stated remedy can't be followed today.

### Decided by the user (2026-10-05)

- **D8. Remove `//core:ide` and `//core:probe` from the product.**
  - `ide` calls itself "Dormant… currently unused by the batch pipeline". `probe` is "a PROBE, deleted at step 4" of
    the platform-architecture untangle. No product code uses either.
  - This is a core edit: announce it in IN_FLIGHT, and tell core's owner.
- **D9. The shipped server knows nothing about Bazel.**
  - The warehouse's DuckDB library and Postgres extension stay declared Bazel dependencies, as they are today:
    `data` of the server, pinned by checksum.
  - Whatever runs the server hands it plain file paths with Bazel's own `$(rootpath)` expansion: `bazel run`,
    tests, the launchers.
  - The server drops its runfiles lookup (`ServerRunfiles`, and the fallback in `WarehouseServer.java:912-960`),
    and the runfiles library leaves the product.
  - Prove it on Windows CI. `.bazelrc` enables runfiles trees there.
- **D10. One shape per app.**
  - `//<app>:bundles` (compile, part of `//:web`) and `//<app>:site` (a deployable folder made by one shared Bazel
    rule, with no per-app packaging script).
  - `//site:dist` combines Query, DataCube and Studio; PR #24 adds Studio.
  - DataCube's `make-dist.mjs` and its second packaging path go.
  - Packaging joins step 1 as a fifth target, `//:sites`.

- **D11. One naming scheme for the backend products, done at the end, as its own change.**
  - The four products: `core`, `db` (today `warehouse`), `sdlc` (today `sdlc-server`) and `depot` (today
    `depot-server`).
  - What it covers: the folders, the Java packages (`com.legend.warehouse` → `com.legend.db`, …), the targets, the docs
    and the CI lane names.
  - It is done after the build work, and coordinated through IN_FLIGHT, because every line of work touches these paths.
- **D12. Depot becomes its own server.**
  - Today `//depot-server` is only `:rules`, and the SDLC server hosts it at `/depot/api` (`SdlcServer.java:35,113`).
  - It gains its own server program beside SDLC's. It stays inside the SDLC page's WebAssembly for the in-browser
    Studio, unless that is decided otherwise.
  - Done with D11, or before it as a product change. It is not build work.

- **D5 (decided). DataCube's non-product files leave the shipped site** in the D10 step: the `remote_bundle` and
  `stress` bundles and pages, and `torture.pure`.
- **TypeScript type checking stays a test for now (decided).** Cleanup recorded: it becomes a build action of `//:web`
  with the native tsc work (section 4.6), and the gaps the audits found are closed then. Today the sdlc-client and
  depot-client tests and the harness scripts are unchecked, and three shared packages are checked twice under
  different settings.
- **D13 (open). `//warehouse:client`, the JDBC driver over the warehouse's HTTP API (`jdbc:warehouse:http://…`).**
  - Today only tests use it: `WarehouseServerTest`; the manual warehouse leg of the relational corpus
    (`DuckWorkspaces.java:174`); and, through the warehouse test library, `postgres_live` and the reachability-metadata
    generator.
  - No shipped server uses it, so it is not in `//:java`.
  - Keep it as product, so the engine can query our database server (it would join `//core:drivers`). Or delete it,
    with the corpus warehouse leg.

## 7. Open items the audits could not settle

Each says what would settle it.

- **Does the spec generator chain reach a fixed point in one run?** Run `//:update_generated` twice after a bump that
  moves `Pure.java`; the second run must change nothing.
- **Do the corpus passes really read `//core:srcs`?** Run each pass sandboxed without it.
- **Can `pct:ratchets` run without the properties only the tests pass?** Read `PctRatchets` against the test
  configuration.
- **Does whole-repo analysis in the light gate download GraalVM, Chromium or the upstream archives on a cold machine?**
  Run a cquery on a fresh output base, then list `external/`.
- **How much of the core compile is NullAway?** Compare a clean `//core:server` build with and without it, using
  execution logs, on a quiet machine.
- **Which DataCube and Studio UI handlers need trusted input?** Port one harness first (step 7).
- **Do the worker bundles import the whole app they take as input?** Check esbuild's metafile.
