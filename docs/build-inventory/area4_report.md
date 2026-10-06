# Area 4: corpus and stress data (scripts/corpus, //core stress/corpus/ladder/differential/scale, fixtures/saved-queries)

Repo: the bazel/exec checkout (branch bazel/exec). Read-only audit, 2026-10-05.
Targets: runs/inventory/area4_targets.tsv (95). Every target has a row below, or sits in a group whose members are named
and shown identical in shape.

**The working tree differs from HEAD.** `git -C <repo> diff --stat` shows uncommitted edits to `scripts/corpus/BUILD.bazel`
(+47 lines), `scripts/corpus/run.py` and `tools/engine-runner/BUILD.bazel` (P3-25 in progress). Three of my targets exist
only in that uncommitted diff: `//scripts/corpus:testable_launcher`, `:engine_stress` and `:run`. The inventory was
extracted from the working tree, so it lists them.

Evidence conventions: `path:line` is relative to the repo. "inv" means `runs/inventory/inventory.json` (users and deps).
"Lane X" means `.github/workflows/gates-run.yml:50-65`. "local" means `gates/BUILD.bazel:11-79` (`//gates:local`).
"build lane" means `bazel build //...` (gates-run.yml:63), which builds every non-manual target and the inputs of every
non-manual test.

Queries I ran (with `env -C <repo> bazel query`):
- Q1 `somepath(//fixtures/saved-queries:gen, //core:core)` returns gen, then //core:server_deploy.jar, //core:server, //core:core
- Q2 `somepath(//core:ladder_report, //core:src/test/resources/ladder/r01_constant.current.sql)` returns ladder_report, then core_tests_lib, then the committed pin
- Q3 `somepath(//core:ladder_report, //core:src/test/resources/stress/94-fanout-services.pure)` returns ladder_report, then core_tests_lib, then the stress file
- Q4 `somepath(//scripts/corpus:run, //core:core)` returns run, then //tools/engine-runner:testable, :runner, //core:core
- Q5 `kind(rule, deps(//scripts/corpus:gen_stress)) intersect //core/...` returns only //core:stress_layout and //core:stress_sources (no Java)
- A `rdeps(//..., …)` query failed: `//...` walks into the `bazel-bazel-exec` convenience link, whose packages fail to load. That is area 1's
  concern, so I used inv's users instead.

---

## The data flow, exactly

```
 HAND-WRITTEN INPUTS (committed)                                 GENERATORS (Bazel actions)                    COMMITTED OUTPUTS / CONSUMERS
 ───────────────────────────────                                 ──────────────────────────                    ─────────────────────────────
 core/stress.bzl  STRESS_GENERATED, LINKED_PROJECTS ──► //core:stress_layout (Starlark write, stress_layout.json)
        │                                                   │  ├─► gen_dense / gen_stress  (env STRESS_LAYOUT)
        │                                                   │  └─► update_stress_corpus_10 ──► core/src/test/resources/com/legend/integration/stress-layout.json
        │                                                                                         (read by gates, model.py fallback, StressCorpus.java)
        ▼
 core/src/test/resources/stress/*.pure  minus the 10 generated  = //core:stress_sources (192 files)
 projects/<11 LINKED_PROJECTS>/**  (//projects:<p>_files)
 scripts/corpus/queries.pure (12 hand-written services)
 scripts/corpus/*.py  (:corpus py_library, 29 modules; tzdata)
        │
        ├──► //scripts/corpus:gen_dense   tool :dense_build  (dense_build.py)  args --out dense --inputs <sources+projects>
        │        outs: dense/59-dense-mapping.pure, dense/60-dense-store.pure, dense/64-combinations.pure
        │        └─► update_stress_corpus_0..2 (write) / _0.._2_test (diff) ──► core/src/test/resources/stress/{59,60,64}-*.pure
        │
        ├──► //scripts/corpus:gen_stress  tool :build (build.py)  args --dense <gen_dense out> --out stress --inputs …
        │        srcs = gen_dense's inputs + :gen_dense   (reads the dense OUTPUTS, not the committed 59/60/64: BUILD:117-118)
        │        outs: stress/92-services … 98-combination-execution.pure (7 files)
        │        └─► update_stress_corpus_3..9 / _3.._9_test ──► core/src/test/resources/stress/{92..98}-*.pure
        │
        ▼
 COMMITTED STRESS CORPUS: all 202 files = //core:stress_files  (generated 10 included)
        │
        ├──► //scripts/corpus:gen_differential  tool :differential (differential.py), srcs = queries.pure + committed layout
        │        + stress_files + linked projects;  outs: diff/seed.sql + tree diff/expected/   (NOT committed)
        │        └─► //core:corpus_differential_test (data)
        ├──► density/executed/stacking/scoreboard/functions _gate py_tests (data; + docs/*.tsv for the last two)
        ├──► engine_stress / run (data; + //tools/engine-runner:testable)            [uncommitted]
        ├──► //core:stress_index (Starlark write of basenames) ──► stress-index.txt (build output, a resource of core_tests_lib)
        └──► core_tests_lib resources = glob(src/test/resources/**)  (core/BUILD.bazel:315-324)
                 ├─► stress_suites, stress_suites_h2, stress_tool, corpus_differential_test  (StressCorpus readers)
                 └─► EVERY other core_tests_lib user too (36 users per inv: 24 core_tests_*, guardrails, census, ladder_report…)

 LADDER:  LadderRender.java (inline model, in core_tests_lib) + core + drivers
        ──► //core:ladder_report (java_run, runs 12 rungs on in-memory DuckDB)  outs ladder/r01..r12.current.sql
        └─► update_ladder_0..11 / _0.._11_test ──► core/src/test/resources/ladder/*.current.sql
                 (the pins are core_tests_lib resources too, so they are inputs of ladder_report itself: Q2)
                 LeanSqlLadderTest (core_tests_ladder) reads pins + hand-written *.lean.sql as resources

 SAVED QUERIES:  make.mjs (records hard-coded in it) + //core:server_deploy.jar + query/demo/models/{trading,runtime-h2}.pure + host JDK
        ──► //fixtures/saved-queries:gen (js_run_binary: starts the server, POSTs 4 records, GETs them, runs each to README counts)
                 outs gen/*.json (4)
        └─► update_generated_0..3 / _0.._3_test ──► fixtures/saved-queries/*.json
                 └─► :records js_library (glob *.json, the COMMITTED copies) ──► datacube/query/query-store tests
```

### Each step: its true trigger, and what reruns it today

| step | inputs Bazel sees (evidence) | TRUE trigger | reruns today when |
|---|---|---|---|
| stress_layout | none: a Starlark `ctx.actions.write` of two constants (core/stress.bzl:39-48) | an edit to core/stress.bzl | exactly that. Correct. |
| gen_dense | dense_build + the whole `:corpus` lib (29 .py), queries.pure, stress_layout, 192 stress_sources, 11 linked projects (scripts/corpus/BUILD.bazel:91-115; inv deps) | dense_build.py's import closure (14 modules: dense_mapping, dense_store, combos, flat, model, oracle, query, seed, partition, rhs, views, expand, exactmath, aggregate; AST walk of the imports), the hand-written stress sources, linked projects, layout | any of the 29 modules (15 are outside its closure: battery, density, executed, stacking, spread, …), and queries.pure. dense_build never calls `query.load()`, the only reader of queries.pure: it is called from build.py, differential.py, executed.py and oracle.py's `__main__` (oracle.py:2722-2729). So queries.pure is over-declared here. Never an engine edit (reaches = projects,python). |
| gen_stress | the same + :gen_dense (BUILD:119-132) | build.py's closure (27 of 29 modules: all but dense_mapping, dense_store), queries.pure, sources, projects, layout, dense outputs | as declared: close to exact. Never an engine edit (Q5). Cost: 287 s, 146 s after P2-05 (bazel-plan/docs/BAZEL_EXECUTION_LOG.md:141). |
| update_stress_corpus_* (write and diff) | the gen outputs | the same as their generator | whenever the generator reruns. The diff tests are in //:generated (BUILD.bazel:29). |
| gen_differential | differential.py + :corpus, queries.pure, the committed layout, committed stress_files, projects (BUILD:189-205) | differential.py's closure (16 modules), the committed corpus, projects | any :corpus module and any stress file. Never an engine edit. |
| stress_index | the basenames of 202 stress files (core/stress.bzl:50-55) | a stress file added, removed or renamed | Bazel re-analyses on any stress edit, but the action is a constant write. Effectively correct. |
| ladder_report | core_tests_lib (all of core's test sources and resources, so the 13 MB stress corpus (Q3) and its own committed pins (Q2)), drivers, core | an engine change that moves the SQL emitted for the 12 rungs (lowering, dialect, runner), or an edit to LadderRender.java | every core edit, every core TEST edit, every stress-corpus edit, and every re-pin (its own output is its input). |
| fixtures gen | //core:server_deploy.jar (so all of core: Q1), make.mjs, two demo models, the host Java runtime (fixtures/saved-queries/BUILD.bazel:33-62) | the saved-query record CONTRACT: the server's Query serialisation and /api/pure/v1/query, make.mjs's record literals, the demo models | every engine edit (the deploy jar changes). |

---

## Part A: one row per target (or per proven-identical group)

Column key: **reads** = what it reads that matters; **SHOULD** = the change that should make it run; **TODAY** = what
makes it run today.

### //core (stress, ladder, differential, scale)

| target | kind | what it is | reads | produces | who uses it | SHOULD | TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:core_tests_ladder | java_test (junit_test) | Gate-1 package target for `com.legend.ladder` (core/BUILD.bazel:425-438). It runs LeanSqlLadderTest (it asserts every rung has a committed `.current.sql` pin and prints the distance to `.lean.sql`: LeanSqlLadderTest.java:31-48) and FrameQuotedColumnTest (a rung-12 shape run on a fresh H2: FrameQuotedColumnTest.java:61-82) | core_tests_lib (its classpath resources include the committed ladder pins), drivers | test verdict | //core:core_tests (inv), so lane 1 and local; compiled in the build lane | a change to frames/quoting in core, or a ladder pin or lean file | any core or core-test edit (the package-split target still depends on all of core_tests_lib) | TEST-INTEGRATION | FrameQuotedColumnTest executes against H2. LeanSqlLadderTest is a report that asserts only that the pins exist. |
| //core:corpus_differential_test | java_test (junit_test, memory 768, size large) | CorpusDifferentialTest (@Tag("differential")): compiles the stress model, renders each stress service's SQL with the DuckDB dialect, runs it on DuckDB seeded from seed.sql, and compares with the oracle's expected/ (CorpusDifferentialTest.java:38-80). Quarantine.txt: a quarantined service that agrees fails (core/BUILD.bazel:459-476) | gen_differential's tree (seed.sql, expected/), core_tests_lib (StressCorpus reads the committed corpus as resources) | verdict | local (gates/BUILD.bazel:49), lane 1 (gates-run.yml:50); excluded from core_tests_integration by tag and classname (core/BUILD.bazel:431) | an engine change on the SQL path (lowering, dialect, DuckDB exec), the committed corpus, or the oracle/differential.py | every engine edit and every core-test edit (whole core_tests_lib); every :corpus module edit (via gen_differential) | TEST-CORPUS | "seconds" (gates/BUILD.bazel:48). Running on every engine edit is right. Running on every edit to an unrelated test is not. |
| //core:ladder_report | _java_run (java_run, testonly) | Runs `com.legend.ladder.LadderRender`: 12 rungs of an inline model run in DATABASE mode on in-memory DuckDB, every statement captured, written as `<rung>.current.sql` (core/BUILD.bazel:786-816; LadderRender.java javadoc). The action FAILS if a rung does not pass (LadderRender.java:180) or the rung list differs from BUILD (:144) | core_tests_lib (all test sources and resources: Q2, Q3), drivers, core | ladder/r01..r12.current.sql (12) | update_ladder_0..11, update_ladder_*_test (inv: 25 users incl. guard_classpaths) | an engine change that moves the rungs' SQL, or LadderRender.java | every core edit, core-test edit, stress-corpus edit and ladder re-pin; run by the build lane, and by checks + local through //:generated | GEN-COMMITTED | True trigger is an engine change (legitimately engine-dependent: it pins our emission). But its scope is far wider (all of core_tests_lib), and it is a test hidden in a build action (it fails on a non-passing rung). |
| //core:scale | java_binary (legend_java_binary, manual, testonly) | `bazel run //core:scale -- 10k\|100k\|dense\|complex\|chaotic\|profile`: runs one scale class on the JUnit Platform, -Xmx6656m (core/BUILD.bazel:616-646; Scale.java:15-43) | scale_lib, core_tests_lib (the StressTest* classes live there), drivers, junit engine | timings on stdout | humans (docs/GATES.md:6064 cites a `scale_stresstest100k` run, an earlier name); guard_classpaths (provider-only, no build) | by hand, when measuring parser/compiler scale | manual: built and run only by hand | TOOL | — |
| //core:scale_lib | java_library (testonly) | the one file src/scale/java/.../Scale.java (core/BUILD.bazel:621-629) | junit platform launcher/engine (maven_test) | jar | //core:scale only (inv) | only when :scale builds | compiled by the build lane (not manual) | TOOL | A tool compiled by `bazel build //...` that only a manual binary uses. Tiny. |
| //core:stress_files | filegroup | every stress .pure, the generated 10 included (core/BUILD.bazel:750-755) | 202 committed files | — | gen_differential, the 5 gates, engine_stress, run (inv) | n/a | n/a | WIRING | Still used. |
| //core:stress_sources | filegroup | the stress glob minus STRESS_GENERATED (core/BUILD.bazel:757-764) | 192 files | — | gen_dense, gen_stress (inv) | n/a | n/a | WIRING | Keeps generators from reading their own outputs. Still used. |
| //core:stress_index | stress_index (Starlark rule) | the stress files' basenames, sorted, as `.../stress-index.txt` (core/stress.bzl:50-63; core/BUILD.bazel:743-748) | 202 stress files (names only) | stress-index.txt (build output, not committed) | core_tests_lib resources (core/BUILD.bazel:315), read by StressCorpus instead of listing a directory | a stress file added, removed or renamed | stress-dir edits (an analysis-time write, cheap) | GEN-BUILD | Consumer: core_tests_lib (StressCorpus). |
| //core:stress_layout | stress_layout (Starlark rule) | STRESS_GENERATED + LINKED_PROJECTS as JSON (core/stress.bzl:39-48) | core/stress.bzl constants | stress_layout.json | gen_dense, gen_stress (env STRESS_LAYOUT: scripts/corpus/BUILD.bazel:79); update_stress_corpus_10 (+_test) writes the committed copy | an edit to core/stress.bzl | exactly that | GEN-COMMITTED | It is also GEN-BUILD (generators read the live one). The committed copy feeds the gates, model.py's fallback (model.py:53-54) and StressCorpus.java. True trigger = stress.bzl edit, and that is what reruns it. |
| //core:stress_suites | java_test (junit_test, size enormous, memory 768) | GATE 10 on DuckDB: StressServiceSuitesTest (@Tag("stress")) runs every stress service suite (about 4,736 tests) through legend-lite on shared, write-detected sessions; asserts a count ≥ MIN_PASS=4705 (StressServiceSuitesTest.java:11-33) | core_tests_lib (committed corpus as resources), drivers | verdict | lane 10 only (gates-run.yml:60); NOT in local (gates/BUILD.bazel:4-5 says CI-only); compiled in the build lane | any engine change (compiler, resolver, lowering, dialect, exec) or stress-corpus change | lane 10 on every push; reruns on any core or core-test edit | TEST-STRESS | Long-running by nature: it compiles a 13 MB model and executes thousands of services. `bazel test //...` (docs/GATES.md "the whole chain") also runs it. |
| //core:stress_suites_h2 | java_test (junit_test, enormous) | the same suites on H2 2.4.240, a fresh, freshly seeded session per test, floor MIN_PASS_H2=4676 (StressServiceSuitesH2Test.java:6-17) | same | verdict | lane 10 only | same as stress_suites (plus the H2 dialect) | same | TEST-STRESS | Heavier than the DuckDB lane per test (re-seeds per test). |
| //core:stress_tool | java_binary (manual, testonly) | `bazel run //core:stress_tool -- [--backend] [--sessions] [--only] [--data] [--rows] [--out]`: the stress measuring knobs moved out of the test (core/BUILD.bazel:676-690; StressTool.java:8-17) | core_tests_lib, drivers | ledgers and row dumps under --out | humans (docs/GATES.md:28; tools/wrongrows/README.md) | by hand, when investigating | manual | TOOL | — |
| //core:update_ladder | _write_source_file (aggregate, testonly) | `bazel run //core:update_ladder` writes all 12 pins (core/BUILD.bazel:818-824) | update_ladder_0..11 | — | //:update_generated (BUILD.bazel:56) | n/a | built by the build lane; run by hand | WIRING | Grouping in use. But it sits inside //:update_generated, so a blanket regenerate re-pins an engine golden silently (Part B, B6). |
| //core:update_ladder_0 … _11 (12: _0,_1,_2,_3,_4,_5,_6,_7,_8,_9,_10,_11) | _write_source_file | one per rung r01..r12, the same macro expansion (write_source_files, core/BUILD.bazel:818; each has exactly 1 dep, //core:ladder_report, and 1 user, update_ladder: inv) | ladder_report's output | writes core/src/test/resources/ladder/rNN_*.current.sql | update_ladder | an engine change that moves the emission (a deliberate re-pin) | built by the build lane (that builds ladder_report); written by hand | GEN-COMMITTED | True trigger: engine change. Today: built on every core or core-test edit. |
| //core:update_ladder_0_test … _11_test (12) | _diff_test (small) | compares each committed pin with ladder_report's output; identical shape (1 dep, ladder_report; users guard_markdown + update_ladder_tests: inv) | committed pin + generated pin | verdict | update_ladder_tests, then //:generated, then the checks lane and local | an engine change (emission moved) | every core or core-test edit (via ladder_report) | CHECK-DIFF | An engine-dependent golden inside //:generated (the "generated files" lane). |
| //core:update_ladder_tests | test_suite | the 12 ladder diff tests | — | — | //:generated (BUILD.bazel:29) | n/a | checks lane, local | WIRING | In use. |
| //core:update_stress_corpus | _write_source_file (aggregate) | `bazel run //core:update_stress_corpus` (core/BUILD.bazel:766-777) | _0.._10 | — | //:update_generated (BUILD.bazel:57) | n/a | built by the build lane; run by hand | WIRING | In use. The failure message names it (:768). |
| //core:update_stress_corpus_0, _1, _2 | _write_source_file | the three dense files 59, 60, 64 (inv: dep //scripts/corpus:gen_dense each) | gen_dense output | writes stress/{59-dense-mapping,60-dense-store,64-combinations}.pure | update_stress_corpus | the gen_dense true trigger (dense generator code, hand-written sources, projects, layout) | gen_dense reruns (any :corpus module, queries.pure, any source file); built in the build lane | GEN-COMMITTED | True trigger is a corpus or generator change, never an engine change. Correctly engine-free (reaches projects,python). |
| //core:update_stress_corpus_3 … _9 (7: _3,_4,_5,_6,_7,_8,_9) | _write_source_file | the seven build.py files 92-98 (inv: dep //scripts/corpus:gen_stress each) | gen_stress output | writes stress/{92..98}-*.pure | update_stress_corpus | the gen_stress true trigger | gen_stress reruns; built in the build lane | GEN-COMMITTED | The same as above. |
| //core:update_stress_corpus_10 | _write_source_file | the committed stress-layout.json (dep //core:stress_layout) | stress_layout | writes core/src/test/resources/com/legend/integration/stress-layout.json | update_stress_corpus | a stress.bzl edit | a stress.bzl edit | GEN-COMMITTED | reaches = none (pure Starlark). Correct. |
| //core:update_stress_corpus_0_test … _10_test (11) | _diff_test (small) | the committed copy vs the generator output; identical shape (1 dep each: gen_dense for 0-2, gen_stress for 3-9, stress_layout for 10; users guard_markdown + update_stress_corpus_tests: inv) | committed + generated | verdict | update_stress_corpus_tests, then //:generated, then checks and local | a corpus or generator change | as the generators | CHECK-DIFF | D8 decided they stay in the default `//...` (workplan D8). |
| //core:update_stress_corpus_tests | test_suite | the 11 diff tests | — | — | //:generated (BUILD.bazel:30) | n/a | checks, local | WIRING | In use. |

### //fixtures/saved-queries

| target | kind | what it is | reads | produces | who uses it | SHOULD | TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //fixtures/saved-queries:records | js_library | the 4 COMMITTED records (`glob(["*.json"])`), the contract Query writes and DataCube reads (fixtures/saved-queries/BUILD.bazel:5-16; README.md) | the committed *.json | — | //datacube:saved_queries_test, :verify_features, :verify_features_test, //query:saved_queries_test, :load_test, :build_test, //query-store:share_test (inv) | n/a | n/a (data) | WIRING | In use by 7 targets across the app and misc lanes and local. |
| //fixtures/saved-queries:make | js_binary | make.mjs: starts the server deploy jar on port 0 with `--query-store`, POSTs 4 hard-coded records, GETs them back, re-runs each to README's row counts (throws otherwise: make.mjs:51,89), zeroes timestamps | make.mjs | — | :gen, :gen_runfiles | only when :gen runs | built by the build lane | TOOL | — |
| //fixtures/saved-queries:gen | _run_binary (js_run_binary) | runs :make with --java $(JAVA), --server-jar, two demo models (BUILD.bazel:33-62) | //core:server_deploy.jar (so all of core: Q1), query/demo/models/{trading,runtime-h2}.pure, host Java runtime | gen/*.json (4) + store/ tree | update_generated_0..3, update_generated_*_test | the record contract: the server's Query serialisation or /api/pure/v1/query, make.mjs's record literals, the demo models (and, for the row-count check, query execution) | EVERY engine edit (deploy jar). Run by the build lane, and by checks + local via //:generated | GEN-COMMITTED | Also a server end-to-end check hidden in a build action (the row counts). |
| //fixtures/saved-queries:gen_js_info_files | js_info_files (manual) | js_run_binary's internal JS-info collector (macro expansion, BUILD.bazel:33) | //core:server (inv dep) | providers | :gen | n/a | only as gen's dependency | WIRING | macro-internal |
| //fixtures/saved-queries:gen_runfiles | filegroup (manual) | js_run_binary's internal runfiles group of :make | :make | — | :gen | n/a | as gen's dep | WIRING | macro-internal |
| //fixtures/saved-queries:update_generated | _write_source_file (aggregate) | `bazel run //fixtures/saved-queries:update_generated` (BUILD.bazel:64-69) | _0.._3 | — | //:update_generated (BUILD.bazel:61) | n/a | built by the build lane; run by hand | WIRING | In use. |
| //fixtures/saved-queries:update_generated_0 … _3 (4: data-space-context, default-parameter-values, explicit-context, graph-fetch) | _write_source_file | one per record, the same macro expansion (1 dep :gen, 1 user update_generated: inv) | gen output | writes fixtures/saved-queries/<r>.json | update_generated | the record contract (above) | every engine edit (via gen) | GEN-COMMITTED | True trigger: a server query-store or serialiser change (or make.mjs, the demo models). Today: every engine edit. |
| //fixtures/saved-queries:update_generated_0_test … _3_test (4) | _diff_test (small) | committed record vs gen output; identical shape (1 dep :gen; users guard_markdown + update_generated_tests) | — | verdict | update_generated_tests, then //:generated | the record contract | every engine edit | CHECK-DIFF | — |
| //fixtures/saved-queries:update_generated_tests | test_suite | the 4 diff tests | — | — | //:generated (BUILD.bazel:34) | n/a | checks, local | WIRING | In use. |
| //fixtures/saved-queries:all_files | filegroup (guards_package) | every file in the package, for the repository inventory guard (tools/guards/defs.bzl:109-) | 7 files | — | //tools/guards:repository_files (inv) | n/a | the inventory guard | WIRING | In use. |

### //scripts/corpus (Python, rules_python)

| target | kind | what it is | reads | produces | who uses it | SHOULD | TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //scripts/corpus:corpus | py_library | the 29 modules that build.py and the dense generators import, `imports=["."]`, dep @pypi//tzdata (scripts/corpus/BUILD.bazel:15-54) | 29 .py | — | build, dense_build, differential, 5 gates, engine_stress, run (inv: 10) | n/a | built as a dep | TOOL | One library for three generators and five gates. Every user takes all 29 modules (Part B, B7). |
| //scripts/corpus:build | py_binary | build.py: the L0 driver that writes 92-98 from the seed and the oracle, `--out DIR [--dense DIR]`; writes nothing unless every stage passes (build.py:1-17) | :corpus | — | gen_stress (tool) | when gen_stress runs | built by the build lane | TOOL | — |
| //scripts/corpus:dense_build | py_binary | dense_build.py: writes 59, 60, 64 from the corpus WITHOUT themselves, from committed seed lists (dense_build.py:1-12) | :corpus | — | gen_dense | when gen_dense runs | build lane | TOOL | Stale docstring: it says "//core:update_generated writes the files back" (dense_build.py:9); the writer is //core:update_stress_corpus (core/BUILD.bazel:766). |
| //scripts/corpus:differential | py_binary | differential.py: seed.sql (DuckDB DDL and inserts from the DECLARED schema) + expected/<Svc>.txt, the oracle's normalised answers (differential.py:1-25) | :corpus | — | gen_differential | when gen_differential runs | build lane | TOOL | — |
| //scripts/corpus:gen_dense | _run_binary | see the data flow (BUILD:104-115) | dense_build, :corpus, queries.pure (over-declared), stress_layout, stress_sources, 11 projects | dense/{59,60,64}.pure | update_stress_corpus_0..2 (+tests), gen_stress (inv: 7) | dense_build's closure, hand-written sources, projects, layout | any :corpus module, queries.pure, any source or project file; run by the build lane and by //:generated (checks, local) | GEN-COMMITTED | Trigger: corpus or generator change, never an engine change (reaches projects,python). Slight over-trigger (15 modules plus queries.pure outside its closure). 41 s (workplan P2-01 proof, S5 E3). |
| //scripts/corpus:gen_stress | _run_binary | see the data flow (BUILD:119-132) | build, :corpus, queries.pure, layout, sources, projects, gen_dense | stress/{92..98}.pure | update_stress_corpus_3..9 (+tests) (inv: 14) | build.py's closure, queries, sources, projects, layout, dense outputs | as declared; build lane and //:generated | GEN-COMMITTED | The heaviest generator (146 s single core after P2-05: execution log :141). Correctly engine-free. Because it is in //:generated, every corpus edit costs this in local (D8 accepted that). |
| //scripts/corpus:gen_differential | _run_binary | see the data flow (BUILD:187-205) | differential, :corpus, queries.pure, committed layout, committed stress_files, projects | diff/seed.sql + tree diff/expected (not committed) | //core:corpus_differential_test (data and dep: inv) | differential.py's closure, the committed corpus, projects | as declared; build lane, lane 1, local | GEN-BUILD | Consumer: corpus_differential_test. Engine-free. |
| //scripts/corpus:density_gate | py_test (medium) | density.py --gate: regex counts of mapping/store features over the corpus; fails if the plain-1:1 ratio passes MAX_PLAIN_RATIO or absent features pass MAX_ABSENT (density.py:147-159) | committed stress files, committed layout, queries.pure, projects (BUILD:138-160) | verdict | local (gates/BUILD.bazel:39), checks lane (gates-run.yml:51) | a corpus change or a density.py change | any :corpus module, stress file or project file. Never an engine edit (reaches projects,python) | CHECK-GUARD | A ratchet over corpus TEXT. Light: no engine, no execution. |
| //scripts/corpus:executed_gate | py_test (medium) | executed.py --gate: per taxonomy feature, is there a non-quarantined generated spec that runs through it; fails on any `ok is False` (executed.py:464-467) | same | verdict | local, checks | a corpus change, a quarantine.py change, or a generator change | same | CHECK-GUARD | Evaluates the generated specs in Python (resolves the model): heavier than density but no engine. |
| //scripts/corpus:stacking_gate | py_test (medium) | stacking.py --gate: distinct feature pairs and triples over passing services ≥ SURFACE_BASELINE {258, 815} (stacking.py:44, 233-245) | same | verdict | local, checks | same | same | CHECK-GUARD | Same weight class as executed (it imports it). |
| //scripts/corpus:scoreboard_gate | py_test (medium) | scoreboard.py --gate: every construct of docs/ENGINE_SURFACE.tsv is written by the corpus or listed in docs/SURFACE_BLOCKED.tsv (scoreboard.py:228-236) | the same + 2 docs TSVs (BUILD:164-178) | verdict | local, checks | a corpus change, or a change to either TSV | same, plus the TSVs | CHECK-GUARD | ENGINE_SURFACE.tsv has no generator (docs/BUILD.bazel:8 only exports it). |
| //scripts/corpus:functions_gate | py_test (medium) | functions.py --gate: every function the oracle implements appears in docs/FUNCTIONS_EXECUTED.tsv, the "ran against the engine" record (functions.py:352-362) | the same + ENGINE_FUNCTIONS.tsv, FUNCTIONS_EXECUTED.tsv | verdict | local, checks | an oracle (implemented-function) change, or a TSV change | same | CHECK-GUARD | Its remedy is "Add a case to whichever probe" (functions.py:361), but probe_functions.py, the writer of FUNCTIONS_EXECUTED.tsv (functions.py:42), cannot run (B5). |
| //scripts/corpus:engine_stress | py_test (manual, enormous) [uncommitted] | run.py: every asserted case of the corpus through legend-engine's `//tools/engine-runner:testable`, in batches of 200 (one JVM each, 6 GB, 1200 s), adjudicated against quarantine.py: PASS, KNOWN-FAIL, REGRESSION, FIXED (run.py:1-20 and the working-tree diff) | committed corpus, layout, projects, queries, testable (upstream legend-engine jars + core: Q4) | verdict | no lane, no suite (manual); planned for the weekly heavy suite (workplan D18: P5-08) | an upstream legend-engine bump, or a corpus or quarantine change (it judges the CORPUS against the reference engine, not our engine) | nothing (manual) | TEST-CORPUS | Reads TEST_TOTAL_SHARDS (run.py diff), but the target sets no shard_count, so it runs as one shard. |
| //scripts/corpus:run | py_binary (testonly, NOT manual) [uncommitted] | the same run.py by hand: `bazel run //scripts/corpus:run -- [-v] [--rows=DIR] [--data=…]` (BUILD:239-251) | as engine_stress | stdout / rows dir | humans (tools/wrongrows/README.md, engine-rows.sh) | by hand | the build lane would build it, and so //tools/engine-runner:testable with the upstream engine jars and core (Q4; reaches upstream_jars) | TOOL | A tool that `bazel build //...` will compile once this lands, though nothing in the build runs it. |
| //scripts/corpus:testable_launcher | executable_of (testonly) [uncommitted] | names exactly the testable binary's launcher file, for $(rlocationpath) (tools/runfiles/defs.bzl:1-13) | //tools/engine-runner:testable | 1 file | engine_stress, run (inv) | with them | build lane (not manual) | WIRING | In use (by the two above). |
| //scripts/corpus:pure_sources | filegroup | this package's .pure (queries.pure + repro/verified .pure) for the keyword census (BUILD:180-185) | glob **/*.pure | — | //scripts/parser:our_pure, :keywords (inv) | n/a | n/a | WIRING | In use (P2-20). |
| //scripts/corpus:all_files | filegroup (guards_package) | every file in the package, for the inventory guard | 189 files | — | //tools/guards:repository_files | n/a | n/a | WIRING | This is the only BUILD reference to probe_*.py and the 15 loose scripts. |

### Verdict counts (95)

| verdict | n | members |
|---|---|---|
| GEN-COMMITTED | 32 | ladder_report, stress_layout, update_ladder_0..11 (12), update_stress_corpus_0..10 (11), fixtures gen, update_generated_0..3 (4), gen_dense, gen_stress |
| CHECK-DIFF | 27 | update_ladder_*_test (12), update_stress_corpus_*_test (11), update_generated_*_test (4) |
| WIRING | 15 | stress_files, stress_sources, update_ladder, update_ladder_tests, update_stress_corpus, update_stress_corpus_tests, records, gen_js_info_files, gen_runfiles, fixtures update_generated, update_generated_tests, fixtures all_files, corpus all_files, pure_sources, testable_launcher |
| TOOL | 9 | scale, scale_lib, stress_tool, make, corpus (py_library), build, dense_build, differential, run |
| CHECK-GUARD | 5 | density_gate, executed_gate, stacking_gate, scoreboard_gate, functions_gate |
| GEN-BUILD | 2 | stress_index, gen_differential |
| TEST-STRESS | 2 | stress_suites, stress_suites_h2 |
| TEST-CORPUS | 2 | corpus_differential_test, engine_stress |
| TEST-INTEGRATION | 1 | core_tests_ladder |
| COMPILE, DEAD, OPEN | 0 | Nothing here ships. No target is DEAD: each has a user, a lane or a doc. |

---

## Part B: problems, each with evidence

**B1. The SQL ladder golden depends on all of core's tests, and on itself.** ladder_report's deps are `core_tests_lib` + drivers
(core/BUILD.bazel:805-816). core_tests_lib's resources are `glob(["src/test/resources/**"])` (:315-324). So the action
reruns on any core-test edit, on any stress-corpus edit (Q3), and on its own committed pins (Q2). After `bazel run
//core:update_ladder` the inputs change, so the next build reruns it once more. LadderRender needs only its inline model,
core and drivers (LadderRender.java:39-140). It is also a test inside a build action: it throws when a rung "does not pass
in database mode" (LadderRender.java:180). So `bazel build //...` executes DuckDB queries and can fail on a semantic
regression.

**B2. The saved-query fixtures restart the server on every engine edit, in the build and in //:generated.** gen depends on
//core:server_deploy.jar, which depends on core (Q1). make.mjs starts it, saves 4 records and re-runs each to row counts,
throwing on a mismatch (make.mjs:51, 89). The true trigger is the record contract (the server's Query serialisation and API,
the demo models, make.mjs). Today it reruns on every engine edit in the build lane, the checks lane and local, and it is a
server end-to-end assertion hidden in a build action.

**B3. The stress corpus rides in core_tests_lib, so corpus and tests invalidate each other.** The 13 MB corpus (`du -sh
core/src/test/resources/stress`: 13M; 94-fanout-services.pure alone is 10.4 MB) is a resource of core_tests_lib
(core/BUILD.bazel:313-324). That library has 36 users (inv). Only 7 classes read the corpus: StressCorpus, StressSuites,
StressExclusions, StressServiceSuitesTest, StressServiceSuitesH2Test, StressTool, CorpusDifferentialTest (`grep -rln
"StressCorpus|StressSuites|stress-index|/stress/" core/src/test/java`, plus LegendLiteGapTest via StressExclusions).
Effects:
- A hand-written stress edit reruns all 24 core_tests_* packages, guardrails, census and ladder_report.
- Any core-test edit reruns gate 10 (the enormous stress lanes) and corpus_differential_test.
- Every core test JVM carries 13 MB of corpus on its classpath.

**B4. Generators and goldens run inside `bazel build //...`.** These run in the build lane:
- gen_dense, gen_stress (146 s), gen_differential (Python);
- ladder_report (Java plus DuckDB);
- fixtures gen (a server).

None of them is a compile, and the build lane exists to prove that everything builds (gates-run.yml:63; the BRIEF's
"Why"). The checks lane (//:generated) builds the same actions again, for its diff tests.

**B5. The 16 probe_*.py scripts are broken, and nothing runs them.** The probes are aggregates, boundary_navigation, collection,
column_types, derived_filter, extends_filter, functions, graphfetch_included_mapping, ineq_aggregate, milestoned_join,
missing_setid, project_deps, qualified_broken_chain, relation, remaining and tds. Every one does `import run as runner` and calls
`runner.RUNNER / "cp.txt"`, `runner.JAVA_HOME` and `<RUNNER>/target/classes` (for example probe_functions.py:40, 344-350;
`grep -n "runner\.\(RUNNER\|JAVA_HOME\)" scripts/corpus/probe_*.py` hits all 16).
- At HEAD, run.py defines `RUNNER = REPO/"tools"/"engine-runner"` and a host `JAVA_HOME` (`git show HEAD:scripts/corpus/run.py`:32-33, 90-91).
  But tools/engine-runner has no cp.txt and no Maven build: its pom.xml was deleted by 998f1a41c (2026-09-23, "Bazel replaces Maven"),
  and `git ls-tree HEAD tools/engine-runner/` lists BUILD.bazel, README.md, src and vocab.tsv only. So they have failed since 2026-09-23.
- In the working tree, the P3-25 edit deletes RUNNER and JAVA_HOME from run.py altogether (git diff), so every probe would raise
  AttributeError.
- No BUILD file references them. The only reference is the `all_files` glob, and no lane or gate names them.
- Decision D18 (workplan :392-399) and P7-03 plan them as `bazel run` py_binaries through `run.launch()`.

The consequence today: docs/FUNCTIONS_EXECUTED.tsv, which functions_gate requires (last changed 559ddd287, 2026-08-16), cannot be
regenerated. So the gate's own instructions (functions.py:361) cannot be followed.

**B6. `//:update_generated` silently re-pins the ladder golden.** The ladder pins move only with an engine change, and the diff
test says "if the shape change is deliberate, re-pin… and review the diff" (core/BUILD.bazel:821). The corpus rosters are kept out
of //:update_generated for exactly that reason: "re-blessed only deliberately… never as a side effect" (BUILD.bazel:46-50). Yet
update_ladder is in its additional_update_targets (BUILD.bazel:56). The saved-query records are the same kind of thing
(engine-produced bytes), with the same issue.

**B7. One py_library feeds generators with very different closures.** :corpus has 29 modules. Import closures, from an AST walk:
dense_build uses 14, differential 16, density and functions 12, and build, executed, stacking and scoreboard 23-27. So:
- An edit to density.py's ratchet constant reruns gen_dense (41 s) and gen_differential, then corpus_differential_test.
  (gen_stress reruns too, but build.py really imports density.)
- gen_dense also declares queries.pure, which its code never reads (only build.py, differential.py, executed.py and
  oracle.py's `__main__` call `query.load()`).

**B8. The scale benchmarks are kept out of gate 1 only by class-name luck.** StressTest10K, StressTest100K, StressTestDense,
StressTestComplexQueries, StressTestChaotic and ProfileBuildCost live in `core/src/test/java/com/legend/integration`, so they are
compiled into core_tests_lib. core_tests_integration selects that whole package, excluding only the tags census, differential,
guardrail and stress (core/BUILD.bazel:418-433). Two of them carry `@Tag("heavy")`, which no target excludes. They stay out
because JUnitMain applies `ClassNameFilter.STANDARD_INCLUDE_PATTERN` (JUnitMain.java:184-185), and their names do not end in
Test/Tests. A rename to `…Test` would put a 6.6 GB benchmark (core/BUILD.bazel:620) into gate 1.

**B9. engine_stress and run are not finished wiring (uncommitted).** `:run` is a non-manual testonly py_binary, so once committed,
`bazel build //...` builds //tools/engine-runner:testable (reaches upstream_jars and core: Q4) for a tool nothing in the build
runs. run.py splits batches by TEST_TOTAL_SHARDS, but engine_stress sets no `shard_count`, so it runs as one shard. No lane runs
engine_stress yet; P5-08 is planned.

**B10. Loose scripts with no target.** add_taxonomy_edges, brokerage, coverage, curves, curves2, largeexp, mutate, refdata,
schedule, taxa_{exotics,infra,markets,more,ops} and timeseries are in no `srcs` (scripts/corpus/BUILD.bazel:16-48). By their
docstrings they are one-shot writers of the hand-written stress sources, or measurements (coverage, mutate). Under Bazel only
`all_files` sees them. They are not my targets; I record them so area 7 or 8 can decide whether to keep them as history or
retire them.

**B11. Gate 10 is CI-only and engine-triggered.** A push from `//gates:local` can lower the stress pass count below
MIN_PASS/MIN_PASS_H2, and only lane 10 sees it (gates/BUILD.bazel:4-5; gates-run.yml:60). This is deliberate (heavy). Recorded
so that Part C keeps it as an explicit choice.

---

## Part C: the right shape for this area

Nothing in area 4 ships, so the everyday compile should contain none of it except what tests compile. Proposals, each with
the evidence behind it:

1. **Give the stress corpus its own test library** (B3). Create `//core:stress_lib` (testonly), holding StressCorpus, StressSuites,
   StressExclusions, StressTool, StressServiceSuites{,H2}Test and CorpusDifferentialTest, with `resources` = the stress glob +
   :stress_index + the committed layout + the linked projects. It depends on :core, //testing and the small shared test
   helpers. stress_suites, stress_suites_h2, stress_tool and corpus_differential_test take it instead of core_tests_lib, and
   core_tests_lib's resources glob excludes `stress/**`.
   - Result: corpus edits rerun only the corpus's tests; core-test edits no longer rerun gate 10.
   - LegendLiteGapTest reads StressExclusions; move it too, or keep StressExclusions in a lib both can see.
   - **Depends on** the area owning core_tests_lib (P3-06, the test-lib split).
2. **Narrow the ladder** (B1, B6).
   - Move LadderRender into `//core:ladder_lib` (testonly; one file; deps :core, :drivers, the PureTestRunner it uses). ladder_report
     depends on that. LeanSqlLadderTest reads the pins from a resources-only target over `src/test/resources/ladder/**`, kept out of
     core_tests_lib so a re-pin does not feed back.
   - Remove `//core:update_ladder` from `//:update_generated`'s additional_update_targets and treat it like the rosters: a
     deliberate `bazel run //core:update_ladder`.
   - Move `update_ladder_tests` out of `//:generated` into gate 1, next to core_tests_ladder: its trigger is an engine change, which
     is gate 1's trigger.
3. **Saved queries: separate the contract from the run check** (B2, B6). Its true trigger is the record contract.
   - The row-count assertion in make.mjs is a server integration test. Make it a test (for example a js_test in //query-store or
     the misc lane that starts the deploy jar and runs the 4 committed records), which reruns on server changes as an integration
     test should.
   - Keep `gen` as the record writer, but drop it from `//:update_generated` and from `//:generated` (a deliberate regenerate when
     the Query shape changes). Or keep the diff test, but in the misc or app lane, which already rebuilds the server, not in the
     "generated files" checks.
   - A narrower dependency than server_deploy.jar would need a query-store library split from //core:server. **OPEN:** whether
     such a target can exist. To settle it, look at what `LegendHttpServer --query-store` needs to round-trip a record without the
     engine. That is the server area's decision.
4. **Keep generators out of the compile set** (B4, B9). gen_dense, gen_stress, gen_differential, ladder_report and fixtures gen
   should not run in the build lane's "everything builds" step.
   - Either the build lane builds a compile-only suite or tag filter (for example `--build_tag_filters=-generator`, with the
     tag set by the write_source_files and run_binary call sites), or these targets become manual and are reached only through
     their diff tests and test data.
   - Make `//scripts/corpus:run` manual (as engine_stress already is), so the build lane does not compile the upstream engine
     runner.
   - **Depends on** the decision about the build lane's scope (area 1 or the CI owner).
5. **Split :corpus by closure** (B7).
   - `corpus_core`: model, flat, seed, oracle, query, partition, rhs, views, expand, exactmath, aggregate, combos.
   - `corpus_dense`: dense_mapping, dense_store.
   - `corpus_measure`: density, executed, stacking, spread, battery, aggregates, graphs, hier, stacks, taxonomy, tomany, quarantine,
     functest, emit, deepstack.

   dense_build takes core + dense. differential, build and the gates take what their AST closures show. Drop queries.pure from
   gen_dense's srcs. Then a ratchet-constant edit reruns only the gates. The generators stay engine-free, which they already are
   (Q5; reaches projects,python), and gen_stress stays in //:generated per D8.
6. **The five corpus gates are right as they are** (CHECK-GUARD, checks lane and local, reaching only projects and python).
   Restore the means to satisfy functions_gate (B5): P2-18 (probe_functions `--record` as an action with a diff test over
   FUNCTIONS_EXECUTED.tsv) and P7-03 (the probes as `bazel run` py_binaries through `run.launch()`). Until then, record in
   functions.py that its remedy is unavailable.
7. **The scale benchmarks** (B8): move StressTest10K, 100K, Dense, ComplexQueries, Chaotic and ProfileBuildCost from
   src/test/java into src/scale/java under `scale_lib` (which only the manual :scale uses), and make scale_lib manual or testonly
   and unbuilt by default. Then core_tests_lib no longer compiles them, and gate 1 cannot pick them up by a rename.
8. **engine_stress** (B9): keep it manual, and put it in the weekly heavy suite (P5-08) with `shard_count` set, since run.py
   already shards. Its true trigger is a legend-engine bump or a corpus or quarantine change, never our engine: it judges the
   corpus against the reference. So it also belongs in the bump workflow's checks (tools/bump).
9. **Gate 10** stays a CI lane triggered by engine and corpus changes (B11). After proposal 1 it no longer reruns on unrelated
   test edits.
10. **Small fixes:** dense_build.py:9 should name `//core:update_stress_corpus`. stress_layout and stress_index are correct as
    they are (Starlark writes, triggered exactly by stress.bzl and file-set changes).
