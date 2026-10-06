# Area 2: spec, its generators, docs, tools/generators, tools/census (79 targets)

Repo: `runs/bazel-exec` (branch bazel/exec). Read-only audit; nothing was built, run or edited.
Paths in this report are relative to the repo root. `S:` = `spec/BUILD.bazel`, `C:` = `spec/corpus.bzl`,
`CORE:` = `core/BUILD.bazel`, `ROOT:` = `BUILD.bazel`, `GR:` = `.github/workflows/gates-run.yml`, `GL:` = `gates/BUILD.bazel`.

## Facts every row relies on (cite once)

- **F1. A java_run's inputs are the full runtime jars of its deps, not interface jars.** `tools/java_run/defs.bzl:46`
  (`transitive_runtime_jars`) and `:110` (passed as action `inputs`). So any java_run that reaches `//core` reruns
  whenever any core library's jar bytes change. That includes any code change, and any edit that moves lines, because class files carry line numbers.
  Proof: `bazel aquery 'mnemonic("Generate", //spec:gen_imports)'` lists 30 core jars as inputs (`libplanner.jar`,
  `libexec.jar`, `libserver_lib.jar`, `libdriver.jar`, ... plus `libjson`, `libbase`, `spec/libgenerators.jar`,
  `libclaims.jar`, `libsource_tree.jar`). Yet `ImportsGenerator.java` imports nothing from core (only java.*).
- **F2. A non-manual `write_source_file` makes `bazel build //...` run its generator.** bazel_lib puts `in_file` in the
  target's runfiles (`external/aspect_bazel_lib+/lib/private/write_source_file.bzl:413-416`), and building an
  executable target builds its runfiles. Diff tests build both of their files too.
- **F3. `bazel build //...` runs every non-manual action target, testonly ones included.** Query run:
  `bazel query '(//spec:* + //docs:* + //tools/census:* + //tools/generators:*) except attr(tags, "manual", ...)'`.
  It lists `gen_claims gen_dynafn gen_engine_handlers gen_imports gen_natives gen_prelude judge_host_duckdb
  judge_database_duckdb judge_host_h2 judge_database_h2 native_declarations native_membership_draft ratchets
  update_ratchets* docs:update_generated docs:draft_own_corpus_ledger render_census lanes_diff`.
- **F4. Lanes.** The CI matrix is `GR:50-66`. The `build` lane runs `bazel build //...` (`GR:63,163-164`). `checks` runs `//:generated`
  (`GR:51`), which includes `//core:update_generated_tests`, `//docs:update_generated_tests` and `//spec:update_ratchets_tests`
  (`ROOT:25-43`). Lane `3` runs `//spec:spec_tests`, lane `4` runs `//spec:corpus_duckdb` and lane `5` runs `//spec:corpus_h2` (`GR:52-54`). The local gate
  `//gates:local` holds `//:generated` (`GL:15`) and `//spec:spec_tests` (`GL:52`), but no corpus lane (`GL:4-5`). CI uploads
  `bazel-bin/spec/judge_*/{host.log,database.log,verdict.txt,*-*.txt}` as an artifact (`GR:205-209`).
- **F5. The guard reports do not force builds.** `guard_classpaths` and `guard_markdown` (made by `guards_package`) depend on
  every java_run and test in the package (users in `inventory.json`). Both only `ctx.actions.write` strings computed at
  analysis time (`tools/guards/classpath.bzl:27-39`, `tools/guards/markdown.bzl:9-18`), so they build none of their targets' outputs.
  They are not consumers in any sense that matters here.

## The generator chain (the area-specific question)

The committed copies are written by `//core:update_generated` (`CORE:779-784`, files map `CORE:696-703`). Their diff tests
are `//core:update_generated_{0..5}_test`, where 0 is DynaFn.java, 1 is Pure.java, 2 is NameResolver.java, 3 is engine-handlers.tsv, 4 is native-claims.tsv and 5 is prelude.pure
(index order of `_GENERATED`, confirmed by each generator's `users` in inventory.json). All six are in `//:generated`, so
they run in the CI `checks` lane and in `//gates:local`, and in `build` via F2.

| link | reads (declared inputs) | runs on (classpath) | output → committed at | TRUE trigger | reruns today on a planner edit? why |
|---|---|---|---|---|---|
| `gen_natives` (S:370-389) | upstream engine and pure trees (`UPSTREAM_TREES`, the whole `@legend_*_src//:tree`); committed `native-membership.tsv`; committed `Pure.java`, which it splices (`NativesGenerator.java:47-56`) | `:generators` → `//core` **as committed**. It really uses `Compiler.parseSources` (in `:planner`), `NameResolver.resolve` and `ElementParser` (`NativesGenerator.java:319-328,418`) | `spec/generated/Pure.java` → `core/src/main/java/com/legend/builtin/Pure.java`, `_1_test` | upstream bump; an edit to native-membership.tsv; an edit to Pure.java's hand parts; a parser or resolver behaviour change | **yes**: F1. `Compiler` lives in `:planner` (`CORE:168-173`), so even the narrowest correct deps include the planner's closure |
| `gen_prelude` (S:450-469) | upstream trees; `:gen_natives` (as the Pure.java override); **`//core:main_java`, every core main Java file as text** (`CORE:716-720`). It scans every core .java for FQN tokens ("Java demand", `PreludeGenerator.java:331-356`) and reads Pure.java's hand declarations (`:1601`) | `//core` as committed: parser, lexer and NameResolver, plus **`Claims.claimedBareNames()` over the committed compiled registries** (`PreludeGenerator.java:429,541`) | `prelude.pure` → `core/src/main/resources/com/legend/builtin/prelude.pure`, `_5_test` | upstream bump; gen_natives output; a core Java edit that adds or removes an FQN token naming a spec class; a change to the claim registries' bare names | **yes, twice over**: the text input changes on any core main edit (a comment edit too), and F1 |
| `gen_dynafn` (S:350-366) | the whole engine tree (`Files.walk(engineRoot)`, `DynaFnGenerator.java:65-70`); committed `DynaFn.java` (splice) | `:generators` → `//core`. **DynaFnGenerator imports no core class** | `DynaFn.java` → `core/.../builtin/DynaFn.java`, `_0_test` | upstream bump; a hand edit to DynaFn.java | **yes, wrongly**: F1 only |
| `gen_imports` (S:331-347) | one upstream file, `CompileContext.java`; committed `NameResolver.java` (splice, `ImportsGenerator.java`) | `:generators` → `//core`. **ImportsGenerator imports no core class** | `NameResolver.java` → `core/.../compiler/NameResolver.java`, `_2_test` | that one upstream file changing in a bump; an edit to NameResolver.java | **yes, wrongly**: F1 only (the aquery in F1) |
| `gen_engine_handlers` (S:316-328) | one upstream file, `Handlers.java` | `//core` **as committed**: `Pure.all()`, `Prelude.elements()` (the committed prelude.pure resource), `SignatureMangle`, `Pure.LITE_SURFACE` (`EngineHandlersGenerator.java:79-115`) | `engine-handlers.tsv` → `core/src/main/resources/.../engine-handlers.tsv`, `_3_test` | upstream bump (Handlers.java); a change to Pure.java or prelude.pure as committed | **yes**: F1, though it needs only `:builtin` and `:model` |
| `gen_claims` (S:491-512) | `:gen_dynafn`, `:gen_imports`, `:gen_natives` (as overrides); **`//core:main_java`**, scanned for the `also` column: every file naming a constant (`ClaimRegistryTest.java:40-47`, `ClaimsGenerator.java:95-100`) | `:claims_generator_lib` (S:474-487) → **`//core:core_next`** (`CORE:839-864`): all of core main recompiled with the three generated Java files, plus `core_next_prelude` = `:gen_prelude` (`CORE:866-870`). It reflects over `Pure.class` (`ClaimsGenerator.java:56-80`) | `native-claims.tsv` → `core/src/main/resources/.../native-claims.tsv`, `_4_test` | any of the four upstream links changing; a core Java edit that changes which files name a Pure constant; a registry change. By design an "engine change" generator | **yes**: text input, plus `core_next` (a **second full compile of core**) rebuilds on any core main edit |
| `core_next` (CORE:839-864, area 1's target) | core main Java minus 3 files + the 3 generated files; resources minus prelude, plus gen_prelude | `//base`, `//json` | (build only) | the generators' outputs, or any core main source | **yes**: it globs all core main Java |

**On an arbitrary planner edit (e.g. `core/src/main/java/com/legend/Compiler.java`) these all rerun today:** all six gen_*;
`core_next` (a full recompile of core); the six `core:update_generated_*_test` diff tests; `native_declarations` and
`native_membership_draft` (via `:generators`, F1); `ratchets` and `update_ratchets_test` (via `spec_tests_lib` → `//core`); all
four corpus passes (`judge_host/database_{duckdb,h2}`, via `spec_tests_lib`); and through the docs targets
`//parser-equivalence:gen_roster` and `gen_own_corpus_draft` (which take `//core:srcs` and `pe_tests_lib`; parser-equivalence BUILD:323-345,
393-413). Of these, **gen_dynafn, gen_imports, gen_engine_handlers, native_declarations and native_membership_draft have no
reason to** for a planner edit. gen_natives has a weak one: it uses `Compiler.parseSources`. gen_prelude and gen_claims have a real
but rarely-output-changing one (token scans of core text).

**Single-pass fixpoint is doubtful (OPEN).** `gen_engine_handlers` and the `Claims` part of `gen_prelude` read the
*committed* compiled Pure.java and prelude.pure, not the chain's outputs (`deps = [":generators"]` → `//core`, S:327, 468). So
after an upstream bump, one `bazel run //:update_generated` (the only one `tools/bump/Bump.java:150-151` runs) can write
an engine-handlers.tsv computed from the old Pure.java and prelude, and the next build's `_3_test` would fail. To settle it:
after a bump that moves Pure.java or prelude.pure, run `//:update_generated` twice and see whether the second run changes any file.

## Part A: one row per target (or proven-identical group)

Columns: target | kind | what it is | reads that matter | produces | who uses it | SHOULD run on | runs TODAY | verdict | note

### docs (5)

| target | kind | what it is | reads | produces | users | should run on | today | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //docs:all_files | filegroup | guards_package's file list (`tools/guards/defs.bzl:109-`) | docs/** | – | `//tools/guards:repository_files` | n/a | analysis only | WIRING | used |
| //docs:update_generated | _write_source_file | writes `docs/protocol-roster.tsv` from `//parser-equivalence:gen_roster` (docs/BUILD.bazel:11-19) | gen_roster: upstream trees, engine jars, oracle pins, `//core:srcs`, `//pct:srcs`, `//spec:srcs`, fixtures (parser-equivalence BUILD:323-345) | the committed roster when run | `//:update_generated` (ROOT:60); humans via `bazel run //:update_generated` | upstream bump (pins) and changes to what counts as "covered" | `build` lane runs gen_roster (F2); not in a test lane by itself | GEN-COMMITTED | lives in docs only because bazel_lib wants the writer in the output's package. The only readers are parser-equivalence tests (`_LEDGERS`, parser-equivalence BUILD:142-155) |
| //docs:update_generated_test | _diff_test | roster vs gen_roster | as above | pass/fail | `update_generated_tests` | as above | `checks` lane and `//gates:local` (via `//:generated`), `build` | CHECK-DIFF | gen_roster reads `//core:srcs`, so it reruns on any core edit, test files included |
| //docs:update_generated_tests | test_suite | the macro's suite | – | – | `//:generated` (ROOT:33) | – | checks, local | WIRING | used |
| //docs:draft_own_corpus_ledger | _write_source_file (diff_test=False) | writes a DRAFT of `docs/own-corpus-protocol-diffs.tsv` for a human (docs/BUILD.bazel:21-34) | `gen_own_corpus_draft`: upstream, oracle pins, core/pct/spec srcs, the committed ledger | the draft when run | no target. Humans: GATES.md:63, `docs/own-corpus-protocol-diffs.tsv:4`, OwnCorpusLedgerDraft.java:15 | human request only | **`build` lane runs gen_own_corpus_draft on every core/pct/spec change** (F2, not manual) | GEN-COMMITTED | true trigger: a human asks for it. It should be `manual` |

### spec: generators, their libraries, ratchets (16)

| target | kind | what it is | reads | produces | users | should run on | today | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //spec:source_tree | java_library | `SourceTree` (text tree with overrides), no deps (S:48-53) | – | jar | generators, claims, claims_generator_lib, spec_tests_lib | its source | build | TOOL | correct shape |
| //spec:generators | java_library | all of `src/gen/java/com/legend/generators/*` except SourceTree (S:55-68) | – | jar | the 6 gen_* except claims, native_declarations, native_membership_draft, spec_tests_lib | its sources | build | TOOL | **one library on `//core` + `:claims` for 7 programs, 2 of which use no core class.** This is the root of F1's over-triggering |
| //spec:claims | java_library | Claims + ClaimsGenerator against committed `//core` (S:74-86) | – | jar | `:generators` (PreludeGenerator uses `Claims.claimedBareNames`), spec_tests_lib (ClaimRegistryTest) | its sources | build | TOOL | the same 2 files are compiled twice (also in claims_generator_lib), deliberately (S:70-73) |
| //spec:claims_generator_lib | java_library | Claims + ClaimsGenerator against `//core:core_next` (S:474-487) | core_next | jar | gen_claims | its sources, or gen outputs | build | TOOL | pulls in core_next, a second full core compile |
| //spec:gen_natives | _java_run | NativesGenerator (see chain table) | see chain | `generated/Pure.java` | `core:update_generated_1(_test)`, gen_prelude, gen_claims, core_next | upstream bump; membership/Pure.java edit; parser change | build, checks, local; on every core edit (F1) | GEN-COMMITTED | true trigger is mostly an upstream bump. Today an engine edit reruns it |
| //spec:gen_prelude | _java_run | PreludeGenerator | see chain | `generated/com/legend/builtin/prelude.pure` | `_5(_test)`, core_next_prelude | upstream bump, natives, core FQN tokens | build, checks, local; any core main edit | GEN-COMMITTED | true trigger includes core text by design. Needs early cutoff (Part C) |
| //spec:gen_dynafn | _java_run | DynaFnGenerator | engine tree, DynaFn.java | `generated/DynaFn.java` | `_0(_test)`, gen_claims, core_next | upstream bump; DynaFn.java edit | build, checks, local; every core edit (F1) | GEN-COMMITTED | **today's trigger is wrong**: it uses no core class |
| //spec:gen_imports | _java_run | ImportsGenerator | CompileContext.java, NameResolver.java | `generated/NameResolver.java` | `_2(_test)`, gen_claims, core_next | upstream bump; NameResolver.java edit | same | GEN-COMMITTED | **today's trigger is wrong**: it uses no core class |
| //spec:gen_engine_handlers | _java_run | EngineHandlersGenerator | Handlers.java + committed Pure/prelude | `generated/engine-handlers.tsv` | `_3(_test)` | upstream bump; Pure.java/prelude.pure | same | GEN-COMMITTED | outside the chain (reads committed, not generated). Fixpoint OPEN |
| //spec:gen_claims | _java_run | ClaimsGenerator on core_next | see chain | `generated/native-claims.tsv` | `_4(_test)` | gen outputs + core text/registries | same + core_next compile | GEN-COMMITTED | CORE:84-85: native-claims.tsv "is not product data (retiring at step 5)", read only by ClaimRegistryTest |
| //spec:native_declarations | _java_run | NativeDeclarations: every upstream declaration of a membership FQN (S:415-433, `NativeDeclarations.java`) | upstream trees, membership.tsv | `generated/native-declarations.tsv` (build output, never committed) | no target. Humans: GATES.md:65 `bazel build //spec:native_declarations` | human request | **build lane, on every core edit** (F1, F3) | TOOL | a by-hand report. It should be `manual` |
| //spec:native_membership_draft | _java_run | NativeMembershipDraft: draft membership from compiled Pure constants (S:435-445) | committed compiled Pure | `generated/native-membership.draft.tsv` | `//core:draft_native_membership` (CORE:827-833, diff_test=False) | human request | **build lane, every core edit** (F1, F2, F3) | GEN-COMMITTED | a draft. True trigger: a human. It and core:draft_native_membership should be `manual` |
| //spec:ratchets | _java_run | SpecRatchets: corpus census, `dynafn.unsupported`, implementation-table kinds, upstream path count (S:391-405, `SpecRatchets.java:23-45`) | upstream trees; classpath = all of `spec_tests_lib` | `generated/ratchets.tsv` | `update_ratchets(_test)` | upstream bump; DynaFn.java; catalog (Pure/upstream) change | build, checks, **local**; on any core edit and any spec test/resource edit | GEN-COMMITTED | its own committed output is a resource of its classpath (S:94 excludes only rcorpus rosters), so re-blessing reruns it. Harmless but circular |
| //spec:update_ratchets | _write_source_file | writer for `src/test/resources/com/legend/generators/ratchets.tsv` (S:407-413) | :ratchets | – | `//:update_generated` (ROOT:67) | as ratchets | build | GEN-COMMITTED | writer handle |
| //spec:update_ratchets_test | _diff_test | committed vs :ratchets | – | – | suite | as ratchets | checks, local, build | CHECK-DIFF | |
| //spec:update_ratchets_tests | test_suite | macro suite | – | – | `//:generated` (ROOT:39) | – | checks, local | WIRING | used |

### spec: the corpus lanes (corpus_lane macro, C:28-156) (38)

The **passes** (`judge_host_<lane>`, `judge_database_<lane>`) are java_runs of `com.legend.tools.junit.JUnitAction` over
`--select-class=com.legend.rcorpus.MinimalCorpusTest` (C:72-115). JUnitAction runs JUnit inside the action, writes the exit
code to `verdict.txt` and the output to the log, and **succeeds whether the tests pass or fail** (`tools/junit/JUnitAction.java:6-17,27-36`).
So they **are tests in disguise**: the full relational corpus (MinimalCorpusTest, `@Tag("heavy")`), executed as a build action.
The database pass reads the host pass's verdict, log, ledger and measured rosters (C:94-109). Consumers:
`corpus_<lane>_verdict` (CorpusVerdictTest, the red or green, C:122-136); the roster diff tests (C:139-151); the database pass
(of the host pass); the CI artifact upload (GR:205-209).

| target(s) | kind | what it is | reads | produces | users | should run on | today | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //spec:judge_host_duckdb, //spec:judge_host_h2 | _java_run (testonly) | the host-judge corpus pass (C:72-90); h2 adds `-Drcorpus.backend=h2` (S:200) | `:corpus_srcs` + **`//core:srcs`** (C:75, S:122-125); upstream trees; classpath `spec_tests_lib` + drivers | `judge-host.tsv`, `verdict.txt`, `host.log`, 4 measured rosters | verdict test, database pass, `update_rcorpus_<lane>_{0..3}(_test)`, CI artifact | engine change (core main), upstream bump, corpus code in spec (rcorpus/harness) | **build lane** (non-manual, F3), lanes 4/5; not local | TEST-CORPUS | **heavy (4096 MB for h2, S:197) and run by `bazel build //...`.** The passes appear never to read `//core:srcs` or `:corpus_srcs` as files: rcorpus reads only `Corpus.ENGINE_ROOT/RELATIONAL/M2M_TESTS` (`Corpus.java:48-58`, `MinimalCorpus.java:227-248,418-434,601`) and classpath resources (`MinimalCorpusTest.java:53-103`). If so, a core *test* edit reruns both passes for nothing. OPEN: drop those srcs and run the pass sandboxed; an undeclared read fails |
| //spec:judge_database_duckdb, //spec:judge_database_h2 | _java_run (testonly) | the database-judge pass, joined per assert to the host pass (C:91-115) | as host, + `:judge_host_<lane>` outputs | `judge-database.tsv`, `verdict.txt`, `database.log`, 1 measured register | verdict test, `update_rcorpus_<lane>_4(_test)`, CI artifact | as host | build lane, lanes 4/5 | TEST-CORPUS | same notes |
| //spec:judge_host_warehouse, //spec:judge_database_warehouse | _java_run (testonly, manual) | both passes with every connection through the native warehouse (S:164-190) | as above + `//warehouse:server_native` (GraalVM native image), `//warehouse:duckdb_library`, `//warehouse:client`, DuckDB's committed rosters | verdicts, logs, ledgers | `corpus_warehouse_verdict` | engine change, warehouse change | manual; **no CI lane** (absent from GR:50-66); humans: docs/WAREHOUSE_W1_DESIGN_2026_09_26.md:208, BI_AND_ETL_PLAN:155 | TEST-CORPUS | golden=False, so it has no writers |
| //spec:corpus_duckdb_verdict, //spec:corpus_h2_verdict | java_test (small) | CorpusVerdictTest over both verdicts and logs (C:122-136) | the 4 verdict/log files | pass/fail | `corpus_<lane>` suite | as the passes | lanes 4/5; `build` builds them (and so the passes) | TEST-CORPUS | the lane's red or green |
| //spec:corpus_warehouse_verdict | java_test (manual) | same, warehouse | | | `corpus_warehouse` | | manual | TEST-CORPUS | |
| //spec:corpus_duckdb, //spec:corpus_h2 | test_suite | verdict + roster diff tests (C:152-156) | | | `judge_lanes`; CI lanes 4/5 name them (GR:53-54) | | lanes 4/5 | WIRING | used |
| //spec:corpus_warehouse | test_suite (manual) | the warehouse verdict only | | | humans (docs above) | | manual | WIRING | used by humans |
| //spec:judge_lanes | test_suite | corpus_duckdb + corpus_h2 (S:286-292) | | | users=0. Humans: README.md:444, GATES.md:59 | | via `//...` only | WIRING | used by humans |
| //spec:corpus_srcs | filegroup | spec src minus the 10 measured rosters (S:16-28) | | | the 6 passes | | – | WIRING | probably an unnecessary input (see the passes) |
| //spec:update_rcorpus_duckdb, //spec:update_rcorpus_h2 | _write_source_file (umbrella) | `bazel run` writes the lane's 5 measured files (C:139-150) | | | users=0. Humans: the diff-test message (C:142), ROOT:48-50 | a deliberate re-bless by a human | build (F2 → runs passes) | GEN-COMMITTED | deliberately outside `//:update_generated` (ROOT:48-50) |
| //spec:update_rcorpus_duckdb_{0,1,2,3}, //spec:update_rcorpus_h2_{0,1,2,3} (8) | _write_source_file | per-file writers. In order: fail-roster, skipped-roster, unordered-register, engine-order-register, from `judge_host_<lane>` (C:17,62,144-145) | | | the umbrella | human re-bless | build | GEN-COMMITTED | identical shape: same macro, each dep = that lane's host pass (inventory.json) |
| //spec:update_rcorpus_duckdb_4, //spec:update_rcorpus_h2_4 (2) | _write_source_file | writer of database-engine-order-register from `judge_database_<lane>` (C:19,69) | | | the umbrella | human | build | GEN-COMMITTED | |
| //spec:update_rcorpus_duckdb_{0..4}_test, //spec:update_rcorpus_h2_{0..4}_test (10) | _diff_test | committed roster vs measured | | | `update_rcorpus_<lane>_tests` | engine change, as the passes | lanes 4/5, build | CHECK-DIFF | identical shape |
| //spec:update_rcorpus_duckdb_tests, //spec:update_rcorpus_h2_tests | test_suite | macro suites | | | `corpus_<lane>` (C:151) | | lanes 4/5 | WIRING | used |

Count: 6 passes + 3 verdicts + 3 lane suites + judge_lanes + corpus_srcs + 2 umbrellas + 10 writers + 10 diff tests +
2 suites = 38. Section totals: 5 + 16 + 38 + 16 + 4 = 79.

### spec: manual measurements, probes, reference lane, tests, wiring (16)

| target | kind | what it is | reads | produces | users | should run on | today | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //spec:eager_corpus_compile, //spec:eager_corpus_compile_world2 | _java_run (manual, testonly) | EagerCorpusCompileProbe: types every body of the corpus world. "a MEASUREMENT, not a gate" (`EagerCorpusCompileProbe.java:5-13`); world2 adds `-Deager.world2=1` (S:514-533) | `:srcs` + `//core:srcs` + upstream; spec_tests_lib | `eager-corpus.txt`, `eager-residue.txt`, `run.log` | no target; no CI; no doc beyond its own javadoc (`git grep eager_corpus_compile` hits only the probe and S) | human request | manual only | TOOL | **not a test in disguise**: it has no verdict. A report a human asks for. Also likely over-declares `core:srcs`/`:srcs` |
| //spec:corpus_one | java_binary (manual) | one corpus pass on demand (S:205-227, CorpusOne.java:13) | spec/core srcs, upstream, a roster | – | humans: docs/EXECUTION_PLAN_2026_09_26.md:89 | human | manual | TOOL | |
| //spec:our_resolutions | java_binary (manual) | our side of the reference differential (S:549-563) | | | humans: tools/reference/README.md:20 | human | manual | TOOL | |
| //spec:manifest_world_census | java_test (manual, large, 4096 MB) | ManifestWorldCensusTest (`@Tag("heavy")`): a module's closure typed whole, with ceilings (S:535-547) | spec/core srcs, upstream | pass/fail | humans: GATES.md:62 | front-end (parser/compiler) change, upstream bump | manual; no lane | TEST-CORPUS | |
| //spec:reference_lane_report | _java_run (manual) | ReferenceLaneReport: our side vs the cached legend-pure dump, joined (S:229-258) | upstream, oracle pins, `//tools/reference:ref_dump` (pinned upstream jars, about 8 GB) | `reference-lane/core_relational.txt`, `-examples.tsv` | `update_reference_lane(_test)`, `reference_lane` | front-end change; upstream bump | manual | GEN-COMMITTED | committed golden via update_reference_lane. True trigger: a front-end change, run by hand |
| //spec:update_reference_lane | _write_source_file (manual) | writer of the golden (S:260-268) | | | humans: GATES.md:6763, ReferenceLaneReport.java:21 | human | manual | GEN-COMMITTED | |
| //spec:update_reference_lane_test | _diff_test (manual) | golden vs report | | | humans: ReferenceLaneTest.java:27 | front-end change | manual | CHECK-DIFF | |
| //spec:update_reference_lane_tests | test_suite (manual) | macro by-product | | | users=0; `git grep update_reference_lane_tests` = 0 hits outside S | – | never | DEAD | a write_source_files by-product. It goes only if the macro's shape changes |
| //spec:reference_lane | java_test (manual) | ReferenceLaneTest: every disagreement class has a reason (S:270-279) | the report | pass/fail | humans (ReferenceLaneTest.java:27) | report or reasons.tsv change | manual | TEST-CORPUS | |
| //spec:spec_tests | java_test (large, 1024 MB) | all `com.legend` tests in spec_tests_lib minus `heavy` (S:134-148): upstream-parity checks (CoreImportsParity, FeatureFlagParity, PlatformNamesSpelling, UpstreamPathManifest, Subsumed, DynaFnRegistry, CatalogUpstreamDiff, ImplementationTable, SpecBodyCensus, NativeSignatureGenerator, PreludeGenerator, ClaimRegistry), guards (SpecBoundaryTest, LibraryPlatformNamespaceGuardTest), one unit test (H2VerifyNormTest) | classpath spec_tests_lib → //core; data `:core_main_sources` + `//core:main_srcs` (all core main files); upstream trees (`tools/junit/defs.bzl:58-70`) | pass/fail | `//gates:local` (GL:52); lane 3 (GR:52) | upstream bump; edits to the core files they read (builtin/Pure, DynaFn, NameResolver, PlatformTypes, Feature, normalizer/RelOpTranslator, ...); spec test edits | lane 3, local; reruns on **any** core main edit (data = every core main file) and any spec test edit | TEST-INTEGRATION | one target over four kinds of test. `heavy` excluded correctly |
| //spec:spec_tests_lib | java_library (testonly) | every spec test source + resources minus rosters (S:88-113) | //core, //testing, drivers, archunit, junit | jar | spec_tests, every pass, ratchets, reference lane, eager probes, corpus_one, our_resolutions, `//tools/deps:spec_closure` | its sources | build | TOOL | **one library for parity tests, the corpus, the reference lane and ratchets**: editing any spec test reruns all four corpus passes and ratchets |
| //spec:core_main_sources | file_list (testonly) | the list of core main files for `-Dlegend.sources` (S:128-132, CoreTree.java:12) | `//core:main_srcs` | a list file | spec_tests | – | with spec_tests | WIRING | used |
| //spec:srcs | filegroup | all spec src (S:39-43) | | | core:census_sources, parser-equivalence (ratchets, gen_roster, gen_own_corpus_draft), eager x2, corpus_one, our_resolutions, manifest_world_census | – | – | WIRING | used. It is the reason any spec edit reruns gen_roster and docs:update_generated_test |
| //spec:test_java | filegroup | spec test .java as files, for parser-equivalence's InlineSnippets (S:30-36) | | | 11 parser-equivalence targets | – | – | WIRING | used |
| //spec:all_files | filegroup | guards_package file list | | | `//tools/guards:repository_files` | – | – | WIRING | used |

### tools/census and tools/generators (4)

| target | kind | what it is | reads | produces | users | should run on | today | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //tools/census:render_census | java_binary | RenderCensus over this checkout's core, run at two commits (tools/census/BUILD.bazel:1-16) | //core | launcher + jar | humans: tools/census/README.md:29, RenderCensus.java:33 | human request | **compiled in `build` lane** (non-manual) | TOOL | a cheap compile, but it should be `manual` |
| //tools/census:lanes_diff | py_binary | diffs two runs' lane logs (BUILD:18-23) | – | – | humans: README.md:12, lanes_diff.py:1 | human | build lane (needs the Python toolchain) | TOOL | should be `manual` |
| //tools/census:all_files | filegroup | guards file list | | | `//tools/guards:repository_files` | – | – | WIRING | |
| //tools/generators:all_files | filegroup | guards file list. The package exists only to hold `defs.bzl` (UPSTREAM_TREES, UPSTREAM_ROOTS, program_jvm_flags), loaded by spec, spec/corpus.bzl, pct and parser-equivalence BUILDs | | | `//tools/guards:repository_files` | – | – | WIRING | fine as a `.bzl` home |

### Verdict counts (79)

| verdict | n | targets |
|---|---|---|
| GEN-COMMITTED | 25 | 6 gen_*, ratchets, update_ratchets, native_membership_draft, reference_lane_report, update_reference_lane, docs:update_generated, docs:draft_own_corpus_ledger, update_rcorpus_{duckdb,h2} + their 10 per-file writers |
| WIRING | 16 | 4 all_files, docs:update_generated_tests, update_ratchets_tests, update_rcorpus_{duckdb,h2}_tests, corpus_{duckdb,h2,warehouse}, judge_lanes, corpus_srcs, srcs, test_java, core_main_sources |
| CHECK-DIFF | 13 | docs:update_generated_test, update_ratchets_test, 10 roster diff tests, update_reference_lane_test |
| TOOL | 12 | source_tree, generators, claims, claims_generator_lib, spec_tests_lib, native_declarations, eager x2, corpus_one, our_resolutions, render_census, lanes_diff |
| TEST-CORPUS | 11 | 6 judge passes, 3 verdict tests, manifest_world_census, reference_lane |
| TEST-INTEGRATION | 1 | spec_tests |
| DEAD | 1 | update_reference_lane_tests (a macro by-product) |

## Part B: problems, with evidence

1. **`bazel build //...` runs the full relational corpus, twice over (DuckDB and H2, both judges).** `judge_host_*` and
   `judge_database_*` for duckdb and h2 are non-manual (F3), and the verdict tests and roster diff tests (non-manual) pull them in
   as data or diff inputs (F2). So the `build` lane, whose stated job is "every target builds" (GR:63), runs four heavy JUnit
   passes (up to 4096 MB each, S:193-197). Lanes 4 and 5 run them again. The CI cache key is per lane (GR:119), so whether the
   build lane's results are reused there depends on restore-key luck (GR:120-122).
2. **Five programs rerun on every core edit with no reason to.** `gen_dynafn` and `gen_imports` use no core class, yet carry
   all 30 core jars (F1, aquery). `gen_engine_handlers` needs only `:builtin` and `:model`. `native_declarations` and
   `native_membership_draft` are by-hand reports. The cause is one `:generators` library on the `//core` umbrella (S:55-68)
   plus java_run's full-jar inputs (F1).
3. **gen_prelude and gen_claims rerun on any core text edit, comments included.** Each takes all core main Java as declared
   text input (`//core:main_java`, S:454, 497), because their output depends on FQN tokens and constant references found in
   that text (`PreludeGenerator.java:331-356`, `ClaimsGenerator.java:95-100`). The outputs rarely change, but Bazel cannot cut
   off early because no intermediate digest exists.
4. **`core_next` is a second full compile of core on every core edit** (CORE:839-864, glob of all core main Java). It exists
   only to feed `gen_claims`, whose output, native-claims.tsv, "is not product data (retiring at step 5)" and is read only by
   ClaimRegistryTest (CORE:84-85).
5. **Committed drafts and reports run in every build.** `docs:draft_own_corpus_ledger`, `core:draft_native_membership` (→
   `native_membership_draft`) and `native_declarations` are not manual, so `bazel build //...` runs gen_own_corpus_draft
   (upstream + pe_tests_lib) and the two spec programs on every core/pct/spec change (F2, F3). Their own docs say a human
   runs them (GATES.md:62-65).
6. **The chain is not one-pass (OPEN).** `gen_engine_handlers` and gen_prelude's `Claims` step read the committed core, not
   the chain's outputs (see the chain table). The bump runs `//:update_generated` once (`tools/bump/Bump.java:150-151`).
7. **`spec_tests_lib` is one library for four purposes** (parity tests, corpus passes, reference lane, ratchets; S:88-113
   globs all `src/test/java/**`). Editing a parity test, a reference-lane class or H2VerifyNormTest recompiles it and reruns
   all four corpus passes and `ratchets`.
8. **The corpus passes probably over-declare inputs (OPEN).** They take `//core:srcs` (core's whole tree, tests included)
   and `:corpus_srcs` (C:75, S:122-125), but the rcorpus code reads only upstream paths and classpath resources (citations in
   the pass row). If confirmed, any core *test* edit reruns both heavy passes for nothing. The same applies to
   `eager_corpus_compile*` (`_INPUTS`, S:520).
9. **spec_tests reruns on any core main file.** Its data is every core main file (`:core_main_sources` + `//core:main_srcs`,
   S:139-143), while its tests read a handful of named core files (CoreTree callers: Pure.java, native-membership.tsv,
   native-claims.tsv, RelOpTranslator.java, ...). That is also two declarations of the same file set.
10. **The docs ledgers live in docs/ only because of where the files are.** `docs:update_generated` and
    `draft_own_corpus_ledger` sit in docs because bazel_lib's writer must sit in the output file's package. The generators
    (gen_roster, gen_own_corpus_draft) and the only readers (`_LEDGERS`, parser-equivalence BUILD:142-155) are in
    parser-equivalence. gen_roster reads `//core:srcs`, `//pct:srcs` and `//spec:srcs` (parser-equivalence BUILD:329-331), so any edit there
    reruns a generator whose stated trigger is "the pinned upstream release" (docs/BUILD.bazel:14).
11. **Small items.** `update_reference_lane_tests` is DEAD (a macro by-product). `render_census` and `lanes_diff` build in
    `//...` but only humans run them. `ratchets` has its own committed output on its classpath (S:94).

## Part C: the right shape for this area

Each proposal names its evidence. Items marked [area N] need another area to decide.

1. **Split `:generators` by what each program reads** (evidence: the imports listed in the chain table; F1).
   - `:text_generators`: DynaFnGenerator, ImportsGenerator, UpstreamFiles and SourceTree, with **no core dep**. `gen_dynafn` and
     `gen_imports` then rerun only on an upstream bump or an edit to DynaFn.java or NameResolver.java.
   - `:handlers_generator`: EngineHandlersGenerator on `//core:builtin` + `//core:model` [area 1: these are private today, CORE:59-60; they need
     `//spec` visibility].
   - `:natives_generator`: NativesGenerator and NativeDeclarations on the narrowest core that has `Compiler.parseSources`,
     `NameResolver` and `ElementParser`. [area 1] `Compiler` is in `:planner` (CORE:168-173), so either `parseSources`
     moves down to `:parser`/`:compiler`, or this generator depends on `:planner`'s closure (still not exec, driver or server).
   - `:prelude_generator` on parser, lexer, compiler and claims, and `:claims` on builtin, platform and lowering (Claims' imports:
     `NativeFn`, `Pure`, `CoreFn`, `RegistryKeys`).
2. **Give gen_prelude and gen_claims early cutoff** (evidence: Part B 3). Add one cheap action, `core_java_facts`, that reduces
   `//core:main_java` to (a) the sorted FQN tokens on non-comment lines (exactly PreludeGenerator's rule, `:337-354`) and
   (b) per file, the `Pure.X` constants and FQN strings it names (ClaimsGenerator's `also` rule). gen_prelude and gen_claims
   read that output instead of the tree. A core edit then reruns only the extractor, and the generators rerun when the facts
   change. This needs a small refactor of `SourceTree` consumers.
3. **Make native-claims.tsv a build output, or retire it** (evidence: CORE:84-85). Feed `gen_claims`'s output to
   ClaimRegistryTest as data, remove `_4` from `//core:update_generated`, and `core_next` then exists only for that build.
   Second option [area 1, needs a proof]: replace core_next's full recompile with a jar of just the three regenerated classes
   compiled against core's interface jars, put first on gen_claims' classpath. Safe only if no other class inlines constants from
   them. OPEN until checked.
4. **Close the chain** (evidence: Part B 6). Run gen_engine_handlers, and the Claims step of gen_prelude, on the chain's
   outputs, as gen_claims already does, so one `bazel run //:update_generated` is a fixpoint. Or settle the OPEN item and
   document that two runs are needed.
5. **Keep corpus passes out of `bazel build //...`** (evidence: Part B 1). Tag everything `corpus_lane` makes (passes,
   verdict tests, roster writers and diff tests) with one tag, e.g. `corpus`. The build lane becomes `bazel build
   --build_tag_filters=-corpus //...`; lanes 4 and 5 keep testing `//spec:corpus_<lane>` by name. `manual` on the java_runs
   alone is not enough, because the non-manual verdict test pulls them in (F2). [area deciding the build lane's command, GR:163-164]
6. **Mark the by-hand targets `manual`**: `native_declarations`, `native_membership_draft`, `//core:draft_native_membership`
   [area 1], `//docs:draft_own_corpus_ledger`, `//tools/census:render_census`, `//tools/census:lanes_diff`. `bazel build
   //spec:native_declarations` (GATES.md:65) still works when the target is named. Evidence: Part B 5 and 11.
7. **Split `spec_tests_lib` by trigger** (evidence: Part B 7): `:corpus_lib` (rcorpus, harness, and the generators classes
   they use) for the passes, `:parity_tests_lib` for spec_tests, `:reference_lib` (ReferenceLaneReport, ReferenceJoin, OurResolutions*,
   ManifestWorldCensus) for the reference lane and probes, and `:ratchets_lib` (SpecRatchets and the three measurements it
   calls). Then split `spec_tests` into one junit_test per test package, as core did (CORE:383-386, P3-05), each with data = the
   core files that package reads, not all of `//core:main_srcs`.
8. **Trim the corpus pass inputs** once the OPEN item is settled: drop `//core:srcs` and `:corpus_srcs` from `_CORPUS_INPUTS`
   (S:122-125) and `_INPUTS` for the eager probes. The proof is a sandboxed run of each pass without them.
9. **Put the parser-equivalence ledgers next to their generators and readers** (evidence: Part B 10). Move
   `protocol-roster.tsv` and `own-corpus-protocol-diffs.tsv` (and the other `_LEDGERS` TSVs) into `parser-equivalence/`, so the
   writers sit there and `//docs` goes back to prose. [area owning parser-equivalence decides gen_roster's
   `//core:srcs`/`//pct:srcs`/`//spec:srcs` inputs, which make its "upstream release" trigger false]
10. **The target trigger map** after 1-9. Upstream bump: gen_dynafn, gen_imports, gen_engine_handlers, gen_natives,
    native_declarations (by hand) and ratchets. Hand edits to the spliced files: gen_dynafn, gen_imports and gen_natives. Core text facts:
    gen_prelude and gen_claims (through the extractor). Engine (core main) change: the corpus lanes (lanes 4 and 5 only), spec_tests
    packages by the files they read, and ratchets only via DynaFn or the catalog. Human: the drafts, the probes, the reference lane and
    the census tools. `bazel build //...` then compiles and runs only the committed-file generators whose true inputs changed.
