# Area 1 report: core engine Java (non-stress), base, json, testing

Scope: the 133 targets in `runs/inventory/area1_targets.tsv`. Repo: `runs/bazel-exec` (branch bazel/exec, HEAD `6f1d9aa9a`).
I only read files and ran queries. No build, test or run.

Abbreviations used in "runs TODAY":
- **B**: `bazel build //...`, the CI `build` lane on every platform (`.github/workflows/gates-run.yml:62`, `:163`). All 133 targets
  are non-manual (tsv `manual`=0), so B builds every one of them. B also builds the runfiles of every executable or test target,
  for example the generator outputs a `write_source_file` script carries (cquery evidence in Part B, B2).
- **L**: `bazel test //gates:local` (`gates/BUILD.bazel:11-79`).
- **lane 1 / checks / 7p / misc / app**: the CI lanes in `gates-run.yml:51-63`.
- "inv" means `runs/inventory/inventory.json`: its `users` and `deps` (direct edges).

Two Bazel facts drive most of the verdicts, so here they are with evidence:
- **`bazel build //core:core` compiles none of core.** `bazel cquery --output=files //core:core` gives only `core/libcore.jar`.
  `bazel aquery 'outputs(".*core/libcore.jar", //core:core)'` shows one Javac action, and none of its inputs are under core/, base/ or
  json/ (I parsed the jsonproto inputs and the filtered list was empty). The umbrella's layer jars appear only in its runfiles, and a
  java_library's runfiles are not built at top level. **`//core:server` does compile all of it**: the runfiles of its cquery are
  `base/libbase.jar json/libjson.jar` + all 30 `core/lib<layer>.jar` + `core/libduckdb_load.jar`.
- **core's compile depends on no generator.** `bazel query --noimplicit_deps 'kind(rule, deps(//core:core))'` returns only the 30
  layers, `//core:core`, `//base:base`, `//json:json`, `//tools/nullaway:nullaway` and @maven_tools jars (NullAway's plugin).
  `deps(//core:core) intersect (//spec/... + //scripts/... + //tools/java_run/...)` gives "Empty results". The layers glob the committed
  files (`core/BUILD.bazel:62-205`).

---

## Part A: one row per target (or per proven-identical group)

### A1. Product compiles

| target | kind | what it is | what it reads that matters | produces | who uses it | change that SHOULD run it | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //base:base | java_library | Null annotations (`NonNull`, `Nullable`), the bottom of the repo, no deps ever (`base/BUILD.bazel:3-13`) | `base/src/main/java/com/legend/base/*.java` (2 files) | libbase.jar | inv: every core layer, core, plan_side, product_jars, core_next, json, warehouse (sqlapi, client, server_lib, tests_lib, launcher_test), sdlc-server (rules, server_lib), depot-server:rules, spec:claims_generator_lib | edit under base/src | B; built by almost every lane, since everything depends on it | COMPILE | |
| //json:json | java_library | Strict JSON codec, depends on java.base + base only (`json/BUILD.bazel:1-17`) | `json/src/main/java/com/legend/json/*.java` (1 file) | libjson.jar | inv: core:protocol, core:server_lib, core, plan_side, product_jars, core_next, warehouse ×4, sdlc-server ×4, depot-server:rules, datacube:catalog_facts_main, wasm:boundary, json:tests | edit under json/src | B; every lane that reaches core or warehouse | COMPILE | |
| 30 core layers (sub-table A1a) | java_library ×30, macro legend_java_library | One library per package group of core's main tree, each with its direct deps. "Bazel now refuses a back-edge at compile time" (`core/BUILD.bazel:1-5, 49-57`). Introduced by step 0c, `d8e4a56fa` (2026-09-26). | Each one globs one package (`core/BUILD.bazel:62-205`); together they cover 749 of core's 751 main .java files. `builtin` also carries all of `src/main/resources/**` except native-claims.tsv (`:86-89`): the committed prelude.pure, engine-handlers.tsv and native-membership.tsv | lib<layer>.jar plus a header jar | Each one's direct users are only inside //core: `layer_<name>`, `product_jars`, `core`, `plan_side` (24 of 30) and higher layers. No target outside //core names a layer (inv, in sub-table A1a, `EXT=[]` for all 30). Every layer reaches every consumer of `//core:core` (19 direct users, inv) through `:core`'s exports. | edit to that package's sources, or an ABI change in a lower layer | B; every lane that tests anything depending on core (1, checks, 3, 4, 5, 6, 7, 7p, 8, 9, 10, app, misc) | COMPILE | Three of them ship code that only tests call (Part B, B7) |
| //core:duckdb_load | java_library | DuckDB Appender behind core's BulkLoad seam, found by ServiceLoader, kept out of core's compile (`core/BUILD.bazel:256-267`) | `core/src/main/duckdb/**` (2 .java + META-INF), :core, duckdb_jdbc | libduckdb_load.jar | inv: drivers (runtime), core_tests_lib (runtime), guardrails, product_jars | edit under src/main/duckdb, or an ABI change in core | B; every lane that runs drivers | COMPILE | The 2 files that bring core to 751 |
| //core:server | java_binary (legend_java_binary) | THE PRODUCT: the HTTP server (LSP, query, SQL, diagrams); `server_deploy.jar` is the jar to ship (`core/BUILD.bazel:269-280`, `README.md:303-304`) | runtime :core + :drivers | `server` launcher, server.jar, deploy jar on request | inv: query:verify, query-store:lite_test, query-store:legend_server, datacube:torture_test, datacube:legend_server_for_torture, fixtures/saved-queries:gen and :gen_js_info_files, core:guard_classpaths. Humans: `bazel run //core:server` (`README.md:303`) | any core main edit | B; app lane and L (through query-store:lite_test), misc (query:tests) | COMPILE | Its runfiles hold all 32 product jars, so building it is the minimal "compile everything core ships" (see Q1) |

#### A1a. The 30 layers: same macro, same pattern, member by member

All share the shape: `legend_java_library(name, srcs = glob(<one package>), deps = <lower layers>)`, private visibility, NullAway on.
Their direct deps equal the line for them in `tools/deps/core-layers.txt:17-46`. "Also used by" lists in-core users other than
`layer_<n>`, `product_jars`, `core` and `plan_side`. "Importers outside core main" is from a grep for `com\.legend\.<pkg>\.[A-Z]`
over core/src, spec, pct, parser-equivalence, tools, datacube, engine-client, wasm, sdlc-server and warehouse.

| layer | srcs | in plan_side | also used by (inv) | note |
|---|---|---|---|---|
| spi | 3 | yes | parser | |
| cache | 4 | yes | server_lib, driver, planner | wasm/src/main imports it, through plan_side |
| values | 3 | yes | test, driver, testdatagen, planner, resolver, exec, lowering, compiler, parser, protocol | |
| error | 8 | yes | 16 layers | |
| diagnostics | 1 | yes | driver, exec | |
| lexer | 5 | yes | ide, parser | |
| sql | 25 | yes | 11 layers | |
| protocol | 53 | yes | 14 layers | |
| model | 62 | yes | 20 layers | |
| parser | 44 | yes | server_lib, ide, driver, planner, builtin | |
| builtin | 9 + resources | yes | 13 layers | carries the committed generated resources |
| platform | 7 | yes | 10 layers | |
| compiler_element_type | 5 | yes | 12 layers | |
| sql_dialect | 37 | yes | server_lib, driver, testdatagen, planner, exec, database | |
| database | 2 | yes | server_lib, driver, planner, exec, plan | |
| compiler | 214 | yes | 12 layers | still one 208-class cycle (`core/BUILD.bazel:53-56`) |
| normalizer | 29 | yes | driver, planner | |
| lineage | 5 | yes | driver, testdatagen, planner, plan | |
| validation | 1 | yes | driver, planner | |
| lowering | 75 | yes | server_lib, probe, driver, planner, resolver, exec | |
| plan | 14 | yes | server_lib, driver, testdatagen, planner, resolver, exec | |
| resolver | 57 | yes | driver, testdatagen, planner | |
| exec | 34 | no | test, server_lib, driver, testdatagen | |
| probe | 1 | no | none | `com.legend.probe.Shadow`. Only test code names it, and its ServiceLoader binding is test-only (`:shadow_binding`, "Deleted at step 4", `core/BUILD.bazel:343-353`) |
| testdatagen | 2 | no | driver | |
| planner | 4 | yes | test, server_lib, driver | |
| driver | 23 | no | test, server_lib | |
| ide | 4 | no | none | `package-info.java`: "Dormant ... Currently unused by the batch pipeline". Only core tests use it |
| test | 6 | no | none | PureTestRunner etc. Its importers are core/src/test (43 files), pct/src/test (3) and spec/src/test (3). No main code imports it |
| server_lib | 12 | no | none (its main class runs through //core:server) | |

Source total is 749. With duckdb_load's 2, that is core's 751.

### A2. Wiring

| target | kind | what it is | reads | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:core | java_library (exports only) | THE UMBRELLA: exports the 30 layers + base + json and compiles nothing of its own (`core/BUILD.bazel:207-236`) | none of its own (aquery: empty core inputs) | empty libcore.jar | inv: 19 direct users: tools/census:render_census, tools/deps:core_closure, core tests (section_grammar_registry_test, toy_grammar, postgres_arm_test, core_tests_lib), pct:pct_tests_lib, engine-client:type_facts_main, datacube:catalog_facts_main and :offer_facts_main, sdlc-server:rules, core:server, spec:spec_tests_lib, :generators and :claims, core:duckdb_load, tools/engine-runner:runner, parser-equivalence:pe_tests_lib, tools/legend:compiles_test_lib | n/a | B | WIRING | Still used. Building it compiles nothing (see the head of this report) |
| //core:plan_side | java_library (exports only) | Plan-only umbrella: the planner and its 24 layers + base + json, no exec, driver or server (`core/BUILD.bazel:214-227`, C2a `26d988ff0`) | none | empty jar | inv: wasm:boundary only | n/a | B; app lane (wasm) | WIRING | Still used: the planner's boundary for TeaVM/wasm |
| //core:drivers | java_library (runtime_deps only) | The JDBC drivers (h2, duckdb, sqlite, postgres) + duckdb_load as a runtime choice (`core/BUILD.bazel:238-254`) | maven_core jars | empty jar | inv: 52 users: server, every core behaviour/stress test, ladder_report, all 15 pct suites, pct:ratchets, spec:spec_tests_lib, datacube:app_postgres, tools/deps:drivers_closure | n/a | B; most lanes | WIRING | Still used |
| //core:product_jars | java_jars (testonly) | List file of the 32 product jars + duckdb_load, own jars only (`core/BUILD.bazel:569-577`, P3-27 `ab1ec0db9`) | _CORE_TARGETS' runtime_output_jars | product_jars.jars + jars in its runfiles | inv: guardrails only (ArchitectureTest, NoEagerTypeReferencesTest) | any core main edit (through guardrails) | B; checks, L (through guardrails) | WIRING | Still used |
| //core:core_test_jar | java_jars (testonly) | List file of core_tests_lib's own jar, for ArchitectureTest.upstreamJavaNeverEntersCore (`:579-586`) | core_tests_lib | .jars list | inv: guardrails | any core test edit | B; checks, L | WIRING | Still used |
| //core:guardrails_sources | file_list (testonly) | Declared list of every file under core/src except .md, which the source guards read (`:588-593`) | `glob(src/**)`, 1330 files | .files list + files in runfiles | inv: guardrails | any core/src edit | B; checks, L | WIRING | |
| //core:census_sources | file_list (testonly) | core/src + //parser-equivalence:srcs + //pct:srcs + //spec:srcs + //warehouse:test_java (`:290-295, 595-599`) | those 5 trees | .files list | inv: census | edit in any of the 5 trees | B; checks, L | WIRING | |
| //core:core_tests | test_suite | The 24 per-package behaviour tests (`:453-457`) | n/a | n/a | inv: gates:local. CI lane 1 names it (`gates-run.yml:51`) | n/a | lane 1, L | WIRING | Also holds core_tests_ladder, which is not in this area |
| //core:layer_queries | filegroup (core_layer_queries) | All 30 `layer_*` genquery outputs (`core/layers.bzl:26-30`) | the 30 genqueries | 30 files | inv: tools/deps:core_layering_test | a BUILD edit to any layer's deps | B; checks (`//tools/deps:all`), L | WIRING | |
| //core:srcs | filegroup | `glob(src/**)` minus .md, for spec's parity checks (`:21-28`) | all 1330 files of core/src, tests and .pure included | files | inv, 14 users: parser-equivalence (ratchets, gen_own_corpus_draft, gen_roster); spec (our_resolutions, manifest_world_census, eager_corpus_compile(+_world2), judge_{host,database}_{duckdb,h2,warehouse}, corpus_one) | n/a | B, plus lanes 3, 4, 5, 8 through its users | WIRING | Too wide (Part B, B8) |
| //core:main_srcs | filegroup | `glob(src/main/**)` for spec's CoreTree (`:30-35`) | 757 files | files | inv: spec:spec_tests, spec:core_main_sources | n/a | B; lane 3 | WIRING | |
| //core:main_java | filegroup | `glob(src/main/java/**)` as TEXT for the prelude and claims generators (`:714-720`) | 749 files | files | inv: spec:gen_prelude, spec:gen_claims | n/a | B; checks (through //:generated) | WIRING | |
| //core:test_java | filegroup | `glob(src/test/**/*.java)` for parser-equivalence's InlineSnippets (`:37-43`) | 325 files | files | inv: 11 parser-equivalence targets (parser_parity, corpus_census, fixture_sweep, …) | n/a | B; lane 8 | WIRING | Any core test edit reruns PE (by design) |
| //core:test_pure | filegroup | Every .pure under the test resources, for the keyword census (`:736-741`) | 211 files | files | inv: scripts/parser:our_pure, :keywords | n/a | B; checks | WIRING | |
| //base:all_files, //core:all_files, //json:all_files, //testing:all_files | filegroup ×4 (guards_package) | `glob(**)` of the package, for the repository inventory guard (`tools/guards/defs.bzl:109-116`) | all of the package's files | files | inv: tools/guards:repository_files, which feeds inventory_test | any file added or removed in the package | B; checks, L (inventory_test) | WIRING | Identical: the same macro line in each package's `guards_package()` |
| //core:update_generated | _write_source_file (write_source_files) | Aggregate writer of the 6 generated core files (`:779-784`) | update_generated_0..5 | update script + their runfiles (all 6 generator outputs) | inv: //:update_generated (humans: `bazel run //:update_generated`, `README.md:294`) | an upstream bump etc. (see A4) | B builds it (so all 6 generators run); humans run it | WIRING | Building it under B is wasted work |
| //core:update_generated_tests | test_suite (write_source_files) | The 6 diff tests | n/a | n/a | inv: //:generated, which gates:local and checks name | n/a | checks, L | WIRING | |

### A3. Tools (built only to run inside another action, or by hand)

| target | kind | what it is | reads | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:core_next | java_library | "CORE AS THE GENERATORS REWRITE IT": all 749 main .java except DynaFn/Pure/NameResolver, plus //spec:gen_dynafn, :gen_imports and :gen_natives outputs; all resources except prelude (`core/BUILD.bazel:835-864`) | core main tree, 3 generator outputs, which read upstream (reach: upstream_src) | libcore_next.jar: a second, single-target compile of core, with NullAway (inv dep //tools/nullaway) | inv: **only** //spec:claims_generator_lib, used only by //spec:gen_claims, used only by //core:update_generated_4 and _4_test (and spec:guard_classpaths, an analysis-time report) | an upstream bump or native-membership edit (whatever changes Pure.java, DynaFn.java or NameResolver.java), and only when native-claims.tsv must be regenerated | B; checks and L (through //:generated, update_generated_4_test); **any edit to core main** reruns it (it globs the tree) | TOOL | The everyday build does not need it (Q2) |
| //core:core_next_prelude | java_library (resources only) | core_next's regenerated prelude.pure as a resource (`:866-870`) | //spec:gen_prelude | resource jar | inv: core_next (runtime) | as core_next | B; checks, L | TOOL | |
| //core:draft_native_membership | _write_source_file, diff_test=False | Writes a DRAFT of the hand-owned native-membership.tsv from Pure.java's constants, for a person to finish (`:826-833`, P2-17 `65b9e2a69`) | //spec:native_membership_draft, which runs on :generators, which runs on all of //core | update script + draft tsv in runfiles (cquery: runfiles = `spec/generated/native-membership.draft.tsv`) | inv users: none. Humans: `docs/GATES.md:63` names it as a by-hand draft | a person deciding to re-draft membership | **B runs its generator on every core edit** (draft tsv is in its runfiles) | TOOL | Should be `manual` |

### A4. Generated committed files: writers and diff tests

The six files and their generators are `core/BUILD.bazel:696-703`. Members `_0` to `_5`, in that order: DynaFn.java→gen_dynafn,
Pure.java→gen_natives, NameResolver.java→gen_imports, engine-handlers.tsv→gen_engine_handlers, native-claims.tsv→gen_claims,
prelude.pure→gen_prelude. All 12 come from one `write_source_files` call. In inv, each writer `_N` has one dep (`//spec:gen_X`) and one
user (`:update_generated`). Each test `_N_test` has one dep (the same `//spec:gen_X`) and the users `:update_generated_tests` and
`:guard_markdown`.

| target | kind | what it is | reads | produces | who uses it | SHOULD run on (TRUE trigger) | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:update_generated_0 … _5 (6 members) | _write_source_file | `bazel run` copies the generator output over the committed file | the //spec:gen_* output (cquery runfiles: e.g. `_1` → `spec/generated/Pure.java`) | update script | :update_generated, then humans | **_0 DynaFn**: upstream bump or DynaFn.java hand edit. DynaFnGenerator imports no com.legend class. **_2 NameResolver**: upstream bump (CompileContext.java) or NameResolver hand edit; ImportsGenerator imports no com.legend class. **_3 engine-handlers**: upstream bump (Handlers.java) or prelude/Pure/model change (it imports `builtin.Prelude`, `builtin.Pure`, `model.*`). **_1 Pure.java**: upstream bump, native-membership.tsv edit, or a change to the parser/compiler it runs (imports `Compiler`, `NameResolver`, `parser.*`, `protocol.*`). **_5 prelude**: upstream bump, Pure.java change, or core main Java text (it reads `//core:main_java`). **_4 native-claims**: any change to the compiled registries (Pure/DynaFn/NameResolver/CoreFn/RegistryKeys) | **Today**: every generator runs on `//spec:generators` (or `:claims_generator_lib`), which depends on `//core` (`spec/BUILD.bazel:56-67, 474-487`). java_run puts the deps' full `transitive_runtime_jars` in the action's inputs (`tools/java_run/defs.bzl:46, 110`). So **every core main edit reruns all six generators**, and B reruns them through these scripts' runfiles. The true trigger is not what reruns them | GEN-COMMITTED | `_0` and `_2` have a pure upstream trigger but rerun on every engine edit |
| //core:update_generated_0_test … _5_test (6 members) | _diff_test | Compares the committed file with the generator's output, naming `bazel run //:update_generated` (`:781`) | gen output + committed file | test result | :update_generated_tests → //:generated → checks lane, L | the same as its writer's trigger, plus a hand edit of the committed file | B builds them, so their generator runs. checks and L test them. The test itself is content-cached, so it re-executes only when the bytes change, but its generator re-executes on every core edit | CHECK-DIFF | |

### A5. Guards and checks

| target | kind | what it is | reads | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:layer_<n> ×30 (spi, cache, values, error, diagnostics, lexer, sql, protocol, model, parser, builtin, platform, compiler_element_type, sql_dialect, database, compiler, normalizer, lineage, validation, lowering, plan, resolver, exec, probe, testdatagen, planner, driver, ide, test, server_lib) | genquery (core_layer_queries) | `labels(deps, //core:<n>)` for every core java_library not in `not_layers` (`core/layers.bzl:14-25`, `core/BUILD.bazel:872-885`, P2-12 `73425574c`) | the BUILD graph only (scope `:<n>`) | a text file of direct deps | inv: layer_queries only, then core_layering_test | a deps edit in core/BUILD.bazel | B; checks (`//tools/deps:all`), L | CHECK-GUARD | Identical: one macro loop. Their only use is the layering check. The layers themselves have product users (A1a) |
| //core:guardrails | junit_test | The source checks tagged `guardrail` (19 classes: ArchitectureTest, CodeShapeGuardrailTest, SqlTextRatchetTest, NoEagerTypeReferencesTest, …) on core's own tree and jars (`:528-567`) | guardrails_sources (all of core/src), LiteralUnroll.java, DuckDb.java, product_jars, core_test_jar, core_tests_lib, duckdb_load | test result | inv: gates:local; CI checks (`gates-run.yml:52`) | any core/src edit, main or test | B; checks, L | CHECK-GUARD | Its kind and trigger agree |
| //core:census | junit_test | The 8 `census`-tagged classes (JdbcSurfaceCensusTest, SkipCensusTest, HarnessDisciplineTest, ParserBoundaryArchTest, …), which also walk other modules (`:533-534, 601-614`) | census_sources (core, parser-equivalence, pct, spec srcs, warehouse test_java), core_tests_lib | test result | inv: gates:local; checks | an edit in any of the 5 trees | B; checks, L | CHECK-GUARD | Lives in //core but judges 5 modules |

### A6. Tests and test libraries

| target | kind | what it is | reads | produces | who uses it | SHOULD run on | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| //core:core_tests_lib | java_library (testonly, nullaway off) | All 322 core test sources compiled once, minus PostgresArmTest and the toy grammar (`:297-341`) | src/test/java/**, src/test/resources/**, :stress_index, 12 `//projects:<p>_files` (LINKED_PROJECTS), :core, //testing, archunit, junit, h2 | test jar | inv: 36 users: the 24 core_tests_*, guardrails, census, core_test_jar, duckdb_load_test, corpus_differential_test, planner_on_java_base_test, section_grammar_registry_test, stress_suites, stress_suites_h2, scale, stress_tool, ladder_report | a core test edit, or an ABI change in core | B; lanes 1, 10, checks, L | TEST-UNIT (library) | Carries the stress corpus and linked projects for the stress lanes (Part B, B6) |
| //core:core_tests_<pkg> ×22: architecture, builtin, cache, compiler, exec, ide, lexer, lineage, lowering, model, normalizer, parser, platform, protocol, resolver, root, server, sql, test, testdatagen, testing, values | junit_test (junit_test), medium, 512 MB | One test target per test package: `--select-package=com.legend.<pkg>`, excluding tags census/differential/guardrail/stress. `root` selects `com.legend` minus every listed package (`:383-451`, P3-05 `fbc1f2756`) | runtime: core_tests_lib + drivers (inv: deps = core_tests_lib, drivers, tools/junit:junit for every member) | test result | core_tests suite → L, lane 1; guard_markdown/guard_classpaths (analysis-only reports) | **Intended**: an edit in that package, or in the layers its tests reach. **Actually**: every member has all of core_tests_lib and all of core on its runtime classpath, so any core main or test edit reruns all 24 (Part B, B5) | lane 1, L; B builds them | TEST-UNIT | Identical in shape: one list comprehension. `root` uses the same deps with a different select |
| //core:core_tests_integration | junit_test (same comprehension) | `com.legend.integration` (75 test files: real DuckDB/H2 runs over models, the 1K stress model), excluding CorpusDifferentialTest (`:429-432`) | as above | test result | as above | edits reaching exec/integration | lane 1, L; B | TEST-INTEGRATION | The same macro as the 22. Split out only because of its kind |
| //core:duckdb_load_test | junit_test, small | RowLoadTest: the Appender path lands the same rows (`:491-502`) | core_tests_lib, drivers | result | inv: gates:local; lane 1 | duckdb_load edit, exec bulk-load edit | lane 1, L; B | TEST-INTEGRATION | |
| //core:postgres_arm_test | junit_test, medium | A `type: Postgres` connection through ConnectionResolver against embedded Postgres 16 (`:504-526`) | its own src PostgresArmTest.java, :core, drivers, //testing, @embedded_postgres | result | inv: gates:local; lane 7p (`gates-run.yml:46`) | server/exec connection code, the postgres driver | 7p, L; B (only on compatible platforms) | TEST-INTEGRATION | |
| //core:planner_on_java_base_test | java_test (plain) | PlanOnJavaBase plans DuckDB and Postgres queries under `--limit-modules=java.base` (`:478-489`) | core_tests_lib (runtime) | result | inv: gates:local; lane 1 | edits in planner-side layers | lane 1, L; B | TEST-UNIT | Its classpath carries all of core, including exec, so any core edit reruns it |
| //core:section_grammar_registry_test | junit_test, small | SectionGrammarRegistryTest with the toy grammar only on its classpath (`:368-381`) | its own src, core, core_tests_lib, toy_grammar | result | inv: gates:local; lane 1 | parser/spi edit | lane 1, L; B | TEST-UNIT | |
| //core:toy_grammar | java_library (testonly) | ToySectionGrammar + its ServiceLoader file (`:355-366`) | 1 src, :core | jar | inv: section_grammar_registry_test | as its test | B; lane 1, L | TEST-UNIT (library) | |
| //core:shadow_binding | java_library (testonly, resources only) | The ServiceLoader file that makes `com.legend.probe.Shadow` the DecisionProbe, in test lanes only (`:343-353`) | 1 resource | jar | inv: core_tests_lib, pct:pct_tests_lib, spec:spec_tests_lib | never (it is a fixed file) | B; lanes 1, 3, 6, 7, 9, L | TEST-UNIT (library) | "Deleted at step 4" |
| //testing:testing | java_library (testonly, nullaway off) | Runfile/Repo/Upstream test helpers (`testing/BUILD.bazel:4-14`) | testing/src/main/java (8 files), rules_java runfiles | jar | inv: 21 test targets/libs across core, spec, pct, parser-equivalence, warehouse, datacube, tools/* | testing/ edit | B; nearly every test lane | TEST-UNIT (library) | |
| //json:tests | junit_test, small | json's unit tests (`json/BUILD.bazel:19-30`) | json/src/test (1 file), :json | result | inv: gates:local; misc lane (`gates-run.yml:61`) | json/ edit | misc, L; B | TEST-UNIT | Light: depends on json only |

Verdict count (133): COMPILE 34 (base, json, 30 layers, duckdb_load, server) · WIRING 20 · TOOL 3 · GEN-COMMITTED 6 · CHECK-DIFF 6 ·
CHECK-GUARD 32 (30 layer queries, guardrails, census) · TEST-UNIT 29 (22 core_tests_<pkg>, planner_on_java_base_test,
section_grammar_registry_test, json:tests, plus 4 test libraries: core_tests_lib, toy_grammar, shadow_binding, testing) ·
TEST-INTEGRATION 3 · DEAD 0 · OPEN 0.

---

## The area questions, answered

### Q1. Why ~40 libraries, and what is the minimal set that compiles everything core ships?
- **Count.** `bazel query 'kind(java_library, //core:all)' | wc -l` gives **40**. That is 30 layers, plus 10 non-layers: `core`,
  `plan_side`, `drivers`, `duckdb_load`, `core_next`, `core_next_prelude`, `core_tests_lib`, `shadow_binding`, `toy_grammar` and
  `scale_lib`. The last group is exactly `not_layers` (`core/BUILD.bazel:874-885`).
- **Why.**
  - Execution plan rule 0b.12, "Every stage is its own Bazel target, with only the dependencies it should have" (ruled 2026-09-29,
    `docs/EXECUTION_PLAN_2026_09_26.md:139-142`). The target map is in §1b (`:249`).
  - The split itself is step 0c, `d8e4a56fa` (2026-09-26): "a back-edge from any target is now a compile error in the file being
    written".
  - The policy file and test came with `5a2c8132e` (2026-09-29): `tools/deps/core-layers.txt`, `CoreLayeringTest`. P2-12
    (`73425574c`) then generated the per-layer genqueries from the libraries themselves (`core/layers.bzl:1-8`).
  - Later carve-outs: C3a `database` (`aea95fec3`), C2a `planner` (`26d988ff0`), `diagnostics` (`62e080acd`).
- **Is each layer used by something other than the layering check?** Yes, all 30.
  - Each one is exported by `:core`, and 24 of them by `:plan_side` too (`//wasm:boundary`). Each one is listed in `:product_jars`
    (guardrails), and each ships in `//core:server`'s runfiles.
  - 26 of the 30 are also direct deps of higher layers. The four with no in-core user are `probe`, `ide`, `test` and `server_lib`.
    `server_lib` is the server's main class. The other three are only imported by tests (A1a, Part B B7).
  - The genqueries `layer_*` are the only thing whose sole user is the layering check.
- **Minimal compile set.** `bazel build //core:server` builds all 32 product jars plus duckdb_load (cquery runfiles, head of this
  report), and nothing else of core.
  - `bazel build //core:core` does not do this: it compiles only an empty umbrella jar (aquery).
  - Target for target, the set is: //base:base, //json:json, the 30 layers, //core:duckdb_load and //core:server.
  - Outside this area, the shipped planner also needs `//wasm:boundary` over `:plan_side`. That is Area-wasm's call.

### Q2. core_next / core_next_prelude: who consumes them, and does the everyday build need them?
- **The only consumer chain:** `//core:core_next` → `//spec:claims_generator_lib` (`spec/BUILD.bazel:474-487`) → `//spec:gen_claims`
  (`:491-512`) → `//core:update_generated_4` and `//core:update_generated_4_test`. That chain regenerates and diff-tests
  `native-claims.tsv`, and nothing else (inv users).
- **Who reads native-claims.tsv:** it is excluded from the product's resources. "READ only by the spec module's claim tests: it is not
  product data (retiring at step 5)" (`core/BUILD.bazel:84-89`). Its readers are spec's ClaimRegistryTest, plus a comment in core's
  NativeFunctionTest.
- **Why a second core exists:** ClaimsGenerator reads the COMPILED registries, so it is compiled against core-with-regenerated-files.
  That lets one `bazel run //:update_generated` reach a fixpoint ("At a fixpoint ... it is core, byte for byte", `:835-838`).
  `docs/GATES.md:6717-6718`: "a generator check, not the product".
- **The everyday build does not need it.** Yet because it globs all main Java (`:841-848`), every core edit recompiles 749 files a
  second time, with NullAway (inv dep), under B, checks and L.

### Q3. Committed files inside core/src that generators write
- **Through `//core:update_generated` (`core/BUILD.bazel:696-703, 779-784`):** `builtin/DynaFn.java` (gen_dynafn),
  `builtin/Pure.java` (gen_natives), `compiler/NameResolver.java` (gen_imports), `resources/.../engine-handlers.tsv`
  (gen_engine_handlers), `native-claims.tsv` (gen_claims) and `prelude.pure` (gen_prelude). Diff-tested by
  `update_generated_{0..5}_test` in `//:generated`.
- **Outside area 1** (named for completeness):
  - The 10 `STRESS_GENERATED` stress .pure files plus `stress-layout.json`, through `//core:update_stress_corpus` (`:766-777`,
    `core/stress.bzl:12-`).
  - 12 `src/test/resources/ladder/*.current.sql`, through `//core:update_ladder` (`:786-824`).
  - A by-hand DRAFT over the hand-owned `native-membership.tsv`, through `//core:draft_native_membership` (`:826-833`), with no diff
    test.
- **Does core's compile depend on any generator?** No. The layers glob committed files, and
  `deps(//core:core) ∩ (//spec/... + //scripts/... + //tools/java_run/...)` is empty. Only `core_next` and `core_next_prelude` take
  generator outputs (`:848-852, 869`).
- **The reverse does hold:** every generator depends on all of core (Part B, B1).

### Q4. The core test layout

| target | runs | should run on |
|---|---|---|
| core_tests_<pkg>: 23 in this area including root, plus core_tests_ladder in Area-stress | behaviour tests of one package (tags census/differential/guardrail/stress excluded) | that package's code, and the layers its tests touch (intended, P3-06). Today: any core edit |
| core_tests (suite) | the 24 | lane 1, L |
| core_tests_lib | compiles all 322 test sources + the corpus resources, once | n/a (library) |
| guardrails | 19 `guardrail` source/jar checks on core's own tree | any core/src edit |
| census | 8 `census` checks over core + pe + pct + spec + warehouse tests | an edit in any of those trees |
| product_jars / core_test_jar | jar lists for ArchitectureTest / NoEagerTypeReferencesTest | n/a (data) |
| duckdb_load_test, postgres_arm_test, planner_on_java_base_test, section_grammar_registry_test | one class each, isolated for a classpath or data reason | the code they name |

### Q5. Every //core java_binary and non-test library
- **java_binaries** (`bazel query 'kind("java_binary", //core:all)'`):
  - `//core:server`: the product. See A1 for its users.
  - `//core:scale`: manual, `bazel run //core:scale -- 10k|...`, `core/BUILD.bazel:616-646`.
  - `//core:stress_tool`: manual, `:676-690`.
  - scale and stress_tool are Area-stress targets. Both are `tags=["manual"]` and testonly, so B does not build them.
- **Non-test libraries:** the 30 layers, core, plan_side, drivers, duckdb_load, core_next and core_next_prelude, all with users
  (A1-A3). //base and //json are used by core, warehouse, sdlc-server, depot-server, datacube, wasm and spec (inv).

---

## Part B: problems in this area, with evidence

- **B1. Every engine edit reruns all six core generators, and two of them have a pure upstream trigger.**
  - `//spec:generators` depends on `//core` (`spec/BUILD.bazel:56-67`).
  - java_run's action inputs include the deps' full `transitive_runtime_jars` (`tools/java_run/defs.bzl:46, 110`).
  - DynaFnGenerator and ImportsGenerator import nothing from com.legend. They use no other class of their package either (grep of
    `spec/src/gen/java/com/legend/generators/*.java`). Their true trigger is an upstream bump or a hand edit of their own file.
  - gen_dynafn also takes all of `@legend_engine_src//:tree` and `@legend_pure_src//:tree` (`tools/generators/defs.bzl:10-15`), so
    each rerun walks both upstream trees.
- **B2. `bazel build //...` regenerates every committed file.**
  - The writers `update_generated_N` are executables whose runfiles hold the generator output. cquery: `//core:update_generated_1`
    runfiles = `spec/generated/Pure.java`, and the aggregate holds all six.
  - The `_N_test` diff tests are tests, so B builds them as well.
  - Result: the build lane runs the full generator chain, the core_next compile and gen_claims on every engine edit.
- **B3. A second compile of nearly all of core** (`//core:core_next`, 749 files plus 3 generated) on every core edit. Its only output
  feeds the regeneration of a non-product ledger slated to retire (Q2).
- **B4. A by-hand tool is built by `//...`.** `//core:draft_native_membership` has 0 users and no diff test. Its runfiles hold
  `native-membership.draft.tsv` (cquery), so B runs `//spec:native_membership_draft`, which depends on all of core, on every core
  edit.
- **B5. The per-package test split does not buy what its comment claims.**
  - The comment: "so a change re-runs only the packages whose tests it reaches" (`core/BUILD.bazel:383-386`).
  - Every `core_tests_<pkg>` has the same runtime deps, `core_tests_lib` + `drivers` (inv). core_tests_lib holds all 322 test files
    and depends on `:core`. So any core main or test edit reruns all 24 targets.
  - The workplan admits this: P3-06 "After P3-05 every target still depends on one `core_tests_lib`, which depends on every core
    library" (`bazel-plan/docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md:1656-1667`).
  - What the split does give today: parallelism, 512 MB heaps (`core/BUILD.bazel:413-416`) and per-package reporting.
- **B6. The behaviour tests carry the stress corpus and 12 model projects.** core_tests_lib's resources include `:stress_index` and
  `//projects:<p>_files` for every LINKED_PROJECT (`core/BUILD.bazel:313-324`). Editing a stress .pure file or a linked project
  therefore rebuilds the test jar and reruns all 24 behaviour tests, guardrails, census and the 4 single-class tests, not only the
  stress lanes.
- **B7. Test-only and dormant code ships in the product jar.**
  - `//core:ide`: "Dormant ... Currently unused by the batch pipeline" (`core/src/main/java/com/legend/ide/package-info.java`). It has
    no non-test importer (grep).
  - `//core:probe` (`Shadow`) is bound only by the testonly `:shadow_binding`.
  - `//core:test` (PureTestRunner etc.) is imported only by core, pct and spec tests (43/3/3 files). No main code imports it.
  - All three are in `_CORE_TARGETS` (`:207-212`), so they are in `//core:server`'s runfiles (cquery).
- **B8. `//core:srcs` is too wide a data input.**
  - It is `glob(src/**)`, including core's 325 test .java files and test resources (`:24-28`).
  - It is data for 14 spec and parser-equivalence actions and tests, including the heavy corpus judges (inv).
  - So editing a core TEST reruns the DuckDB/H2/warehouse corpus judges. Its comment says spec needs it only to compare "the committed
    generated files".
  - Spec's `_CORPUS_INPUTS` also uses it (`spec/BUILD.bazel:116-125`). The workplan's P3-07 targets exactly this.
- **B9. `bazel build //core:core` is a trap.** It looks like "compile core" but compiles nothing (aquery). Anyone who wants a quick
  compile check must know to build `//core:server`, or let B build everything.
- **B10. Layer genqueries run in the everyday build.** The 30 `layer_*` genqueries and `layer_queries` are non-manual, so B builds them,
  although their only consumer is `core_layering_test` (checks, L). They are cheap, but they are checks, not compiles.
- **B11. census sits in //core but judges five modules.** It reads parser-equivalence, pct, spec and warehouse sources
  (`:290-295`). An edit in, say, warehouse tests reruns a //core target. This is a placement issue, not a correctness one.

---

## Part C: the right shape for this area

1. **One named compile set.** Add a non-testonly `filegroup(name = "compile")` in //core (or the root) whose srcs are
   `_CORE_TARGETS + [":duckdb_load", ":server"]`. A filegroup over java_library targets builds their full jars. Building it equals the
   minimal set from Q1: 32 jars plus the launcher, with no generator, genquery or test. The everyday "compile everything" becomes this
   set plus the other areas' equivalents, not `//...`. Evidence: Q1 cquery/aquery; `//core:core` compiles nothing (B9).
   - Keep the 30 layers. They are rule 0b.12's mechanism, and their header jars give compile avoidance.
   - **Depends on:** the root-level decision on what replaces the `build` lane.
2. **Take tools and checks out of `//...`.** Tag `manual`: `core_next`, `core_next_prelude`, `draft_native_membership`, the 30
   `layer_*` + `layer_queries`, and the `update_generated` writers.
   - `manual` removes them only from wildcard expansion. A test that depends on them still builds them.
   - So `core_layering_test` and the diff tests keep working, while B and the compile set no longer run generators. Evidence: B2, B4,
     B10.
   - `write_source_files` cannot tag only its writers, so this may mean a small wrapper macro. **Bazel session to review.**
3. **Give each generator its true dependencies (Area-spec owns the change; this area supplies the evidence).** Split
   `//spec:generators` into:
   - `dynafn_gen` and `imports_gen`, with no core dep. They then rerun only on an upstream bump or an edit of their own committed file
     (B1).
   - `engine_handlers_gen`, on `//core:builtin` + `//core:model`.
   - `natives_gen` and `prelude_gen`, on `//core:planner` + parser, or `plan_side`. They legitimately rerun on plan-side edits,
     because they execute core's parser and compiler.

   Their diff tests stay in checks and L, which is the honest trigger.
4. **Retire `core_next`.** Two options:
   - (a) Run gen_claims against committed `//core`, through the existing `//spec:claims` library (`spec/BUILD.bazel:69-85`, already
     compiled against //core), and accept a two-pass `bazel run //:update_generated` after a Pure/DynaFn/NameResolver regeneration.
     `update_generated_4_test` catches a missed second pass.
   - (b) Finish "retiring at step 5" (`core/BUILD.bazel:84-85`) and drop native-claims.tsv.

   Either removes a 749-file duplicate compile from every engine edit (B3). **OPEN for the owner:** whether one-pass regeneration is
   worth a second core. Settle it by asking the spec/generators owner.
5. **Tests where their kind says.**
   - **P3-06:** per-package test libraries with minimal core deps, plus a shared helpers library, so `core_tests_<pkg>` reruns only on
     its own reach (B5). If P3-06 is deferred, fix the comment at `core/BUILD.bazel:384`, as the workplan's own rollback says.
   - Move the stress corpus and linked projects out of `core_tests_lib` into a `stress_tests_lib` used only by stress_suites*,
     stress_tool, scale and the differential (B6). **Depends on:** Area-stress.
   - Keep guardrails and census in checks/L: their kind and trigger agree. Consider moving census to //tools/guards, since it is
     cross-module (B11).
6. **Ship only product code.** Move `:test` (com.legend.test) and `:probe` to testonly libraries outside `_CORE_TARGETS`. Decide
   `:ide`: delete it, or keep it out of the server until the IDE layer exists (B7). These are code-ownership calls. **OPEN**: settled
   by the core owner confirming that no runtime path loads those classes reflectively (a grep found none).
7. **Narrow core's data filegroups (with Area-spec, P3-07).** Spec's parity checks need the six generated files plus `main_srcs`, not
   `//core:srcs`. The corpus judges need what the corpus reads. That way a core test edit stops rerunning corpus lanes (B8).
8. **Optional, Bazel-native layering.** The allowed-edge half of `core-layers.txt` can be expressed as each layer's `visibility`, so a
   new edge fails at analysis with no test. The exactness half (a removed edge must be recorded, `core-layers.txt:4-6`) still needs
   CoreLayeringTest, so keep it unless the owner drops that requirement. Evidence: `CoreLayeringTest.java` javadoc. **Bazel session
   to review.**
