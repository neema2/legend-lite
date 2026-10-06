# Area 8 report: model projects, guards and repo-wide checks, gates, Java/JUnit/JS/Python rule tooling

Scope: the 330 targets in `runs/inventory/area8_targets.tsv`. Repo: `runs/bazel-exec` (branch bazel/exec, HEAD `6f1d9aa9a`).
I only read files and ran `bazel query`, `cquery` and `aquery`. No build, test or run.
Part D (the end of this file) is THE CROSS-CUTTING MAP of every CI lane and `//gates:local`, for the whole repo.

Abbreviations used in "runs TODAY":
- **B**: `bazel build //...`, the CI `build` lane on linux, macos and windows (`.github/workflows/gates-run.yml:63`, `:163`).
  All 330 targets but 3 are non-manual (tsv `manual`), so B builds them. B also builds the runfiles of every executable
  and test target, so it runs the generators that diff tests and `write_source_file` scripts carry (area 1, B2, cquery evidence;
  confirmed for `//:update_generated` below, P8-1).
- **L**: `bazel test //gates:local` (`gates/BUILD.bazel:11-79`), which AGENTS.md:366 tells every session to run before a push.
- **checks / 1 / 3 / app / misc / ...**: the CI lanes (`gates-run.yml:50-65`).
- "inv" means `runs/inventory/inventory.json` (direct `deps`/`users`). "reach" means the tsv `reaches` column.

## Bazel facts this report rests on (each verified)
- **F1. The guard reports and the genqueries have no action inputs.** `bazel aquery '//projects:guard_markdown +
  //projects:guard_classpaths + //tools/deps:core_closure'`: each is one `FileWrite` action with `inputDepSetIds=[]`. Their content
  is computed at analysis time (`tools/guards/markdown.bzl:9-18`, `classpath.bzl:27-39`). So building a report builds none of
  the tests it lists, and a source edit that leaves the graph alone leaves every report byte-identical, so the tests reading
  them stay cached. The cost is analysis. The tsv `reaches` column overstates these targets: it follows graph edges, not
  action inputs.
- **F2. Every legend_java_library's javac, from `bazel aquery 'mnemonic("Javac", ...)'`** (the default rules_java toolchain;
  no custom `java_toolchain` exists; `grep java_toolchain` over *.bzl/BUILD.bazel is empty):
  - Toolchain defaults, on EVERY first-party Javac action: `-source 21 -target 21 -XDskipDuplicateBridges=true
    -XDcompilePolicy=simple --should-stop=ifError=FLOW -g -parameters -Xep:IgnoredPureGetter:OFF -Xep:EmptyTopLevelDeclaration:OFF
    -Xep:LenientFormatStringValidation:OFF -Xep:ReturnMissingNullable:OFF -Xep:UseCorrectAssertInTests:OFF -Xmaxwarns -1`.
    JavaBuilder runs Error Prone in-process; the `-Xep:` defaults show it is on.
  - NullAway targets (44 libraries with sources, all 31 core layers among them; list in A8b) add
    `tools/nullaway/defs.bzl:7-20` (`-XDcompilePolicy=simple --should-stop=ifError=FLOW -Xmaxerrs 10000 -XepDisableAllChecks
    -Xep:NullAway:ERROR -XepOpt:NullAway:AnnotatedPackages=com.legend ...CustomNullableAnnotations=com.legend.base.Nullable
    ...CustomNonnullAnnotations=com.legend.base.NonNull ...JSpecifyMode=true ...CheckOptionalEmptiness=true
    -XDaddTypeAnnotationsToSymbol=true`), then `LEGEND_JAVACOPTS` (`-Xep:StringCaseLocaleUsage:ERROR -Xep:DefaultLocale:ERROR`,
    `tools/java/defs.bzl:17-20`). The `--processorpath` holds NullAway 0.13.8 and its closure (guava, jsr305,
    error_prone_annotations, checker-qual, dataflow-nullaway, jspecify...). Net: **Error Prone runs exactly three checks
    on core**: NullAway and the two locale checks. `-XepDisableAllChecks` switches off every default check.
  - Non-NullAway compiles (26 libraries, 5 binaries, 19 junit_tests with sources; lists in A8b) get toolchain defaults +
    LEGEND_JAVACOPTS only, with no `-XepDisableAllChecks`. So **they run Error Prone's full default check set** plus the two
    locale checks at ERROR. Test and tool code is held to more Error Prone checks than product code.
- **F3. java_run's action inputs are the program's full transitive runtime jars** (`tools/java_run/defs.bzl:46`, `:110`),
  not header jars. Any change to any jar in the closure reruns the action, even a change to a private method body.

## Verdict counts (330 targets)
| verdict | count | members |
|---|---|---|
| WIRING | 91 | 56 `_legend_sources`, 11 used `<p>_files`, `//projects:{tests,contract_files}`, 15 `all_files`, `//:generated`, `//:module_files`, `//gates:local`, `//tools/guards:{repository_files,markdown_reports,classpath_reports}`, `//tools/python:requirements` |
| CHECK-GUARD | 116 | 106 guard reports (53 packages × 2), `//projects:contract_test`, 4 `//tools/deps` tests, 4 `//tools/guards` tests, `//tools/python:lock_matches_requirements` |
| TEST-CORPUS | 57 | 56 `<p>_test`, `//projects:graph_test` |
| DEAD | 49 | 45 unused `<p>_files`, `//projects:srcs`, `//tools/guards:{guard_markdown,guard_classpaths}`, `//tools/python:requirements_test` |
| GEN-BUILD | 6 | 4 `//tools/deps` genqueries, `//tools/java_run:pins`, `//tools:oracle_pins` |
| TOOL | 5 | `//tools/nullaway:nullaway`, `//tools/legend:compiles_test_lib`, `//tools/junit:{junit,compare_testcases}`, `//tools/java_run:print_pins` |
| GEN-COMMITTED | 2 | `//:update_generated`, `//tools/python:requirements.update` |
| TEST-UNIT | 2 | `//tools/junit:{pins_test,runner_test}` |
| CHECK-DIFF | 1 | `//tools/java_run:pins_test` |
| TEST-INTEGRATION | 1 | `//tools/python:requirements.test` (network) |
| COMPILE / TEST-BROWSER / TEST-STRESS / OPEN | 0 | |

---

## Part A: one row per target (or per proven-identical group)

### A1. Model projects (`//projects`, 176 targets)

The 56 projects are the keys of `PROJECT_DEPS` (`projects/BUILD.bazel:11-72`), in four layers:
- layer 0: core-account, core-book, core-calendar, core-fx, core-geo, core-instrument, core-party, core-price, core-ratings,
  core-tenor, core-types, core-units
- layer 1: cash-core, client-core, collateral-core, credit-core, custody-core, event-core, fee-core, funding-core, index-core,
  legal-core, limit-core, order-core, position-keeping, product-core, reference-data, risk-core, settlement-core, static-core,
  trade-capture, valuation-core
- layer 2: client-reporting, collateral-opt, credit-limits, custody-recon, exposure-agg, fee-billing, legal-netting,
  liquidity-view, margin-calc, order-execution, pnl-attribution, product-pricing, regulatory-extract, settlement-flow,
  static-distribution, trade-lifecycle
- layer 3: client-portal, collateral-mgmt, firm-balance-sheet, ops-control, product-catalogue, regulatory-capital,
  risk-dashboard, trade-surveillance

Every project gets the same three targets from one list comprehension each: `legend_library` (`:86-91`) makes `<p>` and
`<p>_test`, and `:139-147` makes `<p>_files`. They are identical in shape. Only `deps` (from PROJECT_DEPS) and one
quarantine row (`firm-balance-sheet`, `:80-84`) differ.

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `//projects:<p>` × 56 (every project above) | `_legend_sources` (legend_library) | The project's own `<p>/*.pure` plus its deps' `LegendLibraryInfo.srcs`, as one postorder depset (`tools/legend/defs.bzl:20-30`) | `projects/<p>/*.pure`; deps' sources | no action, a depset of source files | its `_test`, its dependents, `graph_test` (inv) | n/a (no action) | B (nothing to build) | WIRING | No core dependency. The edges are exactly PROJECT_DEPS (inv `deps`, e.g. `cash-core` → core-account, core-types). |
| `//projects:<p>_test` × 56 (every project above) | java_test (legend_library → junit_test) | CompilesTest: `Compiler.compileModel` over exactly the project's closure (`CompilesTest.java:29-48`, `tools/legend/defs.bzl:32-45`). `firm-balance-sheet_test` is held to fail with `_SCHEMA_VIEW_TWICE` (`projects/BUILD.bazel:78-84`) | data `:<p>` (closure .pure); runtime `//tools/legend:compiles_test_lib` → **`//core` (all 31 core libraries)** + `//testing`, `//base`, `//json`; `//tools/junit` | test.xml | `//projects:tests`; guard_markdown/guard_classpaths (inv) | an edit to `<p>`'s or a dependency's .pure, PROJECT_DEPS, CompilesTest, or the compiler (the closure of `//core:planner`, which owns `com.legend.Compiler`: `rdeps(//core:*, //core:src/main/java/com/legend/Compiler.java, 1)`) | built by B; run by checks and L. Reruns on **any** change to any of the 31 core jars or //testing (F3-like: runtime classpath) | TEST-CORPUS | small, memory 1024, one JVM each. 56 JVM starts on every core edit. |
| `//projects:graph_test` | java_test (legend_graph_test) | The same CompilesTest over 55 projects at once (all but the quarantined one), where cross-project collisions show (`projects/BUILD.bazel:93-98`) | data: 55 `_legend_sources`; same runtime as above | test.xml | `//projects:tests` | any project .pure, or the compiler | B builds; checks and L run | TEST-CORPUS | medium, memory 2048. |
| `//projects:contract_test` | py_test | CONTRACT.md's file-level rule: no class property mapped over a join without a set id (`contract_test.py:1-9`, `:53-60`) | `:contract_files` (`*/mapping.pure`), `PROJECT_DEPS` in env (`BUILD.bazel:106-117`) | test.xml | `//projects:tests` | a `mapping.pure` edit or a PROJECT_DEPS edit | B builds; checks and L run | CHECK-GUARD | No core dependency; reach `projects,python` only. Right-sized. |
| `//projects:contract_files` | filegroup | `glob(["*/mapping.pure"])` (`:101-104`) | 55 mapping files | | contract_test | | B | WIRING | |
| `//projects:tests` | test_suite | 56 `_test` + contract_test + graph_test (`:120-126`) | | | L (`gates/BUILD.bazel:37`), checks (`gates-run.yml:51`), CONTRACT.md:3,75 | | L, checks | WIRING | Used. |
| `//projects:<p>_files` × 11: core-account, core-calendar, core-fx, core-geo, core-instrument, core-ratings, core-tenor, core-types, core-units, fee-core, index-core | filegroup | The project's own `.pure` files, for consumers that link only some projects (`:137-147`) | `projects/<p>/*.pure` | | `//core:core_tests_lib` resources (`core/BUILD.bazel:315`) and 10 `//scripts/corpus` generators/gates (`scripts/corpus/BUILD.bazel:89`); inv: 11 users each | | B | WIRING | Exactly `LINKED_PROJECTS` (`core/stress.bzl:25-36`). So an edit to these 11 projects reruns core_tests and the stress generators (other areas). |
| `//projects:<p>_files` × 45: cash-core, client-core, client-portal, client-reporting, collateral-core, collateral-mgmt, collateral-opt, core-book, core-party, core-price, credit-core, credit-limits, custody-core, custody-recon, event-core, exposure-agg, fee-billing, firm-balance-sheet, funding-core, legal-core, legal-netting, limit-core, liquidity-view, margin-calc, ops-control, order-core, order-execution, pnl-attribution, position-keeping, product-catalogue, product-core, product-pricing, reference-data, regulatory-capital, regulatory-extract, risk-core, risk-dashboard, settlement-core, settlement-flow, static-core, static-distribution, trade-capture, trade-lifecycle, trade-surveillance, valuation-core | filegroup | Same shape, made for every project by the comprehension | | | **none** (inv users=0) | never | B (no action) | DEAD | Proof: inv users=0. `git grep -n 'projects:[a-z-]*_files\|"//projects:%s_files'` hits only the LINKED_PROJECTS consumers above. No lane or doc names one. Harmless (no action) but noise. |
| `//projects:srcs` | filegroup | Every `**/*.pure`, visible to //core and //scripts/corpus (`:128-135`) | 166 files | | **none** (inv users=0) | never | B | DEAD | `git grep -n 'projects:srcs'` is empty. Superseded by the per-project filegroups (P2-03, commit `c75f73b4f`). |
| `//projects:all_files`, `:guard_markdown`, `:guard_classpaths` | | see A2/A3 | | | | | | WIRING / CHECK-GUARD | |

Load-time guard: `projects/BUILD.bazel:150` fails analysis when a project directory has no PROJECT_DEPS entry.

### A2. The guard reports: `guards_package()` → `guard_markdown` + `guard_classpaths` in 54 packages (108 targets)

`guards_package()` (`tools/guards/defs.bzl:82-116`) is the last call of every BUILD file. At load time it runs four checks
over `native.existing_rules()`: every java_test is a junit_test (G16, `:31-45`); every js_test is a node_test (A28, `:49-55`);
every java_library/binary with sources uses the legend macros (A19, `:57-72`); no config_setting outside //tools/platforms
(`:74-80`). Then it declares three targets. The 54 packages:
(root), base, core, datacube, depot-client, depot-server, docs, engine-client, fixtures/saved-queries, gates, json,
parser-equivalence, pct, projects, pure-protocol, query, query-store, scripts/corpus, scripts/parser, sdlc-client, sdlc-server,
site, spec, studio, testing, third_party, tools, tools/browser, tools/bump, tools/cc, tools/census, tools/deps,
tools/engine-runner, tools/generators, tools/graalvm, tools/guards, tools/gunzip, tools/jars, tools/java, tools/java_run,
tools/js, tools/junit, tools/legend, tools/nullaway, tools/par, tools/platforms, tools/postgres, tools/python, tools/reference,
tools/runfiles, tools/teavm, tools/wrongrows, warehouse, wasm.

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `//<pkg>:guard_markdown` × 53 (every package above except tools/guards) | markdown_report | For each `*_test` rule of the package, **manual ones included**, every main-repo `.md` source file in its runfiles (`markdown.bzl:9-18`; `defs.bzl:92-100`) | analysis-time runfiles of the package's tests; no action inputs (F1) | `guard_markdown.tsv` | `//tools/guards:markdown_reports` → `markdown_inputs_test` (inv) | a BUILD/graph change that alters a test's runfiles | B builds (FileWrite); checks and L build it as test data | CHECK-GUARD | 24 are always empty because the package has no `*_test` rule: (root), base, depot-server, gates, site, testing, third_party, tools, tools/cc, tools/census, tools/generators, tools/graalvm, tools/gunzip, tools/jars, tools/java, tools/legend, tools/nullaway, tools/par, tools/platforms, tools/postgres, tools/reference, tools/runfiles, tools/teavm, tools/wrongrows (inv: deps are only the 6 `//tools/platforms` constraint values). |
| `//<pkg>:guard_classpaths` × 53 (same packages) | classpath_report | For each java_test, java_binary and `_java_run` (manual included), every rules_jvm_external jar on its runtime classpath as `group:artifact:version` + pool (`classpath.bzl:27-39`; `defs.bzl:102-108`) | analysis-time `JavaRuntimeClasspathInfo`; no action inputs (F1) | `guard_classpaths.tsv` | `//tools/guards:classpath_reports` → `classpath_test` | a BUILD/MODULE/lock change that alters a JVM classpath | B; checks; L | CHECK-GUARD | 33 always empty (no JVM target): (root), base, depot-client, depot-server, docs, fixtures/saved-queries, gates, pure-protocol, query, query-store, scripts/corpus, scripts/parser, sdlc-client, site, studio, testing, third_party, tools, tools/browser, tools/cc, tools/generators, tools/gunzip, tools/jars, tools/java, tools/js, tools/legend, tools/nullaway, tools/par, tools/platforms, tools/postgres, tools/python, tools/runfiles, tools/wrongrows. |
| `//tools/guards:guard_markdown`, `//tools/guards:guard_classpaths` | markdown_report / classpath_report | The same macro output in the guards' own package | | tsv | **none**: excluded on purpose (`tools/guards/BUILD.bazel:53`, `:75`, "no cycle") | never | B only | DEAD | inv users=0. `guards_package()` makes them unconditionally. |

Per-package fan-out: is it needed? A rule cannot list targets of other packages without naming them, and only a macro
running inside the package sees `native.existing_rules()`. That is why there is one report per package, collected through
`@repo_inventory`'s PACKAGES (`tools/guards/BUILD.bazel:50-54`, `:72-76`). The design rejected a genquery because it "loaded
every platform's downloads" (`markdown.bzl:4-6`). The fan-out works, but it has three costs:
1. 57 of the 108 reports are empty by construction (lists above).
2. Through the two filegroups, `markdown_inputs_test` and `classpath_test` take in every test and JVM target of the repo,
   manual and heavy ones included (reach for both: chromium, native_image, teavm, upstream_src, ...). So L, which skips the
   heavy lanes on purpose (`gates/BUILD.bazel:1-8`), still has to load and analyze the PCT, corpus, native-image and browser
   targets. That fetches whatever repositories their analysis needs (P8-4).
3. An aspect would do the same job without per-package targets (Part C).

### A3. `all_files` (15 in this area; the pattern is in all 54 packages)

| target | kind | what it is | reads | used by | SHOULD run | TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|
| `//:all_files` (60 files), `//gates:all_files` (1), `//projects:all_files` (226), `//third_party:all_files` (5), `//tools:all_files` (27), `//tools/deps:all_files` (7), `//tools/guards:all_files` (9), `//tools/jars:all_files` (2), `//tools/java:all_files` (2), `//tools/java_run:all_files` (4), `//tools/junit:all_files` (11), `//tools/legend:all_files` (3), `//tools/nullaway:all_files` (2), `//tools/python:all_files` (4), `//tools/runfiles:all_files` (2) | filegroup (guards_package) | `glob(["**"])` of the package. At the root it excludes `.git` and `bazel-*/**` (`defs.bzl:109-116`) | every file of the package, **content included** | `//tools/guards:repository_files` only (inv: all 54 `all_files` have exactly that one user) | n/a | B; checks and L via inventory_test | WIRING | Contrary to the inventory doc (`inventory.bzl:14`, "the guards that read contents take them from each package's all_files"), no guard reads contents: `repository_files`' only user is `inventory_test` (inv; `git grep repository_files`). |

### A4. `//tools/guards` (repo-wide checks)

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `:repository_files` | filegroup | Every package's `all_files`, from `@repo_inventory//:packages.bzl` (`BUILD.bazel:9-12`). A package without guards_package() fails analysis here (G0) | 3867 files (`bazel cquery //tools/guards:repository_files --output=files \| wc -l`), 578 of them `.md` | | inventory_test | | B; checks; L | WIRING | |
| `:inventory_test` | java_test (junit_test) | G0: every path in `@repo_inventory//:files.txt` exists in runfiles, so every file is in some `all_files` (`InventoryTest.java:22-33`) | `:repository_files` (all 3867 files) + files.txt (repo rule; re-runs on directory **entry** changes only, `inventory.bzl:12-14`) | test.xml | L, checks | a file added, removed or renamed anywhere | checks, L. Reruns on **any content edit to any file in the repo**, because all 3867 files are runfiles inputs while the test reads only names (`Files.exists`) | CHECK-GUARD | Over-broad inputs (P8-5). |
| `:locks_test` | java_test | G10: every maven.install names its lock, sets `fail_if_repin_required` and `strict_visibility` (`LocksTest.java:17-27`) | `//:module_files` | | L, checks | MODULE.bazel / release.MODULE.bazel edit | checks, L; reruns only on those | CHECK-GUARD | Right-sized. |
| `:markdown_reports` | filegroup (testonly) | 53 `guard_markdown` reports (`BUILD.bazel:50-54`) | | | markdown_inputs_test | | | WIRING | |
| `:markdown_inputs_test` | java_test | G17: every report is empty, so no test reads repo Markdown (`MarkdownInputsTest.java:17-28`) | the 53 reports | | L, checks | a BUILD change that alters a test's runfiles | checks, L; cached on source edits (F1). Its analysis spans every test in the repo | CHECK-GUARD | reach includes chromium, native_image, teavm, wasm_planner, upstream_src. |
| `:classpath_reports` | filegroup (testonly) | 53 `guard_classpaths` reports (`:72-76`) | | | classpath_test | | | WIRING | |
| `:classpath_test` | java_test | G11: no runtime classpath holds two versions of one group:artifact, except ALLOWED (`ClasspathTest.java:19-30`) | the 53 reports | | L, checks | a BUILD/MODULE/lock change | checks, L; cached on source edits (F1) | CHECK-GUARD | Analysis of every JVM target, manual ones included (area 3 problem 9 makes the same point). |

### A5. `//tools/deps` (closures, layering, pools)

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `:core_closure` | genquery | `kind(jvm_import, deps(//core:core) except deps(//tools/nullaway:nullaway))`, `--noimplicit_deps` (`BUILD.bazel:10-15`) | the graph of //core:core (scope) | text file (FileWrite, no inputs, F1) | core_closure_test | a BUILD change under core's closure | B; checks; L | GEN-BUILD | Consumer: core_closure_test. reach `core_libs` is a graph edge, not an input (F1). |
| `:drivers_closure` | genquery | Same for `//core:drivers` (`:20-25`) | | | core_closure_test | same | same | GEN-BUILD | |
| `:spec_closure` | genquery | `kind(jvm_import, deps(//spec:spec_tests_lib))` (`:30-35`) | | | core_closure_test | spec BUILD change | same | GEN-BUILD | |
| `:warehouse_closure` | genquery | `kind(java_library, deps(//warehouse:sqlapi + :client + :server_lib))` (`:57-65`) | | | warehouse_closure_test | warehouse/base/json BUILD change | same | GEN-BUILD | |
| `:core_closure_test` | java_test | Core reaches no external jar; the drivers are core's pool and only the named ones; spec reaches no upstream jar (`CoreClosureTest.java:13-59`) | the three closures | | L, checks (`//tools/deps:all`) | as its genqueries | checks, L; cached on source edits | CHECK-GUARD | |
| `:warehouse_closure_test` | java_test | The warehouse reaches no //core target (`WarehouseClosureTest.java:13-21`) | warehouse_closure | | L, checks | | same | CHECK-GUARD | |
| `:core_layering_test` | java_test | Each core library's direct deps equal `core-layers.txt` (`CoreLayeringTest.java:22-35`) | `core-layers.txt`, `//core:layer_queries` (genqueries from `core/layers.bzl`) | | L, checks | core BUILD / layers.bzl / core-layers.txt | same | CHECK-GUARD | |
| `:pools_list_test` | java_test | `pools.bzl`'s POOLS equal MODULE.bazel's; testonly pools are testonly as a whole; every include()d segment is read (`PoolsListTest.java:24-58`) | `//:module_files`, POOLS/TESTONLY_POOLS as jvm_flags | | L, checks | MODULE.bazel / pools.bzl edit | same | CHECK-GUARD | |

Not present: `//tools/deps:product_closure_test`, which the workplan's P1-25 amendment describes (workplan line 1165). Neither
`tools/deps/BUILD.bazel` nor `bazel query //tools/deps:all` has it. The load-time `check_pool_use` (`pools.bzl:58-80`), called
by legend_java_library, legend_java_binary, junit_test, java_run and teavm_wasm, is the enforcement that exists.

### A6. Root package `//`

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `//:generated` | test_suite | 14 package suites = **47 diff tests**; table in "What //:generated contains" below (`BUILD.bazel:22-43`) | | | L (`gates/BUILD.bazel:15`), checks | each diff test when its generator's true trigger fires | the tests: checks + L. Their generators: **B on every build** (area 1 B2) | WIRING | No guard checks that a new `write_source_files` suite gets added here. "A missing suite fails the build" covers only a listed suite that does not exist. `git grep '//:generated' -- '*.java' '*.py' '*.bzl' '*.bazel'` finds only comments and the two users. |
| `//:update_generated` | `_write_source_file` (write_source_files), testonly, **non-manual** | `bazel run` writes every committed generated file from 15 packages' update targets (`:51-71`) | 15 `additional_update_targets`, **including the manual `//pct:update_ratchets`** | a script; runfiles = every generator output | humans (`bazel run`) | per generator: upstream bump / generator change / corpus change | **B builds it and so runs every generator, including `//pct:ratchets`** (P8-1) | GEN-COMMITTED | True triggers are mixed (see the //:generated table). B reruns the core-reaching ones on every core edit. |
| `//:module_files` | filegroup | MODULE.bazel + `*.MODULE.bazel` (`:12-19`) | 2 files | | locks_test, pools_list_test | | | WIRING | |
| `//:all_files`, `//:guard_markdown`, `//:guard_classpaths` | | A2/A3 (both reports empty) | | | | | | | |

### A7. `//gates`

| target | kind | what it is | used by | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|
| `//gates:local` | test_suite | 49 entries = **292 tests**. It equals, exactly, checks ∪ lane 1 ∪ lane 3 ∪ app ∪ misc ∪ {`//pct:pct_discipline`, `//core:postgres_arm_test`}. Verified: the union of the `tests()` expansions `diff`s identical to `bazel query 'tests(//gates:local)'` | humans per AGENTS.md:366-367 | humans only (no CI lane names it) | WIRING | Used. It duplicates the lane strings in `gates-run.yml:50-62` by hand; P5-01 is meant to fold them together (`gates/BUILD.bazel:6-8`). |
| `//gates:all_files`, `:guard_markdown`, `:guard_classpaths` | | A2/A3 (both reports empty) | | | | |

### A8. Rule tooling targets

| target | kind | what it is | reads | produces | used by | SHOULD run when | runs TODAY | verdict | note |
|---|---|---|---|---|---|---|---|---|---|
| `//tools/nullaway:nullaway` | java_plugin (no processor_class) | Puts NullAway on javac's `--processorpath`; Error Prone's ServiceLoader finds it (`BUILD.bazel:9-14`) | `@maven_tools//:com_uber_nullaway_nullaway` | no compile (no srcs) | 53 targets (inv) | | with any user | TOOL | The core compile's only plugin. |
| `//tools/legend:compiles_test_lib` | java_library (testonly, NullAway) | CompilesTest (`tools/legend/BUILD.bazel:6-16`) | `//core` (umbrella: 31 core libs), `//testing`, jupiter api | jar | 56 `_test` + graph_test | CompilesTest edit (compile). Its users should rerun on compiler changes only | B; checks; L | TOOL | Depends on `//core` where `//core:planner` (21 core libs) would do (P8-3). |
| `//tools/junit:junit` | java_library (testonly, nullaway=False) | JUnitMain/JUnitAction: the Platform Launcher speaking Bazel's test protocol (`BUILD.bazel:5-30`) | @maven_test junit platform jars | jar | every junit_test via `runtime_deps` (`defs.bzl:81`); 141 users (inv) | its own edit | with any test | TOOL | Full default Error Prone (F2). |
| `//tools/junit:compare_testcases` | java_binary (legend_java_binary) | Compares two testlogs trees, by hand: `bazel run //tools/junit:compare_testcases -- <a> <b>` (`BUILD.bazel:58-65`) | | binary | humans; its package's guard_classpaths (inv) | never automatically | B only (no lane depends on it except through the report, F1) | TOOL | B compiles it on every build where it changed. |
| `//tools/junit:pins_test` | java_test | The JVM pins every junit_test runs under (`BUILD.bazel:32-40`; pins in `defs.bzl:50-56`) | `tools/junit/defs.bzl`'s flags | | L, checks | `tools/junit/**` edit | checks, L | TEST-UNIT | Right-sized: no core. |
| `//tools/junit:runner_test` | java_test | JUnitMain against fixture suites (`:42-56`) | `:junit`, JUnit 4 from `@maven_upstream` | | L, checks | `tools/junit/**` edit | checks, L | TEST-UNIT | reach `upstream_jars` (JUnit 4 jar only). |
| `//tools/java_run:print_pins` | java_library (nullaway=False) | Prints what a java_run action sees (`BUILD.bazel:10-15`) | | jar | `:pins` | | | TOOL | |
| `//tools/java_run:pins` | `_java_run` | Runs PrintPins as a build action (`:17-23`) | print_pins jar, exec JDK | `pins.txt` | pins_test | a `tools/java_run/defs.bzl` edit or a JDK change | B; checks; L | GEN-BUILD | Consumer: pins_test. |
| `//tools/java_run:pins_test` | `_diff_test` | `pins.txt` vs the committed, hand-written `pins/pins.expected` (`:25-29`) | | | L, checks | as `:pins` | checks, L | CHECK-DIFF | Works as java_run's unit test. No write_source_files: the expected file is hand-kept on purpose. |
| `//tools:oracle_pins` | `_copy_file` | `@oracle_pins//:oracle-pins.env` (generated from release.MODULE.bazel) under its old label (`tools/BUILD.bazel:1-12`) | release pins | `oracle-pins.env` | 15 parser-equivalence targets + `//spec:reference_lane_report` (inv) | an upstream bump (release.MODULE.bazel) | with its users; B | GEN-BUILD | Right-sized: reach `oracle_pins` only. |
| `//tools/python:lock_matches_requirements` | py_test | Offline: the lock holds every requirements.in pin with hashes (`tools/python/BUILD.bazel:16-30`) | requirements.in, requirements_lock.txt | | L, checks | `tools/python/requirements*` edit | checks, L | CHECK-GUARD | |
| `//tools/python:requirements` | filegroup (compile_pip_requirements) | The macro's source filegroup (`:9-14`) | requirements.in | | requirements.test/.update (inv) | | B | WIRING | Not manual, despite `tags=["manual"]` on the macro (tsv manual=0). |
| `//tools/python:requirements.test` | py_test (manual, requires-network) | Re-resolves against PyPI | | | humans ("run it when this directory changes", `:7-8`) | requirements.in edit | manual; no lane | TEST-INTEGRATION | Appears in no lane (Part D). |
| `//tools/python:requirements.update` | py_binary (manual) | `bazel run` rewrites `requirements_lock.txt` (`:5-7`; MODULE.bazel:413-414) | requirements.in, PyPI | committed lock | humans | requirements.in edit (the true trigger, and the only one) | manual | GEN-COMMITTED | True trigger: a deliberate dependency change. lock_matches_requirements is its offline check. |
| `//tools/python:requirements_test` | alias (manual) | rules_python's back-compat alias for `requirements.test` | | | none | never | never | DEAD | inv users=0; `git grep requirements_test` (outside runs/) is empty. A macro by-product. |

#### A8b. What each macro adds to every Java compile or action
- **legend_java_library** (`tools/java/defs.bzl:22-41`): (1) `check_pool_use` at load time (`pools.bzl:58-80`); (2) javacopts
  `NULLAWAY_OPTS` (unless `nullaway=False`) + `LEGEND_JAVACOPTS` + the target's own; (3) plugin `//tools/nullaway:nullaway`
  (unless opted out); (4) private visibility by default. Applied today:
  - NullAway on (44 with sources): //base:base; //core: builtin, cache, compiler, compiler_element_type, core_next, database,
    diagnostics, driver, duckdb_load, error, exec, ide, lexer, lineage, lowering, model, normalizer, parser, plan, planner,
    platform, probe, protocol, resolver, scale_lib, server_lib, spi, sql, sql_dialect, test, testdatagen, validation, values;
    //depot-server:rules; //json:json; //sdlc-server: page_boundary, rules, server_lib; //tools/legend:compiles_test_lib;
    //warehouse: client, server_lib, sqlapi, sqlapi_wasm_entry.
  - NullAway off (26 with sources, so full default Error Prone): //core: core_tests_lib, toy_grammar; //datacube:
    catalog_facts_main, offer_facts_main; //engine-client:type_facts_main; //parser-equivalence: harvest_shims, pe_tests_lib;
    //pct:pct_tests_lib; //spec: claims, claims_generator_lib, generators, source_tree, spec_tests_lib; //testing:testing;
    //tools/bump:bump_lib; //tools/engine-runner:runner; //tools/gunzip:gunzip; //tools/java_run:print_pins; //tools/junit:junit;
    //tools/par:par_generator; //tools/reference: ref_imports_lib, ref_resolutions; //warehouse:tests_lib; //wasm: boundary,
    jvm_main, zone_main.
- **legend_java_binary** (`:43-57`): check_pool_use + `LEGEND_JAVACOPTS`, no NullAway. 5 have sources: //datacube:app_postgres,
  //tools/census:render_census, //tools/graalvm:sysroot_native_image, //tools/junit:compare_testcases, //tools/teavm:compile.
- **junit_test** (`tools/junit/defs.bzl:29-85`): check_pool_use; args = `select` + `--exclude-tag=` + `--fail-if-no-tests`;
  jvm_flags `-Duser.timezone=GMT -Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8 -Xmx<memory_mb>m` (an `-Xmx` in
  jvm_flags fails); tag `resources:memory:<memory_mb>`; `main_class=JUnitMain`, `use_testrunner=False`; runtime dep
  `//tools/junit`; javacopts `LEGEND_JAVACOPTS` (full default Error Prone on test sources, F2); with `upstream=True`, the two
  upstream source trees + pom.xml as data and their `-D...root.rlocation` flags. 19 junit_tests have sources of their own
  (the other 55 + 56 + 1 + 3 corpus_lane run `runtime_deps` libraries).
- **java_run** (`tools/java_run/defs.bzl:44-157`): runs `main_class` on the exec JDK over the deps' TARGET-configuration
  `transitive_runtime_jars` (no second exec-config compile). Pinned flags `-Duser.timezone=GMT -Duser.language=en
  -Duser.country=US -Dfile.encoding=UTF-8 -Djava.io.tmpdir=<declared scratch dir>`; `memory_mb` → `-Xmx` + a resource_set
  (1024/2048/4096/8192/12288); everything in a param file (Windows' command-line limit); returns `JavaRuntimeClasspathInfo`
  so G11 sees it. Inputs = full runtime jars + srcs + roots + JDK (F3).
- **java_jars / file_list** (`tools/jars/defs.bzl`): write a list of a target's jars (or files) as a declared input, with the
  jars or files in runfiles. No compile. 5 java_jars and 5 file_list targets repo-wide (query `label_kind`).
- **executable_of** (`tools/runfiles/defs.bzl`): returns one file, a binary's launcher. 7 uses.
- **Python** (`tools/python`): no repo macro. Every py_test/py_binary uses rules_python directly. The pip hub `@pypi` comes
  from `requirements_lock.txt` (MODULE.bazel:428-431).
- **guards_package** (A2): load-time checks in every package, plus 3 targets each.

---

## What `//:generated` contains (every diff test), and what it means

`bazel query 'tests(//:generated)'` = 47 `_diff_test` targets. File pairs come from `bazel query --output=build`. Each
generator's reach comes from inv.

| package suite | diff tests | generator (file1) → committed file (file2) | generator reaches | runs on a core edit? |
|---|---|---|---|---|
| `//core:update_generated_tests` | `_0`…`_5` (6) | `//spec:gen_dynafn`→DynaFn.java, `gen_natives`→Pure.java, `gen_imports`→NameResolver.java, `gen_engine_handlers`→engine-handlers.tsv, `gen_claims`→native-claims.tsv, `gen_prelude`→prelude.pure | core_libs (+upstream_src; gen_claims +core_next) | yes |
| `//core:update_ladder_tests` | `_0`…`_11` (12) | `//core:ladder_report` → `core/src/test/resources/ladder/r01…r12*.current.sql` | core_libs, projects | yes |
| `//core:update_stress_corpus_tests` | `_0`…`_10` (11) | `//scripts/corpus:gen_dense` (59/60/64), `gen_stress` (92–98) → `core/src/test/resources/stress/*.pure`; `//core:stress_layout` → stress-layout.json | projects, python (not core) | no |
| `//datacube:update_generated_tests` | `_0`…`_3` (4) | `offer_facts`→offer-facts.ts, `catalog_rules`→catalog-facts.ts, `catalog_corpus`→catalog-corpus.ts (core_libs); `test_imports`→test_imports.bzl (js) | core_libs ×3; npm ×1 | 3 of 4 |
| `//engine-client:update_generated_tests` | 1 | `lite_facts` → lite-facts.ts | core_libs | yes |
| `//docs:update_generated_tests` | 1 | `//parser-equivalence:gen_roster` → docs/protocol-roster.tsv | core_libs, upstream | yes |
| `//fixtures/saved-queries:update_generated_tests` | `_0`…`_3` (4) | `:gen` (js_run_binary) → 4 json fixtures | core_libs | yes |
| `//parser-equivalence:update_generated_tests` | `_0`, `_1` (2) | `gen_manifest`→corpus-manifest.tsv, `gen_fixtures`→engine-grammar-fixtures.jsonl | core_libs, upstream | yes |
| `//parser-equivalence:update_ratchets_tests` | 1 | `ratchets` → ratchets.tsv | core_libs, upstream | yes |
| `//query:update_generated_tests` | 1 | `icons_gen` → src/ui/icons.ts | none of the tracked reaches | no |
| `//scripts/parser:update_keyword_coverage_tests` | 1 | `keyword_coverage` → keyword-coverage.tsv | core_libs, python, upstream | yes |
| `//spec:update_ratchets_tests` | 1 | `ratchets` → spec ratchets.tsv | core_libs, upstream | yes |
| `//tools/engine-runner:update_vocab_tests` | 1 | `vocab` → vocab.tsv | core_libs, upstream_jars | yes |
| `//warehouse:update_reachability_metadata_tests` | 1 | `reachability_metadata` → reachability-metadata.json | warehouse_server | no |

What this means for when committed-file checks run:
- The **checks** run (the 47 tests) only in CI's checks lane and in L.
- The **generators** run in every B. They are runfiles of non-manual tests and of the non-manual `//:update_generated`
  (P8-1). 33 of the 47 have a generator that reaches core, and java_run's inputs are full runtime jars (F3). So **every core
  edit reruns those generators in B, L and checks**, whatever their true trigger. For most of them the true trigger is an
  upstream bump or a generator change, which the owning areas establish. A diff test whose generator output comes out
  byte-identical is then a cache hit; the generator reran anyway.
- Outside //:generated there are 13 more `_diff_test`s: the 10 rcorpus tests (lanes 4 and 5), `//tools/java_run:pins_test`
  (checks), and two manual ones in no lane: `//pct:update_ratchets_test` and `//spec:update_reference_lane_test` (Part D).

---

## Part B: problems in this area, with evidence

- **P8-1. `//:update_generated` defeats `manual` and makes B run PCT's ratchet measurement.** `//pct:ratchets` is a 2 GB
  java_run over `pct_tests_lib` + the upstream trees, tagged manual "at the suites' cost" (`pct/BUILD.bazel:253-272`).
  `//pct:update_ratchets` is manual too (`:274-282`). But the root `//:update_generated` is non-manual (tsv) and lists
  `//pct:update_ratchets` (`BUILD.bazel:64`). `bazel cquery //:update_generated --output=starlark --starlark:expr=...default_runfiles`
  lists `pct/generated/ratchets.tsv`. So B builds `//pct:ratchets`, which reaches core_libs (inv), and every core edit reruns
  the Channel B discovery run inside `bazel build //...`. The same root target puts all 15 packages' generators into B a
  second time. Area 3 noted `pct:ratchets`' analysis via the guards (its problem 9) but not this execution path.
- **P8-2. The 56 project tests depend on all of core and start 56 JVMs.** `_compiles_test` puts `//tools/legend:compiles_test_lib`
  on every test's runtime classpath (`tools/legend/defs.bzl:43`), and that library depends on `//core` (`tools/legend/BUILD.bazel:12`).
  Its closure is all 31 core libraries (`bazel query 'kind(java_library, deps(//tools/legend:compiles_test_lib)) intersect //core:*'`
  = 31). So any core edit, server, exec, ide or testdatagen included, reruns 57 JVM tests. Each test calls only
  `Compiler.compileModel` (`CompilesTest.java:39`, `:43`). Its class lives in `//core:planner`, whose closure has 21 core
  libraries and excludes driver, exec, ide, probe, server_lib, test, testdatagen and diagnostics (query above).
- **P8-3. Dead targets created by comprehension.** 45 `<p>_files` and `//projects:srcs` have 0 users (inv; greps in A1). The
  per-package comprehension builds a filegroup for all 56 projects, though only `LINKED_PROJECTS` (11) are consumed.
  `//projects:srcs`' visibility to //core and //scripts/corpus outlived its last consumer (P2-03, `c75f73b4f`).
- **P8-4. The light local gate analyzes the whole repo.** `markdown_inputs_test` and `classpath_test` (both in L) depend,
  through 53 reports each, on every test and JVM target in the repo, manual ones included (`defs.bzl:94`, `:104`, "manual
  ones included" `:33-35`). Their reach includes chromium, native_image, teavm, wasm_planner, upstream_src (tsv). L
  deliberately excludes the heavy lanes (`gates/BUILD.bazel:1-8`), yet must load and analyze them. Whether that analysis
  downloads GraalVM, Chromium or the upstream archives on a cold machine is **OPEN**. Settle it with `bazel cquery
  'deps(//tools/guards:markdown_inputs_test)'` on a fresh `--output_base`, then `ls <output_base>/external` (or
  `--experimental_repository_resolved_file`). Execution cost is nil (F1).
- **P8-5. `inventory_test` reruns on every edit to any file.** Its data is all 3867 repository files (cquery count), but it
  checks only that each inventoried path exists (`InventoryTest.java:24-32`). Its true trigger is a file add, remove or
  rename. `@repo_inventory` already re-evaluates on exactly that (`inventory.bzl:12-14`). Every content edit anywhere,
  Markdown included, invalidates the test.
- **P8-6. 57 of the 108 guard reports are empty by construction**, and 2 (tools/guards') are consumed by nothing (A2). A
  package with no `*_test` rule, or no JVM target, still gets a report.
- **P8-7. Error Prone coverage is inverted.** Product libraries with NullAway run only NullAway + 2 locale checks
  (`-XepDisableAllChecks` precedes them, `tools/java/defs.bzl:36-37`). Tests, binaries and the 26 libraries without
  NullAway run the whole default Error Prone set (F2 aquery). Nothing records this as intended. On cost: NullAway's dataflow
  pass is inside every core layer's Javac action, so it is part of the core compile. How much of the compile it costs is
  **OPEN** (settle with an `--experimental_execution_log` of a clean `bazel build //core:server` with and without NullAway on
  a branch; timing is out of this brief's scope).
- **P8-8. Nothing guards `//:generated`'s completeness.** A new package's `write_source_files` suite missing from
  `//:generated` is silently never run (B only builds it). Evidence: the grep in A6. The same holds for lanes in general: two
  non-manual tests, `//warehouse:postgres_live` and `//warehouse:postgres_live_native`, are in no lane (Part D).
- **P8-9. Lane lists are kept twice.** `gates-run.yml:50-62` (inline strings) and `gates/BUILD.bazel:11-79` (a test_suite).
  They agree today: L = checks ∪ 1 ∪ 3 ∪ app ∪ misc ∪ {pct_discipline, postgres_arm_test}, verified identical. Wildcards
  (`//tools/deps:all`, `//wasm:all`) in CI versus named tests in L are kept equal by hand (`gates/BUILD.bazel:6-8`).
  `bazel test //wasm:all` also builds every non-test target of //wasm (planner, startup, ...; `bazel query`).
- **P8-10. `//tools/junit:compare_testcases` (a by-hand tool) is compiled by B.** Only its package's report depends on it
  (inv), and the report does not build it (F1). Small cost; it is a TOOL in the everyday build.

## Part C: the right shape for this area

1. **Projects** (evidence P8-2, P8-3):
   - `//tools/legend:compiles_test_lib`: `deps = ["//core:planner", "//testing", jupiter api]` instead of `//core`. The test
     reruns on compiler changes, not on server/exec/ide edits. Needs one strict-deps compile to prove `com.legend.Compiler` and
     `ModelSource` are reachable from planner's exports (OPEN until compiled; area 1 decides whether planner is the right API
     target).
   - Collapse the 56 `<p>_test` JVMs into **one** `//projects:closures_test` that compiles each project's closure separately in
     one JVM. CompilesTest already takes the file list from the environment (`CompilesTest.java:31`). Pass one
     `LEGEND_LIBRARY_<p>` per project plus the QUARANTINE map, and report one dynamic test per project so the names stay.
     Keep `_legend_sources` (no actions) and `graph_test`. Same semantics; 2 JVMs instead of 57 on each compiler edit.
   - Generate `<p>_files` only for `LINKED_PROJECTS` (load `//core:stress.bzl`), and delete `//projects:srcs`.
   - Leave `contract_test` as it is.
   - Lane: projects belong in checks and L, triggered by `projects/**` and compiler changes. They already are.
2. **The guards** (evidence F1, P8-4, P8-5, P8-6):
   - Replace the per-package reports with one **aspect** run by the build lane: `bazel build //... --aspects=//tools/guards:guards.bzl%guard_aspect
     --output_groups=+guard_reports`, or with validation actions (`_validation` output group), which B runs where everything
     is analyzed anyway. G11 and G17 then fail B and stop costing L a whole-repo analysis. If the aspect cannot cover manual
     targets (`//...` skips them), keep a per-package report for manual targets only. Prove it on Windows CI (Bazel-native
     first). Until then: skip creating a report when its target list is empty, and skip creating any in tools/guards (2
     dead targets, 57 empty ones).
   - Keep `classpath_test` and `markdown_inputs_test` out of L, or move them to the build lane: their true trigger is a
     BUILD/MODULE change, and B already analyzes every target.
   - `inventory_test`: compare names, not contents. Have `guards_package()` emit an analysis-time file list (FileWrite of the
     glob's paths, like F1's reports), collect those, and diff them with `files.txt`. The test then reruns only on add, remove
     or rename. Equivalently, `repository_files` could carry the per-package lists instead of the files.
3. **Root and generators** (evidence P8-1, P8-8, "What //:generated contains"):
   - Tag `//:update_generated` `manual` (it is a `bazel run` entry point). That alone stops B from running `//pct:ratchets`
     and builds every generator once instead of twice.
   - Split `//:generated` by true trigger once the owning areas settle each generator's inputs, e.g.
     `//gates:generated_upstream` (spec gen_*, parser-equivalence, keyword coverage, vocab, ratchets: trigger = upstream bump
     or generator change) and `//gates:generated_corpus` (stress corpus, ladder). Their generators should stop depending on
     all of core (areas 1-3). The diff tests should not be built by B: make each package's write_source_files
     `tags=["manual"]` with the suite listing the tests explicitly. An explicitly listed manual test still runs from a suite.
   - Add a guard: `tests(//...) except tests(<every lane suite>)` must equal a reviewed allowlist (each row with its reason).
     That catches a missing `//:generated` entry and the two `postgres_live` tests.
4. **Rule tooling** (evidence F2, F3, P8-7):
   - Keep NullAway inside the compile (a separate pass would compile twice), but make Error Prone coverage a decision. Either
     `-XepDisableAllChecks` + an explicit list for every first-party compile (cheaper, the same for product and test code), or
     the defaults everywhere. Put the list in `LEGEND_JAVACOPTS` (`tools/java/defs.bzl:17-20`) so all four macros share it.
   - java_run's full-jar inputs are correct for a program that runs. The fix is narrower generator deps, which areas 1-3
     decide.
   - Mark `//tools/junit:compare_testcases` manual (a `bazel run` tool), or accept the tiny compile.
5. **Gates and lanes** (evidence P8-9, Part D): make every CI lane a test_suite in `//gates` (P5-01) and have
   `gates-run.yml` name only `//gates:<lane>`. Then L = a suite of suites, the YAML holds no target lists, and the
   allowlist guard in 3 has one source to read.

---

## Part D: THE CROSS-CUTTING MAP (whole repo)

Method. Each lane's targets string (`gates-run.yml:47-65`) was expanded with `bazel query 'tests(<targets joined with +>)'`
(the lists below). `//gates:local` likewise. All tests in the repo: `bazel query --output=label_kind '//... except
//bazel-bazel-exec/...'` filtered to the four test rule classes (js_test 142, java_test 135, _diff_test 60, py_test 9 =
**346 tests**). Manual: `attr(tags, "\bmanual\b", ...)`. None of the 8 manual tests is in any lane's expansion, so the
wildcard lanes' manual filtering does not matter. Platforms: every lane runs on linux, macos (7 GB, `--config=ci-small`)
and windows (`gate.yml:64-90`), except browser (Linux only, `gates-run.yml:69`). linux-arm runs only native
(`gate.yml:95-101`). `gate.yml:23-29` skips CI entirely for Markdown-only and `progress*.txt` commits.

Lane totals: 1 = 28, checks = 127, 3 = 1, 4 = 6, 5 = 6, 6 = 6, 7 = 1, 7p = 6, 8 = 1, 9 = 5, 10 = 2, app = 117, misc = 17,
native = 2 (+ builds `//datacube:app`), browser = 12 (+ `bazel run`s `//datacube:install_browser`, `//query:verify`,
`//site:verify`, the `attr(tags, "browser-ci", //query:* + //site:*)` harnesses, `gates-run.yml:151`, `:178-195`), build =
`bazel build //...` (1044 non-manual targets, tests built but not run) + `bazel build --nobuild --config=bazel10 //...` + the
A25 `--nobuild` analysis of non-test targets of //warehouse, //pct, //datacube for `//tools/platforms:unlisted_test`
(`:166-177`). CI's test lanes together run **336** distinct tests.

**`//gates:local` = 292 tests** = checks ∪ 1 ∪ 3 ∪ app ∪ misc ∪ {`//pct:pct_discipline`, `//core:postgres_arm_test`},
verified identical. Every L test is also in some CI lane. CI-only (44): all of lanes 4, 5, 8, 9, 10, native, browser;
lane 6 except pct_discipline; lane 7; lane 7p except postgres_arm_test. Named: //core:stress_suites, //core:stress_suites_h2,
//datacube:{run_stress_test, verify_charts_test, verify_cubes_test, verify_features_test, verify_page_test,
verify_picker_test, verify_remote_test, verify_smoke_test, verify_upload_test, verify_wasm_browser_test},
//parser-equivalence:parser_parity, //pct:pct_channel_b_{essential,grammar,relation,standard,unclassified},
//pct:pct_duckdb_{essential,grammar,relation,standard,unclassified}, //pct:pct_h2,
//pct:pct_postgres_{essential,grammar,relation,standard,unclassified}, //spec:corpus_duckdb_verdict,
//spec:corpus_h2_verdict, //spec:update_rcorpus_duckdb_{0..4}_test, //spec:update_rcorpus_h2_{0..4}_test,
//studio:verify_test, //warehouse:launcher_test, //warehouse:tests_native.

Third workflow: `.github/workflows/diagnostics.yml:7-20`, `:49-50` runs `bazel test //parser-equivalence:diagnostics`
(manual) on a push/PR that touches MODULE.bazel or release.MODULE.bazel.

### D1. Each lane's tests (expanded)
#### lane 1: 28 tests
- `//core` (28): core_tests_architecture, core_tests_builtin, core_tests_cache, core_tests_compiler, core_tests_exec, core_tests_ide, core_tests_integration, core_tests_ladder, core_tests_lexer, core_tests_lineage, core_tests_lowering, core_tests_model, core_tests_normalizer, core_tests_parser, core_tests_platform, core_tests_protocol, core_tests_resolver, core_tests_root, core_tests_server, core_tests_sql, core_tests_test, core_tests_testdatagen, core_tests_testing, core_tests_values, corpus_differential_test, duckdb_load_test, planner_on_java_base_test, section_grammar_registry_test

#### lane checks: 127 tests
- `//core` (31): census, guardrails, update_generated_0_test, update_generated_1_test, update_generated_2_test, update_generated_3_test, update_generated_4_test, update_generated_5_test, update_ladder_0_test, update_ladder_1_test, update_ladder_10_test, update_ladder_11_test, update_ladder_2_test, update_ladder_3_test, update_ladder_4_test, update_ladder_5_test, update_ladder_6_test, update_ladder_7_test, update_ladder_8_test, update_ladder_9_test, update_stress_corpus_0_test, update_stress_corpus_1_test, update_stress_corpus_10_test, update_stress_corpus_2_test, update_stress_corpus_3_test, update_stress_corpus_4_test, update_stress_corpus_5_test, update_stress_corpus_6_test, update_stress_corpus_7_test, update_stress_corpus_8_test, update_stress_corpus_9_test
- `//datacube` (4): update_generated_0_test, update_generated_1_test, update_generated_2_test, update_generated_3_test
- `//docs` (1): update_generated_test
- `//engine-client` (1): update_generated_test
- `//fixtures/saved-queries` (4): update_generated_0_test, update_generated_1_test, update_generated_2_test, update_generated_3_test
- `//parser-equivalence` (3): update_generated_0_test, update_generated_1_test, update_ratchets_test
- `//projects` (58): cash-core_test, client-core_test, client-portal_test, client-reporting_test, collateral-core_test, collateral-mgmt_test, collateral-opt_test, contract_test, core-account_test, core-book_test, core-calendar_test, core-fx_test, core-geo_test, core-instrument_test, core-party_test, core-price_test, core-ratings_test, core-tenor_test, core-types_test, core-units_test, credit-core_test, credit-limits_test, custody-core_test, custody-recon_test, event-core_test, exposure-agg_test, fee-billing_test, fee-core_test, firm-balance-sheet_test, funding-core_test, graph_test, index-core_test, legal-core_test, legal-netting_test, limit-core_test, liquidity-view_test, margin-calc_test, ops-control_test, order-core_test, order-execution_test, pnl-attribution_test, position-keeping_test, product-catalogue_test, product-core_test, product-pricing_test, reference-data_test, regulatory-capital_test, regulatory-extract_test, risk-core_test, risk-dashboard_test, settlement-core_test, settlement-flow_test, static-core_test, static-distribution_test, trade-capture_test, trade-lifecycle_test, trade-surveillance_test, valuation-core_test
- `//query` (1): update_generated_test
- `//scripts/corpus` (5): density_gate, executed_gate, functions_gate, scoreboard_gate, stacking_gate
- `//scripts/parser` (1): update_keyword_coverage_test
- `//spec` (1): update_ratchets_test
- `//tools/browser` (1): revision_test
- `//tools/bump` (1): bump_test
- `//tools/deps` (4): core_closure_test, core_layering_test, pools_list_test, warehouse_closure_test
- `//tools/engine-runner` (1): update_vocab_test
- `//tools/guards` (4): classpath_test, inventory_test, locks_test, markdown_inputs_test
- `//tools/java_run` (1): pins_test
- `//tools/js` (1): lock_matches_package_json_test
- `//tools/junit` (2): pins_test, runner_test
- `//tools/python` (1): lock_matches_requirements
- `//warehouse` (1): update_reachability_metadata_test

#### lane 3: 1 tests
- `//spec` (1): spec_tests

#### lane 4: 6 tests
- `//spec` (6): corpus_duckdb_verdict, update_rcorpus_duckdb_0_test, update_rcorpus_duckdb_1_test, update_rcorpus_duckdb_2_test, update_rcorpus_duckdb_3_test, update_rcorpus_duckdb_4_test

#### lane 5: 6 tests
- `//spec` (6): corpus_h2_verdict, update_rcorpus_h2_0_test, update_rcorpus_h2_1_test, update_rcorpus_h2_2_test, update_rcorpus_h2_3_test, update_rcorpus_h2_4_test

#### lane 6: 6 tests
- `//pct` (6): pct_discipline, pct_duckdb_essential, pct_duckdb_grammar, pct_duckdb_relation, pct_duckdb_standard, pct_duckdb_unclassified

#### lane 7: 1 tests
- `//pct` (1): pct_h2

#### lane 7p: 6 tests
- `//core` (1): postgres_arm_test
- `//pct` (5): pct_postgres_essential, pct_postgres_grammar, pct_postgres_relation, pct_postgres_standard, pct_postgres_unclassified

#### lane 8: 1 tests
- `//parser-equivalence` (1): parser_parity

#### lane 9: 5 tests
- `//pct` (5): pct_channel_b_essential, pct_channel_b_grammar, pct_channel_b_relation, pct_channel_b_standard, pct_channel_b_unclassified

#### lane 10: 2 tests
- `//core` (2): stress_suites, stress_suites_h2

#### lane app: 117 tests
- `//datacube` (112): adhoc_mode_test, adhoc_query_test, adhoc_session_test, adhoc_state_test, adhoc_transactions_test, app_test, apply_refusal_test, board_test, bundle_budget_test, calc_fix_test, calc_test, calc_vocabulary_test, cancel_test, catalog_model_test, chart_echarts_test, chart_tiles_test, child_groups_test, column_editor_test, column_kind_test, columns_panel_test, columns_selector_test, config_readers_test, config_test, cube_adhoc_shell_test, cube_document_test, cube_editors_app_test, cube_library_test, cube_lifecycle_test, cube_open_test, cube_state_test, cube_store_test, cube_transactions_test, dimensions_test, dist_complete_test, drill_test, duckdb_cancel_test, duckdb_test, editor_test, editors_live_test, engine_remote_test, epoch_test, escaping_test, export_doc_test, export_model_test, export_rich_test, export_test, filter_editor_test, form_test, format_scale_test, format_test, fuzz_test, grid_basics_test, grid_dom_test, grid_resize_test, grid_test, group_derived_test, guardrails_test, host_test, infer_test, json_read_test, json_shape_test, live_snap_test, menu_ids_test, menu_test, menu_view_test, multi_cube_test, offer_facts_test, page_document_test, pivot_panel_test, pivot_rows_test, pivot_total_test, pivot_values_test, plane_test, planner_test, portability_test, query_test, relation_type_test, remote_test, runner_test, sample_test, save_dialog_test, saved_queries_test, scale_test, screen_colours_test, selection_test, share_link_test, shell_test, snap_test, sorting_test, source_picker_test, state_guardrail_test, style_test, tile_layout_test, torture_test, tree_test, treeview_test, type_columns_test, typecheck_mjs_test, typecheck_test, typed_values_new_york_test, typed_values_test, typed_values_tokyo_test, undo_coverage_test, upload_test, values_test, verify_app_test, warehouse_session_test, wasm_differential_test, wasm_flag_test, wasm_planner_test, window_columns_test, window_test
- `//query-store` (1): lite_test
- `//warehouse` (2): sqlapi_wasm_build_test, tests
- `//wasm` (2): differential_test, zone_test

#### lane misc: 17 tests
- `//depot-client` (2): server_test, wasm_test
- `//json` (1): tests
- `//pure-protocol` (1): twins_test
- `//query-store` (2): local_test, share_test
- `//query` (4): build_test, load_test, saved_queries_test, typecheck_test
- `//sdlc-client` (2): server_test, wasm_test
- `//sdlc-server` (1): git_repository_test
- `//studio` (3): demo_test, typecheck_test, workspace_test
- `//tools/engine-runner` (1): smoke_test

#### lane native: 2 tests
- `//warehouse` (2): launcher_test, tests_native

#### lane browser: 12 tests
- `//datacube` (11): live_snap_test, run_stress_test, verify_charts_test, verify_cubes_test, verify_features_test, verify_page_test, verify_picker_test, verify_remote_test, verify_smoke_test, verify_upload_test, verify_wasm_browser_test
- `//studio` (1): verify_test


### D2. Targets in NO lane and NO gate

**Tests that no CI lane and not `//gates:local` runs (10 of 346):**
| test | manual | why / who runs it |
|---|---|---|
| `//parser-equivalence:diagnostics` | yes | diagnostics.yml on a MODULE.bazel/release.MODULE.bazel change (the only one of the 10 that any workflow runs) |
| `//pct:update_ratchets_test` | yes | Gate 9's Channel B tests compare live discovery with the committed file instead (`pct/BUILD.bazel:280`). By hand only |
| `//scripts/corpus:engine_stress` | yes | No workflow, gate or doc names it (`git grep ':engine_stress' -- '*.md' '*.yml' '*.bazel'` is empty) |
| `//spec:corpus_warehouse_verdict` | yes | No workflow or doc names it (the same `git grep` is empty) |
| `//spec:manifest_world_census` | yes | By hand (docs/GATES.md) |
| `//spec:reference_lane` | yes | By hand (docs/GATES.md, tools/reference/README.md) |
| `//spec:update_reference_lane_test` | yes | By hand only (the same `git grep` is empty) |
| `//tools/python:requirements.test` | yes | By hand, network (`tools/python/BUILD.bazel:7-8`) |
| `//warehouse:postgres_live` | **no** | Needs a DSN (`warehouse/BUILD.bazel:180`). Built by B, never run by any lane |
| `//warehouse:postgres_live_native` | **no** | As above, and it also pulls `:server_native` into B |

**Manual non-test targets that no lane builds** (`bazel query` deps of every lane's tests + `//datacube:app` + the browser
lane's `bazel run` targets, excluding the guard reports' analysis-only edges, F1). 34 targets. Several are reached by flags
rather than deps: `//tools/platforms:unlisted_test` (the A25 `--platforms`) and `//tools/platforms:windows_aarch64`.
The rest are `bazel run` tools, probes, censuses and update targets for humans:
`//core:scale`, `//core:stress_tool`, `//datacube:link_dictionary_next_copy_srcs_to_bin`, `//datacube:link_dictionary_next_js_info_files`, `//datacube:link_dictionary_next_runfiles`, `//datacube:verify_app`, `//parser-equivalence:corpus_census`, `//parser-equivalence:diagnostics_reports`, `//parser-equivalence:fixture_sweep`, `//parser-equivalence:grammar_keyword_census`, `//parser-equivalence:migration_sizing`, `//parser-equivalence:parse_speed_benchmark`, `//parser-equivalence:pmcd_reachability_census`, `//parser-equivalence:probe_wire_shapes`, `//parser-equivalence:z_fixture_adjudication_probe`, `//pct:ratchets`, `//pct:update_ratchets`, `//pct:update_ratchets_tests`, `//spec:corpus_one`, `//spec:corpus_warehouse`, `//spec:eager_corpus_compile`, `//spec:eager_corpus_compile_world2`, `//spec:judge_database_warehouse`, `//spec:judge_host_warehouse`, `//spec:our_resolutions`, `//spec:reference_lane_report`, `//spec:update_reference_lane`, `//spec:update_reference_lane_tests`, `//tools/platforms:unlisted_test`, `//tools/platforms:windows_aarch64`, `//tools/python:requirements_test`, `//tools/python:requirements.update`, `//tools/reference:ref_dump`, `//tools/reference:ref_imports`.

**Non-manual, non-test targets that only B touches** (no test lane depends on them): 189, as follows.
- 45 unused `//projects:<p>_files` (A1, DEAD; `//projects:srcs` is in the last bullet).
- 66 `write_source_file` targets (per-file `update_*_N` scripts and the package umbrellas). These are `bazel run`
  writers, and B builds their generator runfiles. The umbrellas: `//:update_generated`, `//core:update_generated`, `//core:update_ladder`, `//core:update_stress_corpus`, `//datacube:update_generated`, `//docs:update_generated`, `//engine-client:update_generated`, `//fixtures/saved-queries:update_generated`, `//parser-equivalence:update_generated`, `//parser-equivalence:update_ratchets`, `//query:update_generated`, `//scripts/parser:update_keyword_coverage`, `//spec:update_ratchets`, `//spec:update_rcorpus_duckdb`, `//spec:update_rcorpus_h2`, `//tools/engine-runner:update_vocab`, `//warehouse:update_reachability_metadata`.
- 32 test_suites (wiring; B builds nothing for a suite).
- `//tools/guards:guard_markdown` and `//tools/guards:guard_classpaths` (DEAD).
- 44 other targets, mostly `bazel run` tools and by-hand harnesses that B compiles or bundles on every build: `//core:draft_native_membership`, `//core:scale_lib`, `//datacube:chaos`, `//datacube:cut_link_dictionary`, `//datacube:link_dictionary_next`, `//datacube:make_link_dictionary`, `//datacube:make_sample`, `//datacube:measure_startup`, `//datacube:run_stress`, `//datacube:shots`, `//datacube:torture`, `//datacube:verify_calc_vocabulary`, `//datacube:verify_charts`, `//datacube:verify_cubes`, `//datacube:verify_engine`, `//datacube:verify_engine_differential`, `//datacube:verify_features`, `//datacube:verify_page`, `//datacube:verify_picker`, `//datacube:verify_real_data`, `//datacube:verify_remote`, `//datacube:verify_smoke`, `//datacube:verify_upload`, `//datacube:verify_wasm_browser`, `//docs:draft_own_corpus_ledger`, `//parser-equivalence:gen_own_corpus_draft`, `//projects:srcs`, `//query:serve`, `//scripts/corpus:run`, `//scripts/corpus:testable_launcher`, `//site:serve`, `//spec:native_declarations`, `//spec:native_membership_draft`, `//studio:serve`, `//tools/bump:bump`, `//tools/census:lanes_diff`, `//tools/census:render_census`, `//tools/junit:compare_testcases`, `//tools/python:requirements`, `//tools/reference:ref_imports_lib`, `//tools/reference:ref_resolutions`, `//tools/wrongrows:compare`, `//warehouse:server`, `//wasm:startup`.
