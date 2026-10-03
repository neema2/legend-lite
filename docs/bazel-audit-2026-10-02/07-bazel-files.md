# Auditor 7: fresh-eyes review of every Bazel file in legend-lite

Audited at origin/main 16c8120d5 (2026-10-02)

**Slice:** an independent review of every Bazel file: MODULE.bazel, MODULE.bazel.lock, .bazelrc, .bazelversion, .bazelignore, all BUILD.bazel files, all .bzl files outside `experiments/`, `third_party/*`, the `maven_*_install.json` lock headers and the pnpm-lock headers.

The audit checkout was used read-only. Nothing tracked was modified. One probe directory under the gitignored `runs/` was created and deleted.

**Commands run (all succeeded unless noted):**
- `bazel query 'tests(//...)' --output label_kind` and `--output=build`: 171 tests.
- `attr(tags, manual, …)`, `kind(genrule, …)`, `kind("sh_.*|native_binary|native_test", …)`: the last returned none.
- `attr(tags, browser-ci, …)`.
- `bazel build --nobuild //...`: 359 targets, no warnings.
- `bazel build --nobuild` once under each of 13 upcoming `--incompatible_*` flags.
- `bazel mod deps --lockfile_mode=error`: rc=0, so MODULE.bazel.lock is committed and current.
- `bazel mod graph`.
- A cross-pool overlap script over all 8 `maven_*_install.json` files.
- A script listing every BUILD reference to an undeclared Maven artifact.

## Section 1 — NEW findings

| ID | Sev | File:line | What | Idiomatic fix |
|---|---|---|---|---|
| N1 | P1 | `.github/workflows/gates-run.yml:53-68` | **9 non-manual tests are never run by any CI lane**: `//json:tests`, `//pure-protocol:twins_test`, `//query:build_test`, `//query:load_test`, `//query:saved_queries_test`, `//query:typecheck_test`, `//query-store:lite_test`, `//query-store:local_test`, `//query-store:share_test`. No workflow runs `bazel test //...` or `bazel build //...`. Non-test targets outside every lane are never even built in CI (`//tools/engine-runner:*`, `//tools/bump`, `//site:dist`, `//site:serve`, `//wasm:startup`, the untagged datacube harnesses). Lanes are a hand-kept target list, so new packages fall out silently. | One lane (or a final job) runs `bazel test //...` (manual and incompatible targets are skipped automatically) plus `bazel build //...`. Keep per-gate lanes as `test_suite`s declared in BUILD files and have CI name only those suites. |
| N2 | P1 | `MODULE.bazel:80,91,102,124,216,250,262,277`; `tools/bump/Bump.java:131-132` | `fail_if_repin_required = False` on all 8 `maven.install`: an artifact or BOM edit without a repin silently keeps the old lock (warning only). Concrete failure: `LEGEND_ENGINE_RELEASE` drives both `@maven_upstream` and `@maven_runner` (artifacts and BOM, `MODULE.bazel:230-248`), but Bump repins **only** `@maven_upstream`. After the next bump, `maven_runner_install.json` keeps 4.145.0 jars (both locks are 4.145.0 today) and nothing fails. `//tools/deps:one_release` compares MODULE.bazel with `oracle-pins.env` only, never the lock contents. | `fail_if_repin_required = True` everywhere (the rules_jvm_external default intent). Bump should run `REPIN=1 bazel run @<pool>//:pin` for every pool whose inputs reference the release, or loop over all pools. Also add `common:ci --lockfile_mode=error`. |
| N3 | P1 | `.bazelignore:1-2` | `runs/` is gitignored but **not bazelignored**. In the main checkout, worktrees live under `runs/<name>/`, each with its own BUILD files. Verified with a probe: `runs/a7probe/BUILD.bazel` showed up in `bazel query //runs/...`, even with a nested `MODULE.bazel` beside it. So `bazel test //...` from the main checkout (which Bump runs, `Bump.java:148`) expands every worktree's packages too. The comment also says "Two Bazel prototype workspaces" while `experiments/` has 8 dirs. | Add `runs` (plus `datacube/node_modules` and `query/node_modules`, per rules_js guidance) to `.bazelignore`, or use Bazel 8+ `REPO.bazel` `ignore_directories(["runs/**", "**/node_modules"])`. |
| N4 | P1 | `warehouse/BUILD.bazel:87-93,177-192`; `WarehouseArrowTest.java:125-150`; `gates-run.yml:123-128,141-143` | `//warehouse:tests` and `//warehouse:tests_native` run a host `python3` or `python` with `pyarrow` found on PATH. Locally (strict action env, `PATH=/bin:/usr/bin:/usr/local/bin`) the test silently `Assumptions.abort`s. CI pip-installs pyarrow and passes `--test_env=PATH --test_env=PYTHONPATH=$RUNNER_TEMP/... --test_env=WAREHOUSE_ARROW_CHECK=required`, so the cache key depends on the runner's PATH. The verdict depends on the host; this is not hermetic. | Get Python and pyarrow through Bazel: rules_python hermetic interpreter plus a `pip.parse` lock, `py_binary` comparator in `data`, path via `$(rlocationpath)`. Or replace the comparator with a Java Arrow reader from a Maven pool. Drop `--test_env=PATH`. |
| N5 | P1 | `datacube/BUILD.bazel:747-837`; `site/BUILD.bazel:27-35`; `query/BUILD.bazel:120-140`; `gates-run.yml:129-131,155-169` | Browser harnesses are `js_binary`s carrying `tags=["browser-ci"]`; CI loops `bazel run` over a tag query. Problems: (a) **`//site:verify` is tagged browser-ci but the query scope is `//datacube:* + //query:*`, so it never runs.** (b) Results are never cached and are not tests. (c) Chromium is installed outside Bazel into `~/.cache/ms-playwright` by `//datacube:install_browser --with-deps` (apt on the runner). (d) The step runs with `set -u` only, so a failed `bazel query` gives zero harnesses and a green step. This is a tag used as a policy carrier that CI interprets. | Make them `js_test` with `tags=["browser", "requires-network"]` as needed, fetch Chromium as a pinned `http_archive` (or rules_playwright), and group them in `test_suite(name="browser")`. CI then runs `bazel test //:browser`. |
| N6 | P2 | `tools/postgres/postgres.bzl:23-35`; `MODULE.bazel:109-111` | The `embedded_postgres` repository rule selects binaries with `rctx.os.name/arch`. The repo is host-shaped and ignores exec/target platforms: remote execution from a Mac would ship darwin binaries to Linux executors, and there is no cross-build. No windows/arm64 entry. The doc string names a non-existent `//testing:embedded_postgres`. | One `http_archive` per platform (lazy fetch) plus an `alias`/`select` on `@platforms//os`/`cpu` in a hub BUILD. Or `http_file` per platform with the extraction done as a build action. |
| N7 | P2 | `MODULE.bazel:290-313`; `third_party/rules_graalvm_command_line_tools.patch` | (a) GraalVM is fetched for the host OS (`graalvm_bindist.bzl` uses `ctx.os`). The lockfile shows `toolchain_gvm` registered with **empty** `exec_compatible_with`/`target_compatible_with`, so it claims every platform. (b) native-image links with the host C toolchain. The patch sends Macs without an Xcode Bazel knows down a different path (no `DEVELOPER_DIR`/`SDKROOT`/`MACOSX_DEPLOYMENT_TARGET`), so CI (Xcode) and the dev Mac (CLT only) produce differently configured actions. (c) The patch adds an `apple_common` use: `--incompatible_stop_exporting_language_modules` fails loading `//warehouse` at patched `rules.bzl:144,158` (verified). | Register per-platform GraalVM toolchains with constraints (or upstream a constrained toolchain). Upstream the CLT fix to rules_graalvm, or move to a BCR release, instead of carrying a patch. Track the `apple_common` removal. |
| N8 | P2 | `MODULE.bazel:343-352`; `warehouse/defs.bzl:42-47` | The DuckDB postgres extension is fetched over plain `http://extensions.duckdb.org`. The sha256 protects integrity, but corporate proxies or HSTS policies break the download. There is no Windows entry, and `POSTGRES_EXTENSION` has no `//conditions:default` and no `no_match_error` (masked only by `target_compatible_with`). | Use `https://`, add a mirror URL, add `no_match_error=` to the select, and define Windows explicitly (incompatible or an extension). |
| N9 | P2 | various BUILD files | rules_jvm_external `strict_visibility` is not set, so targets depend directly on **about 45 undeclared transitive artifacts**. Their versions come from transitive resolution, not the declared list. Examples: all 30 extra `@maven_upstream` labels in `tools/reference/BUILD.bazel:14-50`; jackson, antlr, `language_pure_compiler`, `protocol_pure`, `shared_core` in `parser-equivalence/BUILD.bazel:27-36`; `@maven_upstream//:org_postgresql_postgresql` (`pct/BUILD.bazel:161`); `@maven_teavm//:org_teavm_teavm_core` (`tools/teavm`, `wasm`); 5 in `tools/engine-runner`; `@maven_test` jupiter_api/params/archunit. | `strict_visibility = True` on each pool, and declare what targets use. |
| N10 | P2 | `tools/java_run/defs.bzl:25,46-63` | (a) `target.files` is the deprecated default-provider field. It fails with `--incompatible_disable_target_default_provider_fields` (verified: `gen_fixtures`, `gen_own_corpus_draft`, `gen_dynafn`, `gen_natives`). (b) java_run actions get no `-Duser.timezone`, `-Dfile.encoding`, `-Duser.language` or `-Duser.country`, whereas `junit_test` pins GMT. So `zone_jvm`, `lite_facts`, `catalog_*`, `offer_facts`, `cube_jvm_answers`, `jvm_answers` and spec's `gen_*` run with host TZ and locale; outputs can differ by machine, which is a remote-cache hazard. Only parser-equivalence passes `program_jvm_flags`. | Use `target[DefaultInfo].files`. Have java_run always prepend `-Duser.timezone=GMT -Dfile.encoding=UTF-8 -Duser.language=en -Duser.country=US`. |
| N11 | P2 | `warehouse/BUILD.bazel:60-61` vs `:34-35` | `server_lib` resources `glob(["src/main/resources/**"])` also packs `META-INF/services/java.sql.Driver` (`com.legend.warehouse.client.jdbc.WhDriver`), which is `:client`'s. The server jar and native image therefore carry a ServiceLoader entry whose class is not on their classpath, and `tests_lib` gets the file twice. | Exclude `META-INF/services/java.sql.Driver` from `server_lib`, or split resources per target. |
| N12 | P2 | `warehouse/BUILD.bazel:100-103,148-219`; `datacube/BUILD.bazel:592-595,723-726` | Platform `config_setting`s (`linux_x86_64`, `linux_aarch64`, `macos_arm64`, `macos_x86_64`) and `NOT_ON_WINDOWS` live in one app's BUILD; datacube re-inlines the Windows select twice. The selects (`duckdb_library`, `POSTGRES_EXTENSION`) have no Windows or default branch. | Create a `//tools/platforms` package with the config_settings and an `INCOMPATIBLE_WINDOWS` constant in a .bzl, and use `no_match_error`. |
| N13 | P2 | `core/BUILD.bazel:34-202`; `datacube/BUILD.bazel:134-596`; `pure-protocol/BUILD.bazel:26-34`; `query-store/BUILD.bazel:12-19` | Boilerplate. 27 core `java_library`s repeat `javacopts`/`plugins`/`visibility`; the comment "BUILD files allow no **kwargs" is the reason a macro is needed. The package default is public and then overridden private 27 times. About 10 datacube js_tests copy-paste the same `node_options`, `strict-reporter` wiring and wasm flags. pure-protocol and query-store reach `../datacube/test/strict-reporter.mjs` by a relative path. | A `legend_java_library` macro (NullAway built in) and a `node_test` macro under `//tools`, with the reporter as a label. Default core visibility private, public only on `:core`/`:drivers`/`:server`. |
| N14 | P2 | `base`, `json`, `core`, `warehouse` BUILD files: `package(default_visibility=public)`; `tools/nullaway/BUILD.bazel:6`; `third_party/*.BUILD:10`; `wasm/BUILD.bazel:49,63`; `tools/par/BUILD.bazel:10`; `tools/teavm/BUILD.bazel:9`; `tools/junit/BUILD.bazel:8`; `parser-equivalence/BUILD.bazel:252`; `docs/BUILD.bazel:9,20` | Public almost everywhere. `//tools/junit:junit` is not `testonly`. `//tools/par:par_generator` is neither testonly nor restricted, although `@maven_upstream` is declared "TEST ONLY" and `//pct:adapter_par` is testonly. `core_next_prelude` and `main_java` are public. | Default private, explicit `__pkg__` lists, and `testonly=True` on test infrastructure and on upstream-pool binaries. |
| N15 | P2 | `.bazelrc:32-36` vs `resources:memory:*` tags | Heap policy is split and contradictory. CI gives every JVM `-Xmx8g`; Bazel schedules by tags such as 1536 MB (`core_tests`, `spec_tests`, `corpus_duckdb`) under `HOST_RAM*.6`, so on 16 GB runners several 8 GB-capable JVMs pack together. CI-only `--jvmopt` also changes every java_test's launcher and action key, so CI and local can never share test cache hits. `--host_jvmopt=-Xmx3g` is silently beaten by `ref_resolutions`' `-Xmx12g` (target `jvm_flags` come last). | Derive `-Xmx` in `junit_test` from the same number as the tag (for example `memory_mb=` gives both the tag and `-Xmx`), so policy lives in BUILD files. Keep `.bazelrc` free of per-config jvmopts. |
| N16 | P2 | `.bazelrc:2,49`; `gates-run.yml:113-122` | `--disk_cache=~/.cache/bazel-disk` grows without bound; use `--experimental_disk_cache_gc_max_size`. `common --enable_bzlmod` is a no-op on Bazel 9. No `--lockfile_mode=error` for CI (default `update`). The actions/cache key is a hash of the lock files only, and an exact key hit never re-saves, so the disk cache freezes at its first save until a lock changes. | Add a GC size, `common:ci --lockfile_mode=error`, delete `--enable_bzlmod`, and key the cache on `github.sha` with restore-keys, or use a remote cache. |
| N17 | P2 | `gates-run.yml:146-152` | The `GENERATED` expansion is `bazel query … 2>/dev/null` inside a process substitution. If the query fails, zero diff tests run and the lane stays green. bazel_lib already creates the suites `//core:update_generated_tests`, `//datacube:…`, `//docs:…` and `//parser-equivalence:…`. | Add a root `test_suite(name="generated", tests=[those four suites])` and have CI name it. |
| N18 | P2 | see Section 4 | Committed generated files with no generator target and no diff test: `query/src/ui/icons.ts` (from react-icons 5.5.0, which is not in query's lock); 10 `core/src/test/resources/stress/*.pure` (`scripts/corpus/*.py`); `fixtures/saved-queries/*.json` (`make.mjs` against a live server); `spec/src/test/resources/reference-lane/core_relational.txt` and the other test-written goldens refreshed by `unzip …/test.outputs/outputs.zip` by hand; `scripts/parser/mutants.tsv`. | Generator targets plus `write_source_files`, or explicitly mark them hand-frozen like `link-p1.ts`. For test-written goldens, a `bazel run` update target reading `test.outputs`. |
| N19 | P2 | `MODULE.bazel:29-47`; `datacube/package.json` | `npm_translate_lock` never verifies the lock against `package.json` (`update_pnpm_lock` is off and no check runs), so a `package.json` edit without a relock is ignored silently. datacube has `"private": "true"` (a string, not a boolean) and caret ranges, while query pins exact versions. Lifecycle scripts are correctly disabled (`onlyBuiltDependencies: []`, which rules_js honours via `data`). | Add a CI step (`pnpm install --frozen-lockfile --lockfile-only` through `@pnpm//:pnpm`, or a diff test), and fix `private`. |
| N20 | P2 | `query-store/BUILD.bazel:52-65`; `tools/junit/defs.bzl:34` | `lite_test` is `size="small"` (60 s) but boots `//core:server` (a JVM) and binds a port. `junit_test` defaults every JVM test to `large` (900 s) regardless of measured time, so sizes carry no information. | `lite_test` should be `medium`, and sizes or timeouts set from measured times. |
| N21 | P2 | `MODULE.bazel:323-337` | The upstream sources come from GitHub's auto-generated `archive/refs/tags/*.tar.gz`, whose bytes have changed in the past (checksum churn), with no mirror. | Add a mirror URL (or a release asset) and switch to `integrity=`. |
| N22 | P2 | third-party | Bazel 10 readiness (each run separately with `--nobuild //...`). Pass: `auto_exec_groups`, `check_testonly_for_output_files`, `config_setting_private_default_visibility`, `no_implicit_file_export`, `resolve_select_keys_eagerly`, `disable_starlark_host_transitions`, `check_external_repo_source_dir_package_boundary`, `disable_non_executable_java_binary`. Fail: `disable_target_default_provider_fields` (bazel_lib `write_source_file.bzl:442` and our `tools/java_run/defs.bzl:25`); `stop_exporting_build_file_path` (bazel_lib `diff_test.bzl:89`); `no_rule_outputs_param` (rules_java `java_info.bzl`); `noincompatible_enable_deprecated_label_apis` (rules_license); `stop_exporting_language_modules` (rules_graalvm and its patch). | Fix `java_run` (N10) and track upgrades of bazel_lib, rules_java and rules_graalvm. |
| N23 | P2 | `BUILD.bazel:6-15`; `MODULE.bazel:57-61` | Root `exports_files` omits `maven_warehouse_install.json`, and that pool is also absent from `pools_are_disjoint`. The MODULE.bazel comment "Every artifact belongs to exactly ONE pool" is false (see K17). | Generate the export list and the pool list from a single constant in a .bzl. |

## Section 2 — KNOWN items re-confirmed, with added evidence

- **K8** `warehouse/defs.bzl:53-59` runs `gzip -dc` through `run_shell` (needs host gzip and sh). `:69-81` writes a `#!/usr/bin/env bash` launcher that `cd`s to `BUILD_WORKING_DIRECTORY`. Both consumers (`//warehouse:serve`, `//datacube:app`) are Windows-incompatible only because of this.
- **K15** Coarse data globs:
  - `_CORE_READS = glob(["src/**"])` is data for every core lane, so any core source edit reruns every lane.
  - pct lanes take `glob(["src/**"])`.
  - The upstream trees are passed whole: `@legend_engine_src//:tree` is 12,900 files and `@legend_pure_src//:tree` is 2,693. They are direct inputs of 15 targets (6 java_run generators and 9 tests), on top of `exports_files(glob(["**"]))`.
  - `//docs:ledgers = glob(["*.tsv"])` includes historical snapshots (`NATIVE_CLAIMS_CENSUS_2026_09_10.tsv` and others), so editing one reruns `parser_parity`.
- **K16** `pct/BUILD.bazel:21-29`: `$(location)`, `$$(dirname …)` (needs bash, including on Windows), and an exec-config `java_binary` (`par_generator`, which also gets CI's `--host_jvmopt=-Xmx3g`). `tools/reference/BUILD.bazel:57-65`: `2> /dev/null` swallows errors, and `-Xmx12g` sits in the tool's `jvm_flags`. These are the only 2 genrules. There are no `sh_*` or `native_binary` targets.
- **K17** Measured over all 8 lock files: 661 distinct coordinates, **396 appear in more than one pool**.
  - 383 are shared runner+upstream, 7 runner+tools+upstream.
  - `duckdb_jdbc` is in 3 pools: core 1.4.4.0, runner 1.3.0.0, warehouse 1.5.5.1.
  - `h2` is in 3 pools: core 2.1.214, h2_modern 2.4.240, runner 2.2.224.
  - `guava` is in 3: tools 31.1-jre, runner/upstream 33.4.6-jre.
  - `slf4j-api` is in test 2.0.12 and upstream 1.7.36; the test allowlists it.
  - The test reads 4 of 8 pools and parses the JSON with a regex. The real invariant is "no java_test runtime classpath holds two versions of one coordinate", which an aspect over `JavaInfo` should check instead.
- **K18** Re-confirmed:
  - `_GENERATOR_SRCS` lives in `spec/src/test/java` but feeds the non-testonly `:generators` and `gen_*`.
  - `"core/src/main/java"` is hard-coded at `spec/BUILD.bazel:291` and `:330`.
  - `corpus_lanes` and `judge_lanes` are identical (`spec/BUILD.bazel:165-182`).
  - `postgres_live` and `postgres_live_native` are manual.
  - `corpus_warehouse` and `scale_*` are in no lane.
  - `use_testrunner=False` has extra consequences. `JUnitMain` ignores `TESTBRIDGE_TEST_ONLY` (so `--test_filter` does nothing) and the `TEST_SHARD_*` variables (so no sharding). It writes JUnit XML to `$TEST_UNDECLARED_OUTPUTS_DIR/junit`, not `$XML_OUTPUT_FILE`, so the `bazel-testlogs/**/test.xml` CI uploads is Bazel's one-case synthetic file.
  - `-Dlegend.*.root="../"+Label().repo_name` (`tools/junit/defs.bzl:20-23`) hand-computes a canonical-name runfiles path, while pct uses `$(rlocationpath)`; the two styles are inconsistent. `Repo` resolves through `TEST_SRCDIR`, not the runfiles library.

## Section 3 — Every test target

Lane keys are from `.github/workflows/gates-run.yml` (L = Linux, M = macOS low-memory, W = Windows). "diag" is `diagnostics.yml`.

| Target | Kind | Size | Tags | CI lane | Hermetic? |
|---|---|---|---|---|---|
| //core:core_tests | java_test | enormous | resources:memory:1536 | 1 (LMW) | yes (declared src/**; child `java` from java.home) |
| //core:guardrails | java_test | medium | – | checks | yes |
| //core:census | java_test | medium | – | checks | yes (reads 4 modules, declared) |
| //core:stress_suites | java_test | enormous | resources:memory:2048 | 10 | yes |
| //core:scale_{profilebuildcost,stresstest100k,stresstest10k,stresstestchaotic,stresstestcomplexqueries,stresstestdense} | java_test | enormous | manual | none | yes |
| //core:update_generated_{0..5}_test | _diff_test | small | – | checks | yes |
| //datacube: 105 js_tests in `:tests` (adhoc_mode, adhoc_query, adhoc_session, adhoc_state, adhoc_transactions, app, apply_refusal, board, bundle_budget, calc_fix, calc, cancel, catalog_model, chart_echarts, chart_tiles, child_groups, column_editor, column_kind, columns_panel, columns_selector, config_readers, config, cube_adhoc_shell, cube_document, cube_editors_app, cube_library, cube_lifecycle, cube_state, cube_store, cube_transactions, dimensions, drill, duckdb_cancel, duckdb, editor, editors_live, engine_remote, epoch, escaping, export_doc, export_model, export_rich, export, filter_editor, form, format_scale, format, fuzz, grid_basics, grid_dom, grid_resize, grid, group_derived, guardrails, host, infer, json_shape, menu_ids, menu, menu_view, multi_cube, offer_facts, page_document, pivot_panel, pivot_total, pivot_values, plane, planner, portability, query, relation_type, remote, runner, sample, save_dialog, saved_queries, scale, screen_colours, selection, share_link, shell, snap, sorting, source_picker, state_guardrail, style, tile_layout, tree, treeview, type_columns, undo_coverage, upload, values, warehouse_session, wasm_planner, window_columns, window, typecheck, wasm_differential; all `_test`) | js_test | small | – | app (LMW) | yes (pinned Node, jsdom) |
| //datacube:{pivot_rows,json_read,cube_open,typed_values,typed_values_tokyo,typed_values_new_york}_test (also in `:tests`) | js_test | medium | – | app | yes |
| //datacube:live_snap_test | js_test | medium | – (Windows-incompatible) | browser (L only) | mostly (native image built with host CC/xcrun) |
| //datacube:update_generated_{0..3}_test | _diff_test | small | – | checks | yes |
| //docs:update_generated_test | _diff_test | small | – | checks | yes |
| //json:tests | java_test | small | – | **none** | yes |
| //parser-equivalence:parser_parity | java_test | large | resources:memory:3072 | 8 | yes |
| //parser-equivalence:diagnostics | java_test | large | manual, resources:memory:5120 | diag (L, path-triggered) | yes |
| //parser-equivalence:update_generated_{0,1}_test | _diff_test | small | – | checks | yes |
| //pct:pct_duckdb | java_test | large | resources:memory:9216 | 6 (L, W) | yes |
| //pct:pct_duckdb_{essential,grammar,relation,standard,unclassified} | java_test | large | manual, resources:memory:4096 | 6 (M) | yes |
| //pct:pct_discipline | java_test | small | manual | 6 (M) | yes |
| //pct:pct_h2 | java_test | large | resources:memory:4608 | 7 | yes |
| //pct:pct_postgres | java_test | large | resources:memory:9216 | 7p (L, W) | locally yes; host-keyed repo (N6) |
| //pct:pct_postgres_{essential,grammar,relation,standard,unclassified} | java_test | large | manual, resources:memory:4096 | 7p (M) | same as above |
| //pct:pct_channel_b | java_test | large | resources:memory:1024 | 9 | yes |
| //pure-protocol:twins_test | js_test | small | – | **none** | yes |
| //query:{build,load,saved_queries,typecheck}_test | js_test | small | – | **none** | yes |
| //query-store:lite_test | js_test | small (too small, N20) | – | **none** | yes (spawns //core:server, loopback) |
| //query-store:{local,share}_test | js_test | small | – | **none** | yes |
| //spec:spec_tests | java_test | large | resources:memory:1536 | 3 | yes |
| //spec:corpus_duckdb | java_test | large | resources:memory:1536 | 4 | yes |
| //spec:corpus_h2 | java_test | large | resources:memory:5120 | 5 | yes |
| //spec:corpus_warehouse | java_test | large | manual, resources:memory:1536 | none | yes (child JVM from java.home) |
| //spec:reference_lane | java_test | large | manual, resources:memory:8192 | none | yes (8 GB) |
| //tools/deps:{core_closure_test,core_layering_test,one_release,pools_are_disjoint,warehouse_closure_test} | java_test | small | – | checks | yes |
| //warehouse:tests | java_test | medium | – | app (LMW) | **no**: host python/pyarrow on PATH (N4) |
| //warehouse:tests_native | java_test | medium | – (Windows-incompatible) | native (L, M) | **no**: same python dependency, plus host-CC native image |
| //warehouse:postgres_live | java_test | medium | manual | none | **no**: external DB via `--test_env` |
| //warehouse:postgres_live_native | java_test | medium | manual (Windows-incompatible) | none | **no**: external DB |
| //wasm:{differential,zone}_test | js_test | small | – | app | yes (zone_jvm runs with host TZ, N10) |

Browser-ci binaries run via `bazel run` (Linux only): the 10 `//datacube` harnesses and `//query:verify`. `//site:verify` is tagged but never run (N5).

**Totals:** 171 tests, 22 manual, 149 non-manual. 140 of the 149 are in some lane; the 9 marked "none" in bold are in no lane. 3 tests are incompatible on Windows.

**Green-ability from a clean checkout (`bazel test //...`):**
- Every platform: analysis passes (macOS host verified). It needs network for maven, npm, GitHub archives, http DuckDB extensions, zonky jars, GraalVM and Node.
- macOS additionally needs Xcode or the Command Line Tools (native image).
- Linux additionally needs host gcc and zlib headers.
- Windows additionally needs bash for `//pct:adapter_par` and admin or developer mode for symlinks.
- The warehouse tests skip locally without pyarrow.

## Section 4 — Committed generated files and their coverage

| File | Generator | Covered by write_source_files / diff test? |
|---|---|---|
| `core/.../builtin/DynaFn.java` | //spec:gen_dynafn | yes, //core:update_generated |
| `core/.../builtin/Pure.java` | //spec:gen_natives | yes |
| `core/.../compiler/NameResolver.java` | //spec:gen_imports | yes |
| `core/.../builtin/engine-handlers.tsv` | //spec:gen_engine_handlers | yes |
| `core/.../builtin/native-claims.tsv` | //spec:gen_claims | yes |
| `core/.../builtin/prelude.pure` | //spec:gen_prelude | yes |
| `core/.../builtin/native-membership.tsv` | hand-owned (declared) | n/a |
| `datacube/src/generated/{lite-facts,offer-facts,catalog-facts}.ts` | :lite_facts, :offer_facts, :catalog_rules | yes, //datacube:update_generated |
| `datacube/test/generated/catalog-corpus.ts` | :catalog_corpus | yes |
| `datacube/src/share/link-p1.ts` | :make_link_dictionary, frozen by design (hash-pinned) | intentionally no |
| `docs/protocol-roster.tsv` | //parser-equivalence:gen_roster | yes, //docs:update_generated |
| `docs/own-corpus-protocol-diffs.tsv` | :gen_own_corpus_draft (draft; person-owned) | `diff_test=False`, by design |
| `parser-equivalence/.../corpus-manifest.tsv` | :gen_manifest | yes |
| `parser-equivalence/.../engine-grammar-fixtures.jsonl` | :gen_fixtures | yes |
| `query/src/ui/icons.ts` | `query/tools/icons.mjs` (react-icons 5.5.0, not locked) | **no** |
| `core/src/test/resources/stress/{59,60,64,92..98}*.pure` (10 files) | `scripts/corpus/{build,combos,dense_mapping,dense_store,hier}.py` | **no** |
| `fixtures/saved-queries/*.json` (4 files) | `fixtures/saved-queries/make.mjs` against a live server | **no** |
| `spec/src/test/resources/reference-lane/core_relational.txt` (and other test-written goldens; ~25 test classes write via `Repo.out`) | ReferenceLaneTest output, unzipped by hand | **no** |
| `scripts/parser/mutants.tsv` | `scripts/parser/mutants.py` | **no** (not a Bazel input) |
| `docs/NATIVE_CLAIMS_CENSUS_2026_09_10.tsv`, `docs/type-audit-2026-08/data/*.tsv` | historical tools (/tmp paths) | **no** (snapshots, but the top-level one is in //docs:ledgers) |
| `maven_*_install.json` (8) | rules_jvm_external pin | not enforced (N2) |
| `MODULE.bazel.lock` | Bazel | current; not enforced in CI (N16) |
| `datacube/pnpm-lock.yaml`, `query/pnpm-lock.yaml` | pnpm via @pnpm | lockfileVersion '9.0', autoInstallPeers true; not verified against `package.json` (N19) |

## Section 5 — Coverage statement

Read every line of:
- `MODULE.bazel`, `BUILD.bazel` (root), `.bazelrc`, `.bazelversion` (9.2.0), `.bazelignore`.
- All 32 non-experiments `BUILD.bazel` files. `tools/generators` and `tools/postgres` are 0 bytes; `tools/jars`, `tools/java_run` and `third_party` hold comments only.
- `third_party/legend_engine_src.BUILD`, `third_party/legend_pure_src.BUILD` and the rules_graalvm patch.
- All 8 non-experiments .bzl files: `tools/{generators,jars,java_run,junit,nullaway,teavm}/defs.bzl`, `tools/postgres/postgres.bzl`, `warehouse/defs.bzl`.
- `tools/junit/JUnitMain.java`, `tools/deps/PoolsAreDisjointTest.java`, the relevant parts of `Bump.java`, `Repo.java`, `EmbeddedPostgres.java` and `WarehouseArrowTest.java`.
- All 3 workflows.

Checked headers and consistency, not every artifact:
- `MODULE.bazel.lock`: lockFileVersion 28; extension entries for telemetry (records `ASPECT_TOOLS_TELEMETRY_TEST`), graalvm, rules_android, rules_python and yq; no yanked versions.
- All 8 Maven locks: version 3, Maven Central only, artifact counts 3/1/610/29/15/10/398/1.
- Both pnpm-lock headers and all 4 `package.json` files.

Not done, per the instructions: no `bazel test` or `bazel run`, so runtime determinism (TeaVM output, native-image reproducibility) was not measured, and per-test durations were judged against size only from reading the code. `experiments/` was excluded, as it is .bazelignored.
