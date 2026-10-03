# End-state deletion ledger: what non-Bazel orchestration survives the whole workplan?

Audited tree: `origin/main` @ `23b441852` plus the plan and evidence commits up to `dee6897e0` (those commits add only `.md` files, so the code and BUILD lines below are the `23b441852` lines that the workplan cites). Workplan: `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md`, with all 16 decisions as recorded on 2026-10-03. Read-only inventory: nothing was built or run.

## 1. The answer

**NO.** When every item of the workplan has landed, all of *our own* shell is gone: no genrule, no `run_shell`, no bash launcher, no `.sh` file outside `docs/history/` and `experiments/`, no `jq`/`pip`/`curl`/`bazel query` loop in CI, no test that starts a JVM of itself, and no hand-rolled `TEST_SRCDIR` parsing. But "no non-Bazel orchestration at all" is not the end state. These remain:

**Principled exceptions (class c). Each needs an exact allowlist row (section 4).**

1. **Tests that start a Bazel-built binary from runfiles.** `EmbeddedPostgres` (initdb, pg_ctl, postgres), `TestServer` and `DuckWorkspaces` (the native warehouse), `ServeTest` (renamed from `LauncherTest`; `//warehouse:serve`), the new `//warehouse:dist_test`, `WarehouseArrowTest` (a `py_binary`), the shared `harness.mjs startServer` (query, site, app and torture harnesses), `live-snap.ts`, and `query-store/test/lite.test.ts`.
2. **Python programs that start `//tools/engine-runner:testable`**, kept by decision D3 rows 1, 2 and 5: `scripts/corpus/run.py` (the manual `engine_stress` test), the 15 repaired `probe_*.py`, and `scripts/projects/{check,loadtime}.py` (`check.py` goes when P3-23 lands). This is still Python starting Java. It is allowed because the binary comes from runfiles, but **guard G5 as specified cannot see it** (gap G-1).
3. **Release and developer tools under `bazel run` that call host programs:** `tools/bump/Bump.java` (host `git ls-remote`, three nested `bazel` invocations, `git status`/`diff`), `tools/untangle/move_classes.py` (`git mv`), and `tools/upstream-drift.py` (host `curl` and `git`, partial P-6).
4. **Product code that opens the user's browser:** `WarehouseServer.openBrowser` (`open`, `rundll32`, `xdg-open`) and the dev server `datacube/demo/serve.mjs --open`.
5. **Host prerequisites forced by a licence or by third-party rules:** the macOS CLT SDK files (D1), Windows MSVC (D5), `libxml2.so.2` if no clean `lld` exists (D7), and **bash**: Git for Windows' bash (`.bazelrc:35-36`, kept for rules_jvm_external and rules_js by P5-07), plus `/bin/bash` on Unix for the launchers and update scripts that rules_java, rules_js, bazel_lib (`write_source_files`, `diff_test`) and possibly rules_python generate. **The workplan never declares the Unix half** (gap G-9).
6. **History, kept inert:** 93 scripts move to `docs/history/` (D16), including 3 `.sh` files and 14 Java probes. `experiments/` stays bazelignored by decision: 17 unbuilt scripts, 4 of them `.sh`, with Docker and host DuckDB use (listed in section 2, category X).
7. **Bazel's own disk cache in `$HOME`** (`.bazelrc:62`, and the CI cache paths).

**Remnants no item removes yet (classes d and e, section 3).** The largest:

- **Gaps in the guards themselves.**
  - G5 has no Python patterns and an allowlist that misses 9 real spawn sites.
  - G15's regex misses `new URL('..', import.meta.url)` (no trailing slash), the form 20 harnesses use.
  - G9 reads only `.md` files, and the rule P7-09 uses to pick history docs misses 22 undated ones.
- **An internal contradiction.** P1-04 says generator actions are unaffected, but P3-32 deletes `Upstream.java` and Repo's action mode, which the generators read through `tools/generators/defs.bzl:25-40`.
- **Two host tools.** `taskkill` in `query-store/test/lite.test.ts:57` (P4-07 is silent on the copy in `verify-app.mjs:155`), and `tzutil` in CI, which P5-03 only investigates.
- **One CI shell step nobody owns:** `git config` at `gates-run.yml:105-109`.
- **`Repo.out`/`outDir`/`rel`.** 82 call sites keep `testing/Repo.java` alive after P3-32.
- **D3 row 6 has no item.** `scripts/parser/keywords.py` and `tiers.py` are "keep and repair", but neither P2-18 nor P7-03 wires them.

**Counts (section 2 rows):**

| Class | Rows |
|---|---|
| (a) deleted | 31 |
| (b) converted | 88 |
| (c) allowed exception | 39 |
| (d) gap | 8 |
| (e) partial | 19 |
| **Total** | **185** |

The rows group instances by site, and every instance is listed in its row. The 170 unwired scripts are covered by rows U-01 to U-30. Section 3 turns the 27 d and e rows, plus three guard-only findings, into **11 gap items (G-1 to G-11) and 6 itemised partials (P-1 to P-6)**, with the remaining partials closed by their own items.

## 2. The ledger

Categories:

| # | Category |
|---|---|
| 1 | Process spawning |
| 2 | Shell inside the build |
| 3 | Tracked scripts no target runs |
| 4 | CI logic beyond `bazel` on labels |
| 5 | Hand-rolled repository or runfiles resolution |
| 6 | Writes into the tree, or reads host state |
| 7 | Hand recipes in current docs |
| 8 | Host prerequisites |
| 9 | Orchestrators |
| X | `experiments/` (out of scope) |

Classes: **a** deleted · **b** converted to a Bazel action, target or test · **c** allowed exception · **d** gap · **e** partial.

### Category 1: process spawning

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| S-01 | `tools/junit/JUnitMain.java:96` (prerun `:31-37, 47-54, 77-106`) | the test runner starts a second JVM of itself for the corpus "prerun" pass | 1, 9 | a | P3-01 | replaced by the `//spec:judge_host` `java_run` |
| S-02 | `core/src/test/java/com/legend/PlannerRunsOnJavaBaseTest.java:30` | child JVM from `java.home` with `--limit-modules java.base` | 1, 9 | b | P3-19 | becomes `java_test(main_class = PlanOnJavaBase, jvm_flags = --limit-modules)`; the one G16 exception |
| S-03 | `spec/src/test/java/com/legend/rcorpus/DuckWorkspaces.java:139-146` | child `java -jar` warehouse, `java.home` launcher | 1, 9 | b | P3-19 | the remaining spawn of `//warehouse:server_native` from runfiles is allowed; listed in G5 |
| S-04 | `testing/src/main/java/com/legend/testing/EmbeddedPostgres.java:132` | runs initdb, pg_ctl and postgres from `@embedded_postgres` | 1 | c | P1-06, P3-20 | test fixture over a Bazel-fetched binary; already in G5's list |
| S-05 | `warehouse/src/test/java/com/legend/warehouse/TestServer.java:102` | starts `WAREHOUSE_BINARY` (the native server) | 1 | c | P1-05 (env to `rlocationpath`) | **missing from G5's allowlist** (P6-05) |
| S-06 | `warehouse/src/test/java/com/legend/warehouse/launcher/LauncherTest.java:120` | starts the launcher script or exe with a hand-set `RUNFILES_DIR` | 1 | b | P4-12 (`ServeTest`, `Runfile.env()`) | the remaining spawn of `//warehouse:serve` is **missing from G5's allowlist** |
| S-07 | `warehouse/src/test/java/com/legend/warehouse/WarehouseArrowTest.java:125` | host `python3` plus a repository-relative script path | 1, 6 | b | P1-08 | becomes a `py_binary` from runfiles; in G5's list |
| S-08 | `warehouse/src/test/java/com/legend/warehouse/WarehouseArrowTest.java:143` | probes candidate interpreters with `python3 -c "import pyarrow"` | 1, 6 | a | P1-08 | |
| S-09 | `warehouse/src/main/java/com/legend/warehouse/server/WarehouseServer.java:796-800` | product `--open`: `open`, `rundll32` or `xdg-open` | 1, 6 | c | — | product behaviour for the user; **missing from G5's allowlist** |
| S-10 | `tools/bump/Bump.java:223, 225, 259` (`git ls-remote`), `:131, 140, 145, 268-271` (nested `bazel`), `:135, 151-152, 281-283` (`git status`/`diff`) | release tool: orchestrates repin, regeneration and test through nested `bazel` and host `git` | 1, 9 | c | P0-05, P2-10 | `bazel run` release tool, in G5's list. Optional cleaner form in section 3 (O-1) |
| S-11 | `datacube/demo/install-browser.mjs:7, 14` | `spawnSync(node, [playwright cli, 'install', 'chromium'])` | 1, 6 | a | P4-09 | |
| S-12 | `datacube/demo/serve.mjs:20, 162-164` | dev server `--open` through `execFileSync(open/start/xdg-open)` | 1, 6 | c | P4-08 | dev tool; **missing from G5**. Also a latent bug: `start` is a `cmd` builtin, so `execFileSync('start')` fails on Windows |
| S-13 | `datacube/demo/verify-app.mjs:11, 41` | spawns the serve launcher from runfiles arithmetic | 1, 5 | b | P4-07 | goes through `harness.mjs startServer` |
| S-14 | `datacube/demo/verify-app.mjs:155` | `spawnSync('taskkill', ['/pid', …, '/t', '/f'])` | 1, 6 | e | P4-07 (silent) | host Windows tool; see G-4 |
| S-15 | `datacube/test/live-snap/live-snap.ts:25, 88` | spawns the native warehouse | 1 | c | P1-24, P3-10 | in G5's list ("the live-snap fixture") |
| S-16 | `query-store/test/lite.test.ts:4, 32` | spawns `//core:server` | 1 | c | P1-24, P3-16 | **missing from G5's allowlist** |
| S-17 | `query-store/test/lite.test.ts:57` | `taskkill /t /f` to kill the launcher's process tree on Windows | 1, 6 | d | — | no item; see G-4 |
| S-18 | `query/demo/verify.mjs:12, 34, 47` | spawns the core server and the native warehouse | 1 | b | P4-05 | goes through `harness.mjs startServer` |
| S-19 | `scripts/corpus/run.py:122` | `subprocess.run([~/jdk java, -cp cp.txt, perf.TestableMain …])` | 1, 6, 9 | b | P3-25 | remaining Python spawn of `//tools/engine-runner:testable`; **G5 does not scan Python** (G-1) |
| S-20 | `scripts/corpus/probe_aggregates.py:207`, `probe_boundary_navigation.py:332`, `probe_collection.py:243`, `probe_column_types.py:160`, `probe_derived_filter.py:174`, `probe_extends_filter.py:227`, `probe_functions.py:346`, `probe_graphfetch_included_mapping.py:116`, `probe_ineq_aggregate.py:327`, `probe_milestoned_join.py:165`, `probe_missing_setid.py:108`, `probe_project_deps.py:142`, `probe_qualified_broken_chain.py:184`, `probe_relation.py:339`, `probe_tds.py:190` | 15 probes, each with its own `subprocess.run` of host java, `cp.txt` and `target/classes` | 1, 6, 9 | b | P7-03 (D3 row 2) | each probe keeps its own spawn; G5 is blind to it (G-1). Proposed: route all through one `run.launch()` |
| S-21 | `scripts/corpus/probe_remaining.py:255` | the same, uncited | 1 | c | P7-04 (history) | |
| S-22 | `scripts/projects/check.py:119`, `scripts/projects/loadtime.py:58` | host java runs of `TestableMain` | 1, 9 | b | P7-03; `check.py` then deleted by P3-23 | Python spawn, as S-19 |
| S-23 | `scripts/census_gate.py:64`, `scripts/corpus/coverage.py:56`, `scripts/corpus/mutate.py:283` | Maven or host java runs | 1, 9 | a | P7-05 | |
| S-24 | `scripts/parser/fixtures.py:68`, `scripts/parser/mutants.py:207`, `scripts/parser/parity.py:52` | host java runs | 1 | c | P7-04 (history) | |
| S-25 | `scripts/walldepth.py:8, 14`, `tools/scoreboard.py:90` | host `git log`, `git show`, `git rev-parse` | 1, 6 | c | P7-04 (history) | |
| S-26 | `docs/invention-audit-2026-08-14/probes/final.py:9` | `subprocess.run(f"grep -rn …")`, a shell string | 1 | c | P7-04 (history) | |
| S-27 | `tools/untangle/move_classes.py:227` | host `git -C repo mv` | 1, 6 | c | P7-02 (D3 row 9) | dev codemod under `bazel run`; **needs a G5 row** once G5 scans Python |
| S-28 | `tools/upstream-drift.py:54` (`curl`), `:79` (`git ls-tree`) | host curl to Maven Central and the GitHub API; host git on a checkout | 1, 6 | e | P7-03 | P7-03 makes the checkout optional, but curl stays; see P-6 |
| S-29 | `core/src/test/java/com/legend/ErrorShapeGuardrailTest.java:279`, `datacube/test/portability.test.ts:85-86` | spawn patterns inside a guard's own regex or message text | 1 | c | — | G5 false positives; need allowlist rows |

### Category 2: shell inside the build

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| B-01 | `pct/BUILD.bazel:21-29` | genrule with `$$(dirname …)` around an exec-configuration `java_binary` | 2 | b | P1-22 | becomes a `java_run` |
| B-02 | `tools/reference/BUILD.bazel:57-65` | genrule, `-Xmx12g`, `2> /dev/null` | 2 | b | P1-22 | |
| B-03 | `warehouse/defs.bzl:62-80` | `ctx.actions.run_shell` with host `gzip` | 2, 6 | a | P1-17, P4-12 | replaced by the `Gunzip` `java_run` |
| B-04 | `warehouse/defs.bzl:82-126` (script `:91-112`) | generated `#!/usr/bin/env bash` launcher: `RUNFILES_DIR`, `_main`, `cd BUILD_WORKING_DIRECTORY` | 2, 5 | a | P4-12 | |
| B-05 | `warehouse/defs.bzl:11, 180-187`; `MODULE.bazel:315-319` | the `hermetic_launcher` Windows stub and its 10-argument guard | 2 | a | P4-12 | |
| B-06 | `.bazelrc:24-36` (`common:windows --repo_env=BAZEL_SH`, `build:windows --shell_executable`) | names Git for Windows' bash | 2, 8 | e | P5-07 | expected to stay for rules_jvm_external and rules_js; it becomes a recorded exception only after the aquery measurement |
| B-07 | third-party generated shell, not in our tree: the rules_java `java_binary`/`java_test` stub (Unix), rules_js `js_binary`/`js_test` launchers, bazel_lib `write_source_files` update scripts and `diff_test`, rules_python bootstrap (if the script bootstrap), rules_jvm_external fetch and pin | shell run by the build on every platform | 2, 8 | e | P5-07 (Windows only) | not ours, so allowed, but the workplan names it only in S2's risks and in P5-07's Windows count; see G-9 |
| B-08 | `datacube/BUILD.bazel:234-237` | recipe comment `bazel run … > $PWD/datacube/src/share/link-p2.ts`: a shell redirect writes into the tree | 2, 7 | a | P2-08 | |
| B-09 | `datacube/BUILD.bazel:722-735` | `js_run_binary` `dist` around the hand packager `make-dist.mjs` | 2 | a | P4-11 | |

### Category 3: tracked scripts no target runs

This covers all 170 unwired scripts outside `experiments/` (`script-review.md` rows; unchanged since `23b441852`), plus the "wired only as data" scripts. The decisions applied are D3/D3b as recorded.

| ID | Files | Cat | Class | Item | Notes |
|---|---|---|---|---|---|
| U-01 | `scripts/corpus/{build,aggregate,aggregates,battery,combos,deepstack,density,emit,executed,expand,flat,functest,graphs,hier,model,oracle,partition,quarantine,query,rhs,seed,spread,stacking,stacks,taxonomy,tomany,views}.py` (27) | 3 | b | P2-01, P2-04 (density/executed/stacking gates) | |
| U-02 | `scripts/corpus/dense_mapping.py`, `dense_store.py` | 3 | b | P2-01 | |
| U-03 | `scripts/corpus/differential.py` | 3 | b | P3-18 | |
| U-04 | `scripts/corpus/run.py` | 3 | b | P3-25 | |
| U-05 | 15 probes: `scripts/corpus/probe_{aggregates,boundary_navigation,collection,column_types,derived_filter,extends_filter,functions,graphfetch_included_mapping,ineq_aggregate,milestoned_join,missing_setid,project_deps,qualified_broken_chain,relation,tds}.py` | 3 | b | P7-03 | |
| U-06 | `scripts/corpus/probe_remaining.py` | 3 | c | P7-04 | history |
| U-07 | `scripts/corpus/functions.py` | 3 | b | P7-02 | |
| U-08 | `scripts/corpus/scoreboard.py` | 3 | b | P2-04 | |
| U-09 | `scripts/corpus/{add_taxonomy_edges,refdata,taxa_exotics,taxa_infra,taxa_markets,taxa_more,taxa_ops}.py` (7) | 3 | c | P7-04 | history |
| U-10 | `scripts/corpus/{brokerage,curves,curves2,largeexp,schedule,timeseries}.py` (6), `coverage.py`, `mutate.py` | 3 | a | P7-05 | |
| U-11 | `scripts/census_gate.py`, `scripts/generate_pure_constants.py` | 3 | a | P7-05 | |
| U-12 | `scripts/outstanding.py`, `scripts/walldepth.py` | 3 | c | P7-04 | history; both hard-code a home path |
| U-13 | `scripts/parser/fixtures.py`, `mutants.py`, `parity.py` | 3 | c | P7-04 | history |
| U-14 | `scripts/parser/keywords.py:35` (reads `Path.home()/legend/legend-engine`), `scripts/parser/tiers.py` | 3, 6 | e | P2-18 (`vocab.tsv` only) | D3 row 6 says keep and repair; no item wires the scripts. See P-3 |
| U-15 | `scripts/projects/check.py`, `loadtime.py` | 3 | b | P7-03 (`check.py` interim, then deleted by P3-23) | |
| U-16 | `scripts/projects/spec.py` | 3 | b | P7-02 | |
| U-17 | `tools/census/lanes.sh` (runs 8 lanes with `bazel test`, copies logs to `runs/census`), `tools/census/render.sh` (`bazel build` plus a jar run; macOS arm64 only) | 3, 9 | a | P3-13 | |
| U-18 | `tools/census/lanes_diff.py` | 3 | b | P3-13, P7-02 | |
| U-19 | `tools/wrongrows/compare.py`, `damage.py` | 3 | b | P7-02, P3-25 | |
| U-20 | `tools/wrongrows/engine-rows.sh` | 3, 9 | a | P3-25 | |
| U-21 | `tools/untangle/{bare_tiers,probe_counts,move_classes}.py` | 3 | b | P7-02 | |
| U-22 | `tools/reference/join.py`, `source_drift.py`; `tools/metamodel-census/{build,closure,props,scan2,scan3}.py`; `tools/spikes/fusion_{probes,spike,spike2}_2026_08_28.py`; `tools/golden_shape_survey.py`; `tools/scoreboard.py` | 3 | c | P7-04 | history (12 files) |
| U-23 | `tools/ci-watch.sh` | 3 | a | P7-05 | |
| U-24 | `tools/native-axes.py`, `tools/upstream-drift.py` | 3 | b | P7-03 | `upstream-drift.py` also has row S-28 |
| U-25 | `fixtures/saved-queries/make.mjs`; `query/tools/icons.mjs` | 3 | b | P2-06; P2-07 | |
| U-26 | `docs/burndown-2026-08-14/tools/*.py` (8); `docs/invention-audit-2026-08-14/probes/*` (9 `.java`, 7 `.py`); `docs/parser-audit-2026-08-14/probes/*.java` (5); `docs/parked/*` (3) | 3 | c | P7-04 | history |
| U-27 | `docs/type-audit-2026-08/harness/{Probe.java,jrun.sh,probe.sh,setup.sh}` | 3 | c | P7-04 | history; **3 of the repository's 7 non-experiment `.sh` files** |
| U-28 | `docs/datacube-dashboards-homework-2026-09-28/**` (32 `.js`/`.mjs`/`.ts`) | 3 | c | P7-04 | history |
| U-29 | `datacube/bench/{cell-budget,snap-ceiling,wasm-penalty}.mjs`; `datacube/bench/model/*.py` (8) | 3 | b | P4-15 | today "wired" only as data of `portability`'s glob |
| U-30 | `datacube/package.json` `scripts` (`test`, `typecheck`); `query/test/strict-reporter.mjs` (byte-identical copy) | 3 | a | P4-15; P1-23 | |

### Category 4: CI logic beyond `bazel` on labels

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| C-01 | `.github/workflows/gate.yml:57-60` | `curl … actionlint … \| tar xz` with no checksum | 4 | b | P5-05 | |
| C-02 | `gate.yml:23-29` | `paths-ignore` for `**/*.md` and `progress*.txt` | 4 | a | P5-04 | |
| C-03 | `gate.yml:31-40` (the dispatch inputs `gates`, `platforms`), `:33` (key list), `:63, 73, 83` (`if:` filters) | lane selection by free text | 4 | e | P0-03, P5-03 | with a literal matrix, the `gates` input is dead and no item deletes it |
| C-04 | `gate.yml:56`; `gates-run.yml:110, 112, 180`; `diagnostics.yml:37, 38, 51` | `uses:` pinned by tag | 4 | b | P5-04 | |
| C-05 | `gates-run.yml:33-82` | `setup` job: `jq` matrix, low-memory split (`:46-52`), platform filter (`:71`), key validation (`:72-80`) | 4 | a | P5-03 (with D6) | |
| C-06 | `gates-run.yml:89-91, 98-99` | `shell: bash` default, and `MSYS2_ARG_CONV_EXCL: "//"` for Git Bash path mangling | 4, 8 | e | P5-03 ("if no longer needed") | still needed while steps run under bash on Windows; see P-7 |
| C-07 | `gates-run.yml:105-109` | `git config --global core.autocrlf/eol/longpaths` | 4 | e | P6-08 pre-allows it | no item decides it; see G-11 |
| C-08 | `gates-run.yml:121-130` | `tzutil /s "Eastern Standard Time"` (pwsh) | 4, 6 | e | P5-03 (investigate) | see P-8 |
| C-09 | `gates-run.yml:131-136` | host `python3 -m pip install pyarrow` | 4, 6 | a | P1-08 | |
| C-10 | `gates-run.yml:137-139` | `bazel run //datacube:install_browser -- --with-deps` | 4 | a | P4-09 | |
| C-11 | `gates-run.yml:144-162` | bash arrays, the low-memory branch, `--test_env=PATH/PYTHONPATH/WAREHOUSE_ARROW_CHECK`, the `GENERATED` `bazel query 2>/dev/null` loop | 4 | a | P0-02, P1-08, P5-03 | |
| C-12 | `gates-run.yml:163-177` | `for t in $(bazel query …browser-ci…)`, `bazel run` each, `tee`, `set -u` only | 4, 9 | a | P4-09 | |
| C-13 | `gates-run.yml:117-120`; `diagnostics.yml:43-46` | cache key frozen at first save | 4 | b | P5-04 | |
| C-14 | `diagnostics.yml:12, 20` | path trigger on the committed `tools/oracle-pins.env` | 4 | b | P2-10 | |
| C-15 | `diagnostics.yml:47-50` | `bazel test --config=ci --repository_cache="$HOME/…" --disk_cache="$HOME/…" //parser-equivalence:diagnostics` | 4 | c | — | already matches G8's regex |

### Category 5: hand-rolled repository or runfiles resolution

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| R-01 | `testing/src/main/java/com/legend/testing/Repo.java:44-77` | static init parses `TEST_SRCDIR`, `TEST_WORKSPACE`, `TEST_TARGET`, or `-Dlegend.repo.root/module` | 5 | e | P3-32 | env parsing deleted, but `out` (61 uses), `outDir` (18) and `rel` (3) keep the file; P-1 |
| R-02 | 74 Java files: `Repo.path` (34 uses), `Repo.module` (47) | repository paths via Repo | 5 | b | P1-05, P3-27 | |
| R-03 | `Repo.java:109-121` `listed`, `:148-157` `actionScratch`; 4 `Repo.listed` users | declared-list reader; action temp directory | 5, 6 | a | P3-32, P3-27 | |
| R-04 | `Repo.root()`: `core/…/JdbcSurfaceCensusTest.java:189`, `LegacyReachbackCensusTest.java:134`, `VerdictChannelRegisterTest.java:81`, `parser-equivalence/…/OwnDialectCensusTest.java:146`, `warehouse/…/LauncherTest.java:122` | walk roots and a hand `RUNFILES_DIR` | 5 | b | P3-27, P3-30, P4-12 | |
| R-05 | `testing/src/main/java/com/legend/testing/Upstream.java`; 16 users (`Upstream.engine` ×13, `Upstream.pure` ×13), including the generator-side `parser-equivalence/…/harvest/FixtureHarvestGenerator.java`, `spec/…/generators/{OurResolutions,UpstreamDeclarations}.java`, `spec/…/rcorpus/Corpus.java`, `parser-equivalence/…/Corpus.java` | upstream roots from `-D` properties | 5 | d | P1-04 / P3-32 conflict | P1-04 converts tests and says generators are unaffected; P3-32 deletes `Upstream.java`. See G-3 |
| R-06 | `tools/generators/defs.bzl:25-40` `program_jvm_flags` (`-Dlegend.repo.root=.`, `-Dlegend.repo.module`, `-Dlegend.*.root={…_ROOT}`) | generator actions use Repo's action mode | 5 | d | — | breaks when P3-32 deletes the action-mode branch; G-3 |
| R-07 | `tools/junit/defs.bzl:20-23, 46-48` | `"../" + repo_name` upstream roots | 5 | b | P1-04 | |
| R-08 | `parser-equivalence/…/GrammarKeywordCensusTest.java:27`, `SurfaceCensusTest.java:122` | raw `legend.engine.root` reads | 5 | b | P1-04 | |
| R-09 | `testing/…/EmbeddedPostgres.java:152-167` | third hand-written runfiles and manifest resolver | 5 | b | P1-06 | |
| R-10 | `warehouse/…/WarehouseArrowTest.java:125` | `Path.of("warehouse/src/test/python/…")` | 5 | b | P1-08 | |
| R-11 | `warehouse/…/launcher/LauncherTest.java:122-124` | sets `RUNFILES_DIR` and `BUILD_WORKING_DIRECTORY` for the child by hand | 5 | b | P4-12 | |
| R-12 | `warehouse/BUILD.bazel:140-141, 204-205` | `TestServer` env through `$(rootpath)` | 5 | b | P1-05 | |
| R-13 | `query-store/test/lite.test.ts:13-14, 33` | `RUNFILES_DIR ?? TEST_SRCDIR`, a `'_main'` literal, `JAVA_RUNFILES` | 5 | b | P1-24 (batch 2) | |
| R-14 | `query/demo/verify.mjs:20, 23-24, 35, 46-47` | runfiles arithmetic, `RUNFILES_DIR`/`JAVA_RUNFILES` for children | 5 | b | P4-05 | |
| R-15 | `datacube/demo/verify-app.mjs:33-34, 43` | `new URL('..')`, then `RUNFILES = ../..` | 5 | b | P4-07 | |
| R-16 | `datacube/test/live-snap/live-snap.ts:44-45, 84-85` | `new URL('../../../')` as RUNFILES; env joined onto it | 5 | b | P1-24 | |
| R-17 | `datacube/test/catalog-builder.ts:5`, `cube-open/cube-open.ts:30`, `json-read/json-read.ts:25`, `pivot-rows/pivot-rows.ts:30`, `typed-values/typed-values.ts:31`, `wasm-differential/compare.ts:30-31`, `saved-queries.test.ts:67`; `pure-protocol/test/lite.ts:13` | `new URL('../../wasm/planner/', import.meta.url)` and similar | 5 | b | P1-24 (batch 1) | |
| R-18 | `datacube/test/config-readers.test.ts:24`, `menu-ids.test.ts:26` | source scanners reading `../src` | 5 | b | P3-29 | |
| R-19 | `query/test/lite.ts:16, 54`; `query/test/saved-queries.test.ts:23` | `new URL('../../wasm/planner/')`, `../demo/models`, `../../fixtures/…` | 5 | b | P1-24 (by its Done-when; not in its batch lists) | |
| R-20 | 20 harnesses with `ROOT = fileURLToPath(new URL('..', import.meta.url))` or similar: `datacube/demo/{chaos:37, measure-startup:17, run-stress:9, shots:21, torture:33, verify-calc-vocabulary:32, verify-charts:15, verify-cubes:23, verify-engine-differential:62, verify-engine:78/92/149, verify-features:35/5150, verify-page:15, verify-real-data:33, verify-remote:26, verify-smoke:33, verify-upload:23, verify-wasm-browser:26}.mjs` | path arithmetic from the module URL | 5 | b | P4-01 – P4-08 | **G15's regex `new URL\('\.\./` does not match `new URL('..', …)`**: an unconverted harness passes the guard (G-8) |
| R-21 | `query/demo/serve.mjs:11` (`resolve(…new URL('.')…, '..')`), `datacube/demo/serve.mjs:26`, `site/serve.mjs:9` | dev servers resolve their roots from the module URL | 5 | c | P4-08 | `bazel run` dev tools; G15's scope must exempt them by row, or they must use `@bazel/runfiles` (G-8) |
| R-22 | `site/verify.mjs:15, 17` | `createRequire('../datacube/node_modules/')`, `new URL('.')` | 5 | b | P1-26, P4-05 | |
| R-23 | `wasm/differential.mjs:23`, `wasm/startup.mjs:17`, `wasm/zoneprobe.mjs:16` | `here('./…')` reads same-package data next to the module | 5 | c | — | same-package data in rules_js's output tree. But `//wasm`'s `js_test`s are not in P1-23's `node_test` conversion (G-10) |
| R-24 | 18 targets with `chdir = package_name()`: `datacube/BUILD.bazel` (11), `query/BUILD.bazel` (3), `query-store/BUILD.bazel` (3), `pure-protocol/BUILD.bazel` (1) | cwd-relative reads | 5 | b | P1-24, P3-29 | |
| R-25 | `.bazelrc:20-22` (`startup --windows_enable_symlinks`, `build:windows --enable_runfiles`) | a runfiles tree forced on Windows | 5, 8 | e | P3-32 | `--enable_runfiles` is deleted; `--windows_enable_symlinks` is kept "only if something still needs it" (P-9) |
| R-26 | `tools/jars/defs.bzl` (`java_jars`) and `java.class.path` readers (`TokenDump`, `NoEagerTypeReferencesTest:71-86`) | jar lists by name and class path | 5 | b | P2-18, P3-26, P3-27 | |
| R-27 | `tools/engine-runner/src/main/java/perf/Cwd.java:18`; `warehouse/…/WarehouseServer.java:819`; `tools/bump/Bump.java:60` | `BUILD_WORKING_DIRECTORY`/`BUILD_WORKSPACE_DIRECTORY` under `bazel run` | 5 | c | P4-12 (`named(value, startedIn)`) | the documented `bazel run` protocol; G4 already allows it |
| R-28 | `datacube/demo/make-sample.mjs:28`, `run-stress.mjs:98`, `serve.mjs:36`, `shots.mjs:24`, `verify-features.mjs:275` | `BUILD_WORKING_DIRECTORY ?? process.cwd()` | 5, 6 | b | P4-02, P4-04, P4-08 | |
| R-29 | `fixtures/saved-queries/make.mjs:11` | `readFileSync('query/demo/models/…')` relative to cwd | 5 | b | P2-06 | |
| R-30 | `tools/junit/JUnitMain.java` `expandOutputs`; `:96-101` rewrites `TEST_UNDECLARED_OUTPUTS_DIR` for the child | | 5 | a | P3-01 | |

### Category 6: writes into the tree, or reads host state

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| H-01 | `core/…/JsonM2MChainIntegrationTest.java:954`, `server/ConnectionLeaseTest.java:65, 235`, `server/LegendHttpServerIntegrationTest.java:38`; `warehouse/…/WarehouseArrowTest.java:44, 100`, `WarehouseCorsTest.java:34`, `WarehouseEntitlementsTest.java:45, 279`, `WarehouseJdbcTest.java:53`, `WarehousePostgresLiveTest.java:52`, `WarehouseServerTest.java:53, 323, 351`, `server/PostgresCatalogTest.java:63`; `spec/…/DuckWorkspaces.java:140` | `Files.createTemp*` in the host `java.io.tmpdir` | 6 | b | P0-10 (tmpdir = `TEST_TMPDIR` in `JUnitMain`), P3-21 (`@TempDir`) | |
| H-02 | `testing/…/EmbeddedPostgres.java:88` | falls back to `java.io.tmpdir` | 6 | b | P3-20 | |
| H-03 | `parser-equivalence/…/harvest/FixtureHarvestGenerator.java:33` | action temp directory | 6 | b | P0-10 (`java_run` scratch directory) | |
| H-04 | `warehouse/src/main/java/com/legend/warehouse/server/duck/DuckLibrary.java:77-102` (`:86` path) | shared host temp cache keyed by size | 6 | a | P1-16 | |
| H-05 | `warehouse/…/WarehouseServer.java:925` | product `temporaryData` directory | 6 | c | P3-21 (`close()`) | product use for a real user |
| H-06 | `datacube/demo/verify-cubes.mjs:41`, `verify-features.mjs:54` (fixed name), `verify-real-data.mjs:73`, `verify-smoke.mjs:75`, `verify-upload.mjs:43` | `os.tmpdir()` | 6 | b | P4-01 (`tmpPath`), P4-02, P4-04 | |
| H-07 | `datacube/test/live-snap/live-snap.ts:87`; `datacube/test/portability.test.ts:154`; `query-store/test/lite.test.ts:31` (`TEST_TMPDIR ?? tmpdir()`) | `os.tmpdir()` in tests | 6 | b | P3-10; P3-10; P1-24 | for `lite.test.ts` the fallback should go: fail when unset |
| H-08 | `query/demo/verify.mjs:29-31` | writes `.scratch/verify` under `BUILD_WORKSPACE_DIRECTORY` (the source tree) | 6 | b | P4-05 | |
| H-09 | `datacube/demo/verify-remote.mjs:78-83`, `verify-real-data.mjs:69-72` | write Parquet into cwd (runfiles) | 6 | b | P4-03 | |
| H-10 | `core/…/LeanSqlLadderTest.java:140-141, 151, 253-255` (`-Dladder.record`) | a test writes `*.current.sql` through the runfiles symlink | 6 | b | P2-13 | |
| H-11 | `spec/…/NativeSignatureGeneratorTest.java:67-87` (`natives.bootstrap`, `natives.dump`) | a test writes into the tree | 6 | a | P2-17 | |
| H-12 | `parser-equivalence/…/ProtocolRosterCensusTest.java:48-49` → `PmcdReachabilityCensusTest.java:127-130` | one test writes a file another test reads | 6, 9 | a | P2-19 | |
| H-13 | the maven resolver on `REPIN=1 … :pin` consults `~/.m2/repository` | host Maven state during resolution | 6 | b | P1-25b | |
| H-14 | Playwright's browser cache in `$HOME` (`install_browser`) | host browser cache | 6, 8 | a | P1-14, P4-09 | |
| H-15 | `.bazelrc:62` `--disk_cache=~/.cache/bazel-disk`; CI `$HOME/.cache/bazel-{repo,disk}` | Bazel's own caches in `$HOME` | 6 | c | P1-28 (GC) | Bazel infrastructure, not orchestration |
| H-16 | `scripts/corpus/run.py:33` and the probes' `runner.JAVA_HOME` (`~/jdk/jdk-21.0.11+10`), `cp.txt`, `tools/engine-runner/target/classes` | host JDK and a Maven-era class path | 6 | b | P7-03, P3-25 | |
| H-17 | `tools/native-axes.py:32-33`, `tools/upstream-drift.py:35-36` (`~/legend/legend-{engine,pure}`) | host checkouts | 6 | b | P7-03 | |
| H-18 | `tools/golden_shape_survey.py:14`; `scripts/outstanding.py:14-16`; `scripts/walldepth.py:7` | host checkouts and home paths | 6 | c | P7-04 | history |
| H-19 | `warehouse/…/probe/Shadow.java:88-99` (`TEST_UNDECLARED_OUTPUTS_DIR`), `PrepTrace.java:18, 57-63`, `LL_*`/`LEGEND_LITE_*` debug env | product code reads test-runner and debug env | 6 | b | P7-14 | |
| H-20 | host locale, zone and encoding (`tools/junit/defs.bzl:41`, `tools/java_run/defs.bzl`, node tests); `user.name` (`AppModeTest`) | host state leaking into results | 6 | b | P0-10, P1-23, P3-21, P3-28, P3-29 | |

### Category 7: hand recipes in current docs

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| D-01 | `FAQ.md:312-314, 327-328` | `mvn` | 7 | b | P7-06 | |
| D-02 | `core/README.md:322` | `mvn -pl core test` | 7 | b | P7-06 | |
| D-03 | `AGENTS.md:372` | the retired "common mistake 11" quoting `mvn` | 7 | c | — | load-bearing and historical by design; **needs a G9 allowlist row** |
| D-04 | `docs/GATES.md:142, 149, 228, 282, 924-931, 1021, 1094` | `mvn`, `curl … \| tar` | 7 | b | P7-07 | |
| D-05 | `docs/ENGINEERING_LOG.md:57, 61, 63, 64, 66, 226` | `mvn` | 7 | b | P7-07 | |
| D-06 | `docs/RUNNING_THE_CORPUS.md:23-25, 40-41, 169-172` | `mvn`, `python3 scripts/…` | 7 | b | P7-07 | |
| D-07 | `docs/UPSTREAM_FINDINGS.md:11-12, 23, 219`; `docs/UPSTREAM_BOUNDARY_PROGRAM.md:13` | `mvn`, `java -cp cp.txt`, `version-report.sh` | 7 | b | P7-07 | |
| D-08 | `projects/CONTRACT.md:72` | `python3 scripts/projects/check.py` | 7 | b | P7-08 | |
| D-09 | `projects/*/MANIFEST.md` (26): `cash-core:106`, `client-core:131`, `client-portal:266`, `client-reporting:245`, `collateral-mgmt:218`, `collateral-opt:170`, `core-account:66`, `core-geo:113`, `custody-core:108`, `custody-recon:166`, `fee-billing:148`, `firm-balance-sheet:243`, `funding-core:121`, `index-core:139`, `liquidity-view:165`, `margin-calc:205`, `ops-control:181`, `pnl-attribution:212`, `position-keeping:124`, `product-catalogue:253`, `reference-data:185`, `regulatory-capital:320`, `regulatory-extract:212`, `risk-dashboard:284`, `settlement-flow:161`, `static-distribution:157` | `python3 scripts/projects/check.py <name>` | 7 | d | — | P7-08 names only `CONTRACT.md`; P3-23 makes these `bazel test //projects/<p>/...` (G-5) |
| D-10 | `repro/{collection-over-tomany:49, dayofyear-is-dayofmonth:46, derived-boolean-equals-literal:51, isempty-aggregate-invalid-sql:36, projection-form-picks-the-database:52, qualified-property-broken-chain:47, real-column-type:51, regexp-arity:43, registered-but-unusable:49, relation-first-and-last:39, self-join-aggregate:68}/README.md` | `python3 scripts/corpus/probe_*.py` | 7 | b | P7-08 | |
| D-11 | `scripts/corpus/repro/README.md:6-72` | `mvn`, `java -cp $CP` | 7 | b | P7-08 | |
| D-12 | `scripts/corpus/repro/persistence-npe/README.md:3`; `scripts/corpus/verified/{embedded-associations,property-mappings,store,union-inheritance-m2m}.md:9` | `java -cp …cp.txt perf.ParseMain` | 7 | d | — | not named by P7-08 (G-5) |
| D-13 | `scripts/parser/README.md:28-33, 135` | `python3 fixtures.py`, etc. | 7 | b | P7-08 | |
| D-14 | `scripts/parser/HANDOFF.md:26, 38, 58, 64-71, 77, 83, 210, 282, 379`; `scripts/parser/PERMISSIVENESS.md:150` | `python3`, `mvn`, `java -cp` | 7 | e | P7-04 (history for the scripts) | the `.md` files are not script rows, and `keywords.py` stays in place (P-3) |
| D-15 | `tools/census/README.md:12`; `tools/wrongrows/README.md:38` | `python3 tools/…` | 7 | b | P7-08 | |
| D-16 | `datacube/demo/README-realdata.md:9, 97, 121, 141`; `datacube/bench/README.md:104-107`; `datacube/bench/model/README.md:66-67` | `npm install`, `npx serve`, `node bench/…`, host `duckdb -c`, `python3` | 7 | b | P4-15 | |
| D-17 | `docs/DATACUBE_ON_POSTGRES.md:24` | "a C toolchain: `xcode-select --install` / gcc" prerequisite | 7, 8 | e | P4-12 (touches the doc for DSN paths only) | P1-09/P1-10 make the line false, and no item rewrites it |
| D-18 | `docs/BAZEL_DEPENDENCY_PROPOSAL.md:619, 1963`; `docs/BAZEL_IMPLEMENTATION_PLAN.md:273, 281, 289, 295, 310` | `python tools/…`, `mvn` | 7 | e | P7-09 (banner only) | undated and superseded: move to history |
| D-19 | `docs/SCOREBOARD.md:16` | `mvn` | 7 | c | P2-18, P7-04 | history |
| D-20 | 22 undated history docs: `docs/ARCHITECTURE_REMEDIATION.md:104, 107, 111`; `audit-22a-typing.md:181`; `CHANNELB_BURNDOWN_HANDOFF.md:138, 197`; `CORPUS_BURNDOWN_HANDOFF.md:301, 309`; `CORPUS_SWEEP_PERF.md:71`; `DEEP_AUDIT_HANDOFF.md:144`; `FOUNDATIONS_BASELINE.md:11, 223, 225, 227`; `FOUNDATIONS_PLAN.md:81, 111-114, 238`; `GRAMMAR_EXTENSIBILITY.md:16-17`; `HARNESS_SIMPLIFICATION_PLAN.md:45-46, 259`; `M4_PRELAND_CHARTER.md:1278`; `METAMODEL_STORE_HANDOFF.md:186`; `ONE_PLATFORM_PLAN.md:596`; `PARSER_DROP_IN.md:72`; `PARSER_DROP_IN_STATUS.md:216-218`; `PCT_AUDIT.md:54, 61`; `PCT_BURNDOWN.md:11`; `SECTION_PROGRAM_HANDOFF.md:65, 75`; `STRESS_TEST_BENCHMARKS.md:31, 36`; `V7_ASSERT_VERDICT_CHARTER.md:1490, 1758` | `mvn`, `allgates.sh`, `java -cp` | 7 | d | — | P7-09 moves only docs with a date in the filename (G-5) |
| D-21 | `docs/CLOUD_BACKENDS.md:204` | quotes Spark's `./sbin/start-thriftserver.sh` | 7 | c | — | a quotation, not an instruction; G9 allowlist row |
| D-22 | 169 lines in 47 dated docs (for example `docs/burndown-2026-08-14/README.md` ×13, `docs/UPSTREAM_BOUNDARY_HOMEWORK_2026_09_10.md` ×8, `docs/mapping-normalizer-audit-2026-09-15/findings/09-test-quality.md` ×9, `docs/type-audit-2026-08/harness/HOWTO.md` ×2) | `mvn`, `python3`, `node` | 7 | c | P7-09 (moved to `docs/history/`) | history |
| D-23 | the plan's own docs: `docs/BAZEL_FIRST_CLASS_PLAN_2026_10_02.md` (5), `docs/BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` (3), `docs/bazel-audit-2026-10-02/**` (64 lines in 12 files) | evidence quoting the commands they remove | 7 | d | — | dated, so P7-09's rule would move the *current* plan to history; G9 needs explicit rows (G-6) |
| D-24 | code-comment recipes, which G9 does not scan: `parser-equivalence/…/ParseSpeedBenchmarkTest.java:18`, `ProbeWireShapes.java:15` | `mvn … -Dtest=` | 7 | e | P3-17 (converts the classes; comments not named) | G-7 |
| D-25 | `core/…/integration/StressServiceSuitesTest.java:94` (`tail -f core/target/…`); `warehouse/BUILD.bazel:118-121` (`--test_env=LEGENDLITE_PG_DSN`); `datacube/BUILD.bazel:740-741` (`npx playwright install chromium`); `fixtures/saved-queries/make.mjs:5` (`java -jar … &`); `query/tools/icons.mjs:5, 13` (`node … > icons.ts`); `tools/census/RenderCensus.java:33` (`java -cp core_tests_deploy.jar`) | code-comment recipes | 7 | a | P3-12, P3-22, P4-09, P2-06, P2-07, P3-13 | |
| D-26 | `pct/BUILD.bazel:89`; `pct/…/PctCensusGate.java:47`; `spec/…/rcorpus/Corpus.java:38`; `datacube/demo/serve.mjs:14` | rationale mentioning `mvn` or `python3 -m http.server` | 7 | c | — | explanations, not instructions |
| D-27 | `parser-equivalence/BUILD.bazel:92, 104` (`tools/diagnostics.sh`, `tools/allgates.sh`); the pom comments in `MODULE.bazel` | references to scripts that no longer exist | 7 | a | P7-10 | |

### Category 8: host prerequisites

| ID | Prerequisite | Where | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| P-01 | Linux gcc, zlib headers, libc headers for native-image | `warehouse/BUILD.bazel:107-116` | 8 | b | P1-09 | |
| P-02 | macOS CLT compiler and linker | as above | 8 | b | P1-10 | |
| P-03 | macOS CLT **SDK files** (copied and sha256-checked) | `tools/cc/macos_sdk.bzl` (new) | 8 | c | D1, P1-10 | licence |
| P-04 | Windows MSVC (`cl.exe`/vswhere) | `tools/cc/msvc.bzl` (new) | 8 | c | D5, P1-11 | licence and GraalVM support; `rctx.execute(vswhere)` is a new host spawn in Starlark |
| P-05 | `libxml2.so.2` for LLVM's `ld.lld` on Linux | P1-09 investigation | 8 | c | D7 | recorded exception if no clean archive exists |
| P-06 | Git for Windows bash | `.bazelrc:35-36` | 8 | e | P5-07 | see B-06 |
| P-07 | `/bin/bash` on Linux and macOS for third-party launchers | B-07 | 8 | d | — | never declared (G-9) |
| P-08 | Windows symlink privilege (admin or Developer Mode) | `.bazelrc:21` | 8 | e | P3-32 | kept "if needed" |
| P-09 | Host Python plus pyarrow | `WarehouseArrowTest`, CI pip | 8 | b | P1-07, P1-08 | |
| P-10 | Host JDK (`~/jdk`) and Maven (`~/.m2`, `mvn`) | scripts; repin | 8 | b | P7-03, P1-25b; P7-05 for Maven scripts | |
| P-11 | Playwright Chromium in `$HOME` | `install_browser` | 8 | a | P1-14, P4-09 | |
| P-12 | Chromium's 17 Linux system libraries on a **desk** (outside the CI image) | S4 | 8 | e | P5-02 (CI only) | a Linux developer running `//:browser` must install them; undeclared |
| P-13 | Docker | P1-09/P1-10 proofs (H-docker), `experiments/` | 8 | c | P5-02 (`rules_oci` builds without Docker) | proof-only |
| P-14 | Postgres | `@embedded_postgres` | 8 | b | P1-15, P3-22 | non-root user (P3-20) |
| P-15 | A legend-engine HTTP server on `:6300`, started by hand | `verify-engine.mjs`, `verify-engine-differential.mjs`, `verify-calc-vocabulary.mjs` (engine half), `chaos.mjs` | 8, 9 | e | P1-18 (investigation), P4-07 | if no artifact exists, these stay `js_binary` dev tools that need a hand-started engine |
| P-16 | Host `duckdb` CLI or Python module | `README-realdata.md:141`, `bench/model` | 8 | b | P4-15 | |
| P-17 | Host `git` | `Bump.java`, `move_classes.py`, CI checkout | 8 | c | — | release and dev tools |
| P-18 | Hosted macOS (`macos-14`) and Windows (`windows-2022`) runner images | `gate.yml:76, 86` | 8 | c | P5-02 pins Linux only | |

### Category 9: orchestrators not listed above

| ID | path:line | What it is | Cat | Class | Item | Notes |
|---|---|---|---|---|---|---|
| O-01 | `spec/…/rcorpus/MinimalCorpusTest.java:138-146` | flips the process-wide `legend.exec.engineScanOrder` between passes | 9 | b | P3-01 | |
| O-02 | `pct/…/PctCensusGate.java:16-27, 37-53` | cumulative counters across suites in one JVM | 9 | b | P3-09 | |
| O-03 | `spec/…/PreludeGeneratorTest.java:45-53`, `spec/…/claims/ClaimRegistryTest.java:90-99`, `NativeSignatureGeneratorTest.signatureTextIsCurrent`, `DynaFnRegistryTest.registryMatchesTheCheckout`, `CorpusManifestTest` | tests re-run generators in process | 9 | a | P2-19 | |
| O-04 | `core/…/integration/StressServiceSuitesTest.java:36-183` (`stress.rows`, `data`, `only`, `sessions`) | hidden `-D` modes turn a test into a tool | 9 | b | P3-12 (`//core:stress_tool`) | |
| O-05 | the hidden modes in spec, parser-equivalence and pct (`our.resolutions`, `manifest.census`, `chb.only`, `LL_SHADOW`, …) | the same | 9 | b | P3-13 | |
| O-06 | `fixtures/saved-queries/make.mjs:4-6` | starts `java -jar server_deploy.jar` and drives it over HTTP | 9 | b | P2-06 (D14 (a) or (b)) | under D14 (a) it still spawns a JVM inside a `js_run_binary` action: **missing from G5** |
| O-07 | `scripts/corpus/build.py` (each generator run 3 times) | repeated passes | 9 | b | P2-05 | |

### Category X: `experiments/` (kept by decision, bazelignored, out of scope)

| ID | Files | Notes |
|---|---|---|
| X-01 | `experiments/backend-probes/harness/mariadb-fix.sh`, `experiments/legend_rules_test/test_full_pipeline.sh`, `experiments/postgres-dialect/semantics_probe.sh` (DuckDB CLI plus Docker Postgres), `experiments/tree_artifact_test/test_unused_inputs.sh` | 4 shell scripts |
| X-02 | `experiments/backend-probes/duckdb-census/{cmp,gen_census,gen_ext,gen_r3,gen_r4,gen_r5,gen_r6,gen_strings}.py`, `experiments/warehouse-ffm/cdata/check_types.py`, `experiments/warehouse-w0/{authz,concurrency,lake}_probe.py`, `experiments/warehouse-w1d/arrow_check.py` | 13 Python scripts; host `duckdb` and `pyarrow` modules |
| X-03 | `experiments/backend-probes/harness/src/main/java/Probe.java` and its README/HARNESS.md | Docker-backed probe harness |

## 3. Gaps and partials, with proposed items

### Gaps (class d)

**G-1 · G5 cannot see Python spawns** (S-19, S-20, S-22, S-27, S-28)

- **Change:** extend `//tools/guards:process_spawn_test`'s patterns with `subprocess\.`, `os\.system\(`, `os\.popen\(`, `Popen\(`, `execSync\(`, `execFileSync\(` and `spawnSync\(`, ignoring matches inside string literals. In the same PR, move the 15 probes, `check.py` and `loadtime.py` onto one `launch(args)` function in `run.py` (they already `import run as runner`), so the Python allowlist is one file.
- **Proof:** `bazel test //tools/guards:process_spawn_test`. Negative: a scratch `scripts/x.py` with `subprocess.run(["ls"])` fails, naming it.
- **Done when:** every Python spawn is either `run.py`'s `launch` or an allowlisted row.

**G-2 · G5's allowlist is incomplete** (S-05, S-06, S-09, S-12, S-16, S-29, O-06, plus the new spawns from P1-18 and P4-13)

- **Change:** replace P6-05's list with section 4's exact list.
- **Proof:** the guard passes on HEAD, and removing any one row makes it fail.
- **Done when:** every row carries a date and a reason.

**G-3 · Generator actions lose Repo and Upstream** (R-05, R-06; P1-04 contradicts P3-32)

- **Change (new item P3-32a):**
  - Generators run by `java_run` take their trees as explicit program arguments. Use the existing `roots` tokens (`{ENGINE_ROOT}`, `{PURE_ROOT}`, plus a `{REPO}` token for the declared-input root), read through a small `//testing:ActionArgs` (no environment, no runfiles).
  - Delete `program_jvm_flags`' `-Dlegend.repo.*` and `-Dlegend.*.root`.
  - Convert `FixtureHarvestGenerator`, `ManifestGenerator`, `OurResolutions`, `UpstreamDeclarations` and both `Corpus` classes' generator paths.
- **Proof:** `bazel test //:generated` stays byte-identical. `git grep -n "legend.repo.root\|Upstream\." -- '*.java' '*.bzl'` is empty.
- **Depends on:** P1-04, P1-05.
- **Done when:** P3-32 can delete `Upstream.java` and every branch of Repo's static initialiser without breaking a generator.

**G-4 · `taskkill` in tests** (S-17, S-14)

- **Change:**
  - `//core:server` and the warehouse server exit when stdin reaches EOF, behind an explicit flag such as `--exit-with-parent`, set only by tests.
  - `harness.mjs`'s `startServer().close()` and `query-store/test/lite.test.ts` close the child's stdin and await its exit.
  - Delete both `taskkill` calls.
- **Proof:** on Windows CI, `bazel test //query-store:lite_test` passes. `git grep -n taskkill` is empty.
- **Done when:** no test kills a process tree with a host tool.

**G-5 · Current-doc recipes outside P7-06..P7-09's lists** (D-09, D-12, D-20)

- **Change:**
  - P7-09's rule becomes "every doc AGENTS.md does not list as standing *and* G9 flags", not "a date in the filename". The 22 undated history docs move to `docs/history/`.
  - P7-08 adds the 26 `projects/*/MANIFEST.md` lines (`bazel run //scripts/projects:check -- <p>` now, `bazel test //projects/<p>:all` after P3-23), `scripts/corpus/verified/*.md` and `repro/persistence-npe/README.md`.
- **Proof:** `bazel test //tools/guards:docs_test` with no allowlist rows for these files.
- **Done when:** G9 passes with only section 4's rows.

**G-6 · G9 would flag, and P7-09 would move, the plan's own documents** (D-23, D-03, D-21)

- **Change:** add G9 allowlist rows for `docs/BAZEL_FIRST_CLASS_*.md`, `docs/bazel-audit-2026-10-02/**`, `AGENTS.md:372` and `docs/CLOUD_BACKENDS.md:204`. P7-09 states that the active plan stays in place until the effort closes.
- **Proof:** `docs_test` passes.
- **Done when:** the active plan stays in place and G9 passes.

**G-7 · G9 scans only `.md`** (D-24)

- **Change:** either add `*.java`, `*.ts`, `*.mjs` and `BUILD.bazel` comment lines to G9's scan (the same regexes), or P3-17 deletes the two javadoc recipes when it converts those classes.
- **Proof:** `docs_test`, or `git grep -n "mvn " -- '*.java'` is empty outside history.
- **Done when:** no source comment gives a non-Bazel command.

**G-8 · G15's regex misses two forms of path arithmetic** (R-20, R-21)

- **Change:** G15 also matches `new URL\(\s*['"]\.\.['"]` and `resolve\([^)]*import\.meta\.url[^)]*\)\s*,\s*['"]\.\.`. Its scope covers dev tools, with rows only for the three `serve.mjs` files if they are not converted to `@bazel/runfiles`.
- **Proof:** the guard fails on today's `verify-smoke.mjs:33` before P4-02 and passes after.
- **Done when:** both forms are caught.

**G-9 · Third-party shell is undeclared** (B-07, P-07)

- **Change:**
  - Extend P5-07 from Windows to all three platforms: `bazel aquery` lists the mnemonics whose argv starts with a shell, recorded in a dated `.bazelrc` comment.
  - `README.md` Prerequisites declares "bash (Unix: `/bin/bash`; Windows: Git for Windows), required by rules_java/rules_js/bazel_lib, not by this repository".
  - If rules_python's `bootstrap_impl=script` is in use, set `system_python`, or record it.
- **Proof:** the CI aquery counts sit in the comment.
- **Done when:** every remaining shell in the build is named and attributed.

**G-10 · `//wasm`'s JS tests are outside the `node_test` macro** (R-23)

- **Change:** P1-23 converts `//wasm:differential_test` and `//wasm:zone_test` too, so they get pinned `LANG`/`TZ` and the shared options.
- **Proof:** `bazel test //wasm:all --test_env=LANG=tr_TR.UTF-8`.
- **Done when:** no `js_test` outside the macro remains (`kind(js_test, //...)` minus `node_test`'s).

**G-11 · The CI `git config` step has no owner** (C-07)

- **Change (a P5-03 sub-step):** delete the step. `.gitattributes` already forces `eol=lf`, and the upstream trees arrive by `http_archive`, not git. If Windows checkout still needs `core.longpaths`, keep only that line, as the one G8 row.
- **Proof:** Windows CI green; `git ls-files --eol` on the runner shows `w/lf` for `.pure`.
- **Done when:** the step is gone, or is one allowlisted line.

### Partials (class e)

**P-1 · `Repo.java` survives P3-32** (R-01, 82 call sites)

- **Change:** `Repo.out`/`outDir` become `//testing:TestOutputs.dir()`, which reads only `TEST_UNDECLARED_OUTPUTS_DIR` and fails if it is unset. `Repo.rel` becomes `SourceFiles.rel` (P3-27). Delete `Repo.java`, with G-3.
- **Proof:** `git grep -n "Repo\."` is empty; `//core:guardrails //spec:spec_tests` pass.
- **Done when:** `testing/` holds `Runfile`, `SourceFiles`, `TestOutputs` and `EmbeddedPostgres` only.

**P-2 · CI shell under Git Bash on Windows** (C-06)

- **Change:** with single-line `bazel` steps, use the runner's default shell (pwsh on Windows), which does not mangle `//`. Delete `defaults.run.shell: bash` and `MSYS2_ARG_CONV_EXCL`.
- **Proof:** Windows lanes green.
- **Done when:** no bash is needed by the workflow itself.

**P-3 · D3 row 6 (`keywords.py`, `tiers.py`) has no implementing item** (U-14, D-14)

- **Change:** P7-03 adds `py_binary(name = "keywords")` over `tiers.py`, reading the `.g4` files from `@legend_engine_src` through `rules_python`'s runfiles library (no `Path.home()`). P2-18's `vocab.tsv` action feeds it. `scripts/parser/README.md` and `HANDOFF.md` are rewritten for the kept part and moved to history for the rest.
- **Proof:** `bazel run //scripts/parser:keywords -- --tier1`.
- **Done when:** no kept script reads a host checkout.

**P-4 · `tzutil` in CI** (C-08)

- **Change:** pick one answer in P5-03.
  - Recommended: a JVM test that runs `WarehouseJdbcTest`'s reference session under an explicit non-UTC DuckDB `SET TimeZone`, which proves the same fix on every platform, and delete the step.
  - Otherwise, the step is one dated G8 row.
- **Proof:** the Windows app lane green.
- **Done when:** no `tzutil`, or one row.

**P-5 · Free-text lane selection in `gate.yml`** (C-03)

- **Change:** P5-03 deletes the dispatch input `gates` and its description. `platforms` stays as job-level `if:` (GitHub expressions, not shell).
- **Proof:** actionlint plus G8.
- **Done when:** no input names a lane.

**P-6 · `upstream-drift.py` still runs `curl` and `git`** (S-28)

- **Change:** fetch with `urllib` plus `certifi` from `@pypi` (pinned in `requirements.in`). The pinned side's file list comes from `@legend_*_src` runfiles, not `git ls-tree`.
- **Proof:** `bazel run //tools:upstream_drift -- --help`; G5 has no row for it.
- **Done when:** no host program is spawned.

Other partials, each closed by its item's own choice:

- **B-06 / P-06 (Git bash on Windows):** P5-07 records the measured list; the result becomes class c.
- **R-25 / P-08 (`--windows_enable_symlinks`):** P3-32 records whether it is needed.
- **P-12 (Linux desk Chromium libraries):** add a README prerequisite line, or D4 (c) later.
- **P-15 (engine server):** P1-18's answer.
- **D-17 (`DATACUBE_ON_POSTGRES.md:24`):** add it to P1-10's PR, which makes the line false.
- **D-18 (two superseded Bazel proposals):** move them under G-5's rule.

**O-1 (optional, Bump).** Bump could stop orchestrating:

1. It only rewrites the `MODULE.bazel` constants, resolving the tag SHA through the GitHub API with `java.net.http`, not `git ls-remote`.
2. It prints the three `bazel` commands for the person to run.

That would delete the last nested-`bazel` caller and Bump's G5 row. The workplan keeps it as a release tool, which this ledger accepts as class c.

## 4. Proposed final allowlists

Each row is `path  # YYYY-MM-DD <item>: reason`.

**G2 (unbuilt scripts), `tools/guards/scripts.allow`:** no rows. Structural exemptions only:

- `docs/history/**` (D16);
- `experiments/**`, which is not in the inventory because it is bazelignored.

Also widen G2's extension list to `\.(py|sh|bash|ps1|bat|cmd|mjs|js|cjs|ts)$` plus `docs/**/*.java`.

**G5 (process spawns), `tools/guards/process_spawn.allow`:**

```
testing/src/main/java/com/legend/testing/EmbeddedPostgres.java       # P1-06: initdb/pg_ctl/postgres from @embedded_postgres runfiles
warehouse/src/test/java/com/legend/warehouse/TestServer.java          # P1-05: //warehouse:server_native by rlocationpath
warehouse/src/test/java/com/legend/warehouse/serve/ServeTest.java     # P4-12: //warehouse:serve by rlocationpath
warehouse/src/test/java/com/legend/warehouse/DistTest.java            # P4-13: the extracted //warehouse:dist
warehouse/src/test/java/com/legend/warehouse/WarehouseArrowTest.java  # P1-08: //warehouse:arrow_matches_json (py_binary)
spec/src/test/java/com/legend/rcorpus/DuckWorkspaces.java             # P3-19: //warehouse:server_native by rlocationpath
warehouse/src/main/java/com/legend/warehouse/server/WarehouseServer.java  # product --open: the user's browser (openBrowser only)
tools/bump/Bump.java                                                  # P2-10: release tool under bazel run (git, bazel)
datacube/demo/harness.mjs                                             # P4-01: startServer, every harness's server by rlocationpath
datacube/test/live-snap/live-snap.ts                                  # P3-10: native warehouse (or move onto harness.startServer)
query-store/test/lite.test.ts                                         # P3-16: //core:server (or move onto harness.startServer)
datacube/demo/serve.mjs                                               # P4-08: dev tool --open
scripts/corpus/run.py                                                 # P3-25: launch() of //tools/engine-runner:testable (G-1)
tools/untangle/move_classes.py                                        # P7-02: dev codemod, git mv
fixtures/saved-queries/make.mjs                                       # P2-06: only if D14 (a); delete the row under (b)
tools/engine-runner/start.mjs                                         # P1-18: only if the engine-server artifact exists
core/src/test/java/com/legend/ErrorShapeGuardrailTest.java            # pattern text, not a spawn
datacube/test/portability.test.ts                                     # pattern text, not a spawn
```

That is 18 rows. Two of them are conditional. `live-snap.ts` and `lite.test.ts` drop out if they move onto `harness.startServer`.

**G8 (CI steps), `tools/guards/workflows.allow`:** recommended to be empty. Every `run:` is `bazel (test|build|run) … //labels`, and every `uses:` is SHA-pinned. At most two dated rows, only if G-11 and P-4 decide to keep them:

```
.github/workflows/gates-run.yml  git config --global core.longpaths true   # G-11: Windows checkout depth (only if proven needed)
.github/workflows/gates-run.yml  tzutil /s "Eastern Standard Time"         # P-4: only if the JVM-side zone test is not adopted
```

## 5. Coverage statement

- **Tree:** `git ls-files` of the worktree, 3,818 files. `experiments/` (197 files) is reported only in category X. Since `23b441852` the tree differs only by `.md` files (`git diff --name-only 23b441852 HEAD`), so the script inventory equals `script-review.md`'s 187 rows; 170 are outside `experiments/`, and all 170 are in U-01 to U-30, each file named.
- **Category 1, process spawns.** Over all tracked non-data files outside `experiments/`, `git grep -nE` for `ProcessBuilder|Runtime\.getRuntime\(\)\.exec|\.exec\(|child_process|\bspawn(Sync)?\(|\bexecFile(Sync)?\(|\bexecSync\(|subprocess\.|os\.system\(|os\.popen\(`, then `Popen|check_output|check_call|from subprocess|import subprocess|execFileSync|spawnSync|\bspawn\(|Desktop\.|xdg-open|rundll32|ProcessHandle|process\.execPath|new Worker\(`.
  - Every hit was read.
  - Excluded as non-spawns: 41 regex `.exec(` calls in TS and MJS (`datacube/src`, `datacube/test`, `datacube/demo`, `query/src`, `wasm/*.mjs`, `query/tools/icons.mjs`); 5 `conn.exec`/`c.exec` DuckDB calls in `warehouse/…/duck/Database.java`; Web Worker constructors; `SpecCompilerTest.check_callBodyAgainstBooleanReturn`; `ProcessHandle` self-inspection in `DuckLibrary.java:54` and `LauncherTest.java:169-172`.
- **Category 2, shell in the build.** `git grep -nE 'genrule|run_shell|sh_binary|sh_test|sh_library|bash|/bin/sh|\bcmd\s*=|cmd_bat|launcher|\.sh"|\.bat|\.ps1|is_executable|hermetic_launcher|native_test|run_binary|js_run_binary'` over `*.bazel`, `*.bzl`, `MODULE.bazel` and `third_party/*`.
  - Also read in full: `.bazelrc`, `.bazelignore`, `third_party/rules_graalvm_command_line_tools.patch` (no host calls added).
  - `git grep` for `no-sandbox|local|requires-network|use_default_shell_env|action_env|test_env|PATH` over the same files found only `.bazelrc:17` and a comment at `warehouse/BUILD.bazel:119-120`.
- **Category 3, unbuilt scripts.** `git ls-files | grep -E '\.(py|sh|bash|ps1|mjs|js|cjs|ts|bat|cmd)$'` gives 510 files (493 outside `experiments/`). Matched against `script-review.md`'s rows (built by that review with `bazel query`, which this audit was not allowed to run), plus every `entry_point`, `glob(` and `data` in `datacube`, `query`, `site`, `wasm`, `query-store` and `pure-protocol` `BUILD.bazel`. Also checked: `package.json` `scripts`, and the absence of any Makefile, Dockerfile, `pyproject.toml` or `requirements*.txt` outside `experiments/`.
- **Category 4, CI.** Every line of the three workflow files was read (`diagnostics.yml`, `gate.yml`, `gates-run.yml`). Every `run:` block and `uses:` is a row.
- **Category 5, hand-rolled resolution.**
  - `git grep -nE 'TEST_SRCDIR|RUNFILES_DIR|RUNFILES_MANIFEST|JAVA_RUNFILES|BUILD_WORKSPACE_DIRECTORY|BUILD_WORKING_DIRECTORY|"_main"|'"'"'_main'"'"'|/_main/|import\.meta\.url'` outside `docs` and `experiments`.
  - `git grep -ohE 'Repo\.[a-zA-Z]+\('` gave listed 4, module 47, out 61, outDir 18, path 34, rel 3, root 5, across 74 files. `Upstream\.(engine|pure)` appears in 16 files.
  - Relative `Path.of|Paths.get|new File("…")` repository reads: one real hit (R-10).
  - Also `chdir = package_name()` (18) and `process\.cwd\(\)|readFileSync\(['"]…`.
  - `createRequire(import.meta.url)` is module resolution, not path arithmetic, and was excluded.
- **Category 6, host state.** `git grep -nE 'createTemp(Directory|File)|java\.io\.tmpdir|os\.tmpdir\(\)|tmpdir\(\)|"/tmp|'"'"'/tmp|\.m2|user\.home|getenv\("HOME"\)|process\.env\.HOME|homedir\(\)|expanduser|~/|tempfile\.|mkdtemp'`, and `Path\.home\(\)|expanduser|~/legend|~/jdk|JAVA_HOME|cp\.txt|LEGEND_ENGINE_ROOT|target/classes` over Python. Host tools were found by name in the category 1 hits. SQL literals `'/tmp/…'` inside test data (`ElementParserTest`, `WarehouseEntitlementsTest:126-127`) are inputs, not host access, and were excluded.
- **Category 7, docs.** `git grep -nE '(^|[^a-zA-Z_-])(mvn |npx |npm (install|run|test|ci)|pnpm (install|run|test)|java -(jar|cp)|python3? [^ ]+\.py|node [^ ]+\.m?[jt]s|bash [^ ]+\.sh|\./[^ ]+\.sh|sh [^ ]+\.sh|pip3? install|brew install|apt(-get)? install|duckdb -c|curl -|playwright install)' -- '*.md' ':!experiments'` gave 365 lines.
  - Undated files are rows D-01 to D-21.
  - The 169 lines in 47 dated files are D-22 and D-23.
  - Code comments: the same regex plus `tail -f|caffeinate|xcode-select|\.sh\b|> \$PWD` over `*.bazel *.bzl *.java *.ts *.mjs *.py .bazelrc` (D-24 to D-26).
- **Category 9, orchestrators.** Every caller of `bazel` in code: `git grep -nE "['\"]bazel['\" ]|bazel (test|run|build|query)"`. Only `Bump.java` and the three `.sh` files invoke it; the rest are messages. Plus the workplan's own findings on cross-test coupling, which were re-checked at the cited lines.
- **Not done:** no `bazel query`, `aquery`, build or test (the brief forbids them). So B-07's third-party shell list is from rule knowledge and S2's notes, not measured; that measurement is G-9.
