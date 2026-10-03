# Bazel first-class: audit and plan (2026-10-02)

Audited at `origin/main` @ `16c8120d5`. Rebased onto `23b441852` on 2026-10-03 (Part 0).

**What this plan sets out to do.** Every build, test, generation, packaging and CI step is a Bazel target. Each one is hermetic and declared, and is checked by the build itself. Nothing in the repository depends on a host tool, a hand-run recipe, a script, or a person remembering to do something.

**Evidence.** The seven audit reports are in `docs/bazel-audit-2026-10-02/` (`01`–`07`). The spikes that test this plan's riskiest assumptions are in `docs/bazel-audit-2026-10-02/spikes/`. The per-script review for the user's decisions is `docs/bazel-audit-2026-10-02/script-review.md`. Part 6's source codes map to the reports:

| Code | Report |
|---|---|
| CT | `01-core-tests.md` |
| SP | `02-spec-pct-parser-equivalence-tools.md` |
| WH | `03-warehouse-wasm-core-main.md` |
| DC | `04-datacube-protocol-store.md` |
| HN | `05-harnesses-query-site.md` |
| SC | `06-scripts-docs-ci.md` |
| BZ | `07-bazel-files.md` |

**Status of this document.** This is the audit-level plan: what to fix and why, with acceptance criteria. Once the spikes report, it is turned into implementation-level work items (PR-sized: files, change, proof command, dependencies, size). Effort figures before that are estimates, not commitments.

---

## Part 0. Status at `23b441852` (rebased 2026-10-03)

Sixteen commits landed after the audit. Fifteen are PR #14 (Windows support for the DataCube app and the native warehouse). The other, `2defa10e7` ("the dialect is decided once"), is a core change with no build impact. Every plan item was re-checked against `23b441852`:

| Plan item | Status at `23b441852` |
|---|---|
| 0.1 `runs/` and `node_modules` not bazelignored | **Open.** `.bazelignore` still lists only `experiments`. |
| 0.2 Tests in no CI lane; `//site:verify` never run | **Partly done.** PR #14 added `//query-store:lite_test` to the `app` lane. Still in no lane: `//json:tests`, `//query:{build,load,saved_queries,typecheck}_test`, `//query-store:{local,share}_test`, `//pure-protocol:twins_test`. `//site:verify` is still never selected. |
| 0.3 GENERATED lane fails silently | **Open.** |
| 0.4 Lock files not enforced; Bump repins one pool | **Open.** All 8 pools still set `fail_if_repin_required = False`. |
| 0.5 `run-stress.mjs:157` undefined `offered` | **Open.** |
| 0.6 `PureLspServerTest` fixed port range | **Open.** |
| 0.7 No Postgres driver in `@maven_core` | **Open.** |
| 0.8 `paths-ignore: **/*.md` | **Open.** |
| 0.9 Locale, tmpdir, encoding not pinned | **Open.** Note: PR #14's Windows `app` lane runs in Eastern time (`tzutil`), so a test that reads the machine's zone now shows on CI. That is good coverage, but it is policy in YAML (Phase 5). |
| 0.11 `http://`; selects without a default | **Partly done.** Windows ARM64 is now a clean skip (`NOT_ON_WINDOWS_ARM64` in `warehouse/defs.bzl`). Still open: the new `windows_amd64` extension uses `http://` like the other four, and no select has `no_match_error`. |
| 0.12 `live_snap_test` not in `//datacube:tests` | **Open.** It now also runs on Windows in `bazel test //...`. |
| 1.2 Official runfiles | **Grew.** `warehouse/src/test/java/com/legend/warehouse/launcher/LauncherTest.java` is a new `Repo` user. |
| 1.3 Hermetic C toolchain for `native_image` | **Grew.** Windows now builds the native image with the host's MSVC: Visual Studio 2022 Build Tools are a README prerequisite, and the CI native lane runs on Windows. Spike S3. |
| 1.4 Macros | **Grew.** `warehouse_run` is now a macro without `**kwargs` (no `visibility`, `tags`, `target_compatible_with`): a PR #14 follow-up. |
| 4.3 The warehouse and the app without bash | **Changed.** `warehouse_run` is now three targets behind an alias: the bash launcher (macOS, Linux); a hermetic-launcher stub (Windows, BCR `hermetic_launcher` 0.0.16, at most 10 arguments, mangles `"`, no working directory, no job object); and `<name>_extensions` (the gzip `run_shell`, which now also runs on Windows through Git's bash). The server already resolves a relative `--data` against `BUILD_WORKING_DIRECTORY`. The target state is unchanged, but it now deletes two launchers and a dependency. Spike S2. |
| Phase 5 CI | **Partly done.** The jq Windows exclusion for the native lane is gone: the lane runs on all three platforms and builds `//datacube:app`. The rest of the lane logic is still jq and shell. |
| New: `.bazelrc` names Git for Windows' bash (`--repo_env=BAZEL_SH`, `--shell_executable`) | **New host dependency** (a PR #14 follow-up). It is needed while anything on Windows runs a shell: the genrules, the extension `run_shell`, rules_js's bash launchers, and rules_jvm_external's fetch. Revisit after Phases 2–4 remove ours. Keep it documented until then. |
| New: `.gitattributes` (`* text=auto eol=lf`, `-text` on the 4 CR files) | **Done** by PR #14. |
| PR #14 follow-up: `LauncherTest` pins the JDK's `NumberFormatException` text | Folded into 3.x (test hygiene). |
| `platforms` 1.0.0 → 1.1.0, `bazel_skylib` 1.9.0 (transitive), `hermetic_launcher` 0.0.16 | New dependencies. `hermetic_launcher` goes away with 4.3. |

---

## Part 1. How this audit differs from the earlier two

The three passes covered very different ground:

| Pass | Method | Findings |
|---|---|---|
| 1 | Grep for shell rules and escape hatches; read MODULE.bazel, .bazelrc and CI | 11 |
| 2 | Read every BUILD/.bzl line (3,821 lines) and every process-spawn site | +19 (30) |
| 3 (this one) | Seven parallel slices, each reading every file in its slice, plus an independent re-audit of the Bazel files grounded with `bazel query`, `bazel mod` and `--nobuild` runs under 13 incompatible flags | ~170 raw, consolidated to the 118 items in the traceability table in Part 6 |

**What pass 2 missed, by category.** These are the reasons the earlier plan only covered a subset:

- **The test runner does not follow Bazel's test protocol.** `tools/junit/JUnitMain` ignores `XML_OUTPUT_FILE`, `TESTBRIDGE_TEST_ONLY` and the sharding variables. As a result:
  - `--test_filter` is silently ignored.
  - `test.xml` contains no testcases.
  - Nothing can be sharded.
- **Some tests are never run, and some always skip.**
  - Nine non-manual tests are in no CI lane: `//json:tests`, all of `//query`, all of `//query-store`, and `//pure-protocol`.
  - `//site:verify` is tagged for CI but never selected.
  - Three test classes are never matched by any selector.
  - `CorpusDifferentialTest` can never run under Bazel and skips every time.
  - About 30 tests assert nothing or are `@Disabled` with empty bodies.
- **Failures can be silent.**
  - The CI `GENERATED` lane runs its `bazel query` behind `2>/dev/null`.
  - The browser step runs with `set -u` only.
  - Lock files are not enforced: `fail_if_repin_required = False` appears on all 8 pools, and `Bump` repins only one of the two pools derived from the engine release.
- **Test results depend on the host or on order.**
  - Locale is never pinned.
  - `java.io.tmpdir` is the host's, so the warehouse DuckDB library is cached in a shared host directory keyed by size.
  - Gate 1 depends on test order and on process-wide static state.
  - PCT pass/fail depends on which suites share the JVM.
  - ChannelB reads source files in filesystem order.
  - Wall-clock assertions appear in several places.
  - A gate-1 test binds a fixed range of ports.
- **Product and packaging defects.**
  - The core server cannot open Postgres: no driver is in `@maven_core`.
  - The native-image reachability metadata was recorded by hand ("the script that did it is gone").
  - `//datacube:dist` lacks fonts and CSS.
  - No packaging rule exists for the warehouse.
  - The JDBC service file ships in jars that do not contain the driver.
- **Generated files have drifted from their generators.**
  - Stress files 59 and 60 carry "GENERATED, do not edit" headers but no longer match their generators.
  - The stress generator reads its own output (`92-services.pure`).
  - The PCT oracle manifests were taken from an unpinned 2026-08-06 commit.
  - About 25 goldens, rosters and ratchets are updated by hand from test output, one of them with `unzip … outputs.zip`.
- **Structural problems.**
  - `runs/` (agent worktrees) is not bazelignored, so `//...` from the main checkout includes every worktree's packages.
  - About 45 undeclared transitive Maven artifacts are used directly.
  - The project contract ("every project compiles alone") is checked by nothing: 45 of the 56 projects are never compiled.
  - The stress corpus is never run through legend-engine under Bazel.

**Coverage, stated honestly.**

*Read in full:*
- every Bazel file;
- every test and tool source file;
- every harness (including all 5,179 lines of `verify-features.mjs`);
- every script's header and I/O;
- CI;
- all human-facing build documentation.

*Swept with grep patterns, reading the surrounding code for every hit (not every line):*
- product code: `core/src/main` (746 files), `datacube/src`, `query/src`;
- the bodies of large data modules: `seed.py`, and `oracle.py` / `battery.py` beyond their I/O.

*Not done:* nothing was executed (no `bazel test` or `bazel run`). One read-only in-memory Python comparison of stress files against their generators was run.

**Spot-verified by hand.** I checked each of these directly in the code:
- the `run-stress.mjs` undefined variable;
- the `site:verify` CI gap;
- the shared-temp DuckLibrary cache;
- the missing Postgres driver;
- the `PureLspServerTest` port;
- the `CorpusDifferentialTest` permanent skip;
- the ChannelB unsorted walk;
- the JUnitMain protocol gap;
- the CI lane gaps;
- the `.bazelignore` gap;
- the `fail_if_repin_required` flags.

---

## Part 2. Target end state and the principles behind it

These are the rules. Every plan item serves one or more of them, and Phase 6 turns each rule into an automated guard.

**R1. One command per job, all Bazel.**
- `bazel test //...` is the full gate on every platform.
- `bazel run //:update_generated` is the only way any committed derived file changes.
- Dev tools are `bazel run //pkg:tool`.
- No document tells anyone to run `python3`, `node`, `java`, `npm`, `mvn`, `curl` or a `.sh`.

**R2. Policy lives in BUILD files.**
- Gates are `test_suite`s.
- Memory is a macro argument that produces both the scheduling tag and `-Xmx`.
- Platform support is `target_compatible_with`.
- CI names labels only: no jq, no `bazel query`, no loops, no `--test_env` policy.

**R3. Every committed derived file has a producer action and a diff test.**
- This covers goldens, rosters, ratchet reports, fixtures, generated code, native-image metadata and lock-adjacent pins.
- No test writes into the source tree.
- No "record" or "bootstrap" flags.

**R4. Hermetic tests.**
- Every tool comes from a toolchain or a pinned repository: JDK, Node, Python, Chromium, the C toolchain for native-image, Postgres, the DuckDB library and its extension.
- Inputs are declared at file granularity.
- Locale, time zone, tmpdir and encoding are pinned by the macros.
- Ports are always ephemeral on 127.0.0.1. No network.
- Writes go only to `TEST_TMPDIR` or `TEST_UNDECLARED_OUTPUTS_DIR`.
- No dependence on test order or on another test's output.
- No silent skips: if a required input is missing, the test fails.

**R5. Orchestration is in the build graph.**
- A multi-pass measurement is a chain of actions feeding a test.
- No test launches a child JVM of itself.
- No test reads another test's files.
- No behaviour hides behind `-D` flags or env variables that no target sets.

**R6. Official mechanisms.**
- Runfiles go through `@rules_java//java/runfiles` and `@bazel/runfiles`.
- Packaging goes through `rules_pkg` and `copy_to_directory`.
- Tests follow Bazel's test protocol: XML, filter, sharding.
- Lock files are enforced (`fail_if_repin_required = True`, `--lockfile_mode=error`).
- Dependencies are declared under `strict_visibility`.

**R7. Nothing dead.** No unbuilt scripts, no Maven-era instructions or references, no prototype workspaces in the tree, no vacuous tests.

---

## Part 3. The plan, in phases

Each phase lists its work items, then what to fix, how, and how to check it is done. Phase order follows real dependencies:

| Phase | Depends on | Why |
|---|---|---|
| 0 | nothing | stops silent failure and wrong answers right away |
| 1 | — | foundations that later phases need |
| 2–4 | Phase 1 | each uses the toolchains, runfiles and macros Phase 1 adds |
| 5 | Phases 1–4 | CI can only be "just `bazel test //...`" once tests are hermetic |
| 6 | everything | adds the guards that keep it that way |

Effort is S (≤ ½ day), M (1–3 days) or L (about a week).

### Phase 0 — Stop silent failures and wrong answers (S each, about 3 days in total)

| # | What to fix | How | Done when |
|---|---|---|---|
| 0.1 | `runs/` and `node_modules` are not bazelignored; the comment is wrong | Add `runs`, `datacube/node_modules` and `query/node_modules` to `.bazelignore` (or `REPO.bazel` `ignore_directories`). Rewrite the comment. | `bazel query //runs/...` errors from the main checkout |
| 0.2 | CI never runs 9 tests or `//site:verify` | Add a CI job running `bazel test --config=ci //...` and `bazel build //...` per platform. This interim step is replaced by Phase 5. | `//json:tests`, `//query:*_test`, `//query-store:*`, `//pure-protocol:*` show in CI logs |
| 0.3 | The GENERATED lane fails silently | Root `test_suite(name = "generated", tests = ["//core:update_generated_tests", "//datacube:…", "//docs:…", "//parser-equivalence:…"])`. CI names it. `set -euo pipefail` on every run step. | A broken query, or a missing suite, turns CI red |
| 0.4 | Lock files are not enforced, and Bump leaves `@maven_runner` stale | `fail_if_repin_required = True` on all 8 pools. `common:ci --lockfile_mode=error`. Bump repins every pool whose inputs mention the release. | Editing an artifact without a repin fails the build |
| 0.5 | `run-stress.mjs:157` references an undefined `offered` | Use `new Set(offeredNames)` | The ingest invariant actually evaluates |
| 0.6 | `PureLspServerTest:197` uses a fixed port range | `new LegendHttpServer(0)` plus `getPort()` | No literal ports in `core/src/test` (enforced by guard G6, Phase 6) |
| 0.7 | The core server has no Postgres JDBC driver | Add `org.postgresql:postgresql` to `@maven_core` and `:drivers`; add a test against `@embedded_postgres` | `//core:server` opens a Postgres connection in a test |
| 0.8 | `paths-ignore: **/*.md` is false because two `.md` files are test inputs | Exclude those files from test data. Better, remove `paths-ignore` once caching makes doc pushes cheap. | No test input is matched by `paths-ignore` |
| 0.9 | Locale, tmpdir and encoding are not pinned (JVM tests and `java_run` actions) | `junit_test` and `java_run` always add `-Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8 -Duser.timezone=GMT -Djava.io.tmpdir=$${TEST_TMPDIR}` (actions use a declared scratch directory). The node test macro sets `LANG=C LC_ALL=C TZ=UTC`, overridable per target. | The same outputs and verdicts under `LANG=de_DE` and `tr_TR` (a CI matrix entry) |
| 0.10 | `java_run` uses `target.files` (fails a Bazel 10 flag) | `target[DefaultInfo].files` | `--incompatible_disable_target_default_provider_fields` passes for our code |
| 0.11 | DuckDB extension over `http://`; no select default | `https://`, plus a mirror; `no_match_error=` on every platform select | — |
| 0.12 | `live_snap_test` is missing from `//datacube:tests`; `corpus_lanes` duplicates `judge_lanes` | Add it to the suite; delete one of the duplicates | — |

### Phase 1 — Foundations: toolchains, runfiles, test runner, macros (L)

**1.1 A test runner that follows Bazel's test protocol (M).**
- Replace `tools/junit/JUnitMain` and the `use_testrunner=False` path with `contrib_rules_jvm`'s `java_junit5_test`. If a gap forces it, extend JUnitMain instead, and make it:
  - write the legacy XML to `$XML_OUTPUT_FILE`;
  - map `TESTBRIDGE_TEST_ONLY` to selectors;
  - implement `TEST_TOTAL_SHARDS`, `TEST_SHARD_INDEX` and `TEST_SHARD_STATUS_FILE`;
  - touch `TEST_PREMATURE_EXIT_FILE`.
- Delete the "prerun" mechanism; Phase 3.1 replaces it.
- **Done when:**
  - `bazel test //core:core_tests --test_filter=…` runs one class;
  - `test.xml` lists testcases;
  - `shard_count = 4` works on a PCT target.

**1.2 Official runfiles everywhere (M).**
- **Java.** Delete `testing/Repo.java`'s environment parsing, `testing/Upstream.java` and EmbeddedPostgres's private resolver. Every path comes from `$(rlocationpath …)`, passed in through `jvm_flags` or `env` and resolved with `com.google.devtools.build.runfiles.Runfiles`.
  - `Repo.module`/`Repo.path` become `Runfiles.rlocation` calls on labels the BUILD file passes in. This also removes the "module inferred from `TEST_TARGET`" magic.
  - `junit_test`'s `-Dlegend.engine.root=../<repo>` becomes `$(rlocationpath @legend_engine_src//:pom.xml)`.
  - Generator side reports become declared outputs, not a temp directory.
- **JavaScript.** Add `@bazel/runfiles`. Pass `$(rlocationpath //wasm:planner)`, `:cube_jvm_answers`, `//core:server`, `//warehouse:server_native`, the fixtures and the bundles through `env`. Delete every `new URL('../../…', import.meta.url)`, every `RUNFILES = resolve(ROOT,'..','..')`, and the hard-coded `_main` path.
- **Done when:**
  - every test passes with `--noenable_runfiles` (manifest only) on Linux;
  - `build:windows --enable_runfiles` is removed from `.bazelrc`.

**1.3 Toolchains for every external tool (L).**

| Tool | Today | Target |
|---|---|---|
| Python: the stress generator and its 27-module closure, the warehouse Arrow check (pyarrow), `datacube/bench/model` (duckdb), and whichever analysis tools the script review keeps | host `python3`; pip in CI for pyarrow only | `rules_python` is a **foundation**, not an optional add-on. The repository has 135 Python files and the stress corpus generator must become a Bazel action. So: a hermetic interpreter (one pinned version), and one `pip.parse` hub with `requirements_lock.txt` (pyarrow==23.0.1, tzdata, duckdb at the pinned version, and anything else the review keeps). Every kept script becomes a `py_binary`, `py_test` or `py_library`. pyarrow is just another locked dependency; there is no reason to single it out or to port the Arrow check to Java. |
| Chromium for Playwright 1.63.0 (revision 1243, headless shell; to be confirmed against `browsers.json`) | `install_browser` into `$HOME` plus apt `--with-deps` | `rules_playwright`, or a per-platform `http_archive` of `chromium-headless-shell-<platform>.zip` with sha256, exposed as `PLAYWRIGHT_BROWSERS_PATH` or `executablePath` through runfiles. Add a diff test that the pinned revision equals the locked playwright-core's `browsers.json`. Linux system libraries come from a pinned CI container image. |
| C toolchain for `native_image` | host Xcode/CLT or gcc + zlib; a local patch to rules_graalvm | `toolchains_llvm` (or `hermetic_cc_toolchain`) with a sysroot that includes zlib, registered for the native targets. Upstream the CLT patch, or drop it once the hermetic toolchain is in place. Register GraalVM per platform with constraints. |
| Postgres binaries | a repository rule keyed by host OS (`rctx.os`) | One `http_archive` per platform (zonky jars) plus a hub `alias` with `select` on `@platforms`. `target_compatible_with` on `pct_postgres*` and the live tests. |
| DuckDB native library | extracted at run time into a shared host temp directory (`DuckLibrary.extracted`) | Always the `jar_entry` `:duckdb_library` output, passed through runfiles in JVM mode too. Delete classpath extraction, or key it by content hash under `TEST_TMPDIR`. |
| legend-engine (for engine-dependent harnesses and the engine-side stress run) | a hand-started shaded jar on :6300 | `@maven_runner` / `//tools/engine-runner` (already in the graph), started inside the test on port 0 |

**1.4 Macros and shared definitions (M).**
- **`legend_java_library`**: NullAway built in, private by default. It removes 27 copies of repeated arguments in `core/BUILD.bazel`.
- **`legend_junit_test(memory_mb=…)`**: emits both `resources:memory:<n>` and `-Xmx<n>m`, and pins locale, TZ and tmpdir. Remove `--jvmopt` and `--host_jvmopt` from `.bazelrc`, so CI and local runs can share cache hits.
- **`node_test`**: the shared `node_options`, the reporter as a label, and `wasm = True` to add the planner and `--experimental-wasm-exnref`. It removes about 10 copies in datacube, plus copies in pure-protocol, query-store and query. Retire `query/test/strict-reporter.mjs` and the relative `../datacube/test/strict-reporter.mjs` paths.
- **`//tools/platforms`**: the `config_setting`s, `INCOMPATIBLE_WINDOWS`, and helpers that turn a platform dict into both the select and `target_compatible_with`.
- **`browser_test`** (Phase 4): the pinned Chromium, an ephemeral-port helper, and outputs written to the undeclared-outputs directory.

**1.5 Dependency hygiene (M).**
- `strict_visibility = True` on every pool. Declare the roughly 45 artifacts used directly but only resolved transitively.
- Replace `pools_are_disjoint`, which reads 4 of 8 locks with a regex, with an aspect-based test: "no `java_test`, `java_binary` or `native_image` runtime classpath holds two versions of one Maven coordinate". That is the real invariant. Then correct the MODULE.bazel comment.
- Check pnpm locks against `package.json` (frozen-lockfile `@pnpm//:pnpm` check as a test). Fix `"private": "true"`. Use exact Playwright versions in both apps, or one shared lock.
- Add mirrors and `integrity` for the GitHub archive downloads.
- Add a disk-cache GC limit. Drop `--enable_bzlmod`.

### Phase 2 — Every committed derived file gets a producer and a diff test (L)

Each item ends in a `write_source_files` entry reachable from `//:update_generated`. Any record, bootstrap or "regenerate by hand" path is deleted.

| # | File(s) | Today | Fix |
|---|---|---|---|
| 2.1 | Stress corpus: `92`–`98` written by `scripts/corpus/build.py`; `59`, `60`, `64` claimed by `dense_mapping.py`, `dense_store.py` and `combos.py` | Host python; `--check` run by nothing; 59 and 60 already drifted; 92 is both input and output; `zoneinfo` uses host tz data; `LINKED_PROJECTS` kept in sync by hand in 3 places | (a) Split `92-services.pure` into hand-written queries (a source file) and generated expectations (output). (b) Make `model.load` read only the hand-written sources. (c) Add a `py_binary` (rules_python, `tzdata` pinned) and an action producing all 10 files. (d) For 59 and 60, the generators must reproduce the committed files; the files must not be changed to match the generators. Measured 2026-10-02, regenerating gives 50 of 60 classes different in 59 and every table pick different in 60. That is mostly not hand editing: `dense_mapping.build`/`dense_store.build` choose candidates from the WHOLE corpus (`model.load()` scans every stress file and project), and the corpus has grown since 2026-08-14. The one real hand fix is d0367624b (2026-09-22): `dense_Rollup`'s `~filter` must read the view's own root table, because the platform now refuses a filter over a table the view never reads. Teach the generators in three steps, then gate them:
   1. **Stable input.** Each generator selects from a declared, committed seed list of tables and classes (its picks as of the committed file). It must not select from "whatever the corpus holds", or adding any stress file would churn 59 and 60 through the diff test.
   2. **Encode each hand fix as a generator rule.** `dense_store` emits a NotNull filter over each view's own root table and references it from the view, with the explanatory comment.
   3. **Iterate until regeneration is byte-identical** to the committed 59 and 60, then add the `write_source_files` diff test.

   The same rule applies to every generated file in this plan: reach the fixpoint by changing the generator, never by accepting a diff that drops a hand fix. (e) Keep a single `LINKED_PROJECTS` in one data file read by Python, Java and the engine run. (f) Run `density`, `executed` and `stacking` `--gate` as `py_test`s, or fold them into the generator's checks. |
| 2.2 | `fixtures/saved-queries/*.json` | `java -jar … 18777 &` plus host node; wall-clock timestamps | `js_run_binary` that starts `//core:server` (port 0, `--query-store $TMP`), POSTs, normalises timestamps and writes JSON. Assert the README's counts. |
| 2.3 | `query/src/ui/icons.ts` | `curl \| tar` plus `node > src` | `http_archive` for react-icons 5.5.0 (sha256), then `js_run_binary` |
| 2.4 | `datacube/src/share/link-p1.ts` (and future pN) | `bazel run … > src`; only a hash pin | `js_run_binary` per frozen version, plus a `diff_test` (the frozen output must never change), plus a `write_source_files` entry for creating a new version. Delete the redirect recipe. |
| 2.5 | `warehouse/.../reachability-metadata.json` | Recorded by hand with GraalVM's agent; locale bundles from the recording host | Generate the `foreign` section from `Duck.java`'s descriptor table with a `java_run` generator. Add an action that runs the warehouse suite under `native-image-agent` (from the `@graalvm` toolchain, locale pinned) and merges the output. Diff-test both. |
| 2.6 | `tools/oracle-pins.env` | Hand-kept duplicate of MODULE.bazel, held equal by a test | Generate it from MODULE.bazel constants. Tag SHAs come from Bump's output, stored in one `.bzl` that MODULE.bazel loads (`release.bzl`), plus a `write_file`. Delete `OneReleaseTest`. |
| 2.7 | `pct/src/test/resources/oracle/*_manifest.duckdb.json` | Snapshot of an unpinned 2026-08-06 commit | Read from `@legend_engine_src` as data, or copy them with an action plus a diff test |
| 2.8 | `tools/deps/core-layers.txt` plus the duplicated `_CORE_LAYERS` | Hand golden; a new target escapes it | One `genquery` over `kind(java_library, //core:*)`, then a `write_source_files` golden |
| 2.9 | Test-written goldens and rosters: `reference-lane/core_relational.txt`, the 29 `rcorpus/*` rosters and registers, `ladder/*.current.sql`, the ChannelB, PCT census and own-corpus pins, and roughly 20 ratchet constants in Java | Edited by hand from LOST/GAINED output; `unzip … outputs.zip`; `-Dladder.record` writes through runfiles | Each measurement becomes a `java_run` action (or a test's declared output promoted by a rule) that emits the report. The committed copy is a `write_source_files` target. The test asserts invariants only (shrink-only, monotone). Ratchet numbers move from Java constants into the generated reports. Delete `ladder.record` and `natives.bootstrap`. Version strings in goldens are read from the pins, not hard-coded. |
| 2.10 | `engine-runner/vocab.tsv`, `scripts/parser/mutants.tsv`, `tools/metamodel-census/*.json`, `docs/{corpus-coverage.json, census-baseline.json, OUTSTANDING.md, WALL_DEPTH.txt, SCOREBOARD.md}`, `docs/{ENGINE_FUNCTIONS, ENGINE_SURFACE, FUNCTIONS_EXECUTED, SURFACE_BLOCKED}.tsv` | Hand-generated or dead generators | Per the Part 5 inventory: wire (`vocab.tsv` through `java_run` over a `java_jars` list, and fail if no lexer is found) or delete. A file that stays as a frozen snapshot loses its "generated" claim and leaves `//docs:ledgers`. |
| 2.11 | In-test regeneration that duplicates diff tests (`preludeIsCurrent`, `ledgerIsCurrent`, `signatureTextIsCurrent`, `registryMatchesTheCheckout`, `CorpusManifestTest`, `ProtocolRosterCensusTest`) | Double cost; can disagree with `gen_claims` (core vs core_next) | Delete the comparisons; keep only invariant checks |

### Phase 3 — Restructure the test graph: hermetic, granular, independent (L)

**3.1 Replace the two-pass corpus (prerun) with actions.**
- The host-judge pass of the relational corpus becomes a `java_run` producing `judge-host.tsv`, a cacheable build output, exactly as `//wasm:jvm_answers` already works.
- `corpus_duckdb`, `corpus_h2` and `corpus_warehouse` become database-pass tests that take that ledger as `data`.
- The test-time property flip `legend.exec.engineScanOrder` becomes the target's `jvm_flags`, or an option on the dialect.
- Done when no `legend.prerun` remains.

**3.2 Split gate 1 and give every lane exact inputs.**
- Replace the single "enormous" `core_tests` with per-package `junit_test`s, grouped as `test_suite(name = "core_tests")`.
- Drop `data = glob(["src/**"])`:
  - behaviour tests read nothing by path;
  - the ladder pins and the stress corpus are read as classpath resources (they are already resources);
  - each lane that reads files gets an explicit list.
- Move `StressCorpus.EXCLUDED` into a class without a `Repo` static initialiser.
- Declare `:duckdb_load` on the test library and add a dedicated `:duckdb_load` test (provider found; rows identical to the text path).
- Same treatment for `spec_tests`, `parser_parity` and `pct` (`glob(["src/**"])`), and `//docs:ledgers` (only the TSVs actually read).
- Done when editing one core `.java` file reruns only the targets that depend on it.

**3.3 Remove test-order and shared-state coupling.**
- **Gate 1.**
  - Remove `@TestMethodOrder` from `LegendHttpServerIntegrationTest`; each test seeds itself.
  - Add a test-scoped reset to `ConnectionResolver.STORE`, or use unique store names.
  - `ConnectionIsolationTest` must stop leaking `LEAK_T`.
  - Use one connection per method in `LowerRelationTest`, `ExecuteInDbTest` and `ResolveNestedNavTest`.
  - Move the `ToySectionGrammar` service file into its own testonly library, used only by its test (as `:shadow_binding` already is).
  - Make `CanonicalDivergence` and `Census` counters injectable, or assert per-test deltas only.
- **PCT and ChannelB.**
  - Reset counters per suite and assert per-suite ceilings, so `pct_duckdb` and `pct_duckdb_<suite>` measure the same thing and sharding is legitimate. Then delete the composite and keep per-suite targets in a `test_suite`.
  - Sort `ChannelB.java:105` by relative path.
  - Make the `Corpus` statics instance state.
- **parser-equivalence.** `PmcdReachabilityCensusTest` takes the roster from `:gen_roster` as data, not from a file another test wrote.
- **JavaScript.**
  - `pivot-rows` R9 and `snap.test` restore state, or use their own tables.
  - `cube-fixture.ts` restores the `unhandledRejection` listeners and asserts that none were collected.
- **Unordered results.** About 20 sites assert row order on queries with no ORDER BY (listed in Part 6). Sort them, or compare as multisets.

**3.4 No hidden modes, no silent skips.** Each `-D` or env mode becomes a named target with its arguments in BUILD, or is deleted:

- *JVM tests:* `stress.*` (adds a real `//core:stress_suites_h2` lane enforcing `MIN_PASS_H2`), `our.resolutions`, `manifest.census`, `natives.dump`, `prelude.census`/`prelude.m3`, `eager.world2`, `rcorpus.test`/`rcorpus.trace`/`rcorpus.warehouse.data`/`rcorpus.detachTrace`, `chb.only`, `legend.corpus.containing`, `LEGEND_LITE_PROGRESS`, `LL_SHADOW`, `LL_PCT_CASES`, `WAREHOUSE_ARROW_CHECK`.
- *Harnesses:* `ONLY`, `SHOTS`, `DATA`, `ENGINE`, …

Every `Assumptions.*` skip on a required input becomes a failure: about 13 sites in spec, parser-equivalence and pct, plus `WarehouseArrowTest`. The same goes for silent empties (`Corpus.filesWith`, `InlineSnippets`, `registerTests`, `EngineElementRosterTest`) and missing-root `continue`s in the guards.

**3.5 Wall-clock assertions.** Remove these, or move them to `tags = ["manual", "benchmark"]` targets:
- `MinimalCorpusTest` 60 s per test
- `StackShapeWitnessTest` < 5 s
- `tile-layout` < 16 ms per step
- `torture` < 1 s
- `StreamingIntegrationTest`'s 1 ms sampler

Also inject clocks and timers (`WarehouseEngine` refresh in live-snap, the debounce tests through `mock.timers`), and replace untimed `await` with timed waits.

**3.6 Dead, vacuous and orphaned tests.**
- Delete or fix:
  - `TypeInferenceIntegrationTest.testContainsPrimitive` (no `@Test`);
  - the 15 empty `@Disabled` tests;
  - `testXStore` and `testAggregationAware`;
  - the print-only probes, `assertTrue(true)`, and `|| msg != null`.
- `EagerCorpusCompileProbe`, `ProbeWireShapes` and `ZFixtureAdjudicationProbe` (never selected) become `java_binary` diagnostics or are deleted.
- `FixtureSweep`, `RefImports`, `RenderCensus` and `PlanOnJavaBase` become proper targets.
- `CorpusDifferentialTest`: a generator action (2.1's toolchain) produces `seed.sql` and `expected/` as its data. Otherwise delete it together with its SkipCensus row.
- Diagnostics that assert nothing (`ParseSpeedBenchmarkTest`, `CorpusCensusTest`, `GrammarKeywordCensusTest`, `MigrationSizingTest`, `PmcdReachabilityCensusTest`) become `java_run` report actions.

**3.7 Child JVMs and processes started by tests.**
- `PlannerRunsOnJavaBaseTest` becomes a test target with `main_class = PlanOnJavaBase` and `jvm_flags = ["--limit-modules=java.base"]`.
- `DuckWorkspaces` (`corpus_warehouse`) starts the native warehouse from `$(rlocationpath)` on port 0, as live-snap does, not `java -jar`.
- `EmbeddedPostgres`:
  - retry on bind;
  - keep its cluster under `TEST_TMPDIR`;
  - stop through a JUnit extension, not a shutdown hook;
  - fail clearly when run as root.

**3.8 Warehouse tests.**
- Split `tests_lib` into the HTTP suite (`TestServer`) and unit tests. `tests_native` selects only the HTTP suite.
- `postgres_live` and `postgres_live_native` become non-manual on `@embedded_postgres` plus the extension (gunzipped by its own action, 4.3).
- Pin the user name (`-Duser.name`, or an injected supplier) for `AppModeTest`.
- Add `--enable-native-access=ALL-UNNAMED` on the JVM server and tests.
- Use `@TempDir` everywhere; `CommandLine.temporaryData` cleanup becomes testable.

**3.9 Coverage that is missing today.**
- `legend_library` per project in `projects/`: each compiles alone, and the graph compiles together. This replaces `scripts/projects/check.py`.
- A `teavm_wasm` compile of `//warehouse:sqlapi`, so the "must compile to WASM" rule is tested by compiling.
- An engine-side stress run as a manual test over `//tools/engine-runner:testable`. This replaces the macOS-arm64-only `engine-rows.sh`.
- Smoke tests for the engine-runner binaries.

**3.10 Static-analysis guards.**
- Keep the guardrail and census lanes, but pass each its declared file list (`$(rlocationpaths …)`) instead of walking directories.
- Add `src/main/duckdb` to the roots.
- Censuses over other modules' sources (own-corpus snippet counts, ChannelB ledgers read from Java sources) become generated reports (Phase 2.9). A snippet added in core then changes a generated file, not a constant in another module.
- Turn Error Prone's `StringCaseLocaleUsage` and `DefaultLocale` into errors in the shared javacopts. Fix the product sites (`JoinType`, `RelationalDataType`, `Executor`, `AnsiSqlRenderer`, `CorrelatedSubselects`, `PureDateLiteral`, `PureTimeLiteral`, `ReplayOracle`, `H2Verify`, …), and in datacube use explicit locales in `toLocaleString`.

**3.11 Classpath-order dependence.** Move the `org.finos.legend.*` shims (TestGrammarRoundtrip and others) into a harvest-only library, and assert at harvest start that the loaded class comes from the shim jar.

### Phase 4 — The browser, app and packaging layer (L)

**4.1 Harnesses become tests.** Each harness in Part 6's table becomes one of:
- **(a) `browser_test`** (hermetic, in a suite):
  - `run_stress`, `verify_charts`, `verify_cubes`, `verify_page`, `verify_real_data`, `verify_remote`, `verify_smoke`, `verify_upload`, `verify_wasm_browser`;
  - `verify_picker` (serves the site itself);
  - `//query:verify`, `//site:verify`;
  - `torture` and `verify_calc_vocabulary`'s local half (node only, no browser).
- **(b) `browser_test` tagged manual** (needs a real engine started in-test from the pinned jar, or embedded Postgres): `chaos`, `verify_engine`, `verify_engine_differential`, `verify_app`.
- **(c) a plain `bazel run` dev tool with minimal deps:** `shots`, `measure_startup`, `make_sample`, `serve`.
- **(d) deleted:** `install_browser`.

Rules for all of them:
- Port 0 on 127.0.0.1, read back from the server's "listening on" line.
- Every binary comes from `$(rlocationpath)`.
- Fixtures (Parquet and CSV) are produced in `TEST_TMPDIR` or by a build action. No `COPY` into cwd.
- Outputs (screenshots, results) go to `TEST_UNDECLARED_OUTPUTS_DIR`.
- No env knobs; variants are separate targets.
- Zero checks run means failure, and "skipped" counts as failure.

Additionally:
- Split `verify-features.mjs` (118 order-dependent checks on one page) into sharded tests by section, using the existing `freshCube()`.
- Merge the duplicated harnesses (`verify_real_data` with `verify_remote`; `verify_engine` with `verify_engine_differential`) and the roughly 15 copy-pasted static servers into one shared module.
- Give each target only its own data: the node-only tools should not depend on `:site` or `playwright`.
- Delete the BUILD comment "a browser binary is not something Bazel fetches here".

**4.2 DataCube `dist`.**
- Delete `make-dist.mjs`. `dist` = `copy_to_directory` over `:site`, plus esbuild's CSS bundling for inlining. This fixes the missing `fonts.css`, `vendor/fonts`, `config.json` and `projects/`.
- Add a test that every URL referenced by `dist/index.html` and its CSS exists in `dist`, plus a `browser_test` over `dist`.

**4.3 The warehouse and the app without bash.**
- Gunzip the DuckDB extension in a `java_run` action (`GZIPInputStream`). No `run_shell`.
- Replace `warehouse_run`'s generated bash launcher. The server resolves its library, extension and site from runfiles itself (rules_java runfiles in JVM mode; a small C-free resolver in the native binary reading `RUNFILES_DIR` or the manifest). Its default flags come from a `native_binary`/`java_binary` `args` with `$(rlocationpath)`. `//warehouse:serve` and `//datacube:app` then become ordinary executables, and Windows compatibility is only a toolchain question.
- Add `//warehouse:dist` (`rules_pkg` `pkg_tar`/`pkg_zip` per platform): server_native, the DuckDB library and the unpacked extension, in the "beside the executable" layout. Add a test that extracts it and runs it with no flags.
- Split the `server_lib` resources so `META-INF/services/java.sql.Driver` ships only with `:client`.

**4.4 JavaScript workflow.**
- Delete `scripts` from `datacube/package.json` (its `test` script is already broken for 22 WASM tests) and the "pnpm editor loop" `.gitignore` notes.
- Document `bazel test //datacube:tests` and `ibazel`.
- Point README-realdata at Bazel only: no `npm install`, no `npx serve`, no host `duckdb`.
- Fix `emit_cube_queries` and `emit_offer_queries` data, so they run under `bazel run` too.

### Phase 5 — CI is a list of labels (M)

**Workflows.**
- Delete the jq lane matrix, the low-memory shell split, the platform jq filters, the `GENERATED` query, the pip step, the `install_browser` step and the browser `bazel run` loop.
- Each platform job runs `bazel test --config=ci //...` and `bazel build --config=ci //...`.
  - Platform skipping comes from `target_compatible_with`.
  - Memory comes from the macro tags plus `--local_resources`.
  - The macOS 7 GB runner uses `--config=ci-small`, whose only job is resource budgeting.
- If wall-clock time needs splitting, the matrix lists gate `test_suite` labels declared in a `//gates` BUILD package (`//gates:core`, `//gates:pct`, `//gates:browser`, …), plus a final `bazel test //...` that is cached, so it is cheap. Adding a test never requires a CI edit.

**Caching and pinning.**
- Run in a pinned container image on Linux (it provides the Chromium system libraries).
- Key the cache by `github.sha` with restore-keys, or better, use a remote cache.
- Pin actions by SHA.
- actionlint becomes a Bazel target over a pinned `http_file` (`bazel run //tools:actionlint`), run locally and in CI.
- Add a `LANG=tr_TR` entry to the matrix (proves 0.9).

**Done when:** the workflow files contain only checkout, cache and `bazel` invocations on literal labels.

### Phase 6 — Guards that keep it this way (M)

Each guard is a Bazel test in a `//tools/guards` package, part of `//...`:

| Guard | Fails when |
|---|---|
| G1 | A committed file has a "GENERATED" or "do not edit" marker but no `write_source_files` entry |
| G2 | A tracked `.py`, `.sh`, `.mjs` or `.js` file is not a `srcs` or `data` of some target, unless it is under an allowlisted path |
| G3 | A class with `@Test` methods is selected by no `junit_test` (aspect over test targets, plus a source scan) |
| G4 | A test or BUILD file sets or reads an env var or `-D` property not declared in BUILD (an allowlist of the Bazel protocol variables) |
| G5 | `ProcessBuilder`, `child_process` or `Runtime.exec` appears outside the allowlist (EmbeddedPostgres, server spawns in fixtures) |
| G6 | A literal port appears in tests or harnesses, or `listen(` without `'127.0.0.1'` |
| G7 | `Files.write*` targets a path derived from `Repo` or runfiles, i.e. a write into inputs |
| G8 | A workflow `run:` step contains anything other than `bazel <cmd> <flags> <labels>` (actionlint plus a YAML check) |
| G9 | A document (outside `docs/history/`) contains `mvn`, `python3 `, `node `, `npx`, `npm install`, `java -jar` or `.sh` commands, or names a script that does not exist |
| G10 | `fail_if_repin_required` is false, or a Maven label is used without being declared (strict visibility covers this) |
| G11 | A runtime classpath holds two versions of one coordinate (the aspect from 1.5) |
| G12 | `glob(["src/**"])` is used as test data |
| G13 | `Assumptions.assume*`/`abort`, `@Disabled` without a reason row, or a test method with no assertion (an ArchUnit or Error Prone check) |
| G14 | The Bazel 10 incompatible-flags dry run (`--nobuild` under the flags in Part 4) regresses for our own code |

---

## Part 4. Bazel 9→10 readiness (from the `--nobuild` runs)

- **Passing today:** `auto_exec_groups`, `check_testonly_for_output_files`, `config_setting_private_default_visibility`, `no_implicit_file_export`, `resolve_select_keys_eagerly`, `disable_starlark_host_transitions`, `check_external_repo_source_dir_package_boundary`, `disable_non_executable_java_binary`.
- **Failing:**

  | Flag | Where it fails | Fix |
  |---|---|---|
  | `disable_target_default_provider_fields` | our `java_run` and bazel_lib | 0.10 for our code; track bazel_lib upgrades |
  | `stop_exporting_build_file_path` | bazel_lib | track upgrade |
  | `no_rule_outputs_param` | rules_java | track upgrade |
  | `noincompatible_enable_deprecated_label_apis` | rules_license | track upgrade |
  | `stop_exporting_language_modules` | rules_graalvm, and our patch to it | 1.3, which removes the patch |

  Guard G14 stops new failures in our own code.

---

## Part 5. Script and tooling inventory: what happens to each

**Decision status (user, 2026-10-02):**
- **`experiments/` is kept.** It stays bazelignored, as separate workspaces. The `.bazelignore` comment is corrected, and its scripts go through the same review as everything else; nothing there is deleted.
- **No script is deleted before it has been reviewed.** The classes below are the auditors' first read, not decisions. Each script gets a review row:
  - what it does and what it is evidence for;
  - whether it runs today (many need the Maven-era `cp.txt` or `mvn`, or hard-coded `~/jdk` or `/Users/neemsandv` paths);
  - what committed files it wrote or wrote into;
  - who references it;
  - the options: wire as-is, repair and wire, keep as data/history, or retire.

  The decision is the user's. The work for each script follows from that review.

**A** = must become a Bazel action with a diff test. **B** = keep as a `bazel run` target. **C** = candidate to retire (only after review).

| Path | Class | Plan item |
|---|---|---|
| `scripts/corpus/build.py` and its 26-module closure | A | 2.1 |
| `scripts/corpus/{density,executed,stacking}.py --gate` | A | 2.1(f) |
| `scripts/corpus/{dense_mapping,dense_store,combos}.py` | A or C | 2.1(d) |
| `scripts/corpus/differential.py` | A or C | 3.6 |
| `scripts/corpus/{add_taxonomy_edges,brokerage,curves,curves2,largeexp,schedule,timeseries,refdata,taxa_*}.py` (one-shot codemods) | C | 7 |
| `scripts/corpus/{coverage,functions,mutate,run,scoreboard}.py`, 16 × `probe_*.py`, `scripts/corpus/repro/` | C (broken: need Maven `cp.txt`) | 7; `repro/` instructions rewritten as `bazel run //tools/engine-runner:testable -- …` |
| `scripts/{census_gate,generate_pure_constants,outstanding,walldepth}.py` | C | 7 |
| `scripts/parser/*` plus the fixture corpus | C, or fold the fixtures into parser-equivalence as data | 7 |
| `scripts/projects/check.py` | replaced | 3.9 |
| `scripts/projects/{loadtime,spec}.py` | B or C | 7 |
| `tools/census/{lanes.sh, render.sh, lanes_diff.py}` | B: lanes become test targets with `env`; render becomes a `java_binary`; diff becomes a `py_binary` | 3.4 / 7 |
| `tools/wrongrows/engine-rows.sh` | replaced | 3.9 |
| `tools/wrongrows/{compare,damage}.py` | B | 7 |
| `tools/untangle/{bare_tiers,probe_counts}.py` | B | 7 |
| `tools/untangle/move_classes.py` | C after the untangle | 7 |
| `tools/upstream-drift.py` | B (read `@legend_*_src`, not host checkouts) | 7 |
| `tools/native-axes.py` | B or C | 7 |
| `tools/{ci-watch.sh, scoreboard.py, golden_shape_survey.py}`, `tools/metamodel-census/*`, `tools/spikes/*`, `tools/reference/{join,source_drift}.py` | C | 7 |
| `tools/engine-runner/.gitignore` | C (Maven leftover) | 7 |
| 24 scripts plus TS/Java probes under `docs/` | C (move receipts out of `docs/`) | 7 |
| `datacube/bench/*.mjs` | B: `js_binary`, manual | 7 |
| `datacube/bench/model/*.py` | B or C | 7 |
| `experiments/` (8 directories, incl. 2 Bazel prototype workspaces and a Maven harness) | **Keep** (user). Stays bazelignored; scripts inside are reviewed like the rest. | 7 |
| `repro/` (19 upstream repros) | Keep as data. Instructions use `bazel run`, or the repros become manual engine-runner tests. | 7 |

### Phase 7 — Cleanup and documentation (M; runs alongside the other phases)

- **Review first.** Produce the per-script review (see Part 5's decision status) for all 135 Python files, the shell and mjs tools, the docs/ probes and `experiments/`. The user decides each row, then the decisions are applied. Kept scripts become `py_binary`/`js_binary`/`java_binary` targets under the Phase 1 toolchains. Scripts that need the Maven-era `cp.txt` are repaired onto `//tools/engine-runner` targets, or retired by decision.
- **Rewrite** to Bazel-only instructions:
  - FAQ.md (the build table, single-test command, project tree with `engine/` and `nlq/`, and "no code generation step")
  - README.md (the `engine/` status, surefire statistics, "no ANTLR in any pom")
  - core/README.md (`mvn -pl core test`, `core/pom.xml`)
  - AGENTS.md:39-40
  - the undated present-tense sections of GATES.md (:86-150)
  - ENGINEERING_LOG.md (the build shape at :55-67)
  - RUNNING_THE_CORPUS.md
  - UPSTREAM_FINDINGS.md
  - UPSTREAM_BOUNDARY_PROGRAM.md (version-report, classpath-convergence, allgates)
  - the READMEs in tools/engine-runner, tools/census, tools/wrongrows and tools/reference
  - scripts/parser and projects/CONTRACT.md
- **Move** dated history into `docs/history/`, which guard G9 exempts.
- **Remove dead references:** stale Maven-era mentions in MODULE.bazel comments, BUILD comments ("Maven gate N"), the `tools/par` comment, `TeaVmCompile`, `Nullable`, and references to scripts that no longer exist (`tools/allgates.sh`, `tools/diagnostics.sh`, `tools/oracle-roots.sh`, …).
- **Smaller fixes:**
  - Visibility: private by default; `testonly` on `//tools/junit`, `//tools/par` and the upstream-pool binaries.
  - Generator sources move from `spec/src/test/java` to `spec/src/gen/java`. `gen_prelude` and `gen_claims` take a label for core's sources, not `"core/src/main/java"`. Compile the shared Claims sources once.
  - Set real test `size`s from measured durations (`lite_test` is too small; `junit_test` defaults everything to `large`).
  - Product debug switches (Shadow, PrepTrace, `LEGEND_LITE_DUMP_SQL`, and about 20 more counters) move behind an option object or into test-only targets. Product code stops reading `TEST_UNDECLARED_OUTPUTS_DIR`.

---

## Part 6. Traceability: every finding mapped to its plan item

**Source key:** P1 and P2 are this conversation's earlier passes. The rest are the third pass's auditors:

| Code | Auditor |
|---|---|
| CT | core tests |
| SP | spec, pct, parser-equivalence, tools Java |
| WH | warehouse, wasm, core main |
| DC | datacube src and tests |
| HN | harnesses, query, site |
| SC | scripts, docs, CI |
| BZ | Bazel files |

| Finding | Source | Plan |
|---|---|---|
| Stress corpus generated outside Bazel; input = output; drift in 59/60; tz data; LINKED_PROJECTS ×3 | P2, SC, CT | 2.1 |
| saved-queries fixtures by hand | P2, DC, HN, BZ | 2.2 |
| icons.ts by hand | P2, HN, BZ | 2.3 |
| link-p1 / make_link_dictionary redirect | P2, DC | 2.4 |
| record/bootstrap writes into the tree | P2, CT, SP | 2.9, G7 |
| JUnitMain prerun pipeline | P2, SP | 3.1, 1.1 |
| cross-test roster file | P2, SP | 3.3 |
| warehouse_run bash launcher plus run_shell gzip | P1, P2, WH, BZ | 4.3 |
| make-dist hand packager; dist missing fonts/CSS | P2, HN | 4.2 |
| Bump: nested bazel/git, regex edits, stale maven_runner | P2, SP, BZ | 0.4; Bump stays a `bazel run` release tool but calls `$BAZEL_REAL`/bazelisk, repins all pools and edits `release.bzl` (2.6) instead of regex |
| CI lane logic in jq/shell, browser loop, pip | P1, SC, BZ | 5 |
| GENERATED query silent | SC, BZ | 0.3 |
| 9 tests in no lane; site:verify never run | BZ, HN | 0.2, 5 |
| Repo / Upstream / EmbeddedPostgres hand-rolled runfiles; `../repo` paths; JS URL arithmetic | P2, CT, SP, DC, HN, WH | 1.2 |
| Harnesses: js_binary, Chromium in $HOME, fixed ports (incl. 8741/8732/8734, all interfaces), env knobs, cwd/runfiles writes, silent passes, 5k-line single-page suite, duplicates, two Playwrights | P2, HN, DC | 1.3, 4.1, 1.5 |
| run-stress undefined variable | HN | 0.5 |
| pyarrow / host python, silent skip, `--test_env=PATH` | P1, WH, BZ, SC | 1.3, 3.4 |
| Coarse `glob(src/**)` data; whole upstream trees; docs ledgers | P2, CT, SP, BZ | 3.2 |
| Shell genrules with exec-config java_binary | P2, SP, BZ | `adapter_par` and `ref_dump` become `java_run` (stderr kept) |
| pools_are_disjoint 4/8; 396 shared coordinates | P2, SP, BZ | 1.5, G11 |
| `fail_if_repin_required = False`; no lockfile_mode | BZ | 0.4 |
| `runs/` not bazelignored | BZ | 0.1 |
| JUnitMain ignores XML/filter/shards | SP, BZ | 1.1 |
| Locale not pinned (JVM, java_run, node); locale-less product code | CT, SP, WH, DC, BZ | 0.9, 3.10 |
| `java.io.tmpdir` / host temp leaks | WH, CT, SP, DC, HN | 0.9, 3.8, 4.1 |
| DuckLibrary shared temp cache keyed by size | WH | 1.3 |
| No Postgres driver in core | WH | 0.7 |
| Native-image metadata by hand | WH | 2.5 |
| No warehouse packaging; beside-exe path untested | WH | 4.3 |
| JDBC service file in server_lib | WH, BZ | 4.3 |
| enable-native-access only on the image | WH | 3.8 |
| sqlapi WASM rule unchecked | WH | 3.9 |
| Host C toolchain for native-image; graalvm toolchain unconstrained; patch | WH, BZ | 1.3 |
| Embedded Postgres keyed by host OS | SP, BZ | 1.3 |
| Fixed-port gate-1 test | CT | 0.6 |
| CorpusDifferentialTest permanently skipped | CT, SC | 3.6 |
| Hidden `-D`/env modes (stress, rcorpus, chb, census, …); H2 stress lane unenforced | CT, SP, WH | 3.4 |
| Gate-1 order dependence and static stores | CT | 3.3 |
| PCT cumulative counters; ChannelB filesystem order | SP | 3.3 |
| Test flips engineScanOrder | WH | 3.1 |
| Wall-clock assertions and timing races | CT, SP, DC, HN | 3.5 |
| Vacuous, disabled and dead tests; never-selected classes | CT, SP | 3.6, G3, G13 |
| Unordered-result assertions | CT | 3.3 |
| Silent skips and empties; guards skip missing roots | CT, SP | 3.4, G13 |
| Hand-edited goldens, rosters and ratchets; unzip blessing | SP, CT, BZ | 2.9 |
| In-test regeneration duplicating diff tests | SP | 2.11 |
| PCT oracle manifests unpinned | SP | 2.7 |
| core-layers golden plus duplicate list | SP | 2.8 |
| oracle-pins.env duplicate | P1, SP, SC | 2.6 |
| vocab.tsv / TokenDump via java.class.path | SP, SC | 2.10 |
| NoEagerTypeReferences lists jars by name | CT | 3.10 (pass jars via `java_jars`) |
| PlannerRunsOnJavaBase child JVM | CT | 3.7 |
| duckdb_load untested and unguarded | CT | 3.2 |
| Classpath-order shim shadowing | SP | 3.11 |
| Projects contract unchecked (45/56 never compiled) | SC | 3.9 |
| Engine-side stress run only on macOS arm64 | SC | 3.9 |
| tests_native re-runs unit tests; live test skipped by env | WH | 3.8 |
| user.name dependence | WH | 3.8 |
| Postgres live tests manual with hand env | P2, WH | 3.8 |
| Product debug env switches; product reads TEST_UNDECLARED_OUTPUTS_DIR | WH | 7 |
| strict_visibility off (~45 undeclared artifacts) | BZ | 1.5 |
| npm lock not verified; `"private": "true"` | BZ | 1.5 |
| npm scripts as a second workflow | DC | 4.4 |
| Source-scanning JS tests walk cwd; portability globs drift | DC | 3.10 |
| Boilerplate: 27 core libs, ~10 node_options copies, reporter path | P2, BZ, DC | 1.4 |
| Visibility public everywhere; testonly gaps | BZ | 7 |
| Heap policy split across .bazelrc and tags | BZ | 1.4 |
| Disk cache without GC; enable_bzlmod no-op; cache key frozen | BZ | 1.5, 5 |
| http:// extension; selects without default; platform settings local to warehouse | WH, BZ | 0.11, 1.4 |
| GitHub archive checksum churn | BZ | 1.5 |
| Bazel 10 flags failing | BZ | Part 4, G14 |
| `paths-ignore` `.md` false | SC | 0.8 |
| 7p lane missing from valid keys; actions pinned by tag; actionlint curl | SC, P1 | 5 |
| ~135 unbuilt scripts; docs/ scripts; experiments | P1, SC | Part 5, 7, G2 |
| Non-Bazel instructions in FAQ, README, core/README, GATES, ENGINEERING_LOG, RUNNING_THE_CORPUS, UPSTREAM_*, tool READMEs, repro, projects | SC, WH, DC | 7, G9 |
| References to deleted scripts and poms | SC, WH | 7, G9 |
| Duplicate test_suites; live_snap missing from the suite | P2, DC | 0.12 |
| Generator sources in the test tree; hard-coded paths | P2, SP, BZ | 7 |
| Test sizes uninformative; lite_test too small | BZ, DC | 7 |
| emit_* binaries missing data | DC | 4.4 |
| DuckDB-WASM `COPY TO` relative paths may write into runfiles (unverified) | DC, HN | 4.1 (register buffers, or use TEST_TMPDIR) |

---

## Part 7. Sequencing and checkpoints

Each checkpoint is a green `bazel test //...` on all three platforms, plus the stated guard turning on.

1. **Week 1 — Phase 0 complete.** CI runs everything and fails loudly. Locks are enforced.
2. **Weeks 2–3 — Phase 1.** Runner, runfiles, toolchains and macros. Guards G4, G10 and G11 on.
3. **Weeks 3–5 — Phase 2 and Phase 3 in parallel.** Generators and test-graph restructure. G1, G3, G7, G12 and G13 on.
4. **Weeks 5–6 — Phase 4.** Browser, app and packaging. G5 and G6 on.
5. **Week 7 — Phase 5 and the remaining guards.** G2, G8, G9 and G14 on. Phase 7 has been landing throughout.

Every phase lands as small PRs, each green on its own. No item depends on a later one, except where the dependency is stated.
