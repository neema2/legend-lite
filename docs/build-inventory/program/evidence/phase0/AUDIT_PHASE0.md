# Audit: `build/rebuild` (8 commits over `origin/main` 06a290b76)

Auditor: independent read-only review, 2026-10-06. HEAD `1689703f2`. Worktree `runs/build-rebuild`, clean.

Method: read every commit message and full diff (`git show`), then the resulting files; read the rules the change
leans on in the output base (rules_java 9.9.0 `java_binary_impl.bzl` / `java_common.bzl` / `bazel_java_import_impl.bzl`,
rules_jvm_external 7.1 `jvm_import.bzl`, bazel_lib 3.7.2 `run_binary.bzl` / coreutils toolchain, rules_js 3.4.1
`js_run_binary.bzl`, Bazel 9.2 `@bazel_tools` `http.bzl`); ran `bazel query`, `cquery` (incl. `--transitions=lite`,
`--output=starlark`) and `aquery`; read the existing `build_action_kinds.tsv` and `guard_classpaths.tsv` reports in
bazel-bin; read the throwaway CI runs on `bazel/10-throwaway` (37361390806 … 37381180961) with `gh run view`. Nothing
was built, edited (except this file), committed or pushed.

## Verdict: ready after fixes

The code is sound and the claims hold, with the exceptions below. The one blocker is commit hygiene (a commit titled
"WIP … not for main yet", which a merge commit would put on `main` verbatim). The should-fix items are small: squash the
fixups, land or include the design doc, fix one misplaced/inaccurate `.bazelrc` comment, close two parity/drift gaps in
the esbuild rule, add guard self-tests, re-key the CI cache. Before merge: update IN_FLIGHT and run one full CI on all
three platforms (MODULE.bazel, `.bazelrc` and a workflow changed; the test lanes the driver and bundle changes affect
have not yet run on any CI platform).

## Claims, verified

| # | Claim | Result | Evidence |
|---|---|---|---|
| 1 | Root targets `//:java` (`java_runtime_jars` over the 3 servers), `//:web`, `//:wasm`, `//:native`, `//:sites`; visibility added where needed | **Holds** | `BUILD.bazel:30-73`; `tools/jars/defs.bzl:88-110` reads `java_common.JavaRuntimeClasspathInfo`, which rules_java's `java_binary` returns (`java_binary_impl.bzl:289`). `cquery` of `//:java`: 45 jars = 39 first-party + 4 drivers + 2 runfiles jars. `//:__pkg__` added to `//core:server`, `//sdlc-server:server`, `//sdlc-server:page`, `//site:dist`; `//warehouse:*` (package default public) and `//wasm:planner` (public) needed none. `//warehouse:sqlapi_wasm` is excluded correctly (only `sqlapi_wasm_build_test` uses it). |
| 2 | `compile_only_test`: per-tier (kind, mnemonic) allowlist; "runs a program" from Bazel's `Action.argv`; exec skipped; validations collected; platform kinds | **Holds, with gaps** (S9, N1-N7) | `compile_only.bzl:60-89`, `CompileOnlyTest.java:77-116`. Report on macOS (bazel-bin) and Windows (CI log of 37374699690) inspected line by line: every "none" is a write/symlink/tree/manifest; every "program" is a real spawn. Linux Symlink/SolibSymlink and macOS-CI FileWrite are "none" now; Windows needs DefParser, CppLink, JavaLauncherMaker (all listed). |
| 3 | Product jars on `http_jar` via a module extension, sha256-pinned; users switched; guards updated; 3 lock files deleted | **Holds** | `tools/deps/jars.bzl`, `MODULE.bazel:108-112`. The five unchanged-version sha256s equal the deleted locks' (h2 2.1.214, duckdb 1.4.4.0, sqlite 3.47.1.0, duckdb 1.5.5.1, h2 2.4.240); Postgres 42.7.13 is new. `extension_metadata(reproducible = True)` is right (deterministic table + sha256); `MODULE.bazel.lock` needs no update (reproducible extensions and `use_repo_rule` repos are not recorded; CI with `--lockfile_mode=error` passed). Canonical names `@@+product_jars+<name>` confirmed by `cquery`. No remaining code reference to `maven_core` / `maven_warehouse` / `maven_h2_modern`. G11 still parses pool-jar paths with stamping off (`cquery`: `bazel-out/.../bin/external/rules_jvm_external++maven+maven_teavm/org/teavm/teavm-classlib/0.15.0/teavm-classlib-0.15.0.jar`). |
| 4 | `//:web` without Node: `esbuild_bundle` runs the npm registry's native esbuild (sha512, per platform) through bazel_lib coreutils `env -C` | **Holds** | `tools/js/esbuild.bzl`, `tools/js/BUILD.bazel:5-17`, `MODULE.bazel:329-343`. All five sha512s equal the pnpm locks' `@esbuild/*@0.28.2` entries in datacube, query and studio. `aquery` command line: `coreutils env -C bazel-out/<cfg>/bin/query ../../../../external/+http_archive+esbuild_darwin_arm64/bin/esbuild …`. uutils coreutils 0.9.0 (bazel_lib's) supports `-C/--chdir`. `cquery deps(//:web)` contains no Node toolchain, `js_binary` or launcher. `toolchain = _COREUTILS` present for Bazel 10 automatic exec groups. |
| 5 | Stamping off in `.bazelrc` | **Holds; comment wrong** (S5) | `.bazelrc:87`. Nothing in the repo reads `Target-Label` or depends on `processed_*.jar` names (searched `*.java`, `*.bzl`, `*.bazel`, `*.py`, `*.ts`, `*.mjs`, `*.sh`). Effect (from `jvm_import.bzl:33-77`): runtime jar = the pool's jar unchanged, compile jar loses `Target-Label`, so strict-deps still fails but its fix-up hint can no longer name the target, for every remaining rules_jvm_external pool. |

CI history (throwaway runs on `bazel/10-throwaway`): at `4b960d321`, `bazel build //...` (incl. `--config=bazel10` and
the unlisted-platform analysis) passed on macOS, Linux and Windows, and the checks lane on macOS and Linux; Windows
checks failed only on `cc_library DefParser`, fixed by `1689703f2`, whose run (Windows checks only) passed. The
checks lane's duration is unchanged (Linux 12.7 to 15.8 min, macOS 14.1 to 13.8, Windows 22.4 to 20.5). The local gate at HEAD
(`runs/homework/phase0/gates_local.log`): 289 tests pass.

## Findings

### Blocker

**B1. Commit `0eb4e6b88` is titled "WIP (prototype, not for main yet): …".** PRs here land as merge commits (PR #24 was
"Merge pull request #24"), so the title would reach `main` verbatim and say the opposite of what landing means. Its
author date (15:25) is also earlier than its parent's (15:36), so it was picked from `build/http-jars`. Fix: reword
(e.g. "The product's jars through http_jar, and Postgres 42.7.13 without checker-qual") during the squash in S1.

### Should-fix

**S1. Fixup commits make the history non-bisectable.** On CI, `compile_only_test` fails at `9d99a9aeb` on Linux
(Symlink/SolibSymlink) and on macOS (`_native_image FileWrite`) (run 37361390806). It fails on Windows at `4b960d321`
(DefParser, run 37374699690); since zlib registers DefParser on Windows regardless, that holds for every commit before
`1689703f2`. `669b39ad1` fails the build lane's `--config=bazel10` analysis on macOS and Linux (run 37369736896), and so
does `a9ce99836`, which has the same `esbuild.bzl`, until `4b960d321`. `19d6c7c1d` adds two allowlist entries that `2b3be3cb5` deletes again. Proposed series (4 commits, each green):
(1) `9d99a9aeb` + `19d6c7c1d` + `2b3be3cb5` + `1689703f2` (the guard in its final form, with the allowlist of that
point in the series); (2) `0eb4e6b88`, reworded (B1); (3) `a9ce99836` + `4b960d321`; (4) `669b39ad1` with S5's comment
fix.

**S2. The design doc every new comment cites is not in the repository.** `docs/BUILD_REBUILD_DESIGN_2026_10_05.md`
exists only on `docs/bazel-first-class-plan` (`49b36bad8`), not on `main` or this branch. It is cited in 13 changed files:
`.bazelrc:86`, `BUILD.bazel:22`, `MODULE.bazel:110`, `datacube/BUILD.bazel:651`, `query/BUILD.bazel:61`,
`studio/BUILD.bazel:60`, `tools/deps/CoreClosureTest.java:34`, `tools/deps/jars.bzl:3`, `tools/guards/BUILD.bazel:99`,
`tools/guards/CompileOnlyTest.java:18`, `tools/guards/compile_only.bzl:1`, `tools/jars/defs.bzl:103`,
`tools/js/esbuild.bzl:3`. Those citations name "step 1", "D10", "section 5b", "B1" and "experiment E1"; E1 is not defined
even in the plan-branch doc. On `main`, 92 of the 95 distinct docs cited from code exist. Fix: land the doc on `main`
first, or carry it in this PR; define or drop "E1".

**S3. The IN_FLIGHT announcement does not cover what lands.** `docs/IN_FLIGHT.md` on `main` (`98d168077`) announces
"one line in `core/BUILD.bazel` … plus `warehouse/`, `sdlc-server/` and `depot-server/`. No source edits." The
change set also:
- changes core's runtime drivers (`core/BUILD.bazel:248-268`), including Postgres 42.7.4 to 42.7.13 and dropping checker-qual,
  which is a product behaviour change for the server's Postgres arm;
- edits `datacube/`, `query/` and `studio/` BUILD files, which the Studio line owns;
- edits `MODULE.bazel`, shared with Studio;
- edits `.bazelrc`, `tools/deps`, `pct/` and `spec/`.

It does not touch `warehouse/` visibility or `depot-server/`. Fix: update the
announcement on `main` before merge, per the repository's cross-area rule. State the Postgres bump in the PR body too.

**S4. Run one full CI run on all three platforms before merging.** `MODULE.bazel`, `.bazelrc` and
`.github/workflows/gates-run.yml` changed. The lanes these changes affect have not run on CI since `0eb4e6b88` /
`a9ce99836`:
- the core suite, spec, the corpus lanes, PCT H2/DuckDB/Postgres and the warehouse, which run on the drivers;
- the app and browser lanes, which consume the bundles.

Only `build //...`, checks and linux-arm's native lane ran. The design doc itself says http_jar on Windows is "unproven until its own throwaway CI run". `@esbuild_linux_arm64`
and `@esbuild_darwin_x64` are never exercised by CI. No macOS x86_64 runner exists, and linux-arm ran only the native
lane. The sha512 pins make them low-risk.

**S5. `.bazelrc:81-88`: the stamping block was inserted between the disk-cache comment and its flag, and its rationale is
wrong.** Lines 81-82 ("A local disk cache: … CI passes the same path explicitly") now sit above the stamping comment.
`build --disk_cache` follows at 88. The comment, and `669b39ad1`'s message, say "what is left of the plugin is build
tooling (NullAway, TeaVM)". `tools/deps/jars.bzl:4-5` correctly says rules_jvm_external still serves `maven_test`
(JUnit, ArchUnit, JGit), `maven_upstream` (~396 jars) and `maven_runner` (~610 jars). Stamping off therefore removes the
"add this dependency" target from strict-deps errors for all test code on those pools. Fix: move lines 83-87 above line
81 (or below 92), and state the trade-off accurately (strict-deps still enforced; the fix-up hint shows a jar path, not a
label, for rules_jvm_external jars).

**S6. The esbuild version now has two unlinked sources.** `MODULE.bazel:329-343` pins 0.28.2 "by the sha512 the pnpm
lock records". `datacube/package.json:20`, `query/package.json:10` and `studio/package.json:13` still declare
`"esbuild": "0.28.2"`, which no target uses any more. Nothing in BUILD/.bzl, no script, no JS import. A package.json bump
would leave the build on the old binary and make the MODULE comment false, with nothing failing. Fix: drop esbuild from
the three package.json files and relock, so MODULE.bazel is the only source. Or add a guard, as
`//tools/browser:revision_test` does for Chromium, that the five pins equal the locks'.

**S7. `esbuild_bundle` drops the runfiles that the rule it replaces provided.** `tools/js/esbuild.bzl:60-63` returns
`DefaultInfo(files = …)` only. js_run_binary's underlying `run_binary` returned
`DefaultInfo(files = outputs, runfiles = ctx.runfiles(files = outputs))` (bazel_lib `run_binary.bzl:170-172`). Today's
consumers are js_test/js_binary, filegroup, copy_to_directory and run_binary, all of which read `files`, so nothing
breaks. A future `java_test`/`sh_test` with `data = [":bundle_bundle"]` would silently get no bundle. The commit claims
parity ("as js_run_binary's were"). Fix: add `runfiles = ctx.runfiles(transitive_files = files)`.

**S8. D15 is a hermeticity hole, not only a path leak; disclose it in the PR and track it.** It is pre-existing, and the
Node route had it too. `bazel-bin/datacube/demo/bundle.js` carries 225 comments like
`// ../../../../../../../../../execroot/_main/bazel-out/darwin_arm64-fastbuild/bin/engine-client/src/locale.ts`. esbuild
realpaths the sandbox's input symlinks and then resolves imports in the real execroot. It can therefore read files the
action did not declare, so the sandbox does not catch a missing input. The bytes also differ by platform
(`darwin_arm64-fastbuild` vs `k8-fastbuild`) and by strategy (sandboxed vs local/Windows) under the same action key. The
new rule is now the place to fix it. Options are in D15: rules_esbuild's sandbox plugin needs the JS API, and
`--preserve-symlinks` breaks rules_js's store layout without hoisting.

**S9. The guard has no committed self-test, and two of its claims are stronger than the code.**
- The negative proofs were made by hand (commit messages). Its correctness rests on Bazel internals: `-exec` in
  `bin_dir`, `Action.argv`, rule-kind names. Nothing re-proves it on a Bazel or rules bump, and the validation branch
  (`compile_only.bzl:54-58,78`) has never seen a validation. The report has no Validation line, and `cquery` shows no
  `_validation` group on the tier targets probed (`//core:server`, `//core:drivers`, `//warehouse:server_native`). Fix: add a testonly fixture tier to `action_kinds_report` with a genrule, a java_run, a java_run with
  mnemonic "Javac", and a rule with a non-empty `_validation` group. Have `CompileOnlyTest` assert that exactly those are
  flagged, on all three platforms.
- "its rule kind comes from Bazel, so a java_run calling itself 'Javac' still fails" (`CompileOnlyTest.java:25-26`) and
  "whatever mnemonic it gives itself" (`compile_only.bzl:9-10`) overstate it. `ctx.rule.kind` is the name the rule is
  exported under, so any .bzl may export a rule as `_teavm_wasm` or `_esbuild_bundle` and pass with mnemonic `TeaVM` or
  `Esbuild`. If identity must be unforgeable, key "program" actions on the executable as well, e.g. argv[0] or the
  executable's repository. Otherwise reword the claim to what holds: honest rules cannot hide behind a mnemonic.

**S10. The CI cache key no longer rotates when a product jar is bumped.** `gates-run.yml:119-121` and
`diagnostics.yml:43-45` key on `MODULE.bazel.lock`, `maven_*_install.json` and `.bazelversion`. The driver pins moved
from `maven_core_install.json` (in the key) to `tools/deps/jars.bzl` (not in the key), and the lockfile does not record
reproducible extensions. After a bump, actions/cache restores the old exact key and never saves a new one: every run
re-downloads the jar and re-runs every test downstream of `//core:drivers`. Fix: add `tools/deps/jars.bzl`, and
`MODULE.bazel` for the esbuild/fonts/Chromium pins, to both `hashFiles(...)` calls.

### Nits

- **N1.** `CompileOnlyTest.java:56` `cc_library CppModuleMap` is dead. It is "none" on macOS (bazel-bin report) and
  absent on Windows (CI report). It is a file-write action, so it should also be "none" on Linux (inferred; no Linux
  report was read). The list is meant to hold only program-running pairs, so remove it.
- **N2.** `CompileOnlyTest.java:32-38`: the shared `JAVA_LIBRARIES` map gives `jvm_import CreateCompileJar` to java and
  native, which have none, and `java_import JavaIjar` to wasm and native, which have none. Per-tier lists would be minimal.
- **N3.** `CompileOnlyTest.java:83-103`: "total exec-skipped > 0" is a cheap canary for one failure mode, a wholesale
  rename of exec output directories. In that case the exec subgraph would be walked and fail as offenders anyway. It
  cannot detect the dangerous direction: a target-config subtree misread as exec and silently skipped (only the
  per-tier SIGNATURE catches that, and only when the signature action itself is skipped). Stronger: expect > 0 for java,
  native and wasm (today 4/5/5 on macOS and Windows), or have the report list the skipped labels and assert known
  members (`//tools/nullaway:nullaway`, `@rules_java//toolchains:current_java_runtime`).
- **N4.** `compile_only.bzl:35,67` prunes a java_binary to its classpath edges in every tier. That is justified only
  for `//:java`. No other tier has a java_binary today, so the gap is latent. Apply the pruning only on the java tier,
  e.g. a second aspect on a separate attribute.
- **N5.** Doc drift in `compile_only.bzl`:
  - line 14 still says "esbuild's launcher";
  - line 113's rule doc omits the fifth column (`program|none`);
  - the "what is not walked" list should also name tools' own builds, javac plugins and annotation processors (they run
    inside `Javac`; e.g. rules_java runfiles' `auto_bazel_repository_processor`, NullAway), toolchain-provided inputs, and
    analysis-time Starlark writes (`ctx.actions.write` / `expand_template` pass as "none" by design).

  The argv rule also trusts that every program-running action class exposes argv; templated actions expanded at
  execution time would be the theoretical exception.
- **N6.** In `CompileOnlyTest.java`, the javadoc's platform-only list (27-28) lacks DefParser. The "runs Node" check
  (104-107) sees only target-configuration `js_binary`s, so Node used as an exec tool by an allowlisted kind would not
  trip it (an unlisted kind would still be caught as an offender).
- **N7.** `compile_only.bzl:58` flattens a depset per target (`len(group.to_list()) > 0`); depset truthiness is O(1).
- **N8.** `tools/guards/BUILD.bazel:101-109`: `build_action_kinds` is not `testonly`, unlike its sibling reports, and it
  is analysed by every `bazel build //...`.
- **N9.** `tools/js/BUILD.bazel:7-17` uses a raw `select` with no `no_match_error`. The repository's policy
  (`tools/platforms/defs.bzl`, P1-19) is `platform_select(mapping, what)`. `windows_aarch64` is in `PLATFORMS`, and its
  `@esbuild/win32-arm64` tarball is in the locks, but it is not mapped.
- **N10.** `tools/js/esbuild.bzl:73`: under automatic exec groups, `_esbuild` (`cfg = "exec"`) is configured for the
  default exec group, while the action runs on the coreutils group's platform. These agree with one exec platform (today)
  and could diverge with several. Consider `cfg = config.exec(...)` on the coreutils group. Alternatively, run esbuild
  directly with bin-dir-prefixed args, which drops the coreutils/AEG coupling at the cost of byte-identity with today's
  bundles.
- **N11.** The `esbuild_bundle` macro (`tools/js/esbuild.bzl:89-99`) has rough edges:
  - it creates empty `copy_to_bin` targets for Studio's two workers;
  - it does not forward `tags`, `testonly` or `target_compatible_with` to `<name>_srcs`;
  - it classifies srcs by string prefix, so a cross-package file label is treated as a target and not copied to bin;
  - `workdir` (33) ignores `ctx.label.workspace_root`;
  - wrap `_COREUTILS` (24) in `Label()`.
- **N12.** `tools/deps/jars.bzl:20` loads `http_jar` from `@bazel_tools`. rules_java's
  `@rules_java//java:http_jar.bzl` is the maintained implementation (on this Bazel its compatibility proxy resolves to
  `@rules_java//java/bazel:http_jar.bzl`) and is the forward-compatible choice for the Bazel 10 work. The MODULE comment's "Bazel's own http_jar" would change
  with it.
- **N13.** In `tools/deps/jars.bzl`:
  - `users` for `h2`, `postgresql` and `sqlite_jdbc` (42, 57, 62) include `spec`, which names none of them; only core
    does. The per-jar table allows tighter lists than the old pool did.
  - "the latest release (2026-07-06)" (51) will go stale.
  - `h2_modern` is test-only but sits in "the product's jars".
  - The data table lives in the extension's file, so every BUILD file that loads `pools.bzl` also loads
    `@bazel_tools`' `http.bzl`, and editing `users` re-evaluates the extension. A `jars_table.bzl` would decouple them.
- **N14.** `tools/deps/CoreClosureTest.java:50`: `theDriversAreCoresPoolAndOnlyTheNamedOnes` still names a pool that no
  longer exists.
- **N15.** `tools/guards/LocksTest.java:11-12`: the import order is `TreeSet` before `Set`.
- **N16.** `release.MODULE.bazel:168-169`: "Jars another pool owns are left out … the drivers are core's" no longer
  matches; the drivers are http_jars now. The exclusions are still needed.
- **N17.** `docs/GATES.md:32`: the checks row does not list `//tools/guards:compile_only_test`.
- **N18.** `BUILD.bazel:68-73`: `//:sites` "copies what the tiers above built". `//datacube:site` also builds
  `remote_bundle` and `stress`, which are not in `//:web`, until D5 lands.
- **N19.** `tools/jars/defs.bzl:1-18`: the module docstring describes only `java_jars`.

## Answers to the specific questions

- **Can a program-running action slip through?** Not for honest target-configuration rules:
  - genrules, java_runs and run_shell/run actions all expose argv and are held to the allowlist;
  - a mislabelled mnemonic is caught;
  - output-file labels are followed (`apply_to_generating_rules`).

  The routes that remain:
  - a rule *exported* under an allowlisted kind name (S9);
  - anything in the exec configuration or reached only through a resolved toolchain, by design: tools' own builds,
    javac plugins and annotation processors, toolchain-generated inputs such as rules_java's bootclasspath;
  - analysis-time Starlark generators, which register as "none";
  - in theory, action classes without argv that still spawn.

  None occurs in the tiers today. `cquery --transitions=lite` of `//:native` shows only tools across exec edges:
  native-image, the sysroot launcher, the java toolchain tools, def_parser, nullaway, and the runfiles processor.
- **Are the allowlists minimal?** Almost: N1 (one dead entry) and N2 (the shared map widens three tiers). Every other
  entry is observed on some platform: JavaLauncherMaker, CppLink and DefParser only on Windows; CppLink also on Linux.
- **Does the exec skip hide anything that should be checked?** Only what the design excludes (above). It should be
  written down (N5).
- **Is "total exec-skipped > 0" meaningful?** Only as a rename canary, not as a guarantee (N3).
- **Cross-platform.** No path or separator issue was found. Starlark `File.path` is `/`. The Windows `esbuild.exe` runs via
  uutils `env -C` with a relative `../`-path; CI's Windows `build //...` built the bundles. New files are LF and
  `git diff --check` is clean. Unexercised: esbuild on linux-arm64 and macOS x86_64 (S4), and windows_aarch64, which is
  unmapped (N9).
- **Determinism and caching.**
  - Stamping off is deterministic and nothing reads the stamp. It costs one invalidation of everything compiled against
    rules_jvm_external jars, and strict-deps hints lose labels (S5).
  - http_jar downloads are content-addressed. The ijar is stamped with the canonical label, so a future rename of
    canonical repos recompiles once; CoreClosureTest's `@@+product_jars+` prefix is the canary for that rename.
  - Bundle bytes depend on the platform and the strategy (S8).
  - The CI cache key no longer follows the driver pins (S10).
- **Repository rules and host programs.** None. coreutils comes from bazel_lib's pinned toolchain, esbuild from
  sha512-pinned archives and the jars from sha256 pins, with no PATH use. Tests read only runfiles.
- **Licensing and leakage.** No binaries in the diff (`--numstat`). No `/Users`, `/private`, `/tmp`, `C:\Users`,
  Xcode/MacOSX SDK or Windows Kits strings in the diff or the commit messages. The esbuild (MIT) binary is fetched at
  build time and not shipped. pgjdbc remains BSD-2. The product jars lose rules_jvm_external's
  `maven_coordinates`/PackageInfo metadata; nothing in the repository consumes it today.

## Not findings (checked and fine)

- `MODULE.bazel.lock` needs no update.
- `use_repo` lists exactly the six repos.
- `check_pool_use` resolves `@<name>//jar` to `+product_jars+<name>` and fails closed on unknown names.
- G11 recognises both pool and product coordinates. No target in the pct report has two H2 versions.
- The closure genqueries match `java_import` and `jvm_import`.
- `LocksTest`'s exact equality is a valid replacement for "≥ 8", since it proves its own regex saw every pool.
- `jar_entry(jar = "@duckdb_jdbc_warehouse//jar")` still gets exactly one runtime jar.
- The A25 unlisted-platform analysis passes: `_esbuild` resolves for the exec platform, not the target.
- Buildifier and Java formatter gates: none exist in the repository, so N15 is cosmetic only.
