# Bazel first-class: the execution log

One entry per push to `main`, newest last. The runbook (`BAZEL_EXECUTION_RUNBOOK.md`) takes the first batch
without a **done** entry.

## Before the runbook (2026-10-03)

- **Phase 0, P0-01 … P0-13:** done. PR #19, `5a44c6554` (with the one-driver-path follow-up: PCT's Postgres
  lanes take `//core:drivers`). CI green on Linux, macOS, Windows; Windows `verify_app` 9/9 by a hand-run
  workflow (run 37152622977).
- **Windows teardown race (interim for P4-18):** done. PR #20, `7bbd1dc04`.

## Batch 0: close Phase 0

- PR #21 (P0-14, P0-15, the checks-lane orphans, `gates green`): open, CI green on `4c3493da9`, waiting for
  the user's merge.
- The ruleset on `main` and write access for @johnnymads: after #21 merges.
- PR #22 (P1-14, P1-14b): open, head `5a7889de2`, the independent review's fixes in; CI run 37200662280.
- The ruleset on `main`: done, id 24454941 (2026-10-04). @johnnymads invited with write (invitation 336017981, pending acceptance).
- **P0-90:** done 2026-10-04, recorded in the workplan's §6.5: 14 of 15 yes, P0-14 partial until the invitation is accepted. PR #21 merged as `0662fed80`.
- PR #22: rebased onto `0662fed80` after conflicting with #21 (both edited the lane lists); the runbook's one-line-of-work rule came from it.
- **P1-14, P1-14b:** done. Pushed to `main` directly (`8eabfbea7..253c47750`, PR #22 closed with a pointer); the
  ruleset's owner bypass worked. `main`'s CI run 37203023190: 51/51 on Linux, macOS, Windows, including
  `verify_app_test` in the Windows app lane.
- **Batch 0: done** (2026-10-04).

## Batch 1: the test runner (P1-01, P1-02, P1-21)

- Local: `852372d5d` (P1-01, P1-02) and `d782b2b3f` (P1-21) on `253c47750`. Full proof 178/178 with the measured heaps
  (every manual java test included but `//spec:reference_lane`, red on `main` itself: its golden drifted 1515 → 1517
  after 2026-09-29, outside this program). Identity diff: all 44 shared java tests select the same testcases under
  both runners. Independent audit: running.
- **Batch 1: done.** Pushed `687e56668` (b5657e888, 1b9453c5a, 687e56668). `main`'s CI run 37205628053 first failed one test
  on Windows, `WarehouseServerTest.eachStatementCarriesItsOwnUsersIdentityEvenAtTheSameTime` (a statement's status had
  chunkCount > 0 but no first chunk): it passed 30/30 locally at the same 768 MB heap and 51/51 on the re-run, so it is a
  Windows-timing race in the warehouse, not batch 1. Recorded for P3-21 (warehouse tests) to find and fix. Not reverted:
  a judgment call, stated to the user.

## Batch 2: runfiles (P1-03, P1-04, P1-05, P1-06)

- Pushed `deccc0596` (77b4d1684, b9d978f05, e1e1f55da, b2d02bf89, f04d08f23, cd3fbd559, deccc0596). Full proof 178/178;
  after the audit's fixes 28/28 on the affected targets, `//gates:local` 146/146. Independent audit: approve with nits,
  all fixed (path filters within the tree, Corpus's snapshot, Runfile holder, the skip pin moved for bisect); P1-05
  amended in the workplan with every residual Repo use's owner. `main`'s CI run 37208204540: 51/51. **Done.**

## Batch 3: Python foundation (P1-07, P1-08)

- Pushed `af4607953`. rules_python 2.3.4, CPython 3.12, the @pypi hub; Python binaries start from a bash stub (no host
  python3); the gate's lock check is offline (`//tools/python:lock_matches_requirements`), the PyPI re-resolution manual.
  The Arrow check is a py_binary on the locked pyarrow and RUNS with no host pyarrow ("25000 rows, 0 differences");
  CI's pip step and --test_env flags are gone; taskkill by its full Windows path. Full proof 179/179; after the audit's
  fixes 149/149. Independent audit: one HIGH (the host-python3 stub), fixed.
- **Red on main, reverted** (run 37209395985): Windows' `//warehouse:tests_native` could not load pyarrow's DLLs
  from the test's runfiles: past Windows' path limit (252-259 characters). Reverted at once (`5cc648148`, its tree
  identical to batch 2's proven-green one). Re-landed as `822cfeb47` with `startup:windows --output_user_root=C:/bzl`
  in .bazelrc (checked to apply per host OS): the longest path drops to about 226, on CI and on a Windows desk.
  `main`'s CI on the re-land, run 37211485516: 51/51, Windows native included. **Done.**

## Batch 4: C toolchains (P1-09, P1-13, P1-10, P1-11, P1-12)

- **4a done** (`91cd1f8e0`): Linux links the native warehouse with the hermetic LLVM toolchain through a shell-free
  launcher; Linux arm64 joins CI. The independent audit caught a Windows blocker (toolchains registered in MODULE.bazel
  name targets an empty Windows repository lacks), fixed by registering them in .bazelrc's common:linux. `main`'s CI
  run 37212513283: 53/53, the native lanes on Linux x86_64, Linux arm64, macOS and Windows.
- **4c pushed** (`eda84cb92`): D1 revised (USER: macOS like Windows): the Command Line Tools and MSVC declared and
  checked by @host_cc, their exact versions recorded as a native-image input (USER: "Is that the best way? If yes let's
  do it"), floors MSVC 17.6 and Apple clang 15. Audit: pass with fixes, applied. `main`'s CI run 37214692462:
  one Windows timeout (`//datacube:infer_test`, 60 s, while the lane built the native image), green on re-run (53/53).
  **Done.**
- **P1-12 (a)**: upstream PR https://github.com/sgammon/rules_graalvm/pull/602 after research (no duplicate; GraalVM has no
  sysroot option). (b) per-platform GraalVM toolchains: to investigate.

## Batch 5: platform infrastructure (P1-15, P1-16, P1-17, P1-18, P1-19)

- **5a pushed** (`ecc95b30d`): the WebAssembly-planner tests get medium (the timeout above); P1-17 (a Java gunzip action,
  no shell); P1-19 (`//tools/platforms`: every platform config_setting, platform_select/compatible_with; native, DuckDB,
  pyarrow and Chromium targets skip on an unlisted platform). Audit: pass with fixes, applied (its proof query had
  matched every rule; corrected). `main`'s CI run 37218299154: 53/53. **Done.**
- **5b pushed** (`b0e7a86cd`): P1-15 (the embedded Postgres per platform behind a hub; only the host's fetched; skipped on an
  unlisted platform) and P1-16 (the server finds DuckDB in its runfiles; the extraction into java.io.tmpdir is gone; DuckDB's
  jar left server_lib). The full proof caught //spec:corpus_warehouse, whose child JVM relied on the extraction: fixed by
  declaring the library. Audit: both pass, fixes applied. P1-18 investigated: the engine server artifact exists; its
  target lands with P4-07. `main`'s CI run 37219814277: 53/53. **Batch 5 done.**

## Batch 6: build hygiene, JS, early guards, Phase 1 audit

- **6a built** (local): P1-28 (disk-cache GC; --enable_bzlmod gone), P1-27 (every pin by integrity; all 19 re-fetched into an
  empty cache and verified), P1-22 (the two shell genrules are java_run actions; java_run's memory_mb; the PAR generator and
  the reference dump found non-reproducible, recorded on P2-11/P2-14). Full proof 179/179.
- **6a audit: not ready, two High.** (1) P1-27 broke `//tools/bump` (it still matched `sha256 =`) and nothing tested it.
  (2) My P1-25b "no change needed" was wrong: rules_jvm_external's Maven resolver uses ~/.m2/repository unless
  RJE_UNSAFE_CACHE=0. Medium: removing --host_jvmopt changed TeaVM's heap; the PAR heap was a guess.
- **USER review, 2026-10-04:** "why is a Java unzip better than gzip?" It was not: Bazel's http_archive unpacks a .gz at fetch
  time. Also found: my "no shell steps left" survey was broken (it filtered out every path under runs/), and missed the
  launchers' run_shell `gzip -dc`.
- **6a fixed:** Bump writes integrity and reads tag commits over HTTPS (no host git), with //tools/bump:bump_test on the real
  files; P1-25b done (`run --run_env=RJE_UNSAFE_CACHE=0`; empty-home repin of the three pools byte-identical, ~/.m2 untouched);
  heaps measured (TeaVM 889 MB live → -Xmx2g plus resource_set; PAR 2,099 MB → 4096; reference dump 3,072 MB → 8192, was 12 GB);
  java_run refuses -Xmx beside memory_mb. Full proof 180/180.
- **Windows throwaway 37224856941: red** on app, native, build. Bazel 9.2's fetch-time .gz unpacking (my P1-17 redo) sets a
  modification time 1000x too far out; Windows refuses it ("Permission denied"). The re-audit had predicted it (H1).
  Dropped: the Java gunzip stays; the launchers copy its output with copy_to_directory (the last run_shell still goes).
  Re-audit's lows fixed: Bump inserts values literally; bump_test covers prefix tags; settings.xml/.netrc wording; stale
  comments. Live `bazel run //tools/bump -- 4.145.0 --pins`: no diff. Second Windows throwaway (37225812205): green (checks, app, native, build). Full proof 180/180.
- **6a on main** (4f7448989; P1-28, P1-27, P1-22, P1-25b, P1-17 follow-up). Local gate 148/148. Main CI green (run 37226705706, 53/53).
- P1-25 investigated: 45 undeclared artifacts under strict_visibility (upstream 34, runner 5, test 5, teavm 1).
- **6b built** (local): P1-20 (legend_java_library; 21 libraries say `nullaway = False`: NullAway everywhere found 792
  errors, its own work; core private by default) and P1-25 (strict_visibility, 45 jars declared, locks unchanged but for
  the input list). USER asked "how do we make sure nothing ever leaks to core or other": a pool-user guard was added.
- **6b audit: P1-25 not ready.** //tools/par:par_generator was public and shippable on legend-engine jars; the guard saw
  only direct labels in 5 macros. Marking the engine pools testonly was tried and failed (a pool cannot be testonly as a
  whole). Fixed in three layers (tools/deps/pools.bzl): direct-use check with canonical labels and per-target grants;
  direct engine/runner users must be testonly (par_generator now testonly, //pct-only); //tools/deps:product_closure_test
  over every shipped root. Each layer proven by a reverted negative edit. Full proof 182/182. Re-audit: running.
