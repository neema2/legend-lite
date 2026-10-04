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
  do it"), floors MSVC 17.6 and Apple clang 15. Audit: pass with fixes, applied. `main`'s CI: running.
- **P1-12 (a)**: upstream PR https://github.com/sgammon/rules_graalvm/pull/602 after research (no duplicate; GraalVM has no
  sysroot option). (b) per-platform GraalVM toolchains: to investigate.
