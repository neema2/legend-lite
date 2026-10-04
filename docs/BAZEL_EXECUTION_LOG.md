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
