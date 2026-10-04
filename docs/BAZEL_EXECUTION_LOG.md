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
- P0-90: after the above.
