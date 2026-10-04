# Bazel first-class: the execution runbook (2026-10-04)

How the rest of `BAZEL_FIRST_CLASS_WORKPLAN_2026_10_03.md` gets done, from Phase 0's close to P8-01. One
session executes it, batch by batch, in order. **Every loop iteration starts by reading this file and
`BAZEL_EXECUTION_LOG.md`, then takes the first batch the log does not mark done.** The workplan is the
spec: an item's Change, Proof and Done-when are what "done" means. This file is the process.

## A. The protocol (every batch, the same steps)

1. **Sync.** `git fetch origin`; rebase the working tree onto `origin/main`. Look at the desk's load and at
   other sessions' Bazel servers (`ps`, `uptime`) before starting anything heavy.
2. **Read.** Each item's full spec, and every file it touches, before writing code.
3. **Build it.** The smallest correct change. Standing rules: fix causes, never wrap them; no new shell, no
   new host dependency; the warehouse stays a GraalVM native image linked at build time; a pin, ratchet or
   allowlist moves only with a dated justification naming the item ID (AGENTS.md).
4. **Prove it locally, all of it.** The items' proof commands, then
   `bazel test --lockfile_mode=error //gates:local`, then every heavy lane the batch can affect (PCT,
   corpus, stress, native, browser), in this one Bazel server: the desk has 10 cores and 32 GB. Do not stack
   a 9 GB run on top of another session's.
5. **No PRs, no pre-push CI.** Every batch goes straight to `main` (USER, 2026-10-04: "just straight to main").
   `main`'s CI runs Linux, macOS and Windows on every push; step 9 bounds a break to one CI cycle. A local
   proof the batch cannot run on macOS (a Windows-only path) is named in the commit message.
6. **Independent audit.** A fresh read-only reviewer that wrote none of it checks the diff against the
   items' spec and reports findings with file:line and evidence. Fix every finding (or record, in the commit
   message, why it is not one), re-run the affected proofs, and have the reviewer look at the fixes.
7. **Push to main, once.** Rebase, re-run `//gates:local`, push. The commit message names the items, the
   proof results with numbers, and the review verdict ("local gate: //gates:local green").
8. **Pipeline.** While `main`'s CI runs on that push, start the next batch at step 1 (work, local proofs,
   audit). Never push batch N+1 before batch N is green on `main` on all three platforms.
9. **Red on main.** Revert your own commit at once (`git revert`, push), rebase the next batch's work onto
   the revert, fix batch N, and go back to its step 4.
10. **Record.** In `BAZEL_EXECUTION_LOG.md`: the batch, its items, the commit(s), `main`'s CI run and
    result, anything deferred. Each phase ends with its audit item (P*-90), recorded in the workplan's §6.5,
    before the next phase starts.

**One line of work, never two branches (2026-10-04, after PR #22).** Two branches off the same `main` (#21
and #22) both edited the CI lane lists (`gates-run.yml`, `docs/GATES.md`); the second hit merge conflicts
and missed `//gates:local`, which the first had created. USER: "complete disaster ... add this to runbook so
we dont do this again". So:
- **At most one unpushed batch exists at a time, and it sits on top of the latest `origin/main`.** The
  pipelined batch N+1 (step 8) is built on top of batch N's pushed commit, never on an older `main`.
- **Rebase onto `origin/main` twice:** before the local proofs (step 4) and again right before the push
  (step 7). If the second rebase brings anything in, re-run `//gates:local` and the batch's proofs.
- **A CI lane edit and `gates/BUILD.bazel` change in the same commit.** Every non-heavy test added to a lane
  in `gates-run.yml` is added to `//gates:local` too, and `docs/GATES.md` says the same. (P5-01 later makes
  the lanes suites in `//gates`, which ends the duplication.)
- **No subagent writes code or holds a branch.** Subagents only review, read-only.

**Stop and notify the user instead of guessing** when: a decision D1–D21 does not settle; a change would
weaken the native image or needs a ratchet lowered to get green; `main` goes red twice for one batch; a
permission is denied; a finding would change the plan's scope.

## B. The batches, in dependency order

| # | Batch | Items | Notes |
|---|---|---|---|
| 0 | Close Phase 0 | merge PR #21; the ruleset on main; write access for @johnnymads; PR #22 (P1-14, P1-14b) to main, the last PR; **P0-90** | #22 makes `verify_app_test` guard Windows from here on |
| 1 | Test runner speaks Bazel's protocol | P1-01, P1-02, P1-21 | Critical path. Resume branch `bazel/b2-test-runner` (6500ab9de, 33d054e9e, WIP 29c8a689b): review every line before building on it |
| 2 | Runfiles through the official library | P1-03, P1-04, P1-05, P1-06 | Package by package |
| 3 | Python foundation | P1-07, P1-08 | rules_python, one locked pip hub; the pyarrow check never skips |
| 4 | Hermetic C toolchains | P1-09, P1-13, P1-10, P1-11, P1-12 | Linux, macOS (D1), MSVC declared (D5), Linux arm64 in CI |
| 5 | Platform infrastructure | P1-15, P1-16, P1-17, P1-19, P1-18 | |
| 6 | Build hygiene, JS, early guards | P1-20, P1-22, P1-23, P1-24, P1-25, P1-25b, P1-26, P1-27, P1-28; P6-00, P6-10, P6-11, P6-14, P6-16, P6-17, P6-19; P7-10, P7-12; **P1-90** | Phase 1 closes |
| 7 | Generators I | P2-01–P2-05, P2-20 | The stress corpus from Bazel actions, byte-identical |
| 8 | Generators II | P2-06, P2-07, P2-08, P2-10, P2-11, P2-12, P2-13, P2-14, P2-17 | |
| 9 | Generators III | P2-15, P2-16, P2-18, P2-19, P2-09; **P2-90** | |
| 10 | Test graph A: core | P3-01–P3-06, P3-11, P3-14, P3-15, P3-17 | The largest risk: one item per push where needed |
| 11 | Test graph B: spec, pct, parser-equivalence | P3-07, P3-08, P3-09, P3-12, P3-13, P3-18, P3-30 | |
| 12 | Test graph C: native, JS, misc | P3-10, P3-16, P3-19–P3-26, P3-28, P3-29, P3-31, P3-34 | |
| 13 | `Repo` and `Upstream` deleted | P3-27, P3-27b, P3-33, P3-32; **P3-90** | |
| 14 | Browser, app, packaging | P4-01–P4-18; **P4-90** | No bash launcher, no `install_browser`, no `taskkill` |
| 15 | CI is a list of labels | P5-01–P5-09; **P5-90** | |
| 16 | Guards | P6-01–P6-09, P6-12, P6-13, P6-15, P6-18, P6-20; **P6-90** | |
| 17 | Cleanup and docs | P7-01–P7-15; **P7-90** | |
| 18 | Close-out | **P8-01**; restore D17's guardrail 2; the plan and its evidence move to `docs/history/` | |

A batch may be split into several pushes, each through steps 4–9. Items inside a batch follow the
workplan's §3.2 order.

## C. Where the state lives

- **This runbook:** the process and the batch list. Changed only by the user, or with a dated reason.
- **`BAZEL_EXECUTION_LOG.md`:** what is done, one entry per push. The first batch without a "done" entry is
  the next one.
- **The workplan:** the spec, its §6.5 audit tables, and its decisions. An amendment found while executing
  (a gap, a new item) goes there with an ID, as the phase audits require.
