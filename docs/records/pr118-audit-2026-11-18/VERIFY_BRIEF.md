# Verification brief: PR #118's blocker candidates and wrong answers

You are an adversarial verifier on the audit of PR #118 (legend-lite, `claude/total-wall-d-stage` -> `main`, head
`68f20eb4d`). Read the audit rubric first, for the program's rules and the grading:
`/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/EDGE_RUBRIC.md`.

The owner will decide whether to merge on the strength of these verdicts. A finding wrongly called a BLOCKER delays a
sound PR; a real wrong answer wrongly dismissed ships. Try honestly to refute each finding assigned to you.

The PR head is checked out read-only at
`/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/pr118`. **Do not run
checkout, switch, reset or anything that writes there.** `main` is `4dfbb5b7`; the diff is
`git -C /home/user/legend-lite diff main...68f20eb4d -- <path>`. The auditors' reports are in
`/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/findings/`; read only
the findings assigned to you (and the surrounding report for context).

## For each assigned finding

1. **Facts.** Open every cited line. Is the code as described? Does the claimed path really reach the claimed output?
2. **Construct the failure.** Walk the concrete input (a Pure query, an assert body, a request, a build target) from
   the entry point to the output, citing each hop. Say exactly what the output is and what it should be. If you
   cannot construct one, say so and say why.
3. **New or old.** Is it introduced by this PR, made worse by it, or present on `main` too? Show the evidence.
4. **Ruled?** Does a user ruling cover it: a row of `docs/SEMANTICS_REGISTER.md` at the PR head, or a ruling in the
   plan of record's §3 (`git -C /home/user/legend-lite show origin/claude/total-wall-orch:docs/records/total-wall-edge-program-2026-11-16.md`)?
   Quote it.
5. **Covered?** Would any existing test, lane or golden catch it (CI lanes in `gates/BUILD.bazel`, the PCT lanes,
   `//pct:edge_contract_test`)? If not, write the exact probe that would confirm it: a test method (file, name, body)
   or a command, short enough to run as is.
6. **Grade.** Final verdict, one of:
   - **BLOCKER**: confirmed, and it meets the rubric's narrow definition (a wrong answer, a goal of record broken by
     this PR, a guard made vacuous so that it would hide a regression, a security problem, or an expensive commitment
     that conflicts with the next program);
   - **FIX-AFTER**: real, not a blocker;
   - **NOTE**;
   - **REFUTED**: wrong, unreachable, or ruled.

   Also give the fix: where, what, about how many lines, and what test proves it.

## Output

Write to `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/verify/<NAME>.md`:

```
# <NAME> verification
| Finding | Claimed | Verdict | New in PR? | Ruled? | Covered by a test? | Fix size |
|---|---|---|---|---|---|---|
## Details
(per finding: the walk from input to output with path:line hops, the evidence for each column, the probe, the fix)
```

Reply with a short summary: each finding's verdict in one line, and the blockers you confirm.

## Rules

Read-only: write only under the scratchpad `audit/` directory. Do not run Bazel. Small scripts (node, python3) to
check a regex or a JSON shape are fine. Do not spawn subagents.
