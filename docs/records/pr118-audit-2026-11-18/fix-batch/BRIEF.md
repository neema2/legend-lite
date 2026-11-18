# Brief: the independent audit of PR #118's fix batch (68f20eb4d..eec3c95c7)

You are one of three independent reviewers, working in parallel, on the fix batch that will be pushed onto PR #118
(legend-lite, `claude/total-wall-d-stage` -> `main`, the edge program). PR #118 itself (head `68f20eb4d`) was already
audited in depth by eight area auditors and four adversarial verifiers. That audit produced a fix list (F1-F8, N1-N4,
R1-R3), and a fixer session wrote 26 commits on `claude/pr118-fixes` (71 files, +1,170/-211). Each fix had a targeted
review, and several were corrected by those reviews. **No one has yet audited the batch as a whole at its final head.
You are that audit, for your share.** The owner will push this batch onto the PR and ask the user for a merge on the
strength of your verdict.

The fixer worked fast against a deadline. Its commit messages and its claims are claims to check, not verdicts.

## Read first

- The audit's rubric (its grades, the program's rules: the meaning rule, the hook criteria, K2):
  `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/EDGE_RUBRIC.md`.
- What each fix was meant to do: `/home/user/legend-lite/docs/records/pr118-audit-2026-11-18/README.md` (F1-F8),
  `.../FOLLOW_UPS.md` §6 (the user's rulings R1-R8, and N3), and, for the finding behind a fix, the matching file in
  `.../findings/` and `.../verification/` (V-HOSTS for F1/F3/F5/F6, V-PRODUCT for F4/F6, V-EDGE for F7).
- `AGENTS.md` at the fix head (goals of record G1-G11, the invariants, the rule that a guard's ceiling, pin or
  allowlist entry moves ONLY with a dated justification comment naming the task).

The user's rulings that bind this batch:
- R1: the product's default assert message is lite's "expected true, got false"; legend-pure's "Assert failed" only
  under the PURE and ENGINE styles.
- R2: the test-data generator's two upstream spellings stay for now (a ratifying note). R3: four residue pins
  (DatabaseJudge 851, HostJudgments 679, BodyDriver 545, EngineTextPlanning 151) ratified.
- N3: a data refusal carries its site's `where`; `raise` is a raise's kind alone.
- Simple error messages: no runtime operand values in a raise's words (A3), under every style.
- Pins the user (or the owner, under delegation) approved moving: channel B ratchets (relation judge chars
  1989680->2006804, relation plan 34485688->34502872, standard chars 1547392->1647537, standard plan
  24441771->24542270), INTERNAL_DESUGAR 37->38, NOT_UPSTREAM_MAX 72->73, Intrinsic 734->735, own_corpus.matched
  3309->3310. **Any other pin, ceiling, allowlist or ledger move in the batch is a finding.**

## The code

- The fix head, checked out read-only: `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/fixes`
  (detached at `eec3c95c7`). The PR head is at `.../scratchpad/pr118` (`68f20eb4d`).
  **Do not run checkout, switch, reset, commit or anything that writes in either directory, or in `/home/user/legend-lite`.**
- The batch: `git -C /home/user/legend-lite log --reverse 68f20eb4d..eec3c95c7`; one commit: `git show <sha>`; your
  share's net diff: `git -C /home/user/legend-lite diff 68f20eb4d eec3c95c7 -- <paths>`.

## What to check, for every commit in your share

1. **Does it fix what it claims, fully?** Walk the failing input from the finding (a Pure query, an assert body, a
   request, a build target) through the fixed code to the output, citing `path:line` at the fix head. Is every case
   the finding named covered, on every road (the product query, the test body, every judge: host, database, BOTH;
   every host: Java server, Python, TypeScript/WASM, where the finding spans them)?
2. **Does it break anything else?** Callers of every changed method; any other input that now takes the new branch;
   a meaning inverted (for example a negated assert); an error class or status changed for a request that was right.
3. **Does it keep the program's rules?** Upstream meaning added to the product (the meaning rule); a hook holding
   upstream logic; SQL that builds legend-pure's printed text where Java over typed rows was the design; a runtime
   value in a raise's words; a fallback or default (AGENTS.md invariant 4); a `default ->` in a render method.
4. **Guards and pins.** A guard narrowed, a pattern made to match less, a line count held by joining lines, an
   allowlist grown, a ledger row reworded: is each one approved (the list above) and dated in place? Could the change
   hide a future regression?
5. **Tests.** Does a test prove the fix (fails before, passes after), or does it only restate the code? Is the test in
   a CI lane (`gates/BUILD.bazel`; a target tagged `manual` runs in no lane)?
6. **Quality.** Duplication, a copy of logic that exists elsewhere, dead code, a misleading name or comment.

## Grades

Per finding: kind (WRONG-ANSWER, WRONG-DESIGN, BROKEN-RULE, COSMETIC) and a recommendation:
- **BLOCKER**: must be fixed before this batch goes onto the PR. ONLY for a wrong answer, a goal of record broken, a
  guard made vacuous or a pin moved without approval, a CI lane that will go red, or a fix that does not fix its
  finding. Keep it narrow: the user wants this merged if it is sound.
- **FIX-AFTER**: real; goes in the queue after the merge.
- **NOTE**.

For each BLOCKER give the fix: where, what, about how many lines, and the test that proves it. For a WRONG-ANSWER,
give the concrete input and its output, or say plainly you could not construct one. Confidence: verified (you read it
or ran it) or suspected. Report strengths too: a calibrated verdict is wanted.

## Output

Write `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/fixreview/<NAME>.md`:

```
# <NAME>: verdict SOUND | SOUND-WITH-DEBT | UNSOUND
## Summary (at most 10 lines)
## Per commit (sha, claim, verdict: fixes it / partly / no, one line each)
## Blockers
## Findings
### <NAME>-<n> [kind] [BLOCKER|FIX-AFTER|NOTE] <title>
- Where / What / Why it matters / Instead / Confidence
## Strengths
## Measurements (numbers with the command that produced them)
```

Reply with a short summary: the verdict, each blocker in one line, the report's path.

## Working rules

Read-only everywhere except your report. No Bazel (the fixer and the owner's heavy rounds run the tests). `git`,
`grep`, `rg`, reading, and small scripts (python3 -I, node) to check a regex, a count or a JSON shape are fine; you may
run a single Python test file with `python3 -m pytest` from a scratch copy if you need to, never inside the worktrees.
Do not spawn subagents.
