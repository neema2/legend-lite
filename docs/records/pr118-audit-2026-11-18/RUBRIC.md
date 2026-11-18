# Audit rubric: PR #118, "everything compatibility-related to the edge" (head 68f20eb4d)

## What this audit is for

PR #118 (`claude/total-wall-d-stage` -> `main`, about 1,250 files, +106K/-20K lines) is the edge program: it moves
everything that means legend-engine's or legend-pure's form (their spellings, envelopes, messages, print forms, SQL
shapes) out of the product into `com.legend.edge` and the hosts' edge modules, so that the product answers in lite's
own typed forms and the edge makes upstream's bytes. Its census of guards reads 0.

The owner of the repository now owns this program and will decide whether to merge. They asked for an audit of **the
single goal: pushing everything compatibility-related to the edge — its correctness, its completeness, and its
quality.** This is not a review of the whole platform. Do not report problems in code the PR did not touch unless
they bear directly on that goal.

The PR was written by AI agent sessions at a low reasoning effort. Their documents and reports are **claims to check,
not verdicts**. A passing guard proves only what that guard checks; a census of 0 proves only that the census's rules
match nothing, which is also what a rule that matches nothing at all reports. Verify by reading the code.

## Read first (skim; do not get lost)

- `AGENTS.md` in the PR head (the goals of record G1-G11, the invariants).
- The live handoff: `git -C /home/user/legend-lite show origin/claude/total-wall-orch:docs/records/edge-program-handoff-2026-11-18.md`
  (§1 the goal, §2 the design: the split, the hooks and their criteria, the meaning rule, the checks; §7.1 the final
  train's risky changes; §7.3 the audit's questions; §8 what the PR holds).
- As needed: the plan of record `docs/records/total-wall-edge-program-2026-11-16.md`, the audit list
  `docs/records/edge-audit-2026-11-17.md`, the follow-ups `docs/records/edge-follow-ups-2026-11-18.md`, and the next
  program `docs/records/edge-portability-program-2026-11-18.md` ("one plan, two presentations": presentation in
  Java writers over typed rows, not in SQL; plan import and export), all on `origin/claude/total-wall-orch`.
- The code-quality rubric from the earlier review of `main`, for its lenses and evidence rules:
  `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/RUBRIC.md`.

## The code

- The PR head is checked out, read-only, at
  `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/pr118`.
  **Do not run `git checkout`, `git switch`, `git reset` or anything that writes in that directory.**
- The diff: `git -C /home/user/legend-lite diff main...68f20eb4d -- <path>`; history:
  `git -C /home/user/legend-lite log main..68f20eb4d -- <path>`. `main` is `4dfbb5b7`.
- Your area's file list (added, deleted, path) is given in your brief.

## The program's own rules (judge against them)

- **The meaning rule** (handoff §2.3): what decides is what a thing MEANS (lite's or upstream's), not who reads it or
  what it is named. A fact about the user's program is product API even when only the edge reads it; a product method
  that computes upstream's alternative and hands it to a hook IS a finding; a branch on a two-form kind's answer is
  the designed mechanism; renaming does not close a finding, changing what it means does.
- **The hook criteria** (handoff §2.2): (a) a two-form kind is types only (no statics, no upstream logic); (b) the
  product answers lite's form or refuses by name; (c) every non-product form lives in the edge; (d) every hook is
  pinned.
- **The design intent** (plan of record §2.2, §4 item 1): "the edge is Java over typed results"; "no SQL that
  reproduces upstream's printed text". Known exception, already recorded: about 1,978 edge lines still build
  legend-pure's text in SQL (follow-up D1).
- **Shipped artifacts start at the product** (K2): edge off loads no edge class; legend-engine's routes load only
  through the declared opt-in.

## How to grade each finding

Give every finding TWO grades.

**Kind** (the handoff's §7.3 scale):
- **WRONG-ANSWER**: a product or upstream client can get a wrong value, message, or bytes; or a judge can reach a
  wrong verdict.
- **WRONG-DESIGN**: the structure is wrong for the goal (for example compatibility meaning left in the product,
  presentation built in SQL where Java over typed rows was the design, a hook that holds upstream logic, a move that
  copied code without redesigning it where redesign was the point).
- **BROKEN-RULE**: breaks an AGENTS.md invariant or goal of record G1-G11, or the program's own rules above, or weakens
  a guard or pin without a dated reason.
- **COSMETIC**: readability, naming, comments, style. Report as patterns with counts and three examples.

**Merge recommendation:**
- **BLOCKER**: must be fixed before merging. Use it ONLY for: a wrong answer; a rule of the goals of record broken by
  this PR; a guard made vacuous or weakened so that it would hide a regression; a security problem (a credential
  exposed or logged); or a commitment that is expensive to undo once on `main` AND conflicts with the approved next
  program (a plan-format field, a public API, a shipped artifact's root that would entrench presentation in the plan
  or upstream meaning in the product).
- **FIX-AFTER**: real, goes in the queue after the merge.
- **NOTE**: worth knowing; no action required.

Keep BLOCKER narrow and justified: the owner wants this PR merged if it is sound, not turned into another long-lived
branch.

## Evidence rules

Same as the earlier rubric: every finding has `path:line` at the PR head, what is there, why it matters, what should
be there instead (concretely), and **confidence: verified (you read it) or suspected**. For a WRONG-ANSWER, give the
concrete input that produces the wrong output, or say plainly that you could not construct one. Report strengths too,
with evidence: the owner wants a calibrated verdict. No vague findings.

## Verdict for your area

One of: **SOUND** (does what the goal needs, well), **SOUND-WITH-DEBT** (correct and complete for the goal, quality
debt to queue), **INCOMPLETE** (correct, but compatibility meaning is left where it should not be), **UNSOUND** (wrong
answers, broken rules, or a design that defeats the goal). 3-5 sentences of justification.

## Output

Write to `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/audit/findings/<AREA>.md`:

```
# <AREA>: verdict <SOUND|SOUND-WITH-DEBT|INCOMPLETE|UNSOUND>
## Summary (at most 12 lines; answer your brief's questions first)
## Blockers (if any; each one also appears in Findings)
## Strengths
## Findings
### <AREA>-<n> [WRONG-ANSWER|WRONG-DESIGN|BROKEN-RULE|COSMETIC] [BLOCKER|FIX-AFTER|NOTE] <title>
- Where: path:line
- What:
- Why it matters:
- Instead:
- Confidence: verified | suspected
## Cross-area observations
## Measurements (numbers with the command that produced them)
```

Reply with a short summary: the verdict, any blockers, the top findings, the report's path.

## Working rules

Read-only everywhere: do not edit, create or delete files in the repository or the worktree; do not commit, push, or
switch branches; write only under the scratchpad `audit/` directory. Do not run Bazel. `git`, `grep`, `rg`, reading,
and small scripts are fine (jscpd and `python3 -m lizard` are available as in the earlier rubric). Do not spawn
subagents.
