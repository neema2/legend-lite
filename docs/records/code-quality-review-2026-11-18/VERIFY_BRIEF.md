# Adversarial verification brief

You are an adversarial verifier on a code-quality review of the legend-lite repository (checked out at
/home/user/legend-lite, commit 4dfbb5b7 = `main`). A reviewer wrote the report named in your task, against the rubric at
/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/RUBRIC.md (read it for the
bar, the lenses and the severity scale).

Reviewers at high effort overclaim as well as underclaim. The owner will make expensive decisions (refactor, redesign,
or redo) based on the final report, so every S1 and S2 finding must survive an honest attempt to refute it.

## For each S1 and S2 finding in the report

1. **Check the facts.** Open every cited `path:line`. Is the code as described? Are the counts right (re-count with
   grep or a script)? Are "no callers", "never read", "duplicated", "vacuous", "unused" claims true across the WHOLE
   repository (core, python, engine-client, datacube, wasm, native, tools, pct, spec, tests)?
2. **Look for a forcing constraint the reviewer missed.** Examples: TeaVM (Java compiled to WebAssembly/JS: no
   reflection, a limited JDK), GraalVM native image (ctypes host for Python), the plan being a portable format read by
   Java, TypeScript and Python, byte-for-byte fidelity with legend-engine and legend-pure (see
   `docs/SEMANTICS_REGISTER.md`), the goals in `AGENTS.md`. If a constraint forces the shape, the finding may still
   stand (the code could meet the constraint better), but say so.
3. **Test the expert alternative.** Is it feasible under those constraints? Is it really simpler, or does it move the
   cost elsewhere? Estimate its size (small / medium / large).
4. **Judge the severity.** Would you keep S1/S2, raise it or lower it?

Give each finding one verdict:
- **CONFIRMED**: facts right, severity right.
- **CONFIRMED-WITH-CORRECTION**: the substance stands but a fact, count or severity needs correcting (say exactly
  what).
- **OVERSTATED**: real but materially smaller or less costly than claimed (say why, and the severity you would set).
- **REFUTED**: wrong, or justified by a constraint with no better available option (show the evidence).

## Also

- Spot-check two of the report's S3 pattern counts, and two of its claimed strengths.
- Say whether you would set the unit's overall verdict differently, and why.
- Note any significant problem in the unit the reviewer MISSED, if you came across one (do not go hunting widely;
  that is not your job).

## Output

Write to `/tmp/claude-0/-home-user-legend-lite/e46cefdc-2e0e-59c4-80ed-3422bd6cbcd6/scratchpad/review/verify/<UNIT>.md`:

```
# <UNIT> verification
## Verdict on the unit: <agree | would set X> (reason)
## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
...
## Details
(one short paragraph per finding that is not plainly CONFIRMED: what is wrong or overstated, with path:line evidence)
## S3 and strengths spot-checks
## Missed (if any)
```

Reply with a short summary: counts per verdict, the findings you refuted or downgraded and why, and your view of the
unit verdict.

## Rules

Read-only: do not edit the repository, commit, push or switch branches; write only under the scratchpad `review/`
directory. Do not run Bazel. Do not read the other units' reports or verification files: judge this one report on its
own evidence. Do not spawn subagents.
