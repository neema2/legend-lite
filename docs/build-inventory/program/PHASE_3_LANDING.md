# Phase 3: what is left before it lands (2026-10-07)

Phase 3 ("one table decides, by exact function id"; the plan's Phase 3) is built on branch `build/phase3` (worktree
`runs/build-rebuild`) and audited. This file is the complete list of what is left, so a session can land it without
the conversation that produced it.

## 1. What Phase 3 is (one paragraph)

The compiler used to collect a call's candidate functions by name, drop some by name (the PCT rule, the
platform-owned list), rank the rest by adding per-parameter scores, and recognize language forms, legacy TDS
functions and `agg` by spelling. Phase 3 ranks overloads exactly as legend-pure does (`FunctionMatch`: type matches
parameter by parameter, then multiplicities; the first difference decides; distances from the C3 linearization),
merges candidates by function id with the by-name drops gone, refuses a version of a function the platform declares
that has no implementation row (`Implementation.Reason.NO_ROW`, never running upstream's body), adds rows for the 27
versions the corpus, PCT and the reference lane call, and reads forms by the names a call resolves to.

## 2. The branch

**State at the end of 2026-10-07:** `build/phase3` is rebased on `origin/main` `f306bd698`: five local commits, the
four Phase 3 commits (`d0041969c`, `a2f4da2fc`, `3912d3c12`, `5e8a7c263`) and the audit's fixes (`036bef950`), nothing
pushed. The worktree is clean. The rest of this section describes the branch before the rebase (the old commit ids).

- 4 commits on `293318dda` (main has moved to `f306bd698`, a docs-only IN_FLIGHT commit: rebase is clean):
  `5bc1550ae` step 1 (ranking), `38566af11` step 2 (candidates and implementations by id), `165a1dbff` step 3 (forms,
  TDS functions, `agg`, boot-layer versions by resolved names), `bc0de1f70` (the corpus held: dot calls
  property-first, the statement inliner defers to overload resolution, `executeInDb`'s ConnectionStore version).
- **Uncommitted in the worktree: the audit's fixes** (section 4), 14 files:
  `compiler/spec/InferenceKernel.java`, `FunctionMatch.java`, `Overloads.java`, `Typer.java`, `UserCallInliner.java`;
  `compiler/StatementInline.java`; `compiler/element/FunctionCompiler.java`; `platform/Implementation.java`,
  `ImplementationTable.java`; tests `InferenceKernelTest`, `CompileFunctionTest`, `IdentityGuardrailTest`,
  `ParkedWorkLedgerTest`; `docs/PARKED_WORK_LEDGER.md`.
- Measured before the fixes: local gate 306/306; six corpus passes identical to main's own outputs (every result
  file); reference lane AGREE 73,103 -> 74,586, OVERLOAD 745 -> 58, DRIFT 32 -> 0, PROPERTY_AS_CALL 39 -> 1, our
  failed bodies 1,508 -> 1,469; PCT all pass.

## 3. The audit (2026-10-07, max effort): "ready after fixes"

The full report is in the worktree's scratch: `runs/build-rebuild/runs/homework/phase3x/AUDIT_PHASE3.md`. Its
findings and where each stands:

| Finding | What | Status |
|---|---|---|
| **B1** (blocker) | a function value ranked a function-typed parameter after a type parameter and after `Any` (`from(FunctionDefinition<{->T[m]}>, Runtime)` lost to `from<T>(T[m], Runtime)`); legend-pure matches the carrier classes first (`GenericTypeMatch`) | **Fixed** in `InferenceKernel.typeFit` + `carrierFit`: a carrier pair ranks by the carriers' C3 distance, the function types as the carrier's type argument; a pairing legend-pure rejects (a bare function type on one side) ranks at the existing platform-rule distance (the
first audit suggested 0 for a bare function type; the platform-rule distance keeps one convention for every fit only
our acceptance admits, where 0 would rank such a fit as an exact match). Tests: `InferenceKernelTest.overload_aFunctionCarrierBeatsATypeParameterAndAny`, `overload_theNearerFunctionCarrierWins`, `CompileFunctionTest.fromOverAFunctionTypedParameterRunsTheFunction`; all three fail on the old code and pass on the new (checked). |
| **S1** | typing +18% on the eager compile probe (0-4% end to end): `ResolvedNames.form` recomputes `BareNames.catalog` (the name tried under 32 packages) at every form check | **Recorded as PARK-5** (root cause: a call to a platform function is never resolved once). **Open decision for the user:** land Phase 3 with PARK-5 recorded, or fix it first. Two attempted quick fixes were wrong and are reverted (a second copy of the name rule as string checks, which the identity guard rejected; a reorder of checks that offset the cost elsewhere). |
| **S2** | stale numbers | **To do at landing** (section 5). |
| **S3** | two plan items done differently, unrecorded | Recorded: the legacy TDS functions' rows by id moved to **Phase 4** (the platform's world declares none of them; PARK-11 and the plan's Phase 3 correction); the boot layer's ~12 upstream versions are **PARK-12** (closes in Phase 3b item 1). GATES must say both (section 5). |
| **S4** | port simplifications not listed | `FunctionMatch`'s class javadoc lists the known departures; PARK-7 to PARK-10 record them; `contravariantTypeFit` now ranks a parameter legend-pure rejects at the platform-rule distance (was an arbitrary 1). GATES needs one sentence (section 5). |
| **S5** | a dot call's receiver typed twice | **Recorded as PARK-6** (arguments typed more than once, in seven places). The stale warning "a second synth re-registers typer state" is corrected (`Overloads.checkGenericTyped`: the typer records no state per synth; the cost is time). |
| **S6** | "identical, every output file" too strong | GATES wording: "every result file"; note the reflection rows' function ids renumber (section 5). |
| N1 | pin comments replaced their dated history | **Fixed** (`IdentityGuardrailTest`: new notes prepended, history kept). |
| N2 | a NO_ROW refusal reported as "walled body" | **Fixed** (`UserCallInliner`: "no row for '…'"). |
| N3 | "a ranking never throws" was false | The javadoc now says a class that fails to compile fails the ranking loudly. (A catch returning an empty answer was tried and reverted: `ErrorShapeGuardrailTest` forbids a caught failure returning a value.) |
| N4 | NO_ROW's message said "implements" | **Fixed** ("declares"), with the javadocs. |
| N5 | a comment named an impossible case | **Fixed** (`StatementInline`). |

## 4. The ledger rows that land with Phase 3

`docs/PARKED_WORK_LEDGER.md` PARK-5 to PARK-14, anchored in `ParkedWorkLedgerTest` (guard green):
PARK-5 platform calls never resolved once; PARK-6 arguments typed more than once; PARK-7 `Any` ranked with the type
parameters; PARK-8 tie-breaks legend-pure does not have; PARK-9 the acceptance test admits what legend-pure rejects;
PARK-10 ranking parts not ported; PARK-11 legacy TDS functions by name (closes in Phase 4); PARK-12 the boot layer's
versions (closes in Phase 3b); PARK-13 a debug trace switched by an environment variable; PARK-14 a dot call falling
back to a function.

## 5. What is left, in order

1. **The user's decision on PARK-5** (section 3, S1).
2. **Re-run the checks the fixes touch** (B1 changes the ranking). **Done 2026-10-07, all green, on the worktree with
   every fix in:** core tests (24 targets), guardrails (the ledger anchors included), census and spec tests: 27 pass;
   the six corpus passes rebuilt and all 22 result files identical to `judges_base`; the reference lane rebuilt and
   byte-identical to the committed golden (B1 moves no line: no re-bless); PCT 17 of 17. Rerun only if more code
   changes. The steps were:
   - `bazel test //core:core_tests //core:guardrails //core:census //spec:spec_tests`;
   - the six corpus passes against the baseline (`START_HERE.md` section 5): the baseline is
     `runs/build-rebuild/runs/homework/phase3x/judges_base/` (built before Phase 3; the audit showed it equals main's
     own outputs);
   - the reference lane: build `//spec:reference_lane_report`, diff against the committed golden; if B1 moved lines,
     re-bless with `bazel run //spec:update_reference_lane` and say which lines moved and why;
   - PCT, all suites.
3. **Correct the documents** (S2, S3, S4, S6, plus the stale ones found since). **Done 2026-10-07 (uncommitted, or in
   the local fix commit):** the GATES entry (numbers from the final golden: 51 newly typed, 12 newly failing; 27 rows;
   109; item 3's two plan changes; the departures sentence; "every result file"; a paragraph on the audit and the ledger
   rows); `reasons.tsv` (every row matches a class in the golden; `//spec:reference_lane` passes); the execution plan's
   W1.1b note removed; the plan's Phase 3 Status bullet. **Prepared, not pushed:** IN_FLIGHT's update, as a whole file
   and a diff in the worktree's scratch (`runs/homework/phase3x/IN_FLIGHT.next.md`, `IN_FLIGHT.diff`: 27 rows, the
   fix files, `reasons.tsv`, no PRs, F-L1 now this program's, the START_HERE pointer); push it to main before the code
   (if main's IN_FLIGHT moved meanwhile, re-apply the diff's five edits to the new version). **Still a question for the
   user:** the `AGENTS.md` pointer. The items, for the record:
   - `docs/GATES.md`, the Phase 3 entry ("2026-10-06 — Build rebuild Phase 3"): "Bodies: 50 newly typed, 13 newly
     failing" becomes the counts from the final golden (the audit measured 51 and 12 before the fixes);
     `testViewChainsWithBusinessDate` types again (commit 4's dot-call rule): remove it from the newly failing list;
     "a shrink-only ratchet, 110" becomes 109; item 3 says the legacy TDS functions and `agg` are read by resolved
     names with rows by id moved to Phase 4 (PARK-11), and that about 12 boot-layer upstream versions still run
     upstream's body (PARK-12); one sentence that the port's departures are listed in `FunctionMatch`'s javadoc and
     PARK-7 to PARK-10; "identical, every output file" becomes "every result file identical; the logs differ by
     timings, and the H2 and DuckDB host passes' reflection rows renumber their function ids"; add the B1 fix and the
     ledger rows.
   - `spec/src/test/resources/reference-lane/reasons.tsv`: rows that describe the pre-Phase-3 state. `OVERLOAD isEmpty`
     and `OVERLOAD min` no longer occur; `DRIFT *` is 0; `PROPERTY_AS_CALL connectionByElement` no longer occurs (the
     one left is `string::toString`); `OVERLOAD *` says our ranking "differs from both of our overload algorithms",
     but the ranking is now legend-pure's and the 58 left are argument typing (legend-pure types the argument
     `Number` where we type `Integer`/`Float`, `[0..1]` where we type `[1]`, a supertype where we type the subclass);
     `OVERLOAD average`/`max`/`greaterThan` likewise; `PROPERTY_AS_CALL *` says the arrow and dot spellings "are not
     yet told apart", which commit 4's dot-call rule changed. Make each row match the classes in the final golden.
   - `docs/EXECUTION_PLAN_2026_09_26.md` on the branch says "Taken over (2026-10-06): W1.1b and the typing work it
     measures belong to the build rebuild program's Phase 3b". Wrong since the 2026-10-07 re-scope: W1.1b stays with
     that plan (IN_FLIGHT `f306bd698` already says so). **Done 2026-10-07** (uncommitted in the worktree): the note is
     removed; only the W2.1 note remains.
   - `docs/IN_FLIGHT.md` on main, the Phase 3 entry: "26 versions get rows" becomes 27; add the files the fixes touch
     that it does not list: `compiler/spec/UserCallInliner.java`, `CompileFunctionTest`, `ParkedWorkLedgerTest`,
     `docs/PARKED_WORK_LEDGER.md`. The program's IN_FLIGHT entry still says "one PR per phase": since 2026-10-06 there
     are no PRs (one CI run on the branch, then a push to main). The Bazel line's 2026-10-05 note says F-L1 ("a view
     inside a Schema is lifted twice") is "not fixed by this program ... for the compiler's owner": it is now this
     program's, Phase 3b item 1. This lands on main before the code (standing authorization).
   - The plan's Phase 3 Status bullet (plan branch): four commits plus the fix commit, the final numbers, the audit, the
     ledger rows, PROPERTY_AS_CALL 39 -> 1, the corpus account (four tests the step-2 change broke, fixed by commit 4).
   - `AGENTS.md` on main ("Current work: the compiler rebuild"): point this program's sessions at the plan branch and
     `docs/build-inventory/program/START_HERE.md`. A shared file: ask the user before changing it.
4. **A short audit of the fix delta** (B1's code and tests, the ledger and its anchors, the wording fixes).
5. **Done 2026-10-07:** the short audit of the fix delta ("ready after fixes": 0 blockers, 6 should-fix, 5 nits; all
   applied: two reasons rows for `range` and `propertyMappingsByPropertyName`, the `PACKAGE *` row removed, the ledger's
   closing rule, a "why parked" line per row, PARK-12's acceptance by rows, PARK-13's cost, the bare-function-type case
   in PARK-9, five more anchors for PARK-6 and PARK-10, `contravariantTypeFit` sharing the covariant distance
   (`generalizationDistance`), and the wording nits; report in the worktree's scratch, `AUDIT_FIXES.md`); the lanes
   rerun on the final code (six corpus passes identical, reference lane byte-identical, PCT 17 of 17); the fix
   committed locally and the branch rebased; the local gate (result in the GATES entry once it is in). **Left for the
   next session (the user, 2026-10-07: "leave the park decision and CI/push for next session"):** the PARK-5 decision;
   push IN_FLIGHT's prepared update to main; put `[skip ci]` on the tip commit; then:
6. **Land** (`START_HERE.md` section 4): one fix commit ("Phase 3: the audit's fixes ..."), rebase onto
   `origin/main`, `bazel test --lockfile_mode=error //gates:local`, tip commit with `[skip ci]`, push the branch,
   `gh workflow run gate.yml --ref build/phase3 -f gates= -f platforms=all`, then on green
   `git push origin "${SHA}:refs/heads/main"`.
7. **After landing:** IN_FLIGHT says Phase 3 landed (and announces Phase 3b when its plan is agreed); the plan's
   Status bullet gets the landed commit.
