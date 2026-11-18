# JUDGES: verdict SOUND-WITH-DEBT

All `path:line` are at the PR head `68f20eb4d` (worktree `scratchpad/pr118`), `core/src/main/java/com/legend/` omitted.

## Summary (at most 12 lines; answer your brief's questions first)

1. **Edge off: the judges decide in lite's rules (verified).** Every rule that differs from legend-pure is now a two-form kind: `LiteHostForms`, `LiteVerdictNumbers`, `LiteCrossKindNumbers`, `JudgeJson.STRICT`, `LiteRenderedTexts`, `LiteRenderedTextRule` and the two word sets. The product's forms are strict, and asking `LiteVerdictNumbers.floatsWithin` for a tolerance throws. I found no compatibility leniency left in any product decision. **Edge on:** the decisions are compiled legend-pure's. But some of the "legend-pure words" are lite's own old sentences under a new label (JUDGES-5). I could not check them against the upstream sources, because there is no checkout here.
2. **G2 (judge parity).** I found no case where the PR makes one judge pass and the other fail a body. There are three gaps:
   - The fix for a `let` that raises covers only `LegendCompileException`, and the fix adds a new asymmetry: an effect after the failing let (JUDGES-1).
   - The new `assertNotEq` and `assertIsNot` (#28) inherit a host arm that compares by the carrier's kind, not the declared kind (JUDGES-2, a wrong answer that already existed in `assertEq`).
   - Several host failure texts bypass `HostWords`. Under lite's isolated BOTH run, a failing assert of those kinds reports "the judges disagree" instead of its own message (JUDGES-3).
3. **Known issues:**
   - (a) **Fixed by moving it.** The 2-ULP tolerance is only in the edge now (`edge/EqualToJson.java:90`, `edge/CompiledPureHostForms.java`, `edge/CompiledPureVerdictNumbers.java`).
   - (b) **Fixed** (`testexec/HostJudgments.java:629-633, 825-830`). The fix missed the sibling arm `eqOf` (JUDGES-2).
   - (c) **Partly fixed, and it moved** (JUDGES-1).
   - (d) **Still there, not made worse.** Main had 18 sites; the head has 14 in testplan plus 2 moved to the edge. `NULL_POINTER` and `CLASS_CAST` are still planned failures (JUDGES-4).
4. **Quality.** The hooks are a real redesign, and clean. The engine-text arm, the referee and the appeals were moved 89-98% line for line, so the edge stays wired into the product's test planner through a forwarding facade of about 45 methods. That matches the handoff's own admission (D1).

## Blockers

None. The one wrong answer (JUDGES-2) already existed on `main` for `assertEq`, and the PR does not make it worse. The test-plan format changes are Java-only and in-process, so they are cheap to undo.

## Strengths

- **The rules are hooks, not flags.** The host takes its rules from the run hook (`HostJudgments.java:310`: `new HostRules(run.hearing().hostForms())`); the database judge takes its rules from the style at planning (`testplan/DatabaseJudge.java:371-376`). The product's forms are separate classes holding only types (`testexec/LiteHostForms.java`, `lowering/LiteVerdictNumbers.java`, `testplan/LiteCrossKindNumbers.java`). `testexecutionplan/TestPlan.JudgingRules` and `JudgingRulesReachTest` were deleted with their last user, not left vacuous.
- **Fail-loud product forms.** `LiteVerdictNumbers.floatsWithin` throws (`lowering/LiteVerdictNumbers.java:24-26`). `NoEdgeRun` and `NoEdge` refuse each compatibility call by name (`testexec/NoEdgeRun.java:46-71`, `testexecutionplan/NoEdge.java`). No null stands for "lite".
- **A mixed run is refused.** A plan worded in one style, run with the other style's hook, is refused by name (`testexec/TestRunner.java:99-104`).
- **The edge's facts are opaque to the product.** `HostJudgment.EngineText` (eight compatibility fields) became `EdgeJudged(Extension judgment, ...)` (`testexecutionplan/HostJudgment.java:251-253`).
- **(b) fixed at the root.** A failure message is now the words for a verdict that already failed. `finish` throws if the words return null (`HostJudgments.java:78-83`), so a message can no longer overturn a typed verdict.
- **One copy of the ULP SQL.** Main had two copies in `VerdictSql` (main `:636-644` and `:2634-2663`); the head has one, at `edge/CompiledPureVerdictNumbers.java:43-55`.
- **A strong drift detector under lite.** The isolated BOTH run compares the two judges' messages as well as their verdicts (`testexec/IsolatedRun.java:157-160`). That is why the gaps in JUDGES-3 become visible rather than silent.
- **Product judges are smaller.** testplan, testexec, testexecutionplan and test together: 24,869 → 22,482 lines.

## Findings

### JUDGES-1 [BROKEN-RULE] [FIX-AFTER] Known issue (c): a `let` that raises at planning is held differently by the two judges

- Where:
  - Host: `testplan/HostAsserts.java:175-177` holds ANY `RuntimeException` as `pendingLet`.
  - Host: `testplan/HostAsserts.java:137-143` refuses the body if an effect or a frame-let execute follows the pending let.
  - Database main pass: `testplan/DatabaseAsserts.java:160-164` holds only `LegendCompileException`.
  - Database main pass: `testplan/DatabaseAsserts.java:172-184` closes the segment at an EFFECT without checking `pendingLet`.
  - Database static pass: `testplan/DatabaseAsserts.java:520-526` returns false (the body is refused) for any other `RuntimeException`.
- What: The PR fixed `LegendCompileException` (commit `776984dd6`, "B1"). Two asymmetries remain:
  - (i) A let whose planning raises anything else is handled differently. Examples: `NotImplementedException` or `DialectCapability` from a frame let's `plannedFrame`/`buildFrame`, `IllegalStateException` from `registerHandlesIn`. The host plans the raise at the next assert, after the earlier asserts have run and reported. The database judge refuses the whole body ("database judge's planner refused an assert"). Under BOTH, the refusal comes out of `planIsolated` (`testplan/TestPlanner.java:87`), so nothing runs.
  - (ii) New with the fix. For `let x = <model error>; <an effect statement>; assertEquals(1, 1)`:
    - the host refuses the body (`HostAsserts.java:137-143`, whose own comment says upstream "raised at the let and sent nothing");
    - the database judge plans the effect, sends it, and only then raises the let's error at the assert.
- Why it matters: G2. One body gets two outcomes depending on the judge mode, and the per-assert reports differ. In case (ii), the database road sends a write that upstream never sends. Pass and fail at the body level agree: both fail.
- Instead: Make one `LetScope` policy that both walks share (the earlier review's TESTPLAN-3), holding the same exception classes. Have the database main pass refuse an EFFECT or LET_EXECUTE after a pending let, as the host does.
- Confidence: the code asymmetry is verified. That a concrete body reaches each path is suspected: I did not run one.

### JUDGES-2 [WRONG-ANSWER] [FIX-AFTER] `assertEq`, and the PR's new `assertNotEq`/`assertIsNot`, compare by the carrier's kind on the host and by the declared kind in the database

- Where:
  - Host: `testexec/HostJudgments.java:563-575` (`eqOf` → `PureAsserts.eqHolds`, `testexec/PureAsserts.java:47-67`, which uses `Equality.Typed.of`, the carrier's kind).
  - Host: `testplan/AssertVerdicts.java:990-992` (#28: `HostJudge.negated(HostJudge.planEq(...))`).
  - Database: `testplan/AssertVerdicts.java:653-666` and `testplan/DatabaseJudge.java:91-101` (the declared-kind static gate: "kinds differ").
- What: Known issue (b)'s premise applies: a Decimal-declared property mapped to an INT column arrives as a `Long`. Take `assertEq(1, $p.decimalProp)` where the value is 1:
  - the host compares `Long` with `Long`, finds them equal, and HOLDS;
  - the database judge sees Integer against Decimal, which lite's rule S127 says never equal, and FAILS statically.

  With `assertNotEq(1, $p.decimalProp)`, added as a native in this PR (commit `e9363946f`), the two outcomes swap. The fix for (b) (`26717de29`) made `assertSameElements` and `assertEquals` use declared kinds, but not `eqOf`.
- Why it matters: The host is the default judge. It passes an `assertEq` that lite's own rule fails: a false green of the same class as (b). Under lite's BOTH run the two judges disagree.
- Instead: `eqOf` should type each side by its declaration: `typedSide(List.of(ee), j.e())`, as `sameElementsOf` and `equalsOf` already do. Then `eqHolds` compares two `Equality.Typed` values.
- Confidence: verified that the two host arms type values differently. The wrong outcome on a database is suspected: I did not run it.

### JUDGES-3 [BROKEN-RULE] [FIX-AFTER] Item J29 is incomplete: some host failure texts bypass `HostWords`, so lite's two judges disagree on the message and the edge prints lite's text

- Where (each a product sentence that is neither lite's `AssertSentence` nor routed through the run's words):
  - `testexec/HostJudgments.java:546-547` (`" (forAll-contains subset): missing "`);
  - `testexec/HostJudgments.java:891-892` (`" (rendered " + form + "): " + Verdicts.firstTextDiff(...)`);
  - `testexec/HostJudgments.java:139-141` (`" (TDSRow.values) raw cells do not equal a whole TDS value"`);
  - `testexec/AssertErrorRuns.java:87-90`.

  The database judge words the same cases another way:
  - `lowering/VerdictSql.java:796-797`: the frame over `'subset'` and the side's text;
  - `testplan/AssertVerdicts.java:1873-1877`: `runVerdict` over `textEquals`, framed by `LiteVerdictWords.frame` as an `EQUALS` sentence.
- What: In lite's isolated BOTH run, messages are compared (`IsolatedRun.java:157-160`). So a failing rendered-text or forAll-contains assert ends in `JudgesDisagree` instead of the assert's own failure. With the edge on, the host reports these lite texts where legend-pure prints `"Assert failed"` (for `assert(...)`) or assertEquals' `'\nexpected: %r\nactual:   %r'`. The guard `CompatSpellingGuardTest` cannot see these sites, because it looks only for legend-pure's sentences.
- Why it matters: G2 holds for verdicts but not for messages. Edge-on failure messages diverge from upstream with no register row (the upstream-fidelity tenet).
- Instead: Add `HostWords.subsetMissing`, `HostWords.renderedDiffers` and `HostWords.rawCellsVsTds`. The lite forms write `AssertSentence` rows, the same ones `LiteVerdictWords` builds; the edge forms write legend-pure's texts. Alternatively, register each divergence as a row.
- Confidence: verified that the texts differ. The disagreement under BOTH is suspected: I traced it, I did not run it.

### JUDGES-4 [WRONG-DESIGN] [FIX-AFTER] Known issue (d) still exists: planning turns `NullPointerException` and `ClassCastException` into planned failures

- Where: `catch (RuntimeException` at:
  - `testplan/HostAsserts.java:175, 316, 324, 364, 456, 468`;
  - `testplan/AssertVerdicts.java:95, 450, 847`;
  - `testplan/DatabaseAsserts.java:311, 520, 550, 576`;
  - `testplan/HostSides.java:68`;
  - moved to the edge: `edge/SqlTextVerdicts.java:1275, 1544`.

  The closed list that admits them is `testexecutionplan/Failure.java:68-77` (`NULL_POINTER`, `CLASS_CAST`, `INDEX_OUT_OF_BOUNDS`, `ILLEGAL_STATE`, ...).
- What: The count went from 18 on main to 14 in testplan plus 2 in the edge. The only sites that went were the agreement half's planner (`planEach`). The PR added no site and narrowed none. A defect in lite's planner (an NPE) is still recorded as the assert's raise. It is raised at the assert's turn, without its stack trace, and reported as the test failing.
- Why it matters: G7 (no fallbacks). A planner bug is reported as the test's failure, and the wrong judge's run carries the blame.
- Instead: Catch the closed list of lite's error classes (`LegendCompileException`, `NotImplementedException`, `DataError`, `DialectCapability`, `AssertFailed`). Remove `NULL_POINTER`, `CLASS_CAST`, `INDEX_OUT_OF_BOUNDS`, `ILLEGAL_STATE` and `ILLEGAL_ARGUMENT` from `Failure.Kind`, so that a JDK exception fails planning with its stack.
- Confidence: verified.

### JUDGES-5 [WRONG-DESIGN] [FIX-AFTER] Lite's own main-era sentences relabelled as "legend-pure's words" in the edge

- Where:
  - `edge/EdgeHostWords.java:107-111` (`notEquivalent`: lite's `summarize` sketch, which main's javadoc called "no witness pins its spelling"; plus `TdsCompare.tdsEquivalent`'s `"assertTdsEquivalent: cell i expected x, got y"`);
  - `:164-167` (`": the element references differ"`);
  - `:189-191` (`"assertJsonStringsEqual: FIRST DIFF at "`);
  - `:99-102` (`" (TDSRow.values): cell multiset differs"`);
  - `edge/UpstreamVerdictWords.java:107-118` (the same tdsEquivalent text in SQL);
  - `edge/EqualToJson.java:85-92` (a 2-ULP tolerance that the class javadoc calls "the referee's policy", not legend-engine's `JsonNodeComparator`).
- What: J29 moved every non-`AssertSentence` host text into the edge's legend-pure form byte for byte (compare main `testexec/HostJudgments.java:376-377, 562, 803`). Some of those texts were lite's inventions, not upstream's. Separately, product code computes a text that only the edge prints: `TdsCompare.tdsEquivalent`'s diff string is handed to `HostWords.notEquivalent` (`testexec/HostJudgments.java:372-400`), and `LiteHostWords` ignores it. That is the meaning rule's "a product method that computes upstream's alternative".
- Why it matters: With the edge on, these messages are not upstream's, and no register row says so (the upstream-fidelity tenet).
- Instead: Read each sentence from the pinned legend-pure sources and fix the text or ledger it. Move the cell-diff text into the edge, built from the typed grids.
- Confidence: verified that the texts are lite's and carry no row. What upstream prints is suspected: no checkout was available.

### JUDGES-6 [WRONG-DESIGN] [FIX-AFTER] `judgesEngineText()` serves as an "is this the edge" flag

- Where: `testplan/EngineTextGates.java:93-109` (`planningCall`, `siteWords`, `sizeWords`).
- What: Three unrelated concerns branch on one boolean that is meant to say whether a style judges engine text:
  - recording the words of raise sites;
  - filing site words;
  - wording a "no rows under a required bound" refusal.

  `sizeWords` writes lite's form inline in the product branch (`ErrorKind.SIZE_NOT_MULTIPLICITY.sentence() + ": " + multiplicity`) instead of in a product form of its own. A style that words errors legend-pure's way but judges no engine text would silently get lite's size words.
- Why it matters: It breaks hook criterion (b) in spirit: the product branches on whether the style is the edge's, where it should ask a two-form kind.
- Instead: Make a two-form kind `SizeWords` (`LiteSizeWords`), and a facet for site recording that does not depend on `judgesEngineText`.
- Confidence: verified.

### JUDGES-7 [WRONG-DESIGN] [NOTE] The isolated BOTH comparison matches reports by index, and its javadoc misdescribes main

- Where: `testexec/IsolatedRun.java:143-172`, `:111`, `:146`.
- What:
  - One judge can emit two reports for one assert: UNJUDGED or DECLINED, then the VERDICT (`testexec/RootLoops.java:174-176, 220-224`). The comparison walks the two lists by index, so after one extra report it compares the wrong asserts. The body-level outcome still comes out right, because the `endsAlike` check and the length mismatch catch it. But the message names the wrong asserts, and the rule "declined or unjudged are not compared under compatibility" works only by accident.
  - The javadoc says the host's verdict stood "as it did in the fused agreement". On main, a database decline or UNJUDGED failed the assert: `AgreementArm.undecided`, the user's 2026-11-05 ruling "no fallback mode".
  - "A recorded failure is never swallowed" is false under compatibility: when both judges fail, the database run's error (an NPE, for example) is dropped.
- Why it matters: G2's evidence is fragile and hard to diagnose.
- Instead: Key the reports by assert ordinal; rethrow a database failure whose class differs from the host's; correct the javadoc.
- Confidence: verified by reading.

### JUDGES-8 [WRONG-ANSWER] [NOTE] `negatedOf` reads a declined inner judgment as "held"

- Where: `testexec/HostJudgments.java:501-512`.
- What: The inner judgment returns null when it declines its sides (for example `containsOf` at `:450-452`, when the value is not one value). `negatedOf` discards that result, so the negated assert FAILS with "should not contain" words instead of refusing. Today the planner keeps the shape out (`exactlyOneValue`, `AssertVerdicts.java:993`), so this is latent.
- Instead: `if (judge(n.inner(), sides, run) == null) return null;`
- Confidence: verified (latent).

### JUDGES-9 [WRONG-DESIGN] [NOTE] Presentation text baked into the test plan

- Where: `testexecutionplan/SideShape.java:30-37` (`sizeWords`), `testexecutionplan/TestPlan.java:36-47` (`answerSizeWords`, `words`), `testplan/AssertErrorNative.java:76-82` (a `Failure.message` worded by `PlanStyle.errorWords`).
- What: Under the edge's style, the plan carries legend-pure's sentences as data. That works against the next program's "one plan, two presentations".
- Why it matters: It is cheap to change. Test plans are Java-only and in-process (`TestPlanJson` went v21 → v32 in this PR), so this is not a blocker.
- Instead: Carry the kind and the bound in the plan, and word them in the hearing.
- Confidence: verified.

### JUDGES-10 [COSMETIC] [NOTE] Dead code and stale text left from the fused agreement and train 17

Three patterns:

- **Dead code (2 items):**
  - `testplan/DatabaseAsserts.java:410` `errorVerdictFor` has no caller;
  - `testexec/RootLoops.java:134-158` takes a `messages` argument that is always `true` (`TestRunner.java:279`).
- **Stale javadoc (22 mentions of the deleted agreement judge across testexec, testplan, testexecutionplan and the edge's SqlText* and Appeals). Examples:**
  - `edge/SqlTextRuns.java:45`: "its database half reports to no listener". It does now.
  - `testexec/Equality.java:21`: "Decimal by the engine's scale-sensitive equals, Float exact with ONE declared leniency". Both are now the hooks'.
  - `testplan/AssertErrorNative.java:32`: "EXACT failure spellings".
  - `testexec/TdsCompare.java:28`: "bounded-float-tolerant".
- **Register drift:** `docs/SEMANTICS_REGISTER.md` S15 and S132 still cite `JudgingRules.floatTolerance` and `exec/Equality.java:71-83`, both gone.

Confidence: verified.

## Cross-area observations

- **LOWERSQL:** every verdict row still projects the `__lenient` column, even in lite, where it is always false (`lowering/VerdictSql.java:1115, 1228, 1410, 1798, 1897, 2130`). That is a compatibility-only column in the product's verdict schema (`RootLoop.LENIENT_COLUMN`).
- **EDGE:**
  - The moves are copies. Lines identical to main, as a share of the head's lines: `SqlTextVerdicts` 89%, `SqlTextRuns` 92%, `SqlReplayReferee` 98%, `Appeal` 97%, `SqlTextInputs` 95%, `Appeals` 71%. `EqualToJson` is main's `Equality.serviceDiff` nearly verbatim.
  - The edge reaches into the product planner through `testplan/EngineTextPlanning.java`: about 45 forwarders, `@OpenLine("C4")`.
  - Product plan types still carry fields that only the edge uses: `RootLoop.Row.appeal` and `.decline`, and `VerdictBatch.defer(..., appeal, decline)`. These are designed hook arguments, but they cost something when the portability program removes them.
- **PRODUCT:** `PlanStyle` (in the product's `database` package) still holds hooks shaped by upstream (`errorWords`, `graphKeyDate`, `graphKeyString`, `traceCell`, `siteWords`).
- **Product scope:** the service-test runner is now edge-only (`edge/ServiceTestRunner.java`). With the edge off, the product cannot run a user's Service `testSuites`. Main had no product caller either, so nothing shipped is lost, but there is now no lite form and no refusal by name.
- **Verification gap:** I could not check edge-on fidelity against legend-pure or legend-engine (no reference checkouts here, Bazel not run). The edge golden net and channel B are the only evidence.

## Measurements (numbers with the command that produced them)

- **Product judge packages, main → head: 24,869 → 22,482 lines.**
  - testplan 13,345 → 12,461; testexec 6,092 → 5,628; testexecutionplan 4,143 → 3,751; test 1,289 → 642.
  - Main: `git ls-tree -r --name-only main core/src/main/java/com/legend/<d> | xargs -I{} git show main:{} | wc -l`. Head: `cat <d>/*.java | wc -l`.
- **Edge judging files: 5,264 lines.** `wc -l` over SqlTextVerdicts, SqlTextRuns, SqlTextReferee, SqlReplayReferee, the Appeal* files, PlannedAppeals, RefereeLoad, ReplayGates, EdgeHostWords, EdgePureWords, UpstreamVerdictWords, UpstreamRenderedTexts, the CompiledPure* files, EqualToJson, EdgeEngineTestText, EngineTextJudgment, the Upstream* forms, NotAConstant and JsonSimpleGrammar.
- **`catch (RuntimeException` sites: main testplan 18 → head testplan 14, plus edge 2.**
  - `git grep -n "catch (RuntimeException" main -- core/src/main/java/com/legend/testplan | wc -l`
  - `grep -rn "catch (RuntimeException" testplan | wc -l`
  - `grep -rn "catch (RuntimeException" edge/SqlTextVerdicts.java`
- **Line identity of the moved files.** The script is `scratchpad/audit/verify/judges/sim.py` (difflib over stripped, non-import lines; pairs in `pairs.txt`). Lines identical to main, against the head's line count:

  | File | Identical / head lines | Share |
  |---|---|---|
  | `SqlTextVerdicts` | 1,405 / 1,577 | 89% |
  | `SqlTextRuns` | 242 / 263 | 92% |
  | `SqlReplayReferee` | 148 / 151 | 98% |
  | `Appeal` | 154 / 158 | 97% |
  | `SqlTextInputs` | 58 / 61 | 95% |
  | `Appeals` | 248 / 350 | 71% |
  | `PlannedAppeals` | 35 / 43 | 81% |

- **Test plan format: v21 (main) → v32 (head).** `grep -n "VERSION\s*=" testexecutionplan/TestPlanJson.java`, at both commits.
- **Stale agreement-judge mentions: 22.** `grep -rn "agreement judge\|fused agreement\|AgreementArm\|agreement's\|agreement rule"` over testexec, testplan, testexecutionplan and the edge's SqlTextRuns, SqlTextVerdicts and Appeals.
- **2-ULP code outside the edge: 0 decision sites.** `grep -rn -i "ulp" testexec testplan testexecutionplan` finds only the counters in `CanonicalDivergence` and `DatabaseVerdicts.java:46-47`, which read the database's `LENIENT` column.
