# TESTPLAN verification

## Verdict on the unit: agree, WEAK
The output model is sound: `HostJudgment`, `TestPlan.TestStep` and `TestPlan.Side` are sealed, and every plan
round-trips its JSON (`TestPlanner.java:167`). The planner that makes that data is walk-era evaluator code with the
send branches cut away. It has two family routers that were once one, four copies of the let ladder, and a 1,316-line
precondition predictor that has to mirror the planners' state. Every new assert family, let kind or planner capability
is a change in three to six places, which is the rubric's WEAK. It is not REWORK, because the plan types, `TestDoor`,
`AgreementAsserts`, `EffectPlanner` and the small helpers can stay as they are. The four S1s are two problems, each
reported twice: TESTPLAN-1 and -2 are the database arm's router, and -3 and -4 are the body walks. One redesign of each
fixes both findings in its pair.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| TESTPLAN-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 | Both switches are over `NativeFn.Verdict` in the same family order (`adjudicate` :369-681, `planArm` :898-993), with the same sub-route order for equality: sql-text, rendered, grid pair, order view. `planHost`'s javadoc says so itself: "classified as adjudicate classifies it, in its order". `VerdictArm` has 18 methods and one implementation (`DatabaseJudge.java:770`). Correction: the database gate runs `planHost` (:197), and `gateOf` re-checks `sqlGridPair` (:234-268). So the database road's effective admission is the intersection of the host's, the gate's and `adjudicate`'s. The two differences the reviewer cites are mostly unreachable branches of `adjudicate`, not live drift. History shows the duplication is accidental, not a deliberate difference between judges (details below). |
| TESTPLAN-2 | S1 | CONFIRMED | S1 (one redesign with -1) | `runVerdict` (`DatabaseJudge.java:405-422`) defers and returns `ok()`. `unjudged` is typed `TypedSide` and always throws (:475-484). `Verdicts.fail` throws `AssertFailed`, which `planDatabase` catches and turns into `resolve` (:95-110), so the returned `TypedSide` is discarded. The sentinel tokens are compared by `==` (`SqlTextVerdicts.java:170-171,176,182,232,394,402`), and `textFirst` defers, then calls `orElse(rows::get)`, then returns `ok()` (`AssertVerdicts.java:1936-1957`). The "recording mode" description is accurate, and I0/I2 (`a55c3f633`) confirm it: the arms' send branches were deleted and the record branch was kept. |
| TESTPLAN-3 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | All four let ladders were found at the cited lines. jscpd confirms one exact clone (`DatabaseAsserts.java:598-615` against `:148-164`). The others differ only in frame builder, environment, stamp and try policy. `steps(int)` appears three times and `savepointOf` twice, as cited. Correction: "the same handling of branches, assertError and statement-root asserts" overstates it. The walks share the detection of those statements, not what they do with them (the host recurses through `planBranch`, the database pass closes its segment and recurses, the static pass counts the segment, `planEach` skips). That part is judge strategy, not copy. Missed consequence: the divergent let-raise policies make the judges disagree (details below). |
| TESTPLAN-4 | S1 | CONFIRMED-WITH-CORRECTION | S1 | The production reads are as stated: `TestPlanner.java:90,95,100,126,137`, and `of(..)` with `oneJudge=false` is reached only from tests. EFFECT and QUERY share one path. The `seeded` set mirrors `TestKinds.established` (`BodyClass.java:461-505`). Correction: the claim that the planners already refuse by name at `TestPlanner.java:139-147` is wrong, because those lines are BodyClass's own refusal (`bodyClass == null`). BodyClass also has a real present purpose: the planners rely on it as their precondition checker, and deleting it would let mis-planned bodies through silently (details below). |
| TESTPLAN-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | There are 18 `catch (RuntimeException` sites at the cited lines. The swallow at `DatabaseAsserts.java:531-534` has an empty body, and `SqlTextVerdicts.java:1296` catches and returns null ("no producer, as before"). The NPE, CCE and IOOBE kinds exist in `Failure.Kind` (`Failure.java:61-68`). Correction to the alternative: `Failure.java:69` documents that the ArithmeticException, NumberFormatException, DateTimeException and NoSuchElementException kinds are "raisable by planning's folds and reads", which may be user-level outcomes. Remove NPE, CCE and lite's own ISE refusals, and review the rest rather than dropping every JDK kind. |
| TESTPLAN-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | lizard reproduces the figures: CCN 92, 13 params (`databaseVerdict`); CCN 76 (`frameProgram`); CCN 65 over 313 lines (`adjudicate`). `rowsLegAndVerdict` has 15, 17 and 18 params. Corrections: `sideShape` (`JudgePlans.java:319-325`) has 5 booleans, not 6, and it is a pure forwarder to the `SideShape` constructor, which strengthens the point. lizard counts about 80 functions in `AssertVerdicts`, not 75 (immaterial). |
| TESTPLAN-7 | S2 | CONFIRMED | S2 | `ExecEnv` has 9 components, 4 of them `@Nullable`. `VerdictIds` and `EngineTextAppeals` are shared mutable state (`BodyDriver.java:59-71`). `env.queryLets().putAll` is at :304 and `env.plan().stamp(stamp)` at :395. The `HostAsserts.java:95-101` javadoc reads as quoted. `systemRouted ? bodyEnv : null` together with a separate `systemRouted` boolean is at `TestPlanner.java:128-133`. |
| TESTPLAN-8 | S2 | CONFIRMED-WITH-CORRECTION | S2 | "the walk" appears 283 times, but in 19 files, not 12 (BodyClass 73, AssertVerdicts 50, DatabaseAsserts 37, HostAsserts 36). The stale docs are confirmed: VerdictBatch's "This class sends" (`VerdictBatch.java:26-45`; its `Runner` is now a planner), BodyDriver's class doc (it also says `Execution#runProgram` delegates here), `runVerdict`'s "Execute", and VerdictArm's host arm. The dangling `HostJudge` links were spot-checked: none of the six targets is defined in `HostJudge`. |
| TESTPLAN-9 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | Confirmed: `databaseMode` (the spec match is an unrelated local variable), `containsTreeMarker` and `concat` have no callers; `answer`'s `rider`/`identityLane` and `evalStringArg`'s `env` are unused; the `dbType == null` check is effectively unreachable; `refused(why)` returns `why`. Corrections: `VerdictBatch.frameCensus` is called from `spec/src/test/java/com/legend/rcorpus/MinimalCorpusTest.java:805`. `EFFECT_ARMS` has 11 entries, not 10, and is a live dispatch table (`BodyDriver.java:664`) with 5 live arms (setUpDataSQLs x2, print, println, connectionByElement) and 6 throwing ones. Only `registeredRoutineKeys` exists purely for the governance test. |
| TESTPLAN-10 | S2 | OVERSTATED | S3 | The writes are as cited: `VerdictCensus` is a static `ConcurrentHashMap` (`VerdictCensus.java:28`), and `BodyClass.java:747-752` reads system properties and prints to stderr. But no planning decision reads any counter: the plan output does not depend on them, so G10 (plan determinism) and G4 are not affected. The `HostAsserts.java:33` quote is misread: the same sentence goes on, "its census of the families and routes it planned is planning's own measurement, which the run never reads". Real residual costs: tests assert global census deltas (`AssertVerdictsTest.java:142-146`, `BodyRouterParityTest.java:2011-2060`), which is order-sensitive. Census hygiene was the stated reason for the static pass, which is already counted in TESTPLAN-3. |
| TESTPLAN-11 | S2 | CONFIRMED | S2 | The render forms cross the plan boundary as strings and are parsed by prefix in exec (`TdsCompare.java:204,244`, `CanonicalDivergence.java:97`), so the contract spans three packages. `"__canon"` appears at 7 sites in `DatabaseJudge` (:197,236,241,522,553,723,748), alongside `CanonicalRenderSql.java:322`, `ClassLayouts.java:46` and `CellText.java:380`. The plan's JSON (G11) does not force strings: a sealed form serializes as well. |
| TESTPLAN-12 | S2 | CONFIRMED | S2 | `DatabaseJudge.java:1013-1019` renders, catches `DialectCapability`, then uses the byte compare. This is chosen at planning and nothing is sent, so it is not a run-time retry. It does change the judgment from multiset to bytes on that dialect while the host stays a multiset, which is a G2 risk (the comment at :1000-1004 says so). The render probes at :464 and `JudgePlans.java:139-142` are confirmed. |
| TESTPLAN-13 | S2 | CONFIRMED | S2 | The file is 2,177 lines with 54 `assertParity(` occurrences. The `ThreadLocal COMPAT` is at :51-63. `assertParity` (:1923-1930) checks only that BodyClass admits the body and that the run's moved count is 1. 28 of the calls are bare statements, so for those the outcome is never asserted. |

## Details

**TESTPLAN-1 (are the two classifiers parallel, or deliberately different?)** They are parallel by accident of history, not by design.
- `ec975a079` (2026-09-21, "one dispatch") built one router, `adjudicate`, dispatching through `VerdictArm` to a host, a database or an agreement implementation.
- M6 (`c9847ebe8`, 2026-11-01) added `planHost`/`planArm` as a second router for the host's planned halves.
- I2 (`a55c3f633`, 2026-11-07) deleted the host's and the agreement's in-place arms. That left `VerdictArm` with one implementation and its javadoc describing two.

The judges answer the same question by G2 and differ only in capability, and the reviewer's alternative keeps those capability refusals in each judge's planner:
- The host needs the decoded shape known (`exactlyOneValue`, `oneString`, `typeRefName`; the "S3" comments at :959-974).
- The database needs SQL-expressible grids (`sqlGridPair`).

Part of the alternative already exists as shared helpers: `QuantifiedShape`/`quantifiedShape`, `quantifiedForm` (:1214), `sqlGridPair`, `renderedPlan` and `renderedStatic`.

Correction on "drift shows up as a disagreement". The database road is gated by `planHost` (:197) and `gateOf`'s own switch (:234-268), which is a third partial family switch. So `adjudicate`'s more permissive branches are mostly unreachable, rather than live divergence:
- Its `assertInstanceOf` without a type reference is refused by `planArm` first.
- Its `unjudged` for non-static grids is pre-empted by `gateOf` returning UNPLANNED. The body is then refused by name, not failed as unjudged, despite the comment at :553-558.

The cost is three overlapping classifiers whose intersection is the behaviour, with dead branches a reader cannot spot. That is still S1.

Size of the fix: medium-large. `adjudicate`, `planArm`, `gateOf` and `planQuantified`/`unrolled`/`quantified` collapse into one `classify` plus two judge planners, and `VerdictArm` is deleted. TESTPLAN-2's `DbVerdict` return type is the same redesign.

**TESTPLAN-3.** The true copy is the let ladder: FRAME / ALIAS / STAGED_GRID / `registerHandlesIn`, about 35 lines, in four places (`HostAsserts.java:140-176`, `DatabaseAsserts.java:133-165,450-481,589-625`).
- The STAGED_GRID and the handle branches are byte-identical apart from the environment.
- FRAME differs only in builder and stamp (`plannedFrame` with the host stamp, `plannedFrame` with the segment stamp plus `"frame_"` fragments, `buildFrame` on a fresh `pre` env, `plannedFrame` with the host half's stamp).
- The surrounding walks share statement detection. What each does at a branch, an effect or an assertError is judge-specific segmentation, so "same handling" overstates it. A `LetScope.bind(let, i, frameBuilder)` helper removes the copy; that part is local. A `BodyWalk` with a judge strategy is the unit-level part.

The finding undersells one consequence: the divergent exception policies make the judges disagree. For a body whose let raises at planning:
- The host plans the raise at the next assert (`HostAsserts.java:171,213-218`), which is upstream's turn.
- The database judge's static pass returns false (`DatabaseAsserts.java:474-480`), so `plan` returns null and `TestPlanner.java:139-146` refuses the body as "database judge's planner refused an assert".

That is one body, two outcomes, one per judge, which is a G2 gap. I traced this by reading the code; I did not run it. The static pass's stated reason, that a body left for the walk counts nothing, is moot now that a declined body is refused. So the pass can go once the main pass handles a let's raise the way the host does.

**TESTPLAN-4 (does BodyClass have a real purpose today?).** Production reads it in exactly one place, `TestPlanner.plan`, which every test body passes through via `TestExecution` (PCT, corpus, core). It decides four things:
1. Null means the body is refused by name, with the `BodyKinds` census in the message.
2. ASSERT or FRAME means the judge's planner plans the asserts. EFFECT or QUERY means `Asserts.NONE` (one path).
3. ASSERT alone may be system-routed: a FRAME program never is.
4. FRAME with no assert forces the HOST judge.

What would break if it were deleted? Some rules duplicate refusals the planners make anyway (`TestKinds.plan`'s `NotImplementedException` at :206-217, the judges' null), and for those only the message would change. But the planners also document BodyClass as their precondition for shapes they would otherwise plan wrongly, silently:
- `ProgramFrames.java:117-123`: "two frames whose query lambdas bind one name are the walk's by the class rule ... so the frames a program plans never bind one name two ways". Without the rule, a stale frame reads the later binding, against upstream.
- `TestKinds.java:146-148`: "a statement naming test data after an open script is refused by the class rules". Without it, the seed lands on the wrong side of the script.
- `BodyClass.java:163-168`: a store write reached through a helper. "The program road opens [a transaction] for an Update step alone", so without the rule there would be no rollback.
- The seed-after-a-send ordering against the database judge's fused statement (:487-505).

So the expert alternative is right in shape, but "they already do" is wrong. Each hazard check has to move into the component that owns the state: `ProgramFrames.let` refusing a rebinding read, `TestKinds.seed` refusing a seed across an open script, the store-write planner owning its transaction scope. The re-simulation can be deleted only after that. That is medium-large work, and the S1 stands. Without that move, deleting BodyClass would turn named refusals into wrong verdicts.

**TESTPLAN-9.** "No callers" is wrong for `frameCensus`: the reviewer missed the `spec` module. "`EFFECT_ARMS` exists for the guard" is wrong: it is the live dispatch for `Prepared.Effect` (`BodyDriver.java:664`). What is true is that 6 of its 11 entries are throwing stubs kept so that the registry equals the catalog. The remaining dead items stand.

**TESTPLAN-10.** Lowered to S3, because the counters are write-only measurements: the planner decides nothing from them. The reviewer's G10/G4 framing and the "javadoc claims" contrast do not hold, since the javadoc carves the census out explicitly. What is left is a diagnostics-hygiene pattern: one `System.getProperty`/`System.err` path beside `Diagnostics.on`, `System.out.println` in `TestPlanner`, and tests asserting global deltas.

## S3 and strengths spot-checks
S3 counts (2 of 2 hold):
- **TESTPLAN-14, inline FQNs.** My grep, excluding import, package and comment-led lines, counts 1,579 `com.legend.` names and 331 `java.util.` names, against the reviewer's 1,603 and 334. The difference is methodology; the pattern holds. `TestPlanner`'s unused `IOException`, `UncheckedIOException` and `Map` are confirmed.
- **TESTPLAN-15, dates.** Exactly 341 ISO dates.
- **TESTPLAN-16 (extra check).** The five hand-rolled children-to-`withChildren` loops are at `BodyClass.java:258,296,720` and `GeneratedTestData.java:119,166`, and `TypedSpec.mapChildren` exists (`TypedSpec.java:120`).

Strengths:
- **Sealed plan types: confirmed.** `HostJudgment.java:25` and `TestPlan.java:74,388` are sealed, and the JSON round-trip runs at `TestPlanner.java:167`. `VerdictSql` is called 71 times across 45 distinct functions (reproduced). The only SQL keyword literal is `planReplaySql` (`SqlTextVerdicts.java:1255`).
- **"jscpd finds 0.81%": wrong measurement, conclusion survives.** jscpd's default `maxLines` of 1000 silently skipped the four largest files (AssertVerdicts, SqlTextVerdicts, BodyClass, DatabaseJudge: 6,195 lines, 46% of the unit). It analyzed 26 of 30 files. Re-run with `--max-lines 10000`: 13 clones, 1.3% of lines. That is still low, so "the duplication is structural" stands. Among the new clones, `AssertVerdicts.java:1493-1502` against `1349-1358` is the per-element quantified form written twice, which is direct evidence for TESTPLAN-1's "quantified forms, planned twice".

## Missed (if any)
- **Judge disagreement from the walks' exception policies.** Detailed under TESTPLAN-3: the host plans a let's planning raise, the database judge refuses the body. This is a G2 consequence, not only a maintenance cost.
- **Dead branches in `adjudicate`.** Detailed under TESTPLAN-1: the gate (`planHost` then `gateOf`) pre-empts them, and the comments on those branches describe outcomes the body never reaches.
- **The vestigial `TypedSide` reaches the plan-data package.** `testexecutionplan/Verdicts.fail` is typed `TypedSide` and always throws (`Verdicts.java:76-79`), the same pattern as `DatabaseJudge.unjudged`.
- **Measurement error.** The report's jscpd figure; see the spot-checks above.
