# TESTEXEC: verdict MIXED

## Summary
The skeleton is right. Exec owns the one program walker. The test plan is lowered onto it as callback steps that declare
their statements up front (`TestRun.program`). The host judge dispatches one exhaustive switch over a sealed
`HostJudgment`, and the value core (`Equality`, `PureAsserts`) is cohesive and tested against upstream's own cases.
The debt sits in the judge half:
- A verdict is typed as a nullable "side" plus exceptions.
- The runner works out a verdict's kind from which nullable plan fields are set, and spreads judge-mode dispatch over
  two classes.
- The decision and its failure message are computed twice with different kinds.
- A 700-line instrument for a deleted "byte channel" still runs inside every equality verdict.
- The listener/census protocol is copied five times.
- About 2,800 lines of run-side judge code have no focused unit test; guards pin their field names and line counts
  instead.
- The comments are dominated by process history: 141 dated lines, and 28 references to "the walk" that no longer exists.
- There are 906 inline fully-qualified names, 140 of them for classes in the same package.

The interpreter (`TestRun`) and the value core are close to SOLID. The judge router (`TestRunner.Moved`,
`HostJudgments`, `AssertRuns`) is at the WEAK end.

## Strengths
- **One walker, a clean extension point.** `core/src/main/java/com/legend/testexec/TestRun.java:77-91` lowers the plan
  to exec's `ProgramRunner.Program` with an exhaustive switch over the sealed `TestStep`. Each verdict group or branch
  becomes a `Callback` that declares every statement it may read (`core/src/main/java/com/legend/exec/ProgramRunner.java:59-75`),
  and `OpenProgram.read` refuses an undeclared one (`ProgramRunner.java:111-125`). Savepoints are exec's
  (`open.guarded`, `TestRun.java:165`, `:194`). testexec contains no JDBC and no SQL text, so the test road does not
  re-implement sending, transactions or routing. This is the right division of work.
- **The host judge's dispatch is data-driven and exhaustive.** `core/src/main/java/com/legend/testexec/HostJudgments.java:298-322`
  is a single switch over the sealed `HostJudgment`, with no `default`. Each arm is a small static function of
  (record, sides, rules).
- **Equality is one cohesive owner with explicit rules.** `core/src/main/java/com/legend/testexec/Equality.java:48`
  has a `Kind` enum. The leniencies are gated by the rule set (`rules.decimalScale()` at `:220`,
  `rules.floatTolerance()` at `:242`). There is one 2-ULP home (`withinTwoUlp`, `:94-107`), which `TdsCompare.cellEquals`
  reuses (`TdsCompare.java:327`). The two JSON comparators have honest names (`pureJson` and `serviceJson`).
- **Spec-pinned value tests.** `core/src/test/java/com/legend/testexec/PureAssertsTest.java:44-60` pins upstream's
  success cases and exact failure messages. `InstanceIdentityTest` (15 tests) and `EqualityJson*Test` do the same.
- **AgreementArm is a model of the style the rest should have.** `core/src/main/java/com/legend/testexec/AgreementArm.java:35-63`
  uses a private sealed `Outcome` (Held/Declined/Failed/Errored) and a strict, no-fallback rule. Its messages name
  both judges (`:85-114`).
- **Appeals is well bounded.** The buffering of held reports is a decorating listener
  (`core/src/main/java/com/legend/testexec/Appeals.java:240-286`), and the unsafe cases are refused by name: a write
  after a hold (`:168-174`), and a failed run that wrote (`:129-135`).
- **TestRunTest tests behaviour.** `core/src/test/java/com/legend/testexec/TestRunTest.java:124-180` uses a recording
  sink on a real database and asserts the order of events per judge mode. Every hand-built plan is round-tripped
  through JSON (`:183-187`).
- **assertError cites upstream line by line.** `core/src/main/java/com/legend/testexec/AssertErrorRuns.java:197-251`.
  `core/src/main/java/com/legend/test/PureTests.java` (discovery and the engine's suite order) is clean.
- **Textual duplication is low.** jscpd finds 0.71% duplicated lines. The duplication that matters is structural (see
  below).

## Findings

### TESTEXEC-1 [S1] The runner works out a verdict's kind from nullable fields; judge-mode dispatch is spread over two classes
- Where: core/src/main/java/com/legend/testexec/TestRun.java:147-177, core/src/main/java/com/legend/testexec/TestRun.java:206-214,
  core/src/main/java/com/legend/testexec/TestRunner.java:209-217, core/src/main/java/com/legend/testexec/TestRunner.java:251-305,
  core/src/main/java/com/legend/testexec/TestRunner.java:312-323, core/src/main/java/com/legend/testexec/TestRunner.java:369-378,
  core/src/main/java/com/legend/testexecutionplan/TestPlan.java:185-187, core/src/main/java/com/legend/testexecutionplan/TestPlan.java:313-315
- What: `TestPlan.Verdict(id, name, String arm, base, @Nullable wrapper, @Nullable message, boolean raises, @Nullable host)`
  and `Verdicts(verdicts, @Nullable fused, @Nullable savepoint, rules, @Nullable roots, @Nullable agreement)` together
  carry about seven nullables. Which combinations are valid depends on `plan.judge()`. The runner reconstructs the kind
  at run time in three places:
  - `TestRun.Handler.verdicts` computes `hostAll`, `hostOnly = !hostAll && group.fused() == null && ...anyMatch(v -> v.wrapper() == null && !v.base().isEmpty())`,
    then `judge == DATABASE || judge == AGREEMENT`.
  - `raisingVerdict` repeats the judge tests.
  - `TestRunner.Moved.verdict` routes on `agreementRuns != null`, then `databaseRuns == null` (throwing "a database
    verdict in a body the host judge planned"), then `hostStep != null && verdict.wrapper() != null` (an assertError
    row), then `hostStep != null` with an empty base.

  `fused`, `sides` and `sidesFailed` each repeat the `agreementRuns != null` branch.
- Why it matters: adding a judge mode, or a new verdict shape, means editing two classes and four or more branch
  ladders, and keeping them consistent by hand. Many runtime `IllegalStateException`s ("a fused verdict group in a body
  the host judge planned", `TestRunner.java:302`; "host sides for a verdict the database judge planned", `:373`) exist
  only because the types admit those states.
- Expert alternative: make the judge a strategy chosen once:
  `sealed interface JudgeRun permits HostRun, DatabaseRun, AgreementRun { void group(Verdicts g, OpenProgram open); }`,
  built from `plan.judge()` in `TestRunner.run`. Make the plan's verdict a sum, for example
  `sealed interface Verdict permits HostVerdict(Host, List<Side> base, @Nullable Side message), DatabaseVerdict(TdsResult wrapper), RaisingVerdict(Side base, TdsResult statement, Host host)`,
  so the switch replaces null-probing. Pass the group, not only the verdict, to the callback, so the judge does not
  rebuild `rules` from an id index (see TESTEXEC-13).
- Confidence: verified

### TESTEXEC-2 [S1] A verdict is `@Nullable TypedSide` plus exceptions: three outcomes carried by a dummy value, null and throw
- Where: core/src/main/java/com/legend/testexecutionplan/Verdicts.java:71-79, core/src/main/java/com/legend/testexec/HostJudgments.java:286-323,
  core/src/main/java/com/legend/testexec/HostJudgments.java:363, core/src/main/java/com/legend/testexec/HostJudgments.java:450,
  core/src/main/java/com/legend/testexec/HostJudgments.java:542, core/src/main/java/com/legend/testexec/HostJudgments.java:777,
  core/src/main/java/com/legend/testexec/TestRunner.java:384-389, core/src/main/java/com/legend/testexec/AssertRuns.java:176-179,
  core/src/main/java/com/legend/testexec/HostJudgments.java:617-621, core/src/main/java/com/legend/testexec/AssertErrorRuns.java:171-174,
  core/src/main/java/com/legend/testexec/AssertErrorRuns.java:249-250, core/src/main/java/com/legend/testexec/DatabaseVerdicts.java:154-157
- What: about 50 judge methods return `TypedSide`, the type of a collected assert side.
  - The only non-null value any of them returns is `new TypedSide.Scalar(Boolean.TRUE, "Boolean")` (`Verdicts.ok()`,
    copied at `AssertErrorRuns.java:249`).
  - `Verdicts.fail` is declared to return `TypedSide` and always throws `AssertFailed`.
  - `null` means "declined: the walk's generic path continues" (`HostJudgments.java:284`). The walk was deleted on
    2026-11-06, so every consumer converts null into a `NotImplementedException` with a near-identical message, at four
    sites.
  - `AgreementArm.agree`'s own null return (`AgreementArm.java:78-80`) cannot be reached, because `heldAtItsStep`
    throws first.
- Why it matters: every judge signature lies about what it returns. A reader has to learn three conventions at once.
  The "declined" state is leftover from a deleted design, and it delays a refusal the plan often knew about already:
  `InstanceOf.typeName == null`, at `HostJudgments.java:541`, is a planning fact.
- Expert alternative: a value type, `sealed interface Verdict permits Held, Failed { record Failed(String message, @Nullable String unjudgedReason) }`,
  returned by every judge and turned into `AssertFailed` once, at the reporting seam. A shape the host cannot judge is
  either refused at planning, or is `Failed` with an unjudged reason. Delete the null path and its four conversions.
- Confidence: verified

### TESTEXEC-3 [S2] The decision and its message are computed twice with different kinds, and in one arm the message overturns the decision
- Where: core/src/main/java/com/legend/testexec/HostJudgments.java:756-769, core/src/main/java/com/legend/testexec/HostJudgments.java:586-596,
  core/src/main/java/com/legend/testexec/HostJudgments.java:36-48, core/src/main/java/com/legend/testexec/HostJudgments.java:51-54,
  core/src/main/java/com/legend/testexec/PureAsserts.java:93-101, core/src/main/java/com/legend/testexec/PureAsserts.java:311-314,
  core/src/main/java/com/legend/testexec/Equality.java:31-33
- What:
  - `equalsOf` decides with declared kinds: `Equality.ordered(typedSide(e, j.e()), ...) == null`. It then builds the
    message with `PureAsserts.assertEquals(e, a, ...)`, which re-runs equality with no declared kinds
    (`Typed.all(left, null)`, `PureAsserts.java:312`). `finish` therefore needs a runtime guard,
    `IllegalStateException("verdict/message divergence — the host lattice failed but its message lattice held")`.
  - `sameElementsOf` has no such guard:
    `String d = Equality.sameElements(typed...) == null ? null : PureAsserts.assertSameElements(e, a, ...)`, then
    `finish(..., d == null, ...)`. If the typed check fails but the untyped re-check holds, the assert passes.
  - The comment at `:51-54` says message/decision divergence is "structurally impossible here". That is true only of the
    grid arm (`assertEqualsTyped`).
  - `Equality`'s javadoc promises "never a boolean a caller re-interprets", yet every caller tests `== null`
    (`HostJudgments.java:91`, `:587`, `:759-760`, `TdsCompare.java:74`).
- Why it matters: two equality computations per assert, kept consistent by convention. One arm resolves a disagreement
  in favour of the weaker check. The upstream message is a function of the same typed comparison and should be derived
  from it.
- Expert alternative: one call, `Equality.Diff diff = Equality.ordered(te, ta, rules)` (null or a structured
  first-difference). Then `PureAsserts.equalsMessage(te, ta, eClass, aClass)` formats upstream's text from the same typed
  lists (as `assertEqualsTyped` already does at `PureAsserts.java:67-77`). Delete `finish`'s divergence branch.
- Confidence: verified (the mechanism). The inputs that trigger the `sameElementsOf` override (for example, a
  non-integral carrier under an Integer declaration) are suspected.

### TESTEXEC-4 [S2] A measurement instrument for a deleted byte channel runs inside production verdicts; four instrumentation mechanisms are woven into decision code
- Where: core/src/main/java/com/legend/testexec/CanonicalDivergence.java:271-506, core/src/main/java/com/legend/testexec/CanonicalDivergence.java:52-77,
  core/src/main/java/com/legend/testexec/CanonicalForm.java:13-17, core/src/main/java/com/legend/testexec/HostJudgments.java:532-534,
  core/src/main/java/com/legend/testexec/HostJudgments.java:589-590, core/src/main/java/com/legend/testexec/HostJudgments.java:763-764,
  core/src/main/java/com/legend/testexec/TdsCompare.java:194-195, core/src/main/java/com/legend/testexec/Equality.java:100-105,
  core/src/main/java/com/legend/testexec/Equality.java:221-225, core/src/main/java/com/legend/testexec/TdsCompare.java:172-178,
  core/src/main/java/com/legend/testexec/DatabaseVerdicts.java:21-26, core/src/test/java/com/legend/testexec/V7DualChannelCensusTest.java:24-70
- What:
  - `CanonicalForm`'s own javadoc says "the byte verdict it measured against is deleted ... production verdicts never
    call it". Yet `HostJudgments.eqOf`, `equalsOf` and `sameElementsOf`, and `TdsCompare.renderedText`, call
    `CanonicalDivergence.probe*`, which renders a canonical form of every compared value into process-wide counters.
    Those counters are read only by test assertion messages (`CanonicalDivergence.summary()` in `LiteralChannelTest`
    and `CanonicalFormTest`).
  - The V7 "dual channel" half (`v7Verdict`, `v7Declined`, `METAMODEL_QUARANTINE`, `noteWall`, `v7Summary`,
    `v7Report`; about 236 lines) has no production caller. Only `V7DualChannelCensusTest` calls it, and that test pins
    its output format.
  - `noteWall`, `v7QuarantinedCount`, `v7DeclinedByReason`, `disagreeSamples` and `samples` have no caller at all.
  - Separately, the equality decision itself reads environment variables (`LL_TOL_COUNT` inside `withinTwoUlp`,
    `LL_ORD_COUNT` in `ordLeniency`), bumps census counters, and calls `Diagnostics.judgeSend`. About 45
    instrumentation call sites sit in judge code (Census 5, VerdictCensus 21, Diagnostics 9, CanonicalDivergence 8,
    getenv 2).
- Why it matters: about 700 lines of static, mutable, global state live in main sources and on the hot path of every
  assertEquals, kept alive by a test that tests nothing the product uses. A reader cannot tell measurement from
  judgment without reading every call. Four instrumentation styles make every judge function harder to follow.
- Expert alternative: delete `CanonicalForm`, the probes and the V7 half, together with `V7DualChannelCensusTest` and
  the probe assertions in `CanonicalFormTest`. Keep the two counters the corpus prints (`sqlUlpPolicy`,
  `decimalScaleOnly`) behind one injected `JudgeEvents` interface (no-op by default) that the judges call, with no
  `System.getenv` inside equality.
- Confidence: verified

### TESTEXEC-5 [S2] The listener and census reporting protocol is copied five times, each copy with its own `settled` flag
- Where: core/src/main/java/com/legend/testexec/AssertRuns.java:68-105, core/src/main/java/com/legend/testexec/AssertRuns.java:110-137,
  core/src/main/java/com/legend/testexec/AssertRuns.java:151-193, core/src/main/java/com/legend/testexec/AssertRuns.java:199-218,
  core/src/main/java/com/legend/testexec/RootLoops.java:129-196, core/src/main/java/com/legend/testexec/Appeals.java:198-224
- What: `judgedAtItsStep`, `judgedRowAtItsStep`, `heldAtItsStep` and `agreedAtItsStep` each use the same scaffold:
  `sqlEnter`, then `enter(a)`, then a try block catching `AssertFailed | DataError`, then `sqlRaised`, then
  `listener.unjudged` when `unjudgedReason != null`, then `listener.verdict(false, msg)`, rethrow, and a `finally`
  that checks `!settled`. jscpd flags two of these pairs. `RootLoops.Run.judge` and `Appeals.failed` hand-roll the same
  telling once more. The agreement path also wraps the host result in `HostHeld(verdict, failed)`, rethrows it inside a
  lambda (`AgreementRuns.java:173-177`), and catches it again in `AgreementArm.agree`.
- Why it matters: a change to what the listener is told (a new event, a different unjudged rule) must be made in six
  places, and they already differ: `heldAtItsStep` reports a DataError but not an AssertFailed. This is the most likely
  place for drift between the judges.
- Expert alternative: with a `Verdict` value (TESTEXEC-2), a single
  `static Verdict reported(Assertion a, @Nullable AssertListener l, Supplier<Verdict> judge)` does enter, judge, census
  and tell in straight-line code. The agreement judge holds `Verdict` values instead of the
  throw-wrap-rethrow-catch cycle.
- Confidence: verified

### TESTEXEC-6 [S2] Kinds, forms and carriers are dispatched on strings, including Java class names written into the portable plan
- Where: core/src/main/java/com/legend/testexec/Equality.java:53-63, core/src/main/java/com/legend/testexec/Equality.java:67,
  core/src/main/java/com/legend/testexec/HostJudgments.java:678-689, core/src/main/java/com/legend/testplan/HostJudge.java:238-239,
  core/src/main/java/com/legend/testexec/TdsCompare.java:204-205, core/src/main/java/com/legend/testexec/TdsCompare.java:244,
  core/src/main/java/com/legend/testexec/SideShapes.java:329, core/src/main/java/com/legend/testexec/SideShapes.java:338,
  core/src/main/java/com/legend/testexec/PureAsserts.java:267-301, core/src/main/java/com/legend/testexec/Equality.java:176,
  core/src/main/java/com/legend/testexec/HostJudgments.java:180-181
- What:
  - `Equality.Typed(Object value, @Nullable String kind)`. `declaredNumeric` switches on `"Integer"`, `"Float"` and
    `"Decimal"`, plus `declared.startsWith("Decimal(")`.
  - `HostJudgments.literal` switches on `"String"`, `"Boolean"`, `"Double"`, `"Long"`, `"Integer"` and
    `"BigInteger"`. The planner writes these with `l.value().getClass().getSimpleName()` into a plan that the
    TypeScript and Python hosts are meant to read.
  - `Rendered.form` is a String parsed with `startsWith("CSVJOIN:")` and `equals("TDSTEXT")`.
  - Slot kinds are tested with `slot.startsWith("TIMESTAMP")`.
  - `assertInstanceOf` compares carrier type names as strings.
  - Sentinels are written as literals beside the constants that exist for them: `"TDSNull"` versus
    `SqlExpr.TDS_NULL_CELL`, and `"__type"` versus `SqlExpr.SYNTHETIC_TYPE` two lines later.
- Why it matters: a typo or a new spelling compiles and fails at run time, or fails silently: `declaredNumeric`
  returns null for an unknown name and falls back to the carrier's kind. Java class names in the plan break G11's
  host-agnostic data. Portability does not force this: the JSON reader can parse names once.
- Expert alternative: `TestPlanJson` and `HostJson` read type names into a `PureType`/`Kind` enum, or a sealed type,
  at deserialization. `Typed(Object value, @Nullable Kind declared)`.
  `sealed interface RenderForm permits Csv, Tds, CsvJoin(String separator)`. `RowArg.Literal(PrimitiveType type, String text)`
  should carry the Pure type, not the Java class. Use the `SqlExpr` constants everywhere.
- Confidence: verified

### TESTEXEC-7 [S2] Three decodes of a constant's text, admitting different types; exec's javadoc calls its own "the one decode"
- Where: core/src/main/java/com/legend/exec/PlanRunner.java:734-759, core/src/main/java/com/legend/testexecutionplan/Verdicts.java:145-160,
  core/src/main/java/com/legend/testexec/HostJudgments.java:678-689, core/src/main/java/com/legend/testexec/TestRun.java:124-126,
  core/src/main/java/com/legend/testexec/TestRun.java:307-308, core/src/main/java/com/legend/testexec/TestRun.java:342-348,
  core/src/main/java/com/legend/testexec/SideShapes.java:172-185, core/src/main/java/com/legend/testexec/SqlTextRuns.java:253-258
- What:
  - A branch's constant condition goes through `PlanRunner.constantRows`, which handles String and Boolean and whose
    javadoc says "the one decode of a constant".
  - A verdict's constant side is copied as raw text (`TestRun.constantRows`, and again in `SqlTextRuns.read`), then
    decoded in `SideShapes.shape` by `Verdicts.constantValue(text, typeName)`, which handles String, Boolean and
    Integer.
  - A quantified assert's literal goes through `HostJudgments.literal`, which handles six Java carriers.

  The three admit different types and raise different messages and exception types.
- Why it matters: the same plan datum, a constant cell, means different things depending on where it is read. A fix to
  one decode (the J10 fix to the branch decode is an example) does not reach the others.
- Expert alternative: one `ConstantDecode.decode(PureType type, String text)` in exec or `values`, used by the product
  answer, the branch, the sides and the literals. Delete the two raw-copy helpers.
- Confidence: verified

### TESTEXEC-8 [S2] `TestRunner.Moved` is a god class with a history name
- Where: core/src/main/java/com/legend/testexec/TestRunner.java:141-496
- What: one inner class (about 355 lines, 14 fields) implements both exec's `ProgramSink` and `TestRun.Judges`. It:
  - streams the answer and enforces the walk's cardinality (`columns`, `batch`, `end`, `holdingTheWalksContract`);
  - records the edge's ledger (`seeded`, `effectRan`) and counts effects;
  - routes judges by mode (`verdict`, `fused`, `sides`, `sidesFailed`);
  - builds three plan indexes in its constructor (`rulesOf`, `deferredTextOrigins`, the row appeals);
  - holds the program's reader and the last verdict.

  "Moved" means "a body moved off the walk".
- Why it matters: four reasons to change in one class. The hardest logic in the package is the least testable piece
  (see TESTEXEC-12).
- Expert alternative: `AnswerSink` (answer and cardinality), `EdgeLedger` (seeded/effectRan), and the `JudgeRun`
  strategy from TESTEXEC-1, composed in `TestRunner.run`. Name them for what they do.
- Confidence: verified

### TESTEXEC-9 [S2] Value equality is defined well once, but several neighbours diverge from it in subtle ways
- Where: core/src/main/java/com/legend/testexec/Equality.java:242, core/src/main/java/com/legend/testexec/Equality.java:535-543,
  core/src/main/java/com/legend/testexec/TdsCompare.java:134-148, core/src/main/java/com/legend/testexec/PureAsserts.java:179-194,
  core/src/main/java/com/legend/testexec/HostJudgments.java:452-454, core/src/main/java/com/legend/testexec/HostJudgments.java:500-504,
  core/src/main/java/com/legend/testexec/Equality.java:296-303, core/src/main/java/com/legend/testexec/Equality.java:269-271,
  core/src/main/java/com/legend/test/TestAssertions.java:27-29, core/src/main/java/com/legend/test/TestAssertions.java:48
- What:
  - `Equality.same` applies the 2-ULP tolerance only when `rules.floatTolerance()`. `Equality.serviceDiff` applies it
    unconditionally (`:539`). `TestAssertions`' javadoc says the service comparison is "exact decimals".
  - `PureAsserts.assertEqWithinTolerance` uses exact `BigDecimal` arithmetic for exact kinds; the P2-1 audit fixed a
    double round-trip there. `TdsCompare.tdsEquivalent` still uses `doubleValue()` for every number and
    `Objects.equals` for everything else, outside `Equality.same`.
  - `HostJudgments.containsOf` and `subsetOf` re-implement membership inline while `Equality.contains` exists and has
    no caller.
  - `structural` drops the declared kinds for nested values (`Typed.of(x)`).
- Why it matters: the question "is equality defined once?" gets the answer "mostly". A rule change, such as the ULP
  gate or exact arithmetic, has to be repeated in places that are not obviously equality code.
- Expert alternative: route every leaf comparison through `Equality.same(Typed, Typed, rules)`, including
  `tdsEquivalent`'s non-numeric cells and `serviceDiff`'s numbers (with a named service rule set). Share the
  exact/double tolerance helper between `assertEqWithinTolerance` and `tdsEquivalent`. Use `Equality.contains` or
  delete it.
- Confidence: verified

### TESTEXEC-10 [S2] Logic copied from other layers into the run
- Where: core/src/main/java/com/legend/testexec/SideShapes.java:223-245, core/src/main/java/com/legend/compiler/element/type/Type.java:573-600,
  core/src/main/java/com/legend/testexec/SideShapes.java:116-165, core/src/main/java/com/legend/testexec/SideShapes.java:250-293,
  core/src/main/java/com/legend/lowering/Coercions.java:66, core/src/main/java/com/legend/testexec/HostJudgments.java:575-586,
  core/src/main/java/com/legend/testexec/HostJudgments.java:745-756, core/src/main/java/com/legend/test/PureTestRunner.java:420-425,
  core/src/main/java/com/legend/testexec/AgreementArm.java:132-138
- What:
  - `SideShapes.pivotColumnType` is a near-verbatim copy of the compiler's `Type.RelationType.pivotColumnType`: it
    strips the quotes of a presented pivot name, then matches a per-key template by separator suffix. It parses SQL
    column names at run time.
  - `SideShapes` enforces Pure's cardinality in Java twice, in the `Cardinality` sink and in `valuesShape`, with the same
    two message texts, as `IllegalStateException`. The product does this check in SQL (`Coercions`). Javadoc at
    `SideShapes.java:106-107` calls it a shim held "until the plan carries it".
  - The key-restriction block is duplicated between `equalsOf` and `sameElementsOf`, comment included (a jscpd clone).
  - `PureTestRunner.whole` and `AgreementArm.first` use the same regex.
  - `PureTestRunner` and `ServiceTestRunner` each declare `Status { PASS, FAIL, SKIPPED }`, a `Result`, the
    `LEGEND_LITE_STACKS` switch and a swallowed close.
- Why it matters: copies drift. The pivot rule now has two owners in two layers. The Java cardinality check is
  row-counting in Java, which the charter forbids on the product road; a toOne violation on the test road surfaces as
  `IllegalStateException` instead of the product's error.
- Expert alternative: the planner writes each keyed column's Pure type, or its template index, into `SideShape`, so the
  run matches by index and parses no names. If a shared rule is needed, keep it once in `SqlSource.Pivot`, which both
  layers already import. Plan the cardinality guard into the test plan's SQL as the product does. Extract
  `restrictPair(...)` and one `Messages.oneLine`.
- Confidence: verified

### TESTEXEC-11 [S2] Error handling: an exception nobody catches, one exception type for two meanings, a catch-all that can rescue a text, and messages that name deleted internals
- Where: core/src/main/java/com/legend/testexec/Equality.java:82-86, core/src/main/java/com/legend/testexec/AssertRuns.java:92,
  core/src/main/java/com/legend/testexec/SqlTextRuns.java:158-165, core/src/main/java/com/legend/testexec/SqlTextRuns.java:225-232,
  core/src/main/java/com/legend/testexec/TestRunner.java:386-387, core/src/main/java/com/legend/testexec/AssertRuns.java:177-178,
  core/src/main/java/com/legend/testexec/TestRunner.java:374, core/src/main/java/com/legend/testexec/TestRunner.java:464,
  core/src/main/java/com/legend/testexec/SideShapes.java:161, core/src/main/java/com/legend/testexec/Appeals.java:169-171,
  core/src/main/java/com/legend/testexec/TestRun.java:114-115
- What:
  - `Equality.Unjudged` is caught nowhere in the repository. It escapes the judges' `AssertFailed | DataError` catch,
    so the listener is never told, and the `AssertFailed.unjudged(reason, msg)` mechanism the other judges use for the
    same idea is bypassed.
  - 62 `IllegalStateException`s mix plan-integrity defects ("a callback at a product step") with user-visible outcomes
    ("Cannot cast a collection of size 0 ...", appeal refusals, "golden JSON does not parse").
  - `SqlTextRuns.rowsLegNow` catches any `RuntimeException` (an NPE in shaping included) as "rows underivable". When the
    texts are equal, that returns `ok()`: a defect in our own row read becomes a pass.
  - User-visible messages say "the walk's generic path has no program form" and "a moved body".
- Why it matters: a person reading a failure cannot tell a broken plan from a failing test. One host-side unjudged
  shape does not reach the listener protocol. The catch-all is the kind of rescue G7 rules out.
- Expert alternative: a `PlanDefect extends IllegalStateException` for plan-integrity checks. `AssertFailed.unjudged`
  for host unjudged shapes (delete `Equality.Unjudged`). Narrow the rows-leg catch to the errors a planned read may
  legitimately raise (`DataError`). Write messages in the user's vocabulary.
- Confidence: verified

### TESTEXEC-12 [S2] The run-side judges have no focused tests; guards pin their structure instead
- Where: core/src/main/java/com/legend/testexec/HostJudgments.java, core/src/main/java/com/legend/testexec/TestRunner.java,
  core/src/main/java/com/legend/testexec/Appeals.java, core/src/main/java/com/legend/testexec/RootLoops.java,
  core/src/main/java/com/legend/testexec/AssertRuns.java, core/src/main/java/com/legend/testexec/SqlTextRuns.java,
  core/src/test/java/com/legend/CodeShapeGuardrailTest.java:104-130, core/src/test/java/com/legend/JavaEvalLedgerTest.java:1036-1044
- What: no test under `core/src/test` exercises `HostJudgments` (841 lines), `TestRunner`, `Appeals` (whose report
  re-ordering is a subtle state machine), `RootLoops`, `AssertRuns` or `SqlTextRuns` directly. The only references
  outside main are guards: `CodeShapeGuardrailTest` allowlists their mutable field names (`Appeals.wrote`,
  `TestRunner.lastVerdict`, ...) with dated justifications, and `JavaEvalLedgerTest` pins their non-comment line counts.
  Behaviour is covered only end to end (the integration, PCT and corpus lanes). `V7DualChannelCensusTest` tests dead
  code. On the other side, `TestRunTest`, `PureAssertsTest` and `AgreementArmTest` are good behaviour tests.
- Why it matters: a refactor of the riskiest code (judge routing, appeals ordering, unrolled asserts) gets a lane-sized
  feedback loop. It is also taxed by guard edits that say nothing about correctness.
- Expert alternative: table-driven unit tests of `HostJudgments.judge`, one row per `HostJudgment` variant over
  hand-built `TypedSide`s with the exact message. An `Appeals` test with a recording listener (hold, report, write
  after hold, failure ordering). Router tests in the style of `TestRunTest`, with a recording `JudgeRun`.
- Confidence: verified

### TESTEXEC-13 [S2] State and control flow: holders, a reader pushed through a setter, three index walkers, nested lambdas, long parameter lists
- Where: core/src/main/java/com/legend/testexec/TestRun.java:193-205, core/src/main/java/com/legend/test/PureTestRunner.java:334-337,
  core/src/main/java/com/legend/testexec/TestRun.java:54-56, core/src/main/java/com/legend/testexec/TestRun.java:106-110,
  core/src/main/java/com/legend/testexec/TestRunner.java:325-349, core/src/main/java/com/legend/testexec/TestRunner.java:165-189,
  core/src/main/java/com/legend/testexec/RootLoops.java:317-344, core/src/main/java/com/legend/testexec/AgreementRuns.java:44-69,
  core/src/main/java/com/legend/testexec/AgreementRuns.java:157-193, core/src/main/java/com/legend/testexec/TestRunner.java:74-77,
  core/src/main/java/com/legend/testexec/SqlTextRuns.java:134-137, core/src/main/java/com/legend/testexec/Appeals.java:233-236
- What:
  - Mutable holders: `RuntimeException[] caught`; `boolean[] refereeMatched`, beside three parallel "ahead" maps keyed
    by the next verdict's index.
  - `Judges.reader(SideReader)` is set at the first callback and nulled in `finally`, so the judges hold a reader valid
    only between two events. The `OpenProgram` is already handed to every callback.
  - Three recursive step walkers (`Moved.rulesOf`, `RootLoops.Runs.add`, `AgreementRuns.add`) rebuild id-keyed indexes
    of what the callback could pass directly.
  - `AgreementRuns.decided` nests a lambda in a lambda in a lambda, after copying fields into locals.
  - Long parameter lists: `TestRunner.run` 9, `rowsLegNow` and `decideAppeal` 8, `Appeals.Decider` 7.
  - `RootLoops.Judge` is a functional interface with one implementation (`DatabaseVerdicts::verdictOf`, whose
    `TypedSide` result it discards).
- Why it matters: temporal coupling and hidden state. Behaviour depends on the order of callbacks and on maps built in
  constructors.
- Expert alternative: pass `(group, open)` to the judge strategy (TESTEXEC-1), so rules, segment and reader come with
  the call. A small `PendingReport` record replaces the three maps. Call `DatabaseVerdicts.verdictOf` directly. Group
  the run's inputs in a `RunContext(session, stores, edge, listener, referee, execution)` record.
- Confidence: verified

### TESTEXEC-14 [S3] Inline fully-qualified names everywhere (pattern)
- Where: core/src/main/java/com/legend/testexec/HostJudgments.java:89-91, core/src/main/java/com/legend/testexec/TestRunner.java:99-102,
  core/src/main/java/com/legend/testexec/AgreementRuns.java:32
- What: 906 inline FQN uses outside imports and comments, in 7,381 lines. 140 of them qualify a class of the same
  package (`com.legend.testexec.Equality.grid(...)` inside `com.legend.testexec`). There are 173
  `@com.legend.base.Nullable`, 42 spellings of `com.legend.testexecutionplan.TestPlan.JudgingRules`, and 44 of
  `com.legend.testexecutionplan.Verdicts.ok/fail`. Imports and FQNs are mixed in the same file: `SqlTextRuns`
  statically imports `isPopulationGolden`, then calls it fully qualified at `:71`.
- Why it matters: lines wrap at 120 columns around qualifiers, and the signal is buried. It is the first thing a new
  engineer stumbles on.
- Expert alternative: imports, and an IDE "optimize imports" pass. That is mechanical.
- Confidence: verified

### TESTEXEC-15 [S3] Process history in comments and identifiers (pattern)
- Where: core/src/main/java/com/legend/testexec/TestRunner.java:34-49, core/src/main/java/com/legend/testexec/BodyRun.java:9-16,
  core/src/main/java/com/legend/testexec/DatabaseVerdicts.java:26-27
- What: 1,714 of the 7,381 lines are comments; 141 of them carry a date. "hole N", "slice N", "step N", "M10c item 8",
  "P1.2", "round 25" and "the audit" appear about 150 times. There are 28 references to the deleted walk that describe
  present behaviour ("as the walk does", "the walk's flush"). History also lives in identifiers: `Moved`, `rowsLegNow`,
  `v7SideRows`, `V7_FORMS`, `V7DualChannelCensusTest`, `VerdictWorld2ConsistencyTest`, `heldAtItsStep`. Test display
  names carry ticket codes ("F11", "P1.2", "round 25").
- Why it matters: the javadoc explains how the code got here, not what it does. Readers must learn a vocabulary of
  deleted things.
- Expert alternative: rewrite each javadoc as one or two sentences on present behaviour and invariants. Move history to
  commit messages and docs. Rename `Moved` to `BodyRunSink`, `rowsLegNow` to `replayRows`, and drop the `v7` prefixes.
- Confidence: verified

### TESTEXEC-16 [S3] Stale links, orphan javadoc and small residue (pattern)
- Where: core/src/main/java/com/legend/testexec/HostJudgments.java:236, core/src/main/java/com/legend/testexec/TdsCompare.java:15,
  core/src/main/java/com/legend/test/PureTestRunner.java:44-46
- What:
  - Stale links (7 or more): `AssertVerdicts#structured` (`HostJudgments:236`); `AssertVerdicts#heldAtItsStep`
    (`AgreementRuns:104`); `PureAsserts#equalScalar` (`TdsCompare:15`); `Execution#runProgram` (`PureTestRunner:46`,
    `ServiceTestRunner:64`); `ProgramFacts#seedsInlineCsv` (`PureTestRunner:44`); `com.legend.exec.JsonCompare`
    (`TestAssertions:17`); `#adjudicate` (`AssertRuns:24`, `:29`).
  - Orphaned or stacked javadoc blocks: `HostJudgments:226-230`, `AssertRuns:276-277`, `TdsCompare:85-87`,
    `SqlReplayReferee:102-107`, `ServiceTestRunner:491-502`.
  - An identity function with an unused parameter: `HostJudgments.tdsHostMessage` (`:115-118`).
  - A magic index, `.get(6)`, where `RootLoop.MESSAGE_COLUMN` exists (`RootLoops:295`).
  - An unused local, `name` (`SqlTextRuns:109`).
  - About 11 unused imports (`TestAssertions` has 5; `SqlTextRuns` imports `StatementOrigin`).
  - Dead code: `Equality.contains`; `SqlReplayReferee`'s default bodies (its one implementation overrides all of them).
  - Odd declarations: a redundant alias (`TestRunner:96`) and a split declaration (`TestRunner:254-255`).
  - `HostSideShapes` (42 lines) is a fragment that belongs in `SideShapes`.
- Why it matters: individually trivial. Together they show the code has not been read as a whole since the walk's
  deletion.
- Expert alternative: one cleanup pass with javadoc lint (`-Xdoclint:reference`) and unused-import checks in the build.
- Confidence: verified

## Cross-unit observations
- The root cause of TESTEXEC-1 is in `testexecutionplan`: `TestPlan.Verdict` and `Verdicts`
  (`core/src/main/java/com/legend/testexecutionplan/TestPlan.java:185-187`, `:313-315`) are records of nullables whose
  valid combinations depend on `TestPlan.judge`. `Verdict.arm` is a String.
- The planner writes Java carrier class names into the portable plan
  (`core/src/main/java/com/legend/testplan/HostJudge.java:238-239`, `getClass().getSimpleName()`). This is a G11 issue
  for the TypeScript and Python hosts.
- `TypedSide.returnType` is a String; `Verdicts.ok()` builds `Scalar(TRUE, "Boolean")`
  (`core/src/main/java/com/legend/testexecutionplan/Verdicts.java:71-79`). `Verdicts` is a mixed bag: text diffs,
  golden recognizers, `repr`, the constant decode and the verdict constructors.
- `PlanRunner.constantRows`' javadoc claims "the one decode of a constant"
  (`core/src/main/java/com/legend/exec/PlanRunner.java:734-737`). It is one of three (TESTEXEC-7).
- Guards: `JavaEvalLedgerTest` pins per-file line counts for 10 testexec files, with multi-hundred-character dated
  comments per entry (`core/src/test/java/com/legend/JavaEvalLedgerTest.java:1036-1044`). `CodeShapeGuardrailTest`
  allowlists every mutable field by name (`core/src/test/java/com/legend/CodeShapeGuardrailTest.java:104-130`). Both
  make the refactors above cost guard edits, and neither checks behaviour. The package is a "closed register"
  (`JavaEvalLedgerTest.TESTEXEC_CLASSES`, `package-info.java:13-14`), so even splitting `Moved` needs a ledger edit.
- `spec/src/test/java/com/legend/harness/ReplayReferee.java` is `SqlReplayReferee`'s only implementation. It exposes
  `verify0`/`verify1`/`verifyPlan0` beside the interface methods (`:545`, `:578`, `:665`), which is worth a look by the
  spec or harness reviewer.

## Measurements
- Size: unit main sources, 7,381 lines in 27 files (testexec 22, test 5); unit tests, 1,938 lines (testexec) plus 78
  (`PureTestRunnerTest`). Comment lines: 1,714 (23%).
  `wc -l core/src/main/java/com/legend/testexec/*.java core/src/main/java/com/legend/test/*.java`;
  `grep -c '^\s*\(//\|\*\|/\*\*\)'`.
- Complexity: `python3 -m lizard -l java core/src/main/java/com/legend/testexec core/src/main/java/com/legend/test`
  - Overall: 382 functions, average CCN 4.0, average 11.8 NLOC. 14 warnings at CCN > 12 or more than 60 lines.
  - Worst: `Equality.same` CCN 39; `Equality.serviceDiff` 28; `TdsCompare.renderedTextVerdict` 27 (94 lines);
    `RootLoops.Run.judge` 25 (6 params); `HostJudgments.judge` 24 (an exhaustive switch, acceptable);
    `SqlTextRuns.rowsLegNow` 23 (90 lines, 8 params); `TestRun.Handler.raisingVerdict` 18.
- Copy-paste: `jscpd --min-lines 6 --min-tokens 60 --format java` found 5 clones, 52 lines (0.71%). They are the
  `AssertRuns` reporting pairs, `HostJudgments`' key restriction, `Equality`'s JSON diffs, and `Appeals`/`TestRunner`
  imports.
- Inline FQNs: 906 outside imports and comments. Of these, 140 are same-package `com.legend.testexec.X`, 173 are
  `@com.legend.base.Nullable`, and 42 are `com.legend.testexecutionplan.TestPlan.JudgingRules`. Counted with
  `grep -o "\b\(com\.legend\|java\.util\|java\.io\|java\.time\|java\.math\)\.[a-zA-Z]"` over non-import,
  non-comment lines.
- Process history: 141 dated lines; "the walk" 28; "step N" 32; "slice N" 17; "hole N" 13; "audit" 25;
  "M<digits>" 20. Counted with `grep -Eo` over the unit's main sources.
- Exceptions thrown in testexec: `IllegalStateException` 62, `AssertFailed` 11, `NotImplementedException` 10,
  `UncheckedIOException` 6, `Equality.Unjudged` 2 (caught nowhere: `grep -rn "catch (.*Unjudged"` returns nothing).
- Verdict-as-`TypedSide`: 52 methods in testexec return `TypedSide`. About 50 of them are verdicts (all but
  `SideShapes.shape` and `valuesShape`), and every non-null one is `Scalar(TRUE, "Boolean")`.
- Dead code in `CanonicalDivergence`: the V7 block (lines 271-506, 236 lines) has 0 production callers. `noteWall`,
  `v7QuarantinedCount`, `v7DeclinedByReason`, `disagreeSamples` and `samples` have 0 callers anywhere (a grep per
  symbol over `core/src` and `spec/`).
- Instrumentation calls in judge code (excluding `CanonicalDivergence` itself): `VerdictCensus` 21, `Diagnostics` 9,
  `CanonicalDivergence` 8, `Census.inc` 5, `System.getenv` 2 (plus 3 in `test/`).
- Focused tests: `grep -rln "HostJudgments\|SqlTextRuns\|AssertRuns\|TdsCompare" core/src/test` returns only guard
  tests (ArchitectureTest, JavaEvalLedgerTest, VerdictChannelRegisterTest, HarnessDisciplineTest,
  ErrorShapeGuardrailTest, JdbcSurfaceCensusTest). Appeals and RootLoops are likewise referenced only by ArchitectureTest,
  CodeShapeGuardrailTest and JavaEvalLedgerTest.
