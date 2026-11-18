# TESTEXEC verification

## Verdict on the unit: agree (MIXED)

The skeleton holds up. `TestRun.program` lowers the plan onto exec's one walker, with an exhaustive switch and declared
statements per callback, and `Equality` is a single value owner. The judge half carries the debt the report describes,
and checking it turned up two things the report understates:
- a reachable verdict inconsistency in TESTEXEC-3, where two arms of the host judge answer the same inputs differently;
- an unledgered upstream divergence in TESTEXEC-9 that contradicts a ruled register row.

Two S2s are softer than written. TESTEXEC-2 is S2, not S1. TESTEXEC-12 ignores `AssertBarTest`. Neither changes the
MIXED verdict.

Citation accuracy: four of the report's line citations point past the end of their file:
- `AssertErrorRuns.java:171-174`, `:197-251` and `:249-250`. The file has 113 lines.
- `DatabaseVerdicts.java:154-157`. The file has 61 lines.

The content they describe exists at other lines (given below). Every other citation I opened is accurate to within a
few lines.

## Findings
| ID | Reviewer severity | Verdict | Severity I would set | Evidence checked / correction |
|----|----|----|----|----|
| TESTEXEC-1 | S1 | CONFIRMED-WITH-CORRECTION | S1 (borderline S2) | `TestRun.java:147-177`, `:206-214`; `TestRunner.java:209-217`, `:251-305`, `:312-323`, `:369-378`; `TestPlan.java:185-187` and `:313-315` hold 7 nullables (Verdict 3, Verdicts 4). All as described. Correction: the sketched sum type must also cover two shapes. One is the AGREEMENT verdict, which carries both `host` and `wrapper`. The other is the DATABASE judge's host-judged assertError: `host`+`wrapper`, or `host` with an empty base (`TestRunner.java:265-283`). The plan is JSON (`TestPlanJson`), so the fix changes the plan format, the planners and the runner (medium-large). |
| TESTEXEC-2 | S1 | OVERSTATED | S2 | 51 testexec methods return `TypedSide` (the report says 52). `Verdicts.ok/fail` at `Verdicts.java:71-79`. The non-null value is not a dummy: it is the assert's Pure result, `true`, which the run answers when a body ends on an assert (`TestRunner.java:117-120`, `SideShapes.host(moved.lastVerdict(), answer)`). Throwing `AssertFailed` is upstream's own model. The real residue is the null "declined" state: 4 conversion sites, several of them for shapes the planner already excludes (`AssertVerdicts.java:963-967`; `HostJudge.java:147-153` plans `InstanceOf` with a type it knows is null). Wrong lines: the null conversion is at `AssertErrorRuns.java:32-35`, the `ok()` copy at `:110-111`, and `DatabaseVerdicts.unjudged` at `:57-59`. |
| TESTEXEC-3 | S2 | CONFIRMED (reachability now verified by reading, not by running) | S2, flagged as a likely verdict bug | Mechanism: `HostJudgments.java:586-588` (sameElements), `:756-769` (equals), `:36-48` (`finish`), `PureAsserts.java:311-314` (`Typed.all(.., null)`). The sort is kind-blind (`Equality.java:346-375`), so the two checks differ only through `effectiveKind` (`:148-156`). Reachable inputs are shown in Details. |
| TESTEXEC-4 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The call-site counts are exact: VerdictCensus 21, Diagnostics 9, CanonicalDivergence 8, Census 5, getenv 2. Corrections: (1) the "V7 block 271-506" does have production callers: `v7SideRows` (`:344`) runs on every side decode (`AssertRuns.java:264`, `HostJudgments.java:254`) and fills a histogram that only `V7DualChannelCensusTest` reads, which makes the finding stronger. (2) `samples()` has a caller (`pct/.../channelb/ChannelBDiff.java:86`), and `summary()` is printed by 6 PCT channel-B lanes. (3) `CanonicalForm` should not be deleted: it is the host oracle of `SqlCanonConformanceTest.java:69` and its H2 twin, which pin the production SQL canon (`SidePlanner.java:114-127`, `DatabaseJudge.java:687`). Move it to test sources instead. |
| TESTEXEC-5 | S2 | CONFIRMED-WITH-CORRECTION | S2 | The scaffolds are at `AssertRuns.java:68-105`, `:110-137`, `:151-193`, `:199-218`, plus the repeated telling at `RootLoops.java:188-193` and `Appeals.java:198-224`. Correction: the report's evidence of drift is by design. `heldAtItsStep` holds the `AssertFailed` for the agreement arm, which reports it in `agreedAtItsStep` (`:208-216`). A real difference: `Appeals.failed` never tells `unjudged`. |
| TESTEXEC-6 | S2 | CONFIRMED-WITH-CORRECTION | S2 | Verified: `Equality.java:53-63`, `HostJudgments.java:678-689`, `HostJudge.java:238-239`, `TdsCompare.java:204` and `:244`, `SideShapes.java:329` and `:338`, `"TDSNull"` at `Equality.java:176` against `SqlExpr.TDS_NULL_CELL:36`, `"__type"` at `HostJudgments.java:180-181`. Correction: no TypeScript or Python host reads the test plan today, so the G11 cost is prospective. `declaredNumeric`'s null for a non-numeric name is the intended identity rule (`Equality.java:138-147`); only a misspelled numeric name degrades silently. |
| TESTEXEC-7 | S2 | CONFIRMED | S2 | `PlanRunner.java:734-759` admits String and Boolean and calls itself "the one decode". `Verdicts.java:145-160` admits String, Boolean and Integer, raising `NotImplementedException` otherwise. `HostJudgments.java:678-689` admits six Java carriers, raising `IllegalStateException` otherwise. |
| TESTEXEC-8 | S2 | CONFIRMED | S2 | `TestRunner.java:141-496`: 356 lines and 14 fields, counted. It mixes the answer sink, the edge ledger, judge routing and index-building. |
| TESTEXEC-9 | S2 | CONFIRMED (none of the four is a register row) | S2; (a) is a fidelity defect | See Details. `docs/SEMANTICS_REGISTER.md` has no row for the service-JSON tolerance, `tdsEquivalent`'s arithmetic, or the kind-blind `contains`/`subset`. (a) contradicts S15/S132's ruled narrowing. (c) contradicts S131's "each side typed by its own declared kind". |
| TESTEXEC-10 | S2 | CONFIRMED | S2 | `SideShapes.java:223-245` is a near-verbatim copy of `Type.java:573-600`. The same two cardinality texts appear in `Cardinality` (`SideShapes.java:148-163`) and in `valuesShape`. `PureTestRunner.whole` (`:420-425`) and `AgreementArm.first` (`:132-138`) share a regex. |
| TESTEXEC-11 | S2 | CONFIRMED-WITH-CORRECTION | S2 | `Equality.Unjudged` is thrown at `:193` and `:361` and caught nowhere (grep over the whole repository). 63 lines mention `IllegalStateException`. Correction: the `rowsLegNow` catch-all (`SqlTextRuns.java:161-164`, `:225-232`) cannot turn a failing text into a pass. It returns `ok()` only when the texts already agree, the text being the contract, so what it hides is a defect in our own row read. Real, but milder than "rescue". |
| TESTEXEC-12 | S2 | OVERSTATED | S3, plus one worthwhile Appeals/router test | "Behaviour is covered only end to end (lanes)" is wrong. `core/src/test/java/com/legend/testplan/AssertBarTest.java` runs 84 probes under HOST, DATABASE and BOTH, with upstream's exact messages: the table-driven judge test the report proposes, written against the door. `BodyRouterParityTest`, `EqualityWorldsConformanceTest` and `VerdictWorld2ConsistencyTest` add more. What stands: no focused test for `Appeals`' report ordering, `TestRunner.Moved`'s routing or `RootLoops`, and the guard pins are verified (`CodeShapeGuardrailTest.java:104-130`, `JavaEvalLedgerTest.java:1036-1044`). |
| TESTEXEC-13 | S2 | CONFIRMED-WITH-CORRECTION | S2 (low) | Verified: the holders (`TestRun.java:193`, `PureTestRunner.java:334-337`), the reader setter (`TestRun.java:106-110`), the three index walkers, and `RootLoops.Judge`'s one implementation (`TestRunner.java:289`, `AgreementRuns.java:190`). Correction: `AgreementRuns.decided` nests lambdas two levels inside the call, not three. |

## Details

**TESTEXEC-2 (S1 to S2).** The verdict really carries three conventions: a return value, null, and a throw. Two of
them have a reason the report missed:
- The return value is the assert's Pure value. `Verdicts.ok()` is `Scalar(TRUE, "Boolean")`, and it becomes the body's
  answer when the body's last statement is an assert (`TestRunner.java:117-120`).
- Assert failure by exception mirrors upstream's `PureAssertFail`.

The null "declined" path is the genuine residue of the deleted walk. It has four conversion sites
(`TestRunner.java:385-388`, `AssertRuns.java:176-179`, `HostJudgments.java:617-621`, `AssertErrorRuns.java:32-35`).
At least three judges decline shapes the planner already rules out: `containsOf` and `jsonOf` are guarded at
`AssertVerdicts.java:963-967`, and `InstanceOf.typeName == null` is known at `HostJudge.java:147-153`.

The fix is to delete the null path, refuse at planning, and have the runner build the `true` answer itself. That is
medium and mechanical, not a unit redesign. S2.

**TESTEXEC-3 (reachable).** The divergence needs a side whose declared numeric kind differs from its wire's kind. The
code produces exactly that:
- `DeclaredCoercions.java:150-160`: "NUMERIC declared-vs-column mismatches ... get the TYPE-ONLY assertion
  (typeAsDeclared, no SQL cast): the declared kind drives result typing, the database delivers the raw value."
- `Fold.java:1408-1420`: the value-root envelope converts only Float-declared roots ("A Number- or Decimal-declared
  root keeps the wire kind").
- `JudgmentFacts.java:33-37` writes `declared = t.typeName()`.

Concrete inputs, under the host judge (the default, `ExecuteOptions.java:32`) and LITE rules (no edge):

```
Class t::T { d: Decimal[1]; }          // mapped to a column D INT
{| assertSameElements([1, 2], t::T.all()->map(x | $x.d)) }
```

1. The typed decision: the expected side is INTEGER (Long, declared Integer). The actual side is DECIMAL (Long, declared
   Decimal; `effectiveKind`, `Equality.java:148-156`). `same` returns false at `:196-203` (S127: in clean mode, Integer
   is never equal to Decimal).
2. The message re-check: `PureAsserts.assertSameElements`, then `equal` with `Typed.all(.., null)` (`:311-314`). Both
   sides are Long, so they are equal and the message is null.
3. So `d == null`, and `finish(.., hostHeld = true)` returns `ok()`. **The assert passes** against lite's own typed
   rule.

The same values through `assertEquals` reach `equalsOf`. The typed check fails and the message is null, so they raise
`IllegalStateException("verdict/message divergence")`. Two arms of one judge thus answer identical inputs as a pass and
as a crash.

A second input fails the same way under both rule sets: `d: Decimal[1]` on a DOUBLE column, against
`assertSameElements([1.5, 2.5], ..)`. Typed, the sides are FLOAT and DECIMAL, which are unequal. Re-checked, both are
Double, which are equal.

Whether upstream passes or fails these depends on which of lite's two readings of the engine is right:
- `Equality.java:138-147` says a numeric declaration converts the wire cell (`toDecimal`).
- `DeclaredCoercions.java:153-156` says it is the identity.

Either way, the host judge contradicts itself. The database judge kinds a cell by its declaration
(`CanonicalRenderSql.java:431-447`), so G2 parity is also at risk. I could not run this (no Bazel). One probe in
`AssertBarTest` would settle it.

**TESTEXEC-4 corrections.** The report says the V7 half has no production caller and that `samples()` has no caller at
all. Both are wrong:
- `v7SideRows` runs on every side decode.
- `samples()` is read by the PCT channel-B diff (`ChannelBDiff.java:86`).

That only strengthens the case: the instrument is live on the hot path, and its consumers are lane printouts. The
alternative needs one change. `CanonicalForm` is the reference oracle for the production SQL canon's conformance tests
(`SqlCanonConformanceTest`, `SqlCanonConformanceH2Test`), so it moves to test sources rather than being deleted. The
class's own javadoc (`CanonicalDivergence.java:20-22`) claims it stays "out of every production path by construction",
which is false.

**TESTEXEC-5 correction.** `heldAtItsStep` deliberately does not report an `AssertFailed`: the agreement arm decides
and reports it later (`AssertRuns.java:199-218`). The duplication is real, but that example is not drift.

**TESTEXEC-9 (are these deliberate upstream-fidelity rules?).** No. None of the four is a row of
`docs/SEMANTICS_REGISTER.md`.

- **(a) The service-JSON tolerance contradicts a ruled row.** `Equality.serviceDiff` applies 2-ULP unconditionally
  (`Equality.java:539`), and `serviceJson` takes no rules at all. The S15 and S132 rows were narrowed by the user on
  2026-11-05: "the 2-ULP tolerance is backwards-compatibility mode's alone ... lite's own (clean) runs compare Floats
  exactly". `ServiceTestRunner` is a product surface (its javadoc, lines 30-37). Its own javadoc (`:71-73`) and
  `TestAssertions.java:27-30` both say upstream compares "exact decimals". `git log -L` shows the tolerance dates from
  `928451d67` (2026-09-17) and was untouched by the 2026-11-05 narrowing commit `49338a38d`. It looks like a missed site
  of that ruling. Under AGENTS.md's fidelity tenet ("a divergence that is not a row is a defect"), it is a defect.
- **(b) `tdsEquivalent`'s double arithmetic** (`TdsCompare.java:134-148`) has no row. I could not read upstream's
  `tdsEquivalent.pure` here (no pinned checkout on disk). It also ignores the `TDSNull` sentinel that `Equality.same`
  honours.
- **(c) `containsOf` and `subsetOf` drop the declared kinds the plan carries.** Both use `Typed.of` (`:453`, `:502`), so
  they belong to the same class of inconsistency as TESTEXEC-3. This contradicts S131's "each side typed by its own
  declared kind". `containsOf` also calls `same(element, value)`, the reverse of the unused `Equality.contains`'s
  `same(needle, h)`, which matters for the direction-aware `TDSNull` rule.
- **(d) `structural`'s kind-blind nested values** are partly forced: the plan carries no declared kinds for nested
  values.

**TESTEXEC-12 (S2 to S3).** `AssertBarTest` is the "one row per variant, exact message" test the report asks for,
written against the door (84 probes times 3 judge modes). The one genuine gap is a focused test of `Appeals`' report
ordering and of the router. `Appeals` runs only on the compatibility edge, so it is lane-covered today.

## S3 and strengths spot-checks

S3 counts, recounted over the unit's 27 main files:

- **TESTEXEC-14 (inline FQNs):** my recount is within about 3% of the report, and the gap depends on how trailing
  comments are stripped.

  | Measure | Report | Recount |
  |----|----|----|
  | Inline FQN uses | 906 | 933 |
  | Same-package qualifiers | 140 | 136 |
  | `@com.legend.base.Nullable` | 173 | 168 |
  | `TestPlan.JudgingRules` spelled in full | 42 | 42 |
  | `Verdicts.ok`/`fail` spelled in full | 44 | 42 |

- **TESTEXEC-15 (process history):** exact. There are 141 dated lines, 28 lines naming "the walk", and 1,714 comment
  lines.

Strengths:
- **"TestRunTest tests behaviour":** confirmed. It uses a recording `TestRun.Judges` sink (`TestRunTest.java:124-180`),
  and every plan round-trips through JSON (`:183-187`).
- **"Equality is one cohesive owner; the leniencies are gated by the rule set":** mostly true. There is one 2-ULP home
  (`:94-107`), reused at `TdsCompare.java:327`. The caveat is `serviceDiff:539`, an ungated use of it (TESTEXEC-9a).
- **"Appeals is well bounded":** confirmed. It refuses by name at `:129-135` and `:168-174`.
- **"assertError cites upstream line by line":** the content is true, but the cited `:197-251` does not exist. The
  citations are at `AssertErrorRuns.java:62-108`.

## Missed

- **Grid `assertSameElements` ignores declared kinds.** `HostJudgments.tdsRowValuesSameElements` (`:123-139`) judges
  grid cells with `Typed.all(e, null)` in both the decision and the message. `assertEquals` over the same pair
  (`tdsRowValuesVerdict`, `:83-91`) and `TdsCompare.grids` (`:63-72`) type each side by its declared columns. So two
  asserts over the same two grids apply different equality kinds.
- **Two contradictory readings of the engine's numeric boundary rule**, both in lite's comments:
  - `Equality.java:138-147`: a numeric declaration converts the wire cell.
  - `DeclaredCoercions.java:153-156`: numeric mismatches are the identity in the engine.

  `effectiveKind` is built on the first reading. The inputs that make TESTEXEC-3 reachable come from the second. Which
  one is upstream's should be settled and ledgered.
- **`v7SideRows`** is a write-only production counter (see TESTEXEC-4).
