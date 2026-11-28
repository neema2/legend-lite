# Independent audit: the next batch, ae275024..038a7eca (/home/user/ll-next)

I only read the code. I ran no Bazel and no tests. Every finding below comes from reading the diff and the code
around it.

## BLOCKER

### B1. Two pinned H2 failures now pass after Part 3, so `//pct:pct_h2_essential` (gate 7) goes red. Interaction of Parts 2 and 3. PLAUSIBLE, high confidence; settled by one run of `//pct:pct_h2_essential` at 038a7eca.

- **The pins.** `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_H2_EssentialFunctions_PCT.java:136-137` pin two tests as expected failures:
  - `testFloatToStringWithNegativeExponent`, with "expected: '0.000000013421' actual: '1.3421E-8'";
  - `testFloatToStringWithPositiveExponent`, with "expected: '134210000.0' actual: '1.3421E8'".
- **The audience.** The `pct_h2_<suite>` targets (`pct/BUILD.bazel:214-222`) set no `pct.audience` and no `pct.channela.rules`, so `PctPlanRoute.Audience.current()` is LEGEND_PURE.
- **What Part 3 changed.** Part 3 (`PctPlanRoute.java:168-169`) now calls `PrintedRelations.lift(query, audience == LEGEND_PURE)`. That lifts a last-step Float `toString`: the database returns the double and Java prints it.
- **What Part 3's own test shows.** `PctPlanRoutePrintTest.aFinalFloatToStringAnswersLegendPuresPrint_andATailItsOwnType` asserts `|1.3421e-8->toString()` gives `0.000000013421` and `|1.3421e8->toString()` gives `134210000.0`. Those are exactly the two PCT expressions above. Nothing in the lift depends on the database.
- **What happens on H2.** Both tests now pass on H2. A pinned test that starts passing fails the run (the class javadoc says so).
- **Why nobody saw it.** Part 2's suites were measured on its own base, 6284d002. That is 30 commits before ae275024, and has neither step 6 nor step 8. Gate 7 is not in `//gates:local`. Nothing in the batch shows `pct_h2` was run on the merged head.
- **Records go stale with it:**
  - `docs/GATES.md`: "Essential 345 tests / 84 pinned" would become 82.
  - `docs/records/h2-pct-full-2026-11-26.md:54`: "a Float's toString in exponent form x2" is listed as a dialect defect.
  - The four new H2 classes' javadocs (line 24) still say "not yet in any gate".
- **Fix.**
  1. Run `//pct:pct_h2` at the head.
  2. Delete the two pins, and any others that moved because of steps 4 to 8.
  3. Correct GATES.md and the H2 record.

## SHOULD-FIX

### S1. The record says the condition is "typed by the typer", but the code builds the calls by hand. Step 6's design asked for the typer. CONFIRMED by reading.

- **What the design and the record say.** research-step6-equality.md §4: an assert's condition "must type its calls **through the typer**, so that the comparison setting applies". The plan record (`edition-plan-to-finish-stage-b-2026-11-25.md:772`) says "typed by the typer over the sides as named values".
- **What the code does.**
  - `VerdictQueries.sizeOf` and `sameValue` (`VerdictQueries.java:503-519`) build `TypedNativeCall`s by hand with `native1` and `native2`.
  - Those helpers resolve the function by FQN and take `findFirst()` on the overloads with the right arity.
  - The finished `==` passes only through `typer.compared`. `size` and `not` never meet the typer.
- **Consequences.**
  - Any rewrite the typer does while typing a call is skipped. One example is TdsDesugars' `$t.rows.values->size()`, which becomes rows times columns (`TdsDesugars.java:742-753`). It keys on the protocol `AppliedFunction`.
  - A future overload of `size`, `equal` or `not` would be picked silently.
- **What to check.** Run `assertSize($t.rows.values, k)` and `assertSize($t.rows->at(0).values, k)` under BOTH judges. The host counts decoded values. What the hand-built product `size` counts there is unverified.
- **Fix.** Either type the calls through the typer, as the design asked, or correct the record and add that test.

### S2. Graph sides now go through the product's `size`, against the design. PLAUSIBLE risk.

- **What the design said.** research §4's table and §6.6 say a graph side "counts JSON documents (stays a member)", and the first slice was to be "over non-graph, non-relation sides".
- **What the slice did.** It deletes `typedSizeOfGraph` (`DatabaseJudge.java:409-432`) and plans `size(c)` for graph sides too. Commit 9d880deb says "every test passes".
- **How the judges count.** The host judge counts root objects in the JSON (`HostJudgments.sizeOf:424-427`). The product counts the root query's rows.
- **What to check.** A graph fetch with a to-many child, run under BOTH, should be held by a test so the two cannot drift apart.
- **Records.** The record should say this departs from the design.

## NOTES

- **N1. A NULL size now fails both polarities.** `DatabaseJudge.databaseSize`: the old comparison was `NULL_SAFE_EQUAL`; the product's `==` is a bare `=` (`Scalars.java:110-157`).
  - If the size side's one value is SQL NULL (a nested `toOne()` over a NULL cell: toOne is checked at roots only), the verdict is NULL.
  - `typedVerdictOf` reads only `TRUE` as a pass, so `assertSize` and `assertNotSize` both fail.
  - Before, `assertNotSize` passed. The host judge would pass it too, since its negation flips a fail.
  - This is an edge case; no test holds it.
- **N2. `DatabaseJudge.java:412`: `var cw = requireNonNull(coll.side())` is never used now.** The collection side is also planned twice: once for the envelope check, once inside `size(c)`.
- **N3. Misleading or odd messages on the size arm.**
  - If `typedOverValues` returns null because the count side has no typed value column, the arm reports "a size that is no one Integer".
  - `"the collection's size: " + count.why()` can print "null".
- **N4. `SpecCompiler.comparedAsInAQuery` has no `RunScope.planning(...)` check.** Its sibling entries have one.
  - Its path for a comparison across types (the marker) is unreachable for `size`, where both sides are Integer.
  - The database judge also runs in the corpus under `ENGINE_TESTS`, so "lite's door only" is not literally true. That is harmless for Integer == Integer.
- **N5. `PureText.refusePlanText`'s sentence omits two functions it also refuses.** It leaves out `toNonExecutableSQLString` and `planToStringWithoutFormatting`, which are refused too (every `NativeFn.JavaRoutine` asks the facet). S427 lists `toNonExecutableSQLString`.
- **N6. Product javadoc still links classes that moved to the test-only harness.** `SqlTextReferee.java:16` links `{@link SqlTextRuns}`, `EngineTextJudgment.java:12` links `{@link EdgeCodec}`, and `TestRunner.java:137` links `{@link Appeals#decide}`. There is no doclint, so the build is fine, but the links are broken.
- **N7. The moved files left the shape guards' scans (CodeShape, ErrorShape, Observability, the JavaEvalLedger pins).** Every move is dated and goes down; none went up. Harness code is now as unguarded as `src/test`. `ObservabilityGuardrailTest` went 34 to 30, set to the measurement.
- **N8. `tools/census` depends on `//core:edge` but not `//core:edge_harness`.** If it ever decodes a plan holding the edge's records, the `ExtensionCodec` service loader finds no provider. Low risk; it names no moved class.

## Clean, by category

- **The refusal.**
  - `UpstreamPlans` is asked only at a plan-text routine (`PlanTextRoutines`), `executionPlan` (`PlanAllocations:113`) or a DDL generator (`BodyPlanner:313,330`); the rest of its uses are under `testplan`.
  - The pure/v1 routes reach the edge's plan-text pieces only from their own Java (test-data timestamp, plan support functions). `twoForm` and the activity text are untouched.
  - `ENGINE_PCT` takes UPSTREAM's facets, which equal PURE_V1's minus the refusal. `ENGINE_TESTS` and `UPSTREAM` are unchanged.
  - The golden line matches `EngineEnvelope.error`'s key order and escaping. I did not run it to compare byte for byte.
  - S427 and the differences page say what the code does.
- **Step 9's move.**
  - No class left in `core/src/main` uses a moved class in code; only javadoc or comments name them (N6).
  - The services file moved. No moved class reads a resource.
  - `edge_harness` and `edge_service_tests` are testonly. Every target that depends on them is testonly too: `spec_tests_lib`, `pct_tests_lib`, `core_edge_tests_lib`, the jar lists and the genqueries.
  - `:edge` reaches no exec, testexec or diagnostics. No edge main source imports them, including Part 3's `PrintTail`.
  - `core-layers.txt` and `not_layers` agree with each other.
  - The guard paths moved with their files. The only increase is SidePlanner 97 to 102, which is Part 1's and dated.
- **Merge interactions.** None, apart from B1.
  - No register row is duplicated.
  - JavaEvalLedgerTest's Part 1, 3 and 4 edits are separate and consistent.
  - No old `core/src/main/.../edge/<moved>.java` path remains outside one comment.
- **Tenets and invariants.**
  - `Lowerer.over` names no SQL function and infers no type. It refuses an unbound name or a property read by name.
  - A let-binding of the same name could shadow a bound name, but only the stamp plan parameters are pre-bound.
  - Nothing in the core branches on who is asking.
- **Step 6's NULL and count semantics.** The product's `size` skips NULLs: `compactIfValueLane`, and `count(col)` over a single column (`RelationPredicates:249-258`). Relation sides are refused before planning (S137).
- **CTE names in the fused verdict batch.** The members' CTE names are generic (`__v0`, `__v1`), as the existing `__a` and `__n` are, so they rely on the same renaming. The repeated condition in `whereFails` is a plain `=` and cannot raise.

## Not read

- Part 3 beyond its interaction: PrintTail's members, UpstreamPrints and PureRelationText. It had its own review.
- The four H2 classes' individual pins, beyond the Float rows and a scan for areas this batch changed.
- research-step9-inventory sections A, C and E. I checked the move against the code instead.
- `Typer.compared` internals.
- ArchitectureTest's jar-import wiring in detail.
- The register beyond S427 and S137.
