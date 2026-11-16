# E-Java's ledger audit, item 9 (read-only, 2026-11-16)

This audit reads the Java-evaluation ledger's rows in E-Java's own files. Those are the 18 files the plan
(`docs/records/e-java-plan-2026-11-16.md` §5.3) gives E-Java, 4,448 counted lines. Each place that touches a value is
classified as one of four kinds:

- **JUSTIFIED-HOST:** the host judge's own comparison, the independent witness.
- **MOVABLE:** the database could compute it.
- **ROUTING:** counted by the ledger, but it evaluates nothing.
- **DEFECT.**

Method: a read of each file, with every pin checked against the test's own stripped count (all 18 are exact; none has
headroom). The two defects found were checked by hand.

## The finding

On this road the database already computes every query-time value. The ledger pins whole files, so most of its count
here is planning and plumbing.

| Kind | Lines (approx.) |
|---|---|
| justified host (or edge) comparison | 1,040 |
| movable to the database | 105 |
| routing: plans, dispatch, listener plumbing, diagnostics, dead code | 3,300 |

The 9 planning-side rows evaluate nothing: 2,346 lines in all.

| File | Pin |
|---|---|
| HostJudge | 237 |
| DatabaseJudge | 790 |
| EffectPlanner | 225 |
| BodyPlanner | 343 |
| FramePlanner | 214 |
| LetPlanner | 191 |
| SidePlanner | 97 |
| AggAwareActivities | 211 |
| SystemRoutes | 38 |

`RunScope` and the planner/exec ArchUnit walls already prove that planning runs no values. Three run-side rows only read
the database's verdict or carry listener plumbing (273 lines in all): DatabaseVerdicts 38, HostSideShapes 27 and
AssertRuns 208.

## Per file (lines are source lines under `core/src/main/java/com/legend/`)

| File | Pin | Justified | Movable | Routing | Defects |
|---|---|---|---|---|---|
| testexec/Equality | 452 | ~340 | 0 | ~112 (80 belong to `com.legend.test`) | D1 (+D5 unsure) |
| testexec/HostJudgments | 633 | ~330 | ~93 (key projection; unsure) | ~210 | D3 (+D4 unsure) |
| testexec/TdsCompare | 236 | ~190 | 0 | ~46 | D2 (known, S132) |
| testexec/PureAsserts | 218 | ~110 | 0 | ~108 | 0 |
| testexec/AssertRuns | 208 | 0 | 0 | ~208 | site of D3 |
| testexec/AssertErrorRuns | 70 | ~30 | 0 | ~40 | 0 |
| testexec/SqlTextRuns | 220 | ~40 (edge) | ~12 (to planning) | ~168 | 0 |
| testexec/DatabaseVerdicts | 38 | 0 | 0 | 38 | 0 |
| testexec/HostSideShapes | 27 | 0 | 0 | 27 | 0 |
| testplan/HostJudge | 237 | 0 | (~30 with pairedOf) | 237 | 0 |
| testplan/DatabaseJudge | 790 | 0 | 0 | 790 | 0 |
| testplan/EffectPlanner | 225 | 0 | 0 | 225 | 0 |
| planner/BodyPlanner | 343 | 0 | 0 | 343 | 0 |
| planner/FramePlanner | 214 | 0 | 0 | 214 | 0 |
| planner/LetPlanner | 191 | 0 | 0 | 191 | 0 |
| planner/SidePlanner | 97 | 0 | 0 | 97 | 0 |
| planner/AggAwareActivities | 211 | 0 | 0 | 211 (fidelity unsure) | 0 |
| planner/SystemRoutes | 38 | 0 | 0 | 38 | 0 |

## Defects

**D1. Confirmed by hand.** `testexec/Equality.serviceDiff` (around line 539)

- It tolerates two numbers within 2 ULP unconditionally.
- S15 narrowed that tolerance to backwards-compatibility mode on 2026-11-05.
- The comparison's own javadoc says the engine compares `decimalValue()` exactly.
- Its one caller is the service-test rule (`com.legend.test.TestAssertions.equalToJson`), not the test road's judges.
- No register row covers it.
- Fix: delete the ULP branch. Move `serviceJson…show` (about 80 lines) to `com.legend.test`.

**D2. Known, S132.** `testexec/TdsCompare.cellEquals` (319-336)

- Two spellings of one value (`30.20` vs `30.2`) pass the 2-ULP check. S132 says they must fail ("not yet for a
  rendered text").
- Compatibility mode only.
- Fix: tolerate only when the parsed doubles differ.

**D3. Confirmed by hand.** `testexec/HostJudgments.instanceOfOf` (548) calls `AssertRuns.structured(v)`

- `structured` parses any String whose text starts with `{` or `[` as JSON. That decides by a value's text, while its
  own comment says the declared kind decides.
- The planned facts already say whether a side is class-typed.
- Fix: gate on the planned class, as `structuredSide` (237-239) does.

**D4. Unsure.** HostJudgments' message path (767-768) re-compares with declared kinds dropped.

- Example: an Integer-declared side carried as Doubles fails the typed compare but passes the untyped one.
- `finish` then throws "verdict/message divergence" (44-46) instead of returning a verdict.
- Fix: build the message from the verdict's own comparison.

**D5. Unsure.** `testexec/Equality.withinRank` (369): `new BigDecimal(String.valueOf(n))` throws on a NaN or Infinity
Double during assertSameElements' sort. It is not yet checked whether one can reach it.

## Actions, by lines per hour

| # | Action | Lines | Cost | Needs |
|---|---|---|---|---|
| 1 | Retire the 9 planning-side rows | 2,346 | 1-2 h | a ruling, below (guard files are load-bearing) |
| 2 | Retire the 3 plumbing rows | 273 | 0.5 h | the same ruling |
| 3 | Move the service JSON compare to `com.legend.test`; fix D1 | 80 | 1 h | none |
| 4 | Delete diagnostics and dead code | ~70 | 1 h | none |
| 5 | Simplify the run-side plumbing: AssertRuns' four listener blocks into one helper; a message built from the verdict's own comparison (removes D4) | ~75 | 1.5 h | none |
| 6 | Put the key projection and the zip in SQL (G6's real movable work) | ~95 | several h | a ruling: may the host judge, as an independent witness, read SQL-side projections? |
| 7 | Fix D2 and D3 | 1-3 lines each | small | none |

The diagnostics and dead code in action 4:

- `TdsCompare.ordLeniency` and its callers.
- `firstCanonDiff` and `truncCanon`, moved into `CanonicalDivergence`.
- The `LL_TOL_COUNT` print.
- `PureAsserts.assertSize` and the two-argument overloads.
- `HostJudge.equalsMessage`.
- `HostJudgments.tdsHostMessage`.

## What this means for D5

D5 cut the residue in E-Java's files by 15%, to ≤ 3,800. Two ways to get there:

- **Without a ruling.** Actions 3-5 and 7 bring about 225 lines down, to about 4,220. That is short of the target.
- **With a ruling.** The planning-side rows go to a planner size register, or are deleted on the strength of the
  RunScope and ArchUnit proofs. Actions 1-2 then bring the residue to about 1,800.

The question for the user is whether the ledger should count only run-side code that touches values, rather than whole
planning files. Either answer works; the target follows it.
