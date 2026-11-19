# H7: the ENGINE PCT lane and the oracle's batteries (2026-11-19)

Slice 0.3 and the "Throughout" batteries of the compatibility program's plan
(`docs/records/compat-program-plan-2026-11-19.md` on `claude/compat-program`), owned by B1-R (track O). The user
ratified the program's start at ~10:45 UTC ("yes to 1-5 and the independent review ..."). With it the user ruled:
- CP-9, the per-push checks;
- the oracle's `jars_table` entry;
- each program session pushing its own `claude/compat-*` branch.

Earlier the user switched CP-3 to E-R2, so the manifest table here CLASSIFIES the lane's cases. It does not drive
refusals.

Branch `claude/compat-engine-lane`, from `origin/main` at `b75d6c73a`, with `claude/presentation-r` (slice R's oracle)
merged in. Upstream's answers are read from the pinned sources (`@legend_engine_src`'s `pct-manifests`,
`@legend_pure_src`'s PCT tests) or asked of legend-engine 4.145.0 through the oracle; none is recalled.

## 0. The counts

**DuckDB, 5 suites, 1249 PCT tests:**

| | count |
|---|---|
| legend-engine passes | 1005 |
| legend-engine refuses ("No SQL translation exists for the PURE function ...") | 59 tests, 23 signatures |
| legend-engine fails by its own defect | 185 |
| lite, as legend-engine's audience, passes | 1212 |

| verdict | count | meaning |
|---|---|---|
| SAME | 1004 | both pass |
| LITE_ANSWERS | 55 | legend-engine refuses; lite answers PCT's expected value (E-R2's referee) |
| LITE_FIXES | 153 | legend-engine fails by a defect; lite answers |
| BOTH_FAIL | 36 | |
| LITE_BEHIND | 1 | `abs::testBigFloatAbs`: a 20-digit Float literal, which lite carries as a double (register S120) |

**H2, the relation suite, 469 tests:**

| | count |
|---|---|
| legend-engine passes | 411 |
| legend-engine refuses | 0 (H2's relation manifest has no "No SQL translation" row) |
| legend-engine fails by its own defect | 58 |
| lite passes | 448 |

The verdicts: SAME 411, LITE_FIXES 37, BOTH_FAIL 21, LITE_BEHIND 0.

**Where legend-engine refuses, lite already answers PCT's expected value in 55 of 59 tests.** The 4 that fail:
- `sort` with a key function, twice. The keys are substrings, which lite computes 1-based under legend-engine's
  audience.
- `date(year, month)`, once.
- the 3-argument `indexOf`, once. lite answers it with LOCATE semantics, and PCT expects Pure's.

**The oracle sample:** 11 of 11 DuckDB rows and 10 of 10 H2 rows agree with the manifest's claim when asked of
legend-engine itself (§2).

## 1. The lane (slice 0.3, report-only)

**What runs.**
- PCT channel A, the legend-pure interpreter's suites calling lite's native, runs as legend-engine's audience:
  `-Dpct.audience=engine`, read by `PctPlanRoute`.
  - The case is planned under the pure/v1 route's style (`PureText.PURE_V1`), with no feature flag. That is how
    legend-engine's own PCT adapters run a case: they set none (register S130).
  - The default, legend-pure's audience under the substring flag, is unchanged.
- The suites run with NO expected failures (`Test_LegendLite_Engine_PCT`, its suite named by `-Dpct.engine.suite`),
  so each outcome is the run's own.
- Six build actions, `//pct:engine_lane_<db>_<suite>`: DuckDB's five suites and H2's relation suite. They follow the
  edge golden net's pattern (`JUnitAction`: a failing test is a recorded fact, not a build error).

**Each outcome by its full name.**
- `JUnitMain` gains `-Dlegend.junit.outcomes.full`: one line per test, keyed by its parent's display name and its
  own (`meta::pure::functions::collection::tests::add::testAddWithOffset`).
- The edge net's short-name file cannot join the manifests, because short names collide across a suite's packages:
  DuckDB's essential suite runs 345 tests, but its short-name file has 341 lines.

**The classification** (`tools/engine-runner`'s `planoracle.enginelane.EngineLaneTable`). The PCT module may hold no
comparison machinery (`PctDisciplineTest`), so the join lives beside the oracle. Each lane's outcomes are joined with
legend-engine's manifest for the same database and suite, with the manifest's test name minus its PCT signature:
- **legend-engine's side:**
  - PASS: the test is not in the manifest.
  - REFUSES: the manifest's error is "No SQL translation exists for the PURE function '<signature>'", keyed by that
    signature and the database.
  - DEFECT: any other expected error. Its class is read from the message: UNSUPPORTED (a refusal in other words: "is
    not supported", "Dialect translation ... not implemented", "[unsupported-api]"), WRONG_ANSWER, PLANNER_FAULT,
    DATABASE_ERROR, RUNTIME_FAULT or ERROR_MISMATCH. Every manifest row of the six lanes falls in one; OTHER is 0.
- **lite's side:**
  - PASS: PCT's assert held, so lite answered PCT's expected value.
  - FAIL: classed as WRONG_ANSWER, ERROR_MISMATCH, REFUSED or DATABASE_ERROR.
- Statuses and classes are compared, never words: words are the presenter's, and a message holds run-time ids.
- Every manifest row joined a test the lane ran (no MANIFEST_ONLY row).

**The pin.**
- `pct/engine_lane/` holds:
  - the six tables (`<db>_<suite>.tsv`, one line per test);
  - the counts (`summary.tsv`);
  - legend-engine's refusals by database and signature with lite's outcome on each (`refusals.tsv`, the E-R1 table,
    now a classification under E-R2).
- Regenerate them with `bazel run //pct:update_engine_lane`; check them with `bazel test //pct:engine_lane_test`
  (8 diff tests).
- The pins sit outside `pct/src/test/resources` on purpose. That directory is the PCT library's resources, and a pin
  inside it would make every re-pin rerun the lanes themselves (measured: 3m20s).

**legend-engine's defects on DuckDB, by class:**
- UNSUPPORTED 51;
- WRONG_ANSWER 47;
- PLANNER_FAULT 44;
- DATABASE_ERROR 21;
- RUNTIME_FAULT 17;
- ERROR_MISMATCH 5.

**On H2:** UNSUPPORTED 36 (pivots, as-of joins, list functions), PLANNER_FAULT 20, RUNTIME_FAULT 1, WRONG_ANSWER 1.

**lite's failures on DuckDB:** 37 in all.
- WRONG_ANSWER 18. These include legend-engine's own 1-based `indexOf` and `substring`, which lite reproduces under
  this audience.
- ERROR_MISMATCH 14.
- DATABASE_ERROR 4.
- REFUSED 1.

**On H2:** 21 in all.
- REFUSED 17: "reached a dialect without a list encoding".
- DATABASE_ERROR 4: H2 has no `LATERAL`.

## 2. The oracle sample

A manifest records an expectation, not a run, so a sample of rows was asked of legend-engine 4.145.0 itself through
the oracle (`planoracle.GoldenRunMain`, slice R).

**The method, and its limit.** legend-engine's PCT adapter reprocesses each test function in its own Pure interpreter
before planning (`pct_relational.pure`'s `reprocess`), so a PCT test cannot be replayed through pure/v1 as it is. Each
sampled test's operation was put as a pure/v1 request instead, over the oracle's trades seed. The question: does
legend-engine pass it (answering PCT's expected value), refuse it with the same signature, or fail it as the manifest
says? The requests and legend-engine's answers are in `h7/oracle-sample/`.

| row | manifest | legend-engine through the oracle | agrees |
|---|---|---|---|
| D01 `indexOf::testSimple` | WRONG_ANSWER (actual 5) | 5 | yes |
| D02 `substring::testStart` | WRONG_ANSWER (actual 'the quick ...') | 'the quick ...' | yes |
| D03 `format::testFormatFloat` | REFUSES `format_String_1__Any_MANY__String_1_` | the same refusal | yes |
| D04 `abs::testFloatAbs` | PASS (2.222) | 2.222 | yes |
| D05 `pow::testNumberPow` | WRONG_ANSWER (actual 27.0) | 27.0 | yes |
| D06 `round::testNegativeFloatRoundHalfEvenUp` | WRONG_ANSWER (actual -17) | -17 | yes |
| D07 `parseBoolean::testParseTrue` | UNSUPPORTED ("[unsupported-api] ... parseBoolean") | the same | yes |
| D08 `rem::testRemWithDecimals` | WRONG_ANSWER (0.14, not 0.14D) | 0.14, typed Number | yes |
| D09 `compare::testCompareMixedTypes` | REFUSES `compare_T_1__T_1__Integer_1_` | the same refusal | yes |
| D10 `indexOf::testFromIndex` | REFUSES `indexOf_String_1__String_1__Integer_1__Integer_1_` | the same refusal | yes |
| D11 `testAdjustByDaysBigNumber` | DATABASE_ERROR ("INT64 ... out of range for ... INT32") | the same DuckDB error | yes |

H2, the relation suite:

| row | manifest | legend-engine | agrees |
|---|---|---|---|
| H01 aggregate, H02 groupBy, H03 filter and distinct, H04 window `rowNumber`, H05 sort and limit, H08 extend with `toLower`, H09 concatenate, H10 join | PASS | answered, 200 | yes (8) |
| H06 `asOfJoin::testSimpleAsOfJoin` | UNSUPPORTED ("H2 SQL Dialect does not support AsOfJoin!") | the same | yes |
| H07 `pivot::testPivot_SingleSingle` | UNSUPPORTED ("Dialect translation for node of type ... PivotedRelation not implemented ... H2") | the same | yes |

## 3. The batteries

(written as they land)

## 4. The per-push cost (CP-9)

(measured last)
