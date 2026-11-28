# M3: the relational corpus with legend-engine's plain raises (the database's own errors)

Measured 2026-11-25 in `/home/user/s2` at `da231244` (nothing committed; the probe reverted).

## Where the corpus picks its style (read from the code)

- The corpus harness hands every test body `PureText.UPSTREAM`:
  `spec/src/test/java/com/legend/rcorpus/MinimalCorpus.java` (its audience's `options`,
  `ExecuteOptions.recording(...).withStyle(com.legend.edge.PureText.UPSTREAM)`). PCT's channel A hands the same style
  for legend-pure's audience (`PctPlanRoute.Audience.LEGEND_PURE.style()`).
- `PureText.UPSTREAM.twoForm(LoweringForms.class, ...)` answers `EdgeLoweringForms.PCT` (`plain` = none: every raise
  written as lite's checked raise). `PureText.PURE_V1` (pure/v1's routes, and PCT's legend-engine audience) answers
  `EdgeLoweringForms.PURE_V1` (`plain` = `PureText.ENGINE_UNCHECKED`: DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE,
  SHIFT_OUT_OF_RANGE, SIZE_NOT_MULTIPLICITY, SLICE_BOUNDS_INVERTED, RANGE_STEP_ZERO, CAST_IMPOSSIBLE, TEXT_NOT_INTEGER,
  TEXT_NOT_FLOAT, TEXT_NOT_DATE -- written as the plain operation, the database's error passing through).
- The corpus cannot simply take `PURE_V1`: that style refuses `judgesEngineText()` by name (it plans queries, never a
  test body's asserts).

## The probe (reverted)

Only the corpus gets the plain raises: `UPSTREAM`'s LoweringForms arm answers `EdgeLoweringForms.PURE_V1` when the JVM
has `-Dprobe.corpus.plainRaises=true`, and only the two corpus lanes' passes set it (`spec/BUILD.bazel`, `corpus_duckdb`
and `corpus_h2` `jvm_flags`). PCT's targets never set the flag, so PCT does not change. Patch:
`/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m3-plain-raises.patch.txt`
(applied by `probes/apply_m3.py`). The run kept M5's logging probe (`PROBE-REF`, and `PROBE-RAISE` lines from
`EdgeLoweringForms.raise` naming every unchecked-kind raise a plan writes and whether it was written checked or plain),
so the baseline is M5's run.

```
cd /home/user/s2
python3 <scratchpad>/r1/probes/apply_m5.py /home/user/s2     # the baseline (M5's run)
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results //spec:judge_host_duckdb //spec:judge_host_h2
python3 <scratchpad>/r1/probes/apply_m3.py /home/user/s2     # + the corpus-only plain raises
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results //spec:judge_host_duckdb //spec:judge_host_h2
python3 <scratchpad>/r1/probes/m3.py <lane> <scratchpad>/r1/data/m5_<lane> <scratchpad>/r1/data/m3_<lane> spec/src/test/resources/rcorpus/<lane>-fail-roster.txt
git checkout -- core/src/main/java/com/legend/edge/PureText.java core/src/main/java/com/legend/edge/SqlTextRuns.java \
    core/src/main/java/com/legend/edge/EdgeLoweringForms.java spec/BUILD.bazel
```

## Result: no verdict moved

| | DuckDB lane | H2 lane |
|---|---|---|
| baseline (checked raises) | pass 2,275, fail 282, skipped 7 of 2,613 | pass 2,132, fail 430, skipped 7 of 2,613 |
| probe (plain raises) | pass 2,275, fail 282, skipped 7 of 2,613 | pass 2,132, fail 430, skipped 7 of 2,613 |
| fail roster: baseline = probe = committed (`spec/src/test/resources/rcorpus/<lane>-fail-roster.txt`) | yes (282) | yes (430) |
| tests whose verdict moved | **0** | **0** |
| per-assert verdicts moved (`judge-host.tsv`) | 0 | 0 |
| failing tests whose failure text changed | 0 | 1 (below) |
| SQL-text referee outcomes (M5's `PROBE-REF` lines, per test and assert) | identical | identical |
| unordered register | unchanged | unchanged |

So the moved-verdict list is empty. What the plain raises reach, test by test (the `PROBE-RAISE` lines: every unchecked
kind any plan of the corpus wrote, checked in the baseline, plain under the probe):

| test | raise kind | DuckDB verdict (both runs) | H2 verdict (both runs) |
|---|---|---|---|
| `meta::relational::graphFetch::tests::subType::testInheritanceMappingWithoutSubType` | SIZE_NOT_MULTIPLICITY (toOne) | PASS | FAIL |
| `meta::relational::graphFetch::tests::subType::testSubTypeAtRootLevelWithInheritanceMapping` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::functions::objectReferenceIn::testObjectReferenceInWithObjReferenceOutput` | SIZE_NOT_MULTIPLICITY | PASS | (no such raise planned on H2) |
| `meta::relational::tests::query::drop::testDropWithVariables` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::query::enumPushDown::testPushDownProjectWithParameter` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::query::limit::testLimitWithVariables` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::query::slice::testSliceWithVariables` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::query::take::testTakeWithVariables` | SIZE_NOT_MULTIPLICITY | PASS | FAIL |
| `meta::relational::tests::query::function::rem::testFilterUsingRemFunction` | DIVIDE_BY_ZERO (rem) | PASS | PASS |
| `meta::relational::tests::mapping::sqlFunction::sqrt::testFilter` | SQRT_OF_NEGATIVE | PASS | PASS |
| `meta::relational::tests::mapping::sqlFunction::sqrt::testProject` | SQRT_OF_NEGATIVE | PASS | PASS |
| `meta::relational::tests::query::function::sqrt::testFilterUsingSqrtFunction` | SQRT_OF_NEGATIVE | PASS | PASS |

12 tests on DuckDB and 11 on H2 plan an unchecked-kind raise at all; three kinds occur (SIZE_NOT_MULTIPLICITY,
DIVIDE_BY_ZERO, SQRT_OF_NEGATIVE). No corpus plan writes SHIFT_OUT_OF_RANGE, SLICE_BOUNDS_INVERTED, RANGE_STEP_ZERO,
CAST_IMPOSSIBLE or the three parse kinds. On these tests the plain operation leaves every verdict, every per-assert
verdict and every SQL-text referee outcome as it was (the inputs did not reach the raising case -- an empty `toOne`, a
zero divisor, a negative square root -- or, on H2, the test already fails elsewhere).

The one changed failure text (H2, a test that fails in both runs and in the committed roster):
`meta::relational::tests::query::enumPushDown::testPushDownProjectWithParameter`, kind SIZE_NOT_MULTIPLICITY: the H2
error is the same (`DataError: Invalid value "g" for parameter {1}`); only the SQL statement H2 quotes in it differs
(baseline `SELECT CAST(CASE WHEN ((regexp_replace(...`, probe `SELECT CAST(CASE WHEN CAST(((regexp_replace(...`).

Logs and ledgers of both runs: `<scratchpad>/r1/data/m5_<lane>/` (baseline) and `<scratchpad>/r1/data/m3_<lane>/` (probe),
`<scratchpad>` = `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad`.

Limit: `PROBE-RAISE` sees the raises that go through `LoweringForms.raise`, the one hook the plain/checked choice is made
in; it is the same hook the probe changed, so it covers exactly what the probe could move.
