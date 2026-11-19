# H12: the combined stage-A dry run (2026-11-19)

The user's question (relayed by the orchestrator, 2026-11-19 ~12:40 UTC): "Did we run real core_relational corpus on H2
and duckdb both? And real all 5 PCT slices on duckdb? Things look good across all?" Every change so far had been
measured alone, on `main` or on H5's V2. This run applies them TOGETHER and asks what moves that none of the single
runs predicted (an INTERACTION).

Branch `claude/compat-translator-spike` (scratch, never merges; no pin, roster, register, ledger, golden or allowlist
moved). Evidence under `h12/` beside this file. The translator session.

## The answer

**Yes, the real suites were run, together, and things look as predicted, with one interaction class.**
- Every move on every lane is attributed to one of the four changes, except **3 tests per corpus lane**, the same 3 on
  H2 and DuckDB: `testPlanForDateTimeConstantParameter{NoTimeZone,GMTTimeZone,ESTTimeZone}`. They are legend-engine's
  `assert($planAsString->indexOf('settlementDateTime = ...') != -1 || ...)`: vacuous under the old `indexOf` (0 on a
  miss, so `!= -1` is always true), satisfied by legend-engine's own text under the engine printer. Only with BOTH
  R4's `indexOf` (1) and lite's own SQL text (2) is the assert real and false. So **B2's CP-4 count understates by 3
  per lane**: three more tests judge legend-engine's SQL text, hidden by a vacuous assert.
- **PCT (both channels, DuckDB, all five suites) and the ENGINE lane move only toward PCT's expected values**, and
  pure/v1 moves exactly as the strands-only run said. No interaction outside the corpus.

## What was applied, at once

| # | change | where |
|---|---|---|
| (1) | V2: Pure's `substring`/`indexOf` in the core, 3.1's dynaFunction and frontend splits; no translator | H5's commits (`Scalars`, `RelOpTranslator`, the 5 frontend files) |
| (2) | B2's lite-text probe: every engine-text and plan-text printer answers lite's own SQL | `census-b2-sqltext-probe-2026-11-19.patch` (`claude/census-b2`), applied behind `-Dh5.off=sqltext` |
| (3) | every H5 meaning strand to the product's, `PureText.raw` kept: S20's literal, the plain set, `isNumeric`, `parsedDecimal`, `parsedJson`, `LEGACY_SQL_NULL_UNSAFE_EQUALS` | `base/.../H5Off.java` switches |
| (4) | H2's product settings in place of `MODE=LEGACY`, `DEFAULT_NULL_ORDERING=HIGH` (the edge's `DatabaseSettingsForm`; the referee is legend-engine's own H2 and keeps its settings) | the `settings` switch |

`-Dh5.off=sqltext,s20,plain,isnumeric,parseddecimal,parsedjson,nullunsafe,settings` on every lane (`h12/flags.txt`).
B1-R's ENGINE lane (`claude/compat-engine-lane` at `f922740bb`) merged in for its `//pct:engine_lane_*` targets.

## The lanes

| lane | `main` | combined | moved, attributed |
|---|---|---|---|
| corpus H2 (`//spec:judge_host_h2`) | 2,289 pass, 276 fail | **2,135 pass, 430 fail** | +157 fail: (2) 142, (1)+(2) 4, (1) 4, (3/4) 3, (2)+(3/4) 1, **INTERACTION 3**; +3 pass: (3) S20 2, (3) `parsedJson` 1 |
| corpus DuckDB (`//spec:judge_host_duckdb`) | 2,452 pass, 108 fail | **2,290 pass, 270 fail** | +162 fail: (2) 146, (1)+(2) 5, (1) 4, (3/4) 3, (2)+(3/4) 1, **INTERACTION 3**; no new pass |
| PCT channel A, DuckDB, 5 suites (PURE audience) | every suite as ledgered | Grammar, Relation, Standard, Unclassified unchanged; Essential: 3 ledger moves | (1): S134's 3 `indexOf` known failures now PASS (PCT's expected values) |
| PCT channel B, DuckDB, 5 suites, HOST judge | every suite as ledgered | Grammar, Relation, Standard, Unclassified unchanged; Essential: its census, 3 ledgered failures pass | (1): the same 3 `indexOf` tests |
| ENGINE lane (`//pct:engine_lane_*`: DuckDB 5 suites, H2 relation; B1-R's pin) | pinned | 13 rows move, every one lite FAIL to PASS against PCT's expected value | (1) 7: `indexOf` `testSimple`, `testFromIndex`, `testIndexOfOneElement`; `substring` `testStart`, `testStartEnd`; `sort` `testSimpleSortWithKey`, `testSimpleSortWithFunctionVariables` (substring keys, `sort.pure:108-113`). (3), the plain set, 6: `testSliceError`, `testRemError`, `testSquareRootError`, `testRangeStepError`, `testBitShiftLeft_MoreThan62Bits`, `testBitShiftRight_MoreThan62Bits`. H2's relation suite: 0 |
| pure/v1 net (`//core:edge_pure_v1`, 28 lines) | the golden | 8 lines move | (3), exactly the strands-only run's 8: the plain set's `raise.sqrtOfNegative` (200 null to 500, an answer) and `raise.divideByZero` (words), S20's SQL text in `cell.float`, `cell.floatWhole` and the plans; (2) moves nothing here |

**How the attribution was made.** The predicted fail set per corpus lane is `main`'s, plus each single run's newly
failing tests, minus each single run's newly passing ones: (1) H5's V2; (2) B2's per-test TSV
(`census-b2-sqltext-probe-2026-11-19.tsv`, 147 H2 and 152 DuckDB tests); (3) and (4) H5's V7 (H2) and V8 (DuckDB), per
strand confirmed by V9. Predicted 427 (H2) and 267 (DuckDB) failing; measured 430 and 270; the difference is the 3
interactions, and nothing predicted to fail passes. Per test: `h12/corpus/attribution-{h2,duckdb}.tsv`. The ENGINE
lane's attribution is a second run with (1) alone (`h12/engine_lane/`, V2-only beside the combined tables): 7 rows move
under (1) alone, the other 6 only with (3); no row moves only in the combination.

**Against the orchestrator's expected sum** ("about 143 text-only, plus 4 substring (some inside the 143), plus 2 or 3
parse quirks, plus 1 legacy, minus 2 or 3 wins on H2"):
- H2: 157 newly failing (142 text alone, 4 substring alone, 4 both, 3 strands alone, 1 strand and text, 3 interactions),
  3 newly passing;
- DuckDB: 162 newly failing (146, 4, 5, 3, 1, 3), none passing.
- The "1 legacy" (`testLegacyFlagProjectionEmitsPlainEquals`) is inside (2)'s list too: it asserts plan text.

## The interaction, in detail

`executionPlan/tests/executionPlanTest.pure:928-958` (read at the pin): each test builds a plan for a query with a
DateTime constant parameter and asserts on its printed text:

```
assertEquals($expectedH2, $planAsString);
assert($planAsString->indexOf('settlementDateTime = \'2019-04-08 18:00:00\'') != -1
       || $planAsString->indexOf('settlementDateTime = TIMESTAMP\'2019-04-08 18:00:00\'') != -1);
```

| | `indexOf` on a miss | the plan text | the second assert |
|---|---|---|---|
| `main` | 0 (legend-engine's meaning) | legend-engine's (the engine printer) | true (found) |
| (1) alone | -1 (Pure's) | legend-engine's | true (found) |
| (2) alone | 0 | lite's | **true, vacuously** (`0 != -1`) |
| (1) + (2) | -1 | lite's | **false**: `Assert failed` |

The first `assertEquals` passes in every row of the table (the per-assert ledger, `judge-host.tsv`): under `main` and
(1) alone by text (the engine printer writes `$expectedH2`, which holds `settlementDateTime = TIMESTAMP'...'`,
`executionPlanTest.pure:931`); under (2) by the appeal on rows, since lite's text is not `$expectedH2` (if it were, the
second assert would find its substring). That is why (2) alone does not move these tests. They are SQL-text tests (CP-4, not promised) that B2's probe could
not see, because the old `indexOf` made the second assert vacuous. **B2's 143 is 146 on DuckDB (and 142 on H2) once R4
lands.** No other interaction on any lane.

## Notes

- **The DuckDB corpus lane keeps `PureText.raw`** (as instructed): without it every DuckDB test's setup fails (H5 task 0).
- **PCT's "moves" are known-failure ledger rows passing** (S134's 3), which move only with the user's word; nothing
  else in either channel moves.
- **The ENGINE lane gains 13 rows, all `BOTH_FAIL` to `LITE_FIXES` or `LITE_ANSWERS`**: legend-engine's manifest calls
  every one a DEFECT or a refusal, so each move is lite answering PCT's expected value where legend-engine does not
  (E-R2, CP-12's strict reading).
- **Run times:** the combined invocation (both corpus lanes, 10 PCT suites, the ENGINE lane's 6 actions, pure/v1)
  took about 15 minutes on the 4-core container; DuckDB's corpus pass 547 s, H2's 156 s.
