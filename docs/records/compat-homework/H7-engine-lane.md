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

**The classification** (`tools/engine-runner`'s `com.legend.tools.enginelane.EngineLaneTable`). The PCT module may hold no
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

**The requests** (`tools/engine-runner/src/test/resources/batteries/substring_indexof.tsv`, 49 in all):
- H3's 25 substring and indexOf requests over the trades seed's `desk` column (`col.`);
- the same 24 string expressions over a literal, `'Credit'` (`lit.`). DuckDB's constant folder and its runtime
  disagree on SUBSTRING (H3 §3), so a literal-only battery would test the wrong path.

The cases covered:
- literal and computed starts and ends;
- starts of 0, negative starts, and starts before the first character;
- an end past the string, an end before the start, a negative length;
- the 2- and 3-argument forms, the list form, and nested calls.

**The heavy half** (`//tools/engine-runner:batteries_oracle_test`, in `//gates:heavy`).
- legend-engine 4.145.0 is re-asked through the oracle, one JVM per database: `engine_batteries_{h2,duckdb}`, on the
  goldens' LocalH2 seed and on the DuckDB file seeded from it.
- Its answers are pinned in `tools/engine-runner/src/test/resources/batteries/engine_{h2,duckdb}_answers.tsv`
  (`bazel run //tools/engine-runner:update_batteries`); a drift fails the diff test.
- The same target pins the rows legend-engine's DuckDB file holds (`trades_duckdb_digest.txt`: 128 rows, sum(qty)
  509, and an md5 of every row). That file is written by legend-engine's DuckDB 1.3.0 and read with lite's own 1.5.6
  driver. This is the ruled `jars_table` entry's one use.

**The per-push half** (`//pct:batteries_test`, in `//gates:pct_duckdb`). No legend-engine JVM runs.
- lite answers each request as legend-engine's audience: pure/v1's execute route as the edge ships it
  (`PureV1Routes.execute` over `EdgeServer.runner`; `pct/src/test/java/.../batteries/LiteBatteries.java`).
- It answers on H2, and on a DuckDB file seeded with lite's own driver
  (`//tools/engine-runner:lite_battery_seed`). The seed is the statements legend-engine's parser read from the model
  (`trades_seed.json`, pinned by the heavy half), and the model swaps the connection as text (`lite_battery_model`).
- The comparison (`com.legend.tools.enginelane.Batteries`) refuses to run if lite's file holds other rows than
  legend-engine's: the two digests must be equal.
- Each request is compared by kind and rows: AGREE, BOTH_FAIL, or DIFFER, with both sides' answers listed. Refusals
  compare by status; their classes are listed and words are never compared.
- The agreement is pinned in `pct/batteries/agreement_{h2,duckdb}.tsv` (`bazel run //pct:update_batteries`), so a
  move toward legend-engine, or away from it, shows as a diff.

**Today, `main`'s lite against legend-engine:**

| | AGREE | BOTH_FAIL | DIFFER |
|---|---|---|---|
| H2 (49) | 46 | 1 (the list `indexOf`, which neither plans on H2) | 2 (the 3-argument `indexOf`: legend-engine refuses, lite answers) |
| DuckDB (49) | 31 | 0 | 18 |

The 18 DuckDB differences:
- 16 are a start at or below 0: `col.s01 s06 s07 s11 s14 s15 s18 i07` and `lit.s01 s06 s11 s12 s13 s14 s15 s18`.
- 2 are the 3-argument `indexOf`, column and literal.

On the column operands this is H3's measurement of main exactly: DuckDB 16 of 25. Phase 3's target-aware rewrite
moves these rows to AGREE, and the pin shows each move.

**How a move is recorded:**
- A legend-engine bump or a changed seed fails the heavy half. Run `bazel run //tools/engine-runner:update_batteries`,
  then `bazel run //pct:update_batteries`, and say which way each row moved.
- A product change that moves an answer fails the per-push half. Run `bazel run //pct:update_batteries`.

## 4. The per-push cost (CP-9)

Measured on this container: 4 cores, 15 GB. Each action was timed from a Bazel profile with the disk cache off and
one job at a time. These are action times, not test times.

| check | actions | time each | serial total |
|---|---|---|---|
| the ENGINE lane, `//pct:engine_lane_<db>_<suite>` | 6 | essential 45.2 s, grammar 36.4 s, relation 63.1 s, standard 44.3 s, unclassified 24.3 s, H2 relation 59.5 s | **272.7 s** |
| its tables and summary, `engine_lane_table_*`, `engine_lane_summary` | 7 | about 0.1 s | under 1 s |
| the batteries' per-push half: `battery_seed`, `battery_lite_{h2,duckdb}`, `battery_compare_{h2,duckdb}` | 5 | 0.9 s, 3.0 s, 3.7 s, 0.1 s, 0.1 s | **7.7 s** |
| the batteries' heavy half (heavy only): `trades_duckdb`, `engine_batteries_{h2,duckdb}`, the answers, the file check | 6 | 3.0 s, 16.4 s, 13.8 s, 0.1 s, 0.1 s, 0.6 s | 34.0 s |

**Each target, separately:**

| target | when | its own time | what it pulls |
|---|---|---|---|
| `//pct:engine_lane_test`, the pinned compare | per push | 8 diff tests, about 0.1 s each | the 6 lane actions (272.7 s serial) and the 7 table actions (under 1 s), when their inputs move |
| `bazel run //pct:update_engine_lane`, the update | by hand, on a re-pin | 4.3 s with its actions cached (copying 8 files) | the same actions |
| `//pct:batteries_test` | per push | 2 diff tests, about 0.1 s each | 8 s of actions: `lite_battery_seed`, `lite_battery_model`, `battery_lite_{h2,duckdb}`, `battery_compare_{h2,duckdb}` |
| `bazel run //pct:update_batteries` | by hand | 1.5 s cached | the same |
| `//tools/engine-runner:batteries_oracle_test` | heavy | 4 diff tests | 34 s of actions (legend-engine re-asked) |
| `bazel run //tools/engine-runner:update_batteries` | by hand | 2.2 s cached | the same |

For comparison, B1's edge net came in at 568 s against an estimate of 2-4 minutes (the orchestrator, 2026-11-19).
The ENGINE lane is about half that: it runs the same PCT suites once more, under the other audience.

**What a push pays:**
- **The batteries: about 8 s.**
- **The ENGINE lane: about 4.5 minutes of single-job time,** spread over 6 independent actions.
  - Each action is about 15-20 s of start-up (the interpreted Pure runtime and the adapter's PAR) plus its suite's
    tests. The suites' own times: 27, 18, 45, 27, 6 and 41 s.
  - Wall time on a runner is that 4.5 minutes divided by the actions it runs at once. Each asks 4 GB
    (`memory_mb = 4096`, as the edge net's lanes do).
  - So on a 16 GB Linux runner, about 3 at once: roughly 1.5-2 minutes. On CI's 7 GB macOS runner, one at a time:
    the full 4.5 minutes.
- **The lever:** the lane's live heap peak is 1.6-1.8 GB per suite (each run's log), so `memory_mb = 2560` would let
  more run at once. Not changed here: it is the user's per-push budget to rule (CP-9), and the edge net's lanes hold
  4096 too.
- **The lane reruns only when its inputs change.** Those are the PCT library, the adapter and the core libraries,
  which is nearly every push that touches the core. The tables, the summary and the batteries' comparisons cost
  nothing measurable.

## 5. CP-12: does lite "fix" legend-engine into something other than PCT's expected value?

**No.** None of the 153 DuckDB LITE_FIXES, the 55 LITE_ANSWERS or H2's 37 LITE_FIXES answers anything but PCT's
expected value.

The lane runs with no expected failures, so a pass means every assertion in the test held against legend-pure's
values. To check what each assertion checks, every one of the 245 tests' bodies was read in the pinned sources (all
found; `h7/cp12/assertion-scan.txt`):

| the test's assertions | tests |
|---|---|
| an exact value: `assertEquals`, `assertEq`, `assertSameElements`, `assertTdsEquivalent` (14 of them pivot tests, qualified), `assertJsonStringsEqual` | 191 |
| the Boolean lite computed, `assert($f->eval(\|...))` or `assertFalse`: exact against true or false | 44 |
| emptiness of lite's answer, `assertEmpty` (`head` of `[]`, `zip` three times): exact | 4 |
| a type, `assertIs` (`testConcatenateTypeInference`, `testAssignNewInstance`, `testSingleMinusType`, `testSinglePlusType`): exact on the type, which is what they test | 4 |
| less than a value | 2 |
| an error, `assertError` | 0 |

The 2 that check less than a value are the only places where lite could answer something else and still pass:
- `lang::tests::letFn::testLetChainedWithAnotherFunction`, a LITE_ANSWERS row (legend-engine refuses `letFunction`),
  asserts only `assertNotEmpty`.
- `string::tests::parseDate::testParseDateTypes`, a LITE_FIXES row, asserts only `assertInstanceOf` DateTime and
  StrictDate.

Checking them needs lite's answer beside legend-pure's own. That was not done here.

## 6. The local gate, and what it caught

**The result:** `bazel test --lockfile_mode=error //gates:local` at the final code gives 558 passed, 12 failed and 1
skipped.
- The 12 failures are the Postgres-backed tests (`//core:*_postgres_test`, `is_numeric_probe_test`,
  `postgres_arm_test`, `//datacube:verify_app_test`): `initdb` refuses root in this container. All 12 pass run
  unprivileged (`--run_under` as a non-root user), as in slice R.
- **What the first run caught:** `//core:census` refused the batteries' first PCT runner. It named a database outside
  its owners (a JDBC URL and database names: `DialectLeakRatchetTest`), and it parsed a model by grammar level outside
  the parser (`ParserBoundaryArchTest`).
- **The fix:**
  - the seeding and the connection swap moved into the oracle's package;
  - the seed became legend-engine's own parsed statements, pinned;
  - the PCT runner is handed a model and a file.
- **A trap for later sessions:** the census also reads the word "oracle" as a database. So the classifier lives in
  `com.legend.tools.enginelane`, and the PCT runner in `org.finos.legend.lite.pct.batteries`.

## 7. Files

| file | what |
|---|---|
| `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_Engine_PCT.java` | the lane's suite: one PCT suite by `-Dpct.engine.suite`, no expected failures |
| `pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java` | the audience switch, `-Dpct.audience=engine` (+12 lines; the default unchanged) |
| `tools/junit/JUnitMain.java` | `-Dlegend.junit.outcomes.full`: each test's outcome by its full name (+29 lines) |
| `tools/engine-runner/src/main/java/com/legend/tools/enginelane/EngineLaneTable.java` | the lane's classification against the manifests |
| `tools/engine-runner/src/main/java/com/legend/tools/enginelane/Batteries.java` | the batteries' answer form, comparison, the file check, the seed and the model's connection swap |
| `tools/engine-runner/src/main/java/planoracle/SeedDuckDbMain.java` | `--statements-out`: the seed statements legend-engine's parser read, for the per-push half |
| `pct/src/test/java/org/finos/legend/lite/pct/batteries/LiteBatteries.java` | the batteries' per-push half: lite's answers through pure/v1's route |
| `pct/BUILD.bazel`, `tools/engine-runner/BUILD.bazel` | the actions, the pins, `//pct:engine_lane_test`, `//pct:batteries_test`, `//tools/engine-runner:batteries_oracle_test` |
| `gates/BUILD.bazel` | the two per-push checks in `pct_duckdb`; the heavy half in `heavy` |
| `tools/deps/jars_table.bzl` | `tools/engine-runner` as a `duckdb_jdbc` user (CP-9, the user's ruling, dated) |
| `core/BUILD.bazel` | the trades model's export gains `//pct` (its own `exports_files` block): a Bazel visibility, not a guard file |
| `pct/engine_lane/`, `pct/batteries/`, `tools/engine-runner/src/test/resources/batteries/` | the pins |

No other pin, ceiling, ledger or allowlist moved. The guarded `EDGE_TEST_SRCS` list is not touched: the lite runner
lives in the PCT package, which reaches the edge already.
