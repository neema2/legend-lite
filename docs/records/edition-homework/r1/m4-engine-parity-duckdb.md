# M4: engine-mode parity on DuckDB (channel A, legend-engine's route) vs legend-engine's `relational-duckdb` manifest

Measured 2026-11-25 in `/home/user/s2` at `da231244` (detached; nothing committed). No probe was needed for M4: the
committed ENGINE lane already runs channel A on DuckDB as legend-engine's audience.

## What runs where (read from the code and the BUILD files)

`PctPlanRoute.Audience` (`pct/src/test/java/org/finos/legend/lite/pct/extension/PctPlanRoute.java`) picks one audience
per JVM from two properties:

| audience | how a target selects it | plan style | comparisons across types | an error's words |
|---|---|---|---|---|
| `LEGEND_PURE` (the default; "legend-pure route") | no property | `PureText.UPSTREAM` (lowering forms `EdgeLoweringForms.PCT`: every raise checked) | `PureComparisons.asPures` | `PureMessages.compile` |
| `LEGEND_ENGINE` ("legend-engine route") | `-Dpct.audience=engine` | `PureText.PURE_V1` (lowering forms `EdgeLoweringForms.PURE_V1`: the ten `ENGINE_UNCHECKED` raises plain) | `EngineComparisons.asTheDatabases` | `PureMessages.compile` |
| `LITE` | `-Dpct.channela.rules=LITE` | `PlanStyle.PRODUCT` | none (crossings refused) | lite's own |

No audience sets a feature flag. Channel A targets in `pct/BUILD.bazel`:

- `//pct:pct_duckdb_<suite>` (gate 6), `//pct:pct_h2` (relation only), `//pct:pct_sqlite_<suite>` (manual),
  `//pct:pct_postgres_<suite>`, `pct_mariadb_*`, `pct_mysql_*`, `pct_mongodb_*`, `pct_trino_*`, `pct_clickhouse_*`, the
  clouds: all **legend-pure route**, each judged by its own expected-failure ledger class
  (`Test_LegendLite_[<Db>_]<Suite>Functions_PCT`).
- `//pct:edge_<duckdb|h2|sqlite>_<suite>` (the edge golden net's recordings): **legend-pure route**, same ledgers.
- `//pct:pct_duckdb_lite_<suite>` (manual): **LITE rules**, `Test_LegendLite_LiteRules_*` ledgers.
- `//pct:engine_lane_duckdb_<suite>` (all five suites) and `//pct:engine_lane_h2_<essential|relation|standard>`:
  **legend-engine route**, `Test_LegendLite_Engine_PCT`, NO expected failures, each test's raw outcome written by full
  name (`-Dlegend.junit.outcomes.full`, message cut at 600 characters -- every `relational-duckdb` expectedError is
  under 350, so no comparison below is affected). Its classification table is pinned in `pct/engine_lane/`.

Upstream scopes lite has no target for: **Variant** (`relational-duckdb` VariantFunctions_manifest: 17 exclusions) and
**ScenarioQuant** (0 exclusions). The in-scope manifest rows are therefore 261 - 17 = **244**. (Every channel-A lane
runs the same five suites; no lane runs Variant or ScenarioQuant.)

## Commands

```
cd /home/user/s2
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results \
  //pct:engine_lane_duckdb_essential //pct:engine_lane_duckdb_grammar //pct:engine_lane_duckdb_relation \
  //pct:engine_lane_duckdb_standard //pct:engine_lane_duckdb_unclassified \
  //pct:engine_lane_table_duckdb_essential //pct:engine_lane_table_duckdb_grammar //pct:engine_lane_table_duckdb_relation \
  //pct:engine_lane_table_duckdb_standard //pct:engine_lane_table_duckdb_unclassified //pct:engine_lane_summary_duckdb
# (the same invocation also built M6's probe targets; they do not touch these)
diff pct/engine_lane/summary_duckdb.tsv bazel-bin/pct/engine_lane_table/summary_duckdb.tsv   # empty: the pin holds at da231244
python3 /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m4.py <legend-engine src root> bazel-bin/pct/engine_lane /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/m4.json
python3 /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/m4_report.py /tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/m4.json
```

Outcomes kept at `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/data/engine_lane/duckdb_<suite>/outcomes.tsv` (the full per-test outcome with message);
classification in `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/m4.json`. The audience check (`everyCaseWasPlannedAsLegendEnginesAudience`) passed in all five.

Rules used: a test in a scope's manifest is an upstream failure with that `expectedError`; any other test the lane ran
in that scope is an upstream pass. Every manifest test was run by lite (no manifest-only rows). Upstream matches an
expected failure by **contains** (`PureTestBuilderInterpreted`: `getMessageFromError(e).contains(message)`), so that
rule is reported too.

## Headline

| | count |
|---|---:|
| tests run (5 suites) | 1,249 |
| upstream failures in scope | 244 (of 261; 17 are Variant) |
| (1) both pass | 1,004 |
| (2) both fail with the same wrong value | 1 (exact text: 0; normalized: 1; by upstream's own contains rule: 1) |
| (2b) both fail the same way, not a value (an error expected, none or another thrown) | 3 |
| (3) upstream fails, lite passes | 215 |
| (4) both fail, differently | 25 |
| (5) upstream passes, lite fails | **1** |

| scope | tests lite ran | upstream manifest rows | (1) both pass | (2) same wrong value | (2b) same failure, not a value | (3) upstream fails, lite passes | (4) both fail, differently | (5) upstream passes, lite fails |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Essential | 345 | 146 | 198 | 0 | 1 | 126 | 19 | 1 |
| Grammar | 137 | 44 | 93 | 0 | 1 | 38 | 5 | 0 |
| Relation | 469 | 2 | 467 | 1 | 0 | 1 | 0 | 0 |
| Standard | 205 | 44 | 161 | 0 | 1 | 42 | 1 | 0 |
| Unclassified | 93 | 8 | 85 | 0 | 0 | 8 | 0 | 0 |
| **total** | **1249** | **244** | **1004** | **1** | **3** | **215** | **25** | **1** |

## 2. Upstream's 244 in-scope failures by class

Classes, by the message: **wrong value** = `expected:`/`actual:` or an assert failure (and not an error-expectation
mismatch); **legend-engine refusal** = its own "not supported" family (`No SQL translation exists`, `is not supported`,
`not managed yet`, `doesn't support`, `is supported only`, ... -- `EngineLaneTable.REFUSAL_WORDS`'s list); **database
error** = DuckDB's exception or SQL text (`java.sql.SQLException`, `Binder Error`, `Conversion Error`, ...); **other** =
anything else (legend-engine planner faults such as `Error dynamically evaluating value specification`, `Function does
not exist`, `Cannot cast a collection of size 0`, and the error-expectation mismatches: `No error was thrown`,
`Execution error message mismatch`). Order: error-mismatch to other first, then wrong value, database error, refusal.
(The engine lane's own classes, pinned in `pct/engine_lane/summary_duckdb.tsv`, split "other" further.)

| scope | wrong value | legend-engine refusal | database error | other | total |
|---|---:|---:|---:|---:|---:|
| Essential | 22 | 71 | 15 | 38 | 146 |
| Grammar | 5 | 26 | 6 | 7 | 44 |
| Relation | 1 | 1 | 0 | 0 | 2 |
| Standard | 14 | 2 | 7 | 21 | 44 |
| Unclassified | 5 | 0 | 3 | 0 | 8 |
| **total** | **47** | **100** | **31** | **66** | **244** |

Two examples of each:

- **wrong value**:
  - Essential `meta::pure::functions::collection::tests::indexof::testIndexOfOneElement`: `\nexpected: 0\nactual:   1`
  - Essential `meta::pure::functions::date::tests::testDateDiffWeeks`: `\nexpected: 1\nactual:   0`
- **legend-engine refusal**:
  - Essential `meta::pure::functions::collection::tests::add::testAddWithOffset`: `No SQL translation exists for the PURE function 'add_T_MANY__Integer_1__T_1__T_$1_MANY$_'`
  - Essential `meta::pure::functions::collection::tests::concatenate::testConcatenateMixedType`: `Any is not managed yet!`
- **database error**:
  - Essential `meta::pure::functions::collection::tests::contains::testContainsPrimitive`: `java.sql.SQLException: Conversion Error: Unimplemented type for cast (INTEGER -> DATE)\n\nLINE 1: select 1 in (1, 2, 5, 2, 'a', true, DATE '2014-02-01', 'c')\n               ^`
  - Essential `meta::pure::functions::date::tests::testAdjustByDaysBigNumber`: `java.sql.SQLException: Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
- **other**:
  - Essential `meta::pure::functions::collection::tests::exists::testExists`: `Cannot cast a collection of size 0 to multiplicity [1]`
  - Essential `meta::pure::functions::collection::tests::find::testFindLiteralFromVar`: `Function does not exist 'meta::pure::functions::collection::find(String[3],LambdaFunction<{String[1]->Boolean[1]}>[1])'`

## 3. Every test in one bucket

Per scope above. Bucket (3), split by upstream's class:

| scope | wrong value | legend-engine refusal | database error | other | total |
|---|---:|---:|---:|---:|---:|
| Essential | 22 | 67 | 7 | 30 | 126 |
| Grammar | 5 | 26 | 6 | 1 | 38 |
| Relation | 0 | 1 | 0 | 0 | 1 |
| Standard | 14 | 2 | 7 | 19 | 42 |
| Unclassified | 5 | 0 | 3 | 0 | 8 |
| **total** | **46** | **96** | **23** | **50** | **215** |

Bucket (2) and (2b) -- the four tests where lite fails the way upstream's manifest says (by exact, normalized and
upstream's contains rule; normalized = the `Assert failure at (...), ` frame and outer quotes removed, whitespace
collapsed, instance ids ignored, expected and actual compared):

- (2b) Essential `meta::pure::functions::collection::tests::slice::testSliceError` -- exact: False, upstream's own rule (lite's message contains the expectedError): True, normalized: False
  - upstream: `No error was thrown`
  - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "No error was thrown"\nwhere the expected message was:"The low bound (3) can't be higher than the high bound (2) in a slice operation""`
- (2b) Grammar `meta::pure::functions::collection::tests::range::testRangeStepError` -- exact: False, upstream's own rule (lite's message contains the expectedError): True, normalized: False
  - upstream: `No error was thrown`
  - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "No error was thrown"\nwhere the expected message was:"range step must not be 0""`
- (2) Relation `meta::pure::functions::relation::tests::composition::testVariantArrayColumn_joinStrings` -- exact: False, upstream's own rule (lite's message contains the expectedError): True, normalized: True
  - upstream: `\nexpected: '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',null\n#'`
  - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',\n#'\nactual:   '#TDS\n   id,payload,joined\n   1,\'[1,2,3]\',1,2,3\n   2,\'[4,5,6]\',4,5,6\n   3,\'[7,8,9]\',7,8,9\n   4,\'null\',null\n#'"`
- (2b) Standard `meta::pure::functions::math::tests::bitShiftRight::testBitShiftRight_MoreThan62Bits` -- exact: False, upstream's own rule (lite's message contains the expectedError): True, normalized: True
  - upstream: `"Execution error message mismatch.\nThe actual message was "No error was thrown"\nwhere the expected message was:"Unsupported number of bits to shift - max bits allowed is 62""`
  - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "No error was thrown"\nwhere the expected message was:"Unsupported number of bits to shift - max bits allowed is 62""`

No pair matches as exact text: lite's message always carries legend-pure's assert frame (`Assert failure at
(resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "..."`), which the manifest's text does not.
By upstream's own rule (contains) all four would hold as expected failures.

## 4. Bucket 5 -- upstream passes, lite fails (1 test)

- Essential `meta::pure::functions::math::tests::abs::testBigFloatAbs`: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: 123456789123456789.99\nactual:   123456789123456780.0"`

Recorded with a reason in lite's ledgers:

- `pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_EssentialFunctions_PCT.java:55` (channel A, legend-pure
  route, DuckDB): `one("meta::pure::functions::math::tests::abs::testBigFloatAbs_Function_1__Boolean_1_", "\"\nexpected:
  123456789123456789.99\nactual:   123456789123456780.0\"")`, its comment: "a Float literal's twenty digits: the
  interpreter holds a BigDecimal, lite a double (register S120, outcome)".
- `docs/SEMANTICS_REGISTER.md` row **S120** (outcome): "legend-pure's interpreter (the PCT's reference) holds a Float
  literal as a `BigDecimal` with every digit written ... compiled legend-pure and legend-engine hold a double | a Float is
  the database's double (binary64) on every database lite runs on ...".
- `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md:396-397`: "A Float value read from the database is a double, as in
  legend-engine (a Float literal is item 4). A 20-digit Float literal loses digits in both (only legend-pure's
  interpreter keeps them; S120)."
- `pct/engine_lane/duckdb_essential.tsv:186` pins it as `PASS ... FAIL WRONG_ANSWER LITE_BEHIND`.

Fact to note against those rows: legend-engine's own `relational-duckdb` EssentialFunctions manifest does **not** list
`testBigFloatAbs`, so by the manifest legend-engine passes it on DuckDB; the S120 row and the differences page say
legend-engine holds a double and loses the digits too. On channel B (lite runs the whole body,
`pct/src/test/resources/edge/channel_b_duckdb_essential_outcomes.tsv:184`) the test passes.

## 5. Bucket 4 -- both fail, differently (25 tests, all listed)

upstream class of the bucket-4 tests: wrong value 0, legend-engine refusal 4, database error 8, other 13

1. Essential `meta::pure::functions::collection::tests::at::testAtError` (upstream: legend-engine refusal)
   - upstream: `->at(...) function is supported only after direct access of 1->MANY properties`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 37"`
2. Essential `meta::pure::functions::collection::tests::find::testFindInstance` (upstream: other)
   - upstream: `Error dynamically evaluating value specification (from /platform/pure/essential/collection/iteration/find.pure`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_0030hgh instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0031acq instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
3. Essential `meta::pure::functions::collection::tests::find::testFindUsingVarForFunction` (upstream: other)
   - upstream: `Error dynamically evaluating value specification (from /platform/pure/essential/collection/iteration/find.pure`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_00328lg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_0033044 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
4. Essential `meta::pure::functions::collection::tests::head::testHeadComplex` (upstream: other)
   - upstream: `Cannot cast a collection of size 0 to multiplicity [1]`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_003a8fg instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_003amim instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
5. Essential `meta::pure::functions::date::tests::testAdjustByDaysBigNumber` (upstream: database error) -- the same DuckDB error; only legend-engine's `java.sql.SQLException: ` prefix differs
   - upstream: `java.sql.SQLException: Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
   - lite: `Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
6. Essential `meta::pure::functions::date::tests::testAdjustByHoursBigNumber` (upstream: database error) -- the same DuckDB error; only legend-engine's `java.sql.SQLException: ` prefix differs
   - upstream: `java.sql.SQLException: Out of Range Error: Interval value 12345678912 hours out of range`
   - lite: `Out of Range Error: Interval value 12345678912 hours out of range`
7. Essential `meta::pure::functions::date::tests::testAdjustByMonthsBigNumber` (upstream: database error) -- the same DuckDB error; only legend-engine's `java.sql.SQLException: ` prefix differs
   - upstream: `java.sql.SQLException: Invalid Input Error: Type INT64 with value 9600000000 can't be cast because the value is out of range for the destination type INT32`
   - lite: `Invalid Input Error: Type INT64 with value 9600000000 can't be cast because the value is out of range for the destination type INT32`
8. Essential `meta::pure::functions::date::tests::testAdjustByWeeksBigNumber` (upstream: database error) -- the same DuckDB error; only legend-engine's `java.sql.SQLException: ` prefix differs
   - upstream: `java.sql.SQLException: Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
   - lite: `Invalid Input Error: Type INT64 with value 12345678912 can't be cast because the value is out of range for the destination type INT32`
9. Essential `meta::pure::functions::date::tests::testDayOfMonthError` (upstream: database error)
   - upstream: `DuckDB doesn't support YEAR and YEAR-MONTH`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
10. Essential `meta::pure::functions::date::tests::testHourError` (upstream: database error)
   - upstream: `DuckDB doesn't support YEAR and YEAR-MONTH`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
11. Essential `meta::pure::functions::date::tests::testMinuteError` (upstream: database error)
   - upstream: `DuckDB doesn't support YEAR and YEAR-MONTH`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
12. Essential `meta::pure::functions::date::tests::testNewDateError` (upstream: legend-engine refusal)
   - upstream: `No SQL translation exists for the PURE function 'date_Integer_1__Integer_1__Date_1_'`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 29"`
13. Essential `meta::pure::functions::date::tests::testSecondError` (upstream: database error)
   - upstream: `DuckDB doesn't support YEAR and YEAR-MONTH`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error column mismatch. Actual: 23 where expected: 36"`
14. Essential `meta::pure::functions::lang::tests::match::testMatchWithMixedReturnType` (upstream: legend-engine refusal)
   - upstream: `type not supported: meta::pure::functions::lang::tests::model::LA_GeographicEntityType`
   - lite: `scalar lowering not yet implemented for TypedDeactivate`
15. Essential `meta::pure::functions::math::tests::rem::testRemError` (upstream: other)
   - upstream: `No error was thrown`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "Cannot cast a collection of size 0 to multiplicity [1]"\nwhere the expected message was:"Cannot divide 5 by zero""`
16. Essential `meta::pure::functions::math::tests::testSquareRootError` (upstream: other)
   - upstream: `Unexpected error executing function with params [Anonymous_Lambda]`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "Out of Range Error: cannot take square root of a negative number"\nwhere the expected message was:"Unable to compute sqrt of -1.0""`
17. Essential `meta::pure::functions::math::tests::trigonometry::testArcCosineError` (upstream: other)
   - upstream: `Unexpected error executing function with params [Anonymous_Lambda]`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "the database answered NaN for a Float, which has no NaN in Pure"\nwhere the expected message was:"Unable to compute acos of 2.0""`
18. Essential `meta::pure::functions::math::tests::trigonometry::testArcSineError` (upstream: other)
   - upstream: `Unexpected error executing function with params [Anonymous_Lambda]`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "the database answered NaN for a Float, which has no NaN in Pure"\nwhere the expected message was:"Unable to compute asin of 2.0""`
19. Essential `meta::pure::functions::string::tests::toString::testComplexClassToString` (upstream: legend-engine refusal)
   - upstream: `type not supported: meta::pure::functions::string::tests::toString::ErrorType`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: '// Warning: Good for gin -- Sad times no tonic'\nactual:   '@_o1'"`
20. Grammar `meta::pure::functions::collection::tests::filter::testFilterInstance` (upstream: other)
   - upstream: `Error dynamically evaluating value specification (from /platform/pure/grammar/functions/collection/iteration/filter.pure:49cc46-50); error compiling generated Java code:`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ebqb instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004ebr2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>]\nactual:   [<@_004f4e2 instanceOf meta::pure::functions::collection::tests::model::CO_Person>, <@_004f4e4 instanceOf meta::pure::functions::collection::tests::model::CO_Person>]"`
21. Grammar `meta::pure::functions::collection::tests::first::testFirstComplex` (upstream: other)
   - upstream: `Expected at most one object, but found many`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004gc0a instanceOf meta::pure::functions::collection::tests::model::CO_Person>\nactual:   <@_004gq34 instanceOf meta::pure::functions::collection::tests::model::CO_Person>"`
22. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToMany` (upstream: other)
   - upstream: `Error dynamically evaluating value specification (from /platform/pure/grammar/functions/collection/iteration/map.pure`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004idqv instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idrq instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004idsd instanceOf meta::pure::functions::collection::tests::map::model::M_Location>]\nactual:   [<@_004ikj3 instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004ikj5 instanceOf meta::pure::functions::collection::tests::map::model::M_Location>, <@_004ikj7 instanceOf meta::pure::funct`
23. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromManyToOne` (upstream: other)
   - upstream: `Error dynamically evaluating value specification (from /platform/pure/grammar/functions/collection/iteration/map.pure`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: [<@_004ikrp instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004iksk instanceOf meta::pure::functions::collection::tests::map::model::M_Address>]\nactual:   [<@_004ir13 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>, <@_004ir15 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>]"`
24. Grammar `meta::pure::functions::collection::tests::map::testMapRelationshipFromOneToOne` (upstream: other)
   - upstream: `Error during dynamic reactivation: Error dynamically evaluating value specification (from /platform/pure/grammar/functions/collection/iteration/map.pure`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: <@_004ir81 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>\nactual:   <@_004j265 instanceOf meta::pure::functions::collection::tests::map::model::M_Address>"`
25. Standard `meta::pure::functions::math::tests::bitShiftLeft::testBitShiftLeft_MoreThan62Bits` (upstream: other)
   - upstream: `"Execution error message mismatch.\nThe actual message was "Unexpected error executing function with params [Anonymous_Lambda]"\nwhere the expected message was:"Unsupported number of bits to shift - max bits allowed is 62""`
   - lite: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "Execution error message mismatch.\nThe actual message was "Out of Range Error: Overflow in left shift (1 << 63)"\nwhere the expected message was:"Unsupported number of bits to shift - max bits allowed is 62""`

(`\n` in the messages above stands for a newline; the TDS texts also hold literal `\n` sequences, as Pure prints them.)

## Cross-check

The table the lane's own tool builds (`bazel-bin/pct/engine_lane_table/summary_duckdb.tsv`) equals the committed pin
`pct/engine_lane/summary_duckdb.tsv` byte for byte (verdicts there: SAME 1,004; LITE_ANSWERS 102; LITE_FIXES 113;
BOTH_FAIL 29; LITE_BEHIND 1 -- the same partition as buckets 1, 3, 2+2b+4 and 5 above).
