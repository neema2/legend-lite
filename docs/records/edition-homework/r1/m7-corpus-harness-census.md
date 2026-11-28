# M7: corpus harness constructs whose meaning differs between legend-pure and legend-engine (typed census)

Measured 2026-11-25 in `/home/user/s2` at `da231244` (nothing committed; the probe reverted). Requested by the
coordinator after M3.

## Question

In each corpus test, OUTSIDE the argument of `execute()` (the harness: lets, helper calls, assert arguments), which
tests use one of these, judged by the operand's TYPE:

- text of a Float, DateTime, StrictDate or Date: `toString`, `format`, `makeString` / `joinStrings` over such values, string
  concatenation with them (over String and Integer, `toString` and `makeString` do not count);
- a comparison across number kinds (Integer vs Float vs Decimal);
- `substring`, `indexOf`, `toOne` of a possibly-empty value;
- `parseInteger` / `parseFloat` / `parseDecimal` / `parseDate`;
- `isNumeric`;

and how many reach a verdict through such a value.

## Method (a typed probe, logging only)

Modelled on the s1 crossing probe (`/home/user/s1/docs/records/edition-homework/probe-crossings-outside-the-query.patch.txt`,
read only): instead of hooking the typer, the probe types each corpus test's body exactly as the corpus runner does
(`Compiler.resolveQuery` then `Compiler.queryResolved`, before any door's rewrite), walks the typed tree and prints one
line per finding; under `-Dprobe.m7.only` (set on the two corpus lanes only) the runner then skips the test, so nothing
runs on a database.

- **What is the query (excluded):** the first argument of `execute`, `executeInternal`, `executionPlan`,
  `executeLegendQuery`, `toSQLString`, `toSQLStringPretty`, `toSQL`, `generatePlan`, `executeInDb`; a `let` whose
  variable is handed to one of those as its query (`let q = {|...}; execute($q, ...)`: 397 lets in 349 tests); a helper
  parameter that the helper hands to one of those as its query (skipped at its 16 call sites); and every `TypedFrom` (an
  executed query spliced into a read, e.g. `$result.values`). Findings inside the other arguments of a query call
  (mapping, runtime, extensions) are counted apart and kept out of the totals.
- **Helpers:** each user function the harness calls is typed (`SpecCompiler.compile`, memoized) and walked, up to depth
  6; findings there count for the calling test. Platform natives are not walked into.
- **Types:** from each node's `ExprType`; for a literal collection, each element's type.
- **Reaching a verdict:** a finding reaches a verdict when it lies in an argument of an `assert*` call, in a `let` whose
  variable (transitively) reaches one, in the value a helper returns into either, or in a helper that itself asserts
  over it (data-flow over lets and helper returns; an over-approximation inside a helper, which is taken whole).
- **Tests censused:** the corpus's 2,613 tests minus the 7 each lane's committed skipped roster names (the same 7 on both
  lanes) = 2,606, of which 2,584 typed; 22 fail to type in lite (listed in `m7_tables.txt`; e.g. unknown functions
  `routeFunction`, `meta::legend::compileLegendGrammar`). The census is the same on both lanes: run on DuckDB and H2 with
  the earlier probe version, the two lanes' probe lines were identical (typing happens before any database), so the final
  version ran on the DuckDB lane.

Probe: `/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad/r1/probes/M7Probe.java.txt`
(new file `core/src/main/java/com/legend/test/M7Probe.java`) and `probes/m7-harness-census.patch.txt` (the runner's hook,
`//core:test`'s one extra dependency `:compiler_element_type`, the lanes' `-Dprobe.m7.only=true`).

```
cd /home/user/s2
python3 <scratchpad>/r1/probes/apply_m7.py /home/user/s2 <scratchpad>/r1/probes/M7Probe.java.txt
# plus core/BUILD.bazel: //core:test deps += ":compiler_element_type"
bazel build --lockfile_mode=error --keep_going --jobs=3 --noremote_upload_local_results //spec:judge_host_duckdb   # (and //spec:judge_host_h2)
python3 <scratchpad>/r1/probes/m7b.py bazel-bin/spec/judge_host_duckdb/host.log \
    spec/src/test/resources/rcorpus/duckdb-skipped-roster.txt spec/src/test/resources/rcorpus/h2-skipped-roster.txt
git checkout -- core/BUILD.bazel core/src/main/java/com/legend/test/PureTestRunner.java spec/BUILD.bazel
rm core/src/main/java/com/legend/test/M7Probe.java
```

The log is kept at `<scratchpad>/r1/data/m7v5_duckdb/host.log` (`<scratchpad>` =
`/tmp/claude-0/-home-user-legend-lite/e82d575c-3b18-52af-b239-5bdd496a53f4/scratchpad`); full tables at
`<scratchpad>/r1/m7_tables.txt`.

## Counts (2,584 tests the DuckDB and H2 lanes run; the two lanes run the same tests)

| construct | tests | of which reaching a verdict | in the test body | only in helpers | sites |
|---|---:|---:|---:|---:|---:|
| text of a Float / DateTime / StrictDate / Date | 77 | 77 | 76 | 1 | 123 |
| -- `toString` | 51 | 51 | | | 97 |
| -- `makeString` | 25 | 25 | | | 25 |
| -- `format` | 1 | 1 | | | 1 |
| -- `joinStrings`, string concatenation | 0 | 0 | | | 0 |
| comparison across number kinds | 2 | 1 | 2 | 0 | 2 |
| `substring` | 4 | 3 | 0 | 4 | 7 |
| `indexOf` (String) | 7 | 7 | 5 | 2 | 17 |
| `toOne` of a possibly-empty value | 382 | 337 | 195 | 187 | 1,145 |
| `parseInteger` / `parseFloat` / `parseDecimal` / `parseDate` | 0 | 0 | 0 | 0 | 0 |
| `isNumeric` | 0 | 0 | 0 | 0 | 0 |
| **any of them** | **418** | **372** | 231 | 187 | 1,294 |
| any of them except `toOne` | 87 | 85 | | | |

Five more tests have a `toOne` only inside a query call's mapping/runtime/extension argument (not counted above).

Operand types:
- text: `toString` of a StrictDate 46 tests, of a Float 4, of a Date 1; `makeString` over Floats 15, over Dates and
  Floats 6, over StrictDates 4; `format` of a Date 1.
- comparison across number kinds: both Float vs Integer. One is a real harness comparison (`zScoreTest`, an in-memory
  TDS filter, reaching a verdict); the other (`testLessThanFilterAsVariable`) is a query lambda held in a `let` that the
  query reads by capture (`let filter = (t|$t.quantity < 45); execute(|Trade.all()->filter($filter)...)`), which the
  probe did not recognise as the query's -- so the true harness count is 1.
- `toOne` operands: a class `[0..*]` 230 tests, a class `[0..1]` 133, String `[0..1]` 74, Boolean `[0..1]` 57, StrictDate
  `[0..1]` 46, enumeration `[0..1]` 14, Integer `[0..1]` 7, others under 10. In the test body 195 tests; in helpers mostly
  setup and SQL-generator library code: `Runtime$prop$connectionByElement` (115 tests, the setups' connection lookup),
  `testDataGeneration::getTable` (56), `sqlQueryToString::createDbConfig` (54), `loadDbExtension` (53),
  `meta::lite::lineage::relationTreeLines` (49).

## Five examples of each (with the upstream source line where found)

#### `text:toString`
- `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testAnnualizedDateRange` -- operand `STRICT_DATE[1..1]` -- the test body, line 256, col 94 -- **reaches a verdict**
  `let dateRange = [$result.values.rows->map(x|$x.values->cast(@StrictDate))->min()->toOne()->toString(),` (testCalendarFunctions.pure)
- `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testCmeDateRange` -- operand `STRICT_DATE[1..1]` -- the test body, line 274, col 94 -- **reaches a verdict**
  `let dateRange = [$result.values.rows->map(x|$x.values->cast(@StrictDate))->min()->toOne()->toString(),` (testCalendarFunctions.pure)
- `meta::relational::tests::functions::pureToSqlQuery::calendarAggregations::testCwDateRange` -- operand `STRICT_DATE[1..1]` -- the test body, line 292, col 94 -- **reaches a verdict**
  `let dateRange = [$result.values.rows->map(x|$x.values->cast(@StrictDate))->min()->toOne()->toString(),` (testCalendarFunctions.pure)
- `meta::relational::tests::projection::aggregation::testDeepSubAggregation` -- operand `FLOAT[1..1]` -- the test body, line 80, col 111 -- **reaches a verdict**
  `assertSameElements(['19.75 4', '32.0 1', '34.0 1', '35.0 1'], $result.values.rows->map(r|$r.getFloat('a')->toString()+' '+$r.getInteger('b')->toString()));` (testAggregation.pure)
- `meta::relational::tests::projection::aggregation::testSubAggregationWithIf` -- operand `FLOAT[1..1]` -- the test body, line 88, col 150 -- **reaches a verdict**
  `assertSameElements(['Firm A 1.0', 'Firm B 16.0', 'Firm C 1.0', 'Firm X 2.25'], $result.values.rows->map(r|$r.getString('a')+' '+$r.getFloat('b')->toString()));` (testAggregation.pure)
- `meta::relational::tests::projection::aggregation::testDateAggregationWithMax` -- operand `DATE[1..1]` (as lite types `getStrictDate`) -- the test body, line 187, col 199 -- **reaches a verdict**
  `assertSameElements(['FirmA->2013-12-04', ...], $result.values.rows->map(r | $r.getString('legalName') + '->' + $r.getStrictDate('birthDateOfOldestEmployee')->toString()));` (testAggregation.pure)

#### `text:makeString`
- `meta::pure::tds::tests::extensions::columnValueDifferenceTest` -- operand `DATE+FLOAT` -- the test body, line 153, col 48 -- **reaches a verdict**
  `$relationalResult.rows->map(r|$r.values->makeString('|'))->joinStrings(';'));` (testTdsExtension.pure)
- `meta::pure::tds::tests::extensions::columnValueDifferenceWithoutPrevalTest` -- operand `DATE+FLOAT` -- the test body, line 226, col 48 -- **reaches a verdict**
  `$relationalResult.rows->map(r|$r.values->makeString('|'))->joinStrings(';'));` (testTdsExtension.pure)
- `meta::pure::tds::tests::extensions::rowValueDifferenceTest` -- operand `DATE+FLOAT` -- the test body, line 486, col 48 -- **reaches a verdict**
  `$relationalResult.rows->map(r|$r.values->makeString('|'))->makeString(','));` (testTdsExtension.pure)
- `meta::relational::tests::groupBy::testGroupByWithWavgAggregation` -- operand `DATE+FLOAT` -- the test body, line 1292, col 167 -- **reaches a verdict**
  `assertSameElements(['2014-12-02,3,138.8', '2014-12-03,2,27.5', '2014-12-04,2,33.8', '2014-12-05,2,33.49999999999999'],$result.values->at(0).rows->map(r| $r.values->makeString(',')));` (testModelGroupBy.pure)
- `meta::relational::tests::mapping::relation::aggregation::testSubAggregationOnRelationMapping` -- operand `FLOAT` -- the test body, line 37, col 69 -- **reaches a verdict**
  `assertSameElements($expectedRows, $result.rows->map(r|$r.values->makeString(' | ')));` (testRelationFunctionAggregation.pure)

#### `text:format`
- `meta::relational::functions::sqlQueryToString::default::testProcessIdentifierWithQuoteChar` -- operand `DATE` -- helper `meta::relational::functions::sqlQueryToString::default::convertDateToSqlString`, line 151, col 9 -- **reaches a verdict**
  `| format('%t{[' + $timeZone + ']yyyy-MM-dd?[ HH:mm:ss?[.S*]]}', $date),` (extensionDefaults.pure)

#### `cmp:number-kinds`
- `meta::pure::tds::tests::extensions::zScoreTest` -- operand `>= FLOAT vs INTEGER (marked)` -- the test body, line 53, col 73 -- **reaches a verdict**
  `let summary = $result->filter(r:TDSRow[1]|abs($r.getFloat('zscore')) >= 3)->sort(asc('score'));` (testTdsExtension.pure)
- `meta::relational::tests::projection::filter::lessThan::testLessThanFilterAsVariable` -- operand `< FLOAT vs INTEGER (marked)` -- the test body, line 63, col 41 -- no verdict reached
  `let filter = (t:Trade[1]|$t.quantity < 45);` (testFilters.pure)

#### `str:substring`
- `meta::relational::functions::sqlQueryToString::default::testProcessIdentifierWithQuoteChar` -- operand `3 args[1..1]` -- helper `meta::relational::functions::sqlQueryToString::processExtractFromSemiStructured`, line 1073, col 52 -- **reaches a verdict**
  `let elementType = if($p2->endsWith('[]'), | $p2->substring(0, $p2->length() - 2), | $p2);` (dbExtension.pure)
- `meta::relational::functions::toPostgresModel::tests::testConvertLiteral` -- operand `3 args[1..1]` -- helper `meta::relational::functions::sqlQueryToString::parseSemiStructuredPathNavigation`, line 1015, col 28 -- **reaches a verdict**
- `meta::relational::tests::milestoning::applyMilestoningFilters::testMilestoningFilterApplicationOnSemiStructuredRelationalOperationElements` -- operand `2 args[1..1]` -- helper `meta::relational::functions::pureToSqlQuery::buildUniqueName`, line 8280, col 106 -- no verdict reached
  `if($uniqueName->length() > $maxSize, |$uniqueName->substring(0, $maxSize - 35) + '___' + $uniqueName->substring($maxSize - 35)->meta::pure::functions::hash::hash(meta::pure::functions::hash::HashType.` (pureToSQLQuery.pure)
- `meta::relational::functions::toPostgresModel::tests::testConvertSelectSQLQueryWithCTE` -- operand `3 args[1..1]` -- helper `meta::relational::functions::sqlQueryToString::parseSemiStructuredPathNavigation`, line 1015, col 28 -- **reaches a verdict**

#### `str:indexOf`
- `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeConstantParameterESTTimeZone` -- operand `2 args[1..1]` -- the test body, line 956, col 26 -- **reaches a verdict**
  `assert($planAsString->indexOf('settlementDateTime = \'2019-04-08 13:00:00\'') != -1 || $planAsString->indexOf('settlementDateTime = TIMESTAMP\'2019-04-08 13:00:00\'') != -1);` (executionPlanTest.pure)
- `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeConstantParameterGMTTimeZone` -- operand `2 args[1..1]` -- the test body, line 946, col 26 -- **reaches a verdict**
  `assert($planAsString->indexOf('settlementDateTime = \'2019-04-08 18:00:00\'') != -1 || $planAsString->indexOf('settlementDateTime = TIMESTAMP\'2019-04-08 18:00:00\'') != -1);` (executionPlanTest.pure)
- `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeConstantParameterNoTimeZone` -- operand `2 args[1..1]` -- the test body, line 936, col 26 -- **reaches a verdict**
  `assert($planAsString->indexOf('settlementDateTime = \'2019-04-08 18:00:00\'') != -1 || $planAsString->indexOf('settlementDateTime = TIMESTAMP\'2019-04-08 18:00:00\'') != -1);` (executionPlanTest.pure)
- `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeVariableESTTimeZone` -- operand `2 args[1..1]` -- the test body, line 982, col 26 -- **reaches a verdict**
  `assert($planAsString->indexOf('settlementDateTime = \'${GMTtoTZ( "[US/Arizona]" dt)}\'') != -1 || 'settlementDateTime = TIMESTAMP\'${GMTtoTZ( "[US/Arizona]" dt)}\'' != -1);` (executionPlanTest.pure)
- `meta::pure::executionPlan::tests::datetime::testPlanForDateTimeVariableNoTimeZone` -- operand `2 args[1..1]` -- the test body, line 972, col 26 -- **reaches a verdict**
  `assert($planAsString->indexOf('settlementDateTime = \'${dt}\'') != -1|| $planAsString->indexOf('settlementDateTime = TIMESTAMP\'${dt}\'') != -1);` (executionPlanTest.pure)

#### `toOne:possibly-empty`
- `meta::pure::executionPlan::m2m2r::tests::executeProjectWithNestedDerivedProperty` -- operand `GenericType[0..*]` -- the test body, line 241, col 133 -- **reaches a verdict**
  `$plan->meta::pure::executionPlan::execute([], meta::relational::extension::relationalExtensions()).values->cast(@TabularDataSet)->toOne()->meta::json::tdsToJSONKeyValueObjectString()->makeString();` (m2m2rExecutionPlanTests.pure)
- `meta::pure::executionPlan::tests::execution::testPureExecutionStrategyForCreateAndPopulateTempTableExecutionNode` -- operand `ClassType[0..1]` -- the test body, line 150, col 27 -- no verdict reached
  `let t = $service.test->toOne();` (executionPlanExecutionTest.pure)
- `meta::pure::alloy::connections::tests::relationalMapper::testRelationalDatabaseMapper` -- operand `ClassType[0..1]` -- helper `meta::pure::alloy::connections::tests::relationalMapper::databaseMappers`, line 21, col 92 -- **reaches a verdict**
  `schema(meta::relational::tests::dbInc, 'productSchema')->toOne(),` (testRelationalMapper.pure)
- `meta::pure::alloy::connections::tests::relationalMapper::testRelationalDatabaseSchemaMapper` -- operand `ClassType[0..*]` -- helper `meta::pure::alloy::connections::tests::relationalMapper::relationalMapperSqlQuery`, line 77, col 114 -- **reaches a verdict**
  `$result.rootExecutionNode.executionNodes->filter(p|$p->instanceOf(SQLExecutionNode))->cast(@SQLExecutionNode)->toOne().sqlQuery;` (testRelationalMapper.pure)
- `meta::pure::alloy::connections::tests::relationalMapper::testRelationalDatabaseSchemaTableMapper` -- operand `STRING[0..1]` -- helper `meta::relational::functions::toDDL::getTable`, line 225, col 105 -- no verdict reached
  `if ($schema->isEmpty(), | fail('No schema found with name ' + $schemaName + ' in ' + $database.name->toOne()); [];, | []);` (toDDL.pure)

## How many reach a verdict through such a harness value

**372 of the 418 tests** (85 of the 87 without `toOne`): every text-of-Float/Date test (77; e.g. a `toString` of a
StrictDate compared with `assertEquals(['2022-01-01','2022-12-01'], $dateRange)`, a row's `makeString` over Date and
Float cells compared by `assertSameElements`), all 7 `indexOf` tests (`assert($planAsString->indexOf(...) != -1 ...)`),
3 of 4 `substring` tests, 1 of the 2 number-kind comparisons, 337 of 382 `toOne` tests.

## Informational (not counted): text of other operand types, and printed tables

- Tests with any `toString` / `format` / `makeString` / `joinStrings` / `toCSV` in the harness, any operand type: **876**
  (the rough textual count was 626). By operand: `makeString` over Strings 256 tests, over Integers and Strings 103;
  `joinStrings` over Strings 120; `toString` of an enumeration value (`DatabaseType`) 56, of an Integer 49, of a String 31;
  `format` of Integers 24.
- A printed table -- `toString` / `toCSV` / `makeString` of a TDS or Relation -- is **302 tests** (299 reaching a
  verdict); in **25** of them (24 reaching a verdict) the table has a Float, Date, DateTime, StrictDate or Decimal
  column, so its text holds such values (read from the relation type's column types; a column whose type the typer
  leaves open is not seen).
- No `parse*` or `isNumeric` call occurs in the harness of any test the lanes run.
