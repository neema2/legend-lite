| scope | tests lite ran | upstream manifest rows | (1) both pass | (2) same wrong value | (2b) same failure, not a value | (3) upstream fails, lite passes | (4) both fail, differently | (5) upstream passes, lite fails |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Essential | 345 | 146 | 198 | 0 | 1 | 126 | 19 | 1 |
| Grammar | 137 | 44 | 93 | 0 | 1 | 38 | 5 | 0 |
| Relation | 469 | 2 | 467 | 1 | 0 | 1 | 0 | 0 |
| Standard | 205 | 44 | 161 | 0 | 1 | 42 | 1 | 0 |
| Unclassified | 93 | 8 | 85 | 0 | 0 | 8 | 0 | 0 |
| **total** | **1249** | **244** | **1004** | **1** | **3** | **215** | **25** | **1** |

UPSTREAM_CLASS_TABLE
| scope | wrong value | legend-engine refusal | database error | other | total |
|---|---:|---:|---:|---:|---:|
| Essential | 22 | 71 | 15 | 38 | 146 |
| Grammar | 5 | 26 | 6 | 7 | 44 |
| Relation | 1 | 1 | 0 | 0 | 2 |
| Standard | 14 | 2 | 7 | 21 | 44 |
| Unclassified | 5 | 0 | 3 | 0 | 8 |
| **total** | **47** | **100** | **31** | **66** | **244** |

EXAMPLES
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

BUCKET3
| scope | wrong value | legend-engine refusal | database error | other | total |
|---|---:|---:|---:|---:|---:|
| Essential | 22 | 67 | 7 | 30 | 126 |
| Grammar | 5 | 26 | 6 | 1 | 38 |
| Relation | 0 | 1 | 0 | 0 | 1 |
| Standard | 14 | 2 | 7 | 19 | 42 |
| Unclassified | 5 | 0 | 3 | 0 | 8 |
| **total** | **46** | **96** | **23** | **50** | **215** |

BUCKET2
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

BUCKET4
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

BUCKET5
- Essential `meta::pure::functions::math::tests::abs::testBigFloatAbs`: `Assert failure at (resource:/platform/pure/essential/tests/assert.pure line:21 column:5), "\nexpected: 123456789123456789.99\nactual:   123456789123456780.0"`
