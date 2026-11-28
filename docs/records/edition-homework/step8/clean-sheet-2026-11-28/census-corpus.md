*A research or audit subagent's report to the step 8 session, 2026-11-28, kept as it was written (read-only work; its scratch files were in the session's container and are not in the repository).*

# Where corpus tests print values as text, and what else they do with the printed value

Corpus tests print mostly straight into an assert (C1) or once per row inside `->map(r|...)` (C3); when the printed thing is an `execute(...)` result, three quarters of the time the same variable is also asserted on for its SQL text or its size.

Everything below comes from the script's output over legend-engine 4.145.0 `core_relational/relational/**.pure`. Paths are relative to that directory:
`/tmp/claude-0/-home-user-legend-lite/a081db72-2095-5e3c-b0e3-e111003021e6/scratchpad/upstream/legend-engine-legend-engine-4.145.0/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/`

## 0. Scope and roster mapping
- **Analysed:** 2761 `<<test.Test>>` functions, 0 parse errors, 0 unbalanced bodies. This equals `corpus.census.declared 2761` in `/home/user/legend-lite/spec/src/test/resources/com/legend/generators/ratchets.tsv`.
- **Lite's corpus:** 2613 runnable tests after dropping 148 marked `ToFix`/`ExcludeAlloy` (the rule in `MinimalCorpusTest.scanCensus`). This equals `corpus.census.discovered 2613`.
- **Roster:** every name in the six fail/accepted/skipped roster files maps to a runnable test (DuckDB fail 277/277, H2 fail 422/422, none unmapped). The rosters list only failing, accepted and skipped tests, so "in lite's corpus" means the 2613.
- **Numbers below are for the 2613**, with all-2761 figures in brackets where they differ notably.
- **Print sets:**
  - core: `toCSV`, `.csv`, `toString`, `makeString`, `joinStrings`, `format`. `toRepresentation` and `toCSVString` have 0 calls in test bodies.
  - plus: a `+` expression with a string-literal or print operand and at least one non-literal operand.
  - extra (not in your list, kept separate): `serialize`, `planToString`, `toJSON`, `alloyToJSON`, `toJSONStringStream`.
- **Cross-check:** a plain regex over the same bodies finds 1253 core print calls; the parser also finds 1253, plus the one `.csv`.

## 1. Counts by position (runnable 2613)
| Position | core sites | core tests | `+` sites / tests | extra sites / tests |
|---|---|---|---|---|
| C1 direct assert argument | 624 | 463 | 0 / 0 | 56 / 55 (`planToString` 55, `toJSON` 1) |
| C2 `let` then assert | 5 | 4 | 0 / 0 | 49 / 41 (`planToString` 48, `alloyToJSON` 1) |
| C3 inside per-element lambda | 401 | 357 | 115 / 111 | 0 |
| C4 inside the query sent to the store | 47 | 37 | 34 / 24 | 181 / 177 (graphFetch `serialize`) |
| C5 other | 122 | 56 | 34 / 33 | 5 / 3 |
| Total | 1199 | 798 | 183 / 164 | 291 / 274 |

- **Core and plus together:** 1382 sites in 876 tests; 1737 runnable tests have no print site. Tests per position: C1 463, C2 4, C3 406, C4 60, C5 86.
- **All 2761:** core 1254 sites in 843 tests (C1 640/478, C2 16/15, C3 423/378, C4 51/41, C5 124/57); plus 192/173; extra 308/287.
- **Position combinations per test** (core and plus): C1 only 353, C3 only 274, C1+C3 99, C5 only 77, C4 only 36, C3+C4 15, C1+C3+C4 7, C3+C5 7, others 2 each or fewer.
- **Printing tests on the fail rosters:** DuckDB 37 (C1 23, C4 7, C5 6, C3 5); H2 91 (C5 57, C1 22, C3 22, C4 8, C2 1).

### Per print function (core, runnable; sites)
| Function | C1 | C2 | C3 | C4 | C5 | sites | tests |
|---|---|---|---|---|---|---|---|
| `makeString` | 170 | 2 | 309 | 1 | 5 | 487 | 366 |
| `toString` | 256 | 2 | 89 | 2 | 92 | 441 | 242 |
| `toCSV` (the `meta::relational::tests::csv` helper) | 183 | 0 | 0 | 0 | 13 | 196 | 170 |
| `joinStrings` | 14 | 1 | 3 | 43 | 12 | 73 | 52 |
| `.csv` (relation property) | 1 | 0 | 0 | 0 | 0 | 1 | 1 |
| `format` | 0 | 0 | 0 | 1 | 0 | 1 | 1 |

### What is printed (core, runnable; read from the receiver's syntax)
| Printed thing | C1 | C2 | C3 | C4 | C5 |
|---|---|---|---|---|---|
| all cells of one row (`$r.values->makeString`) | 0 | 0 | 300 | 0 | 0 |
| whole `.values` of an execute result (TDS, relation, instances) | 298 | 1 | 0 | 0 | 0 |
| execute result rows, mapped or joined (outer `makeString` over `rows->map(...)`) | 139 | 0 | 15 | 0 | 3 |
| `#TDS ... #` literal on the expected side (`->toString()`) | 127 | 0 | 0 | 0 | 0 |
| Date cell (`getStrictDate`, `cast(@StrictDate)`) | 0 | 0 | 1 | 0 | 92 |
| untyped cell (`$r.get('x')->toString()`) | 0 | 0 | 42 | 0 | 0 |
| Integer cell (`getInteger`) | 3 | 0 | 21 | 0 | 0 |
| String cell (`getString`) | 10 | 2 | 11 | 0 | 0 |
| Float cell (`getFloat`) | 0 | 0 | 4 | 0 | 0 |
| class property or query variable inside a query | 0 | 0 | 0 | 32 | 0 |
| `validate(...)` result `.values` | 18 | 0 | 0 | 0 | 0 |
| other | 29 | 2 | 7 | 15 | 27 |

- No number or date literal is printed in any test body.
- **What a C1 print is compared with** (624 sites, counted from the normalised shapes):
  - 254 are the two sides of 127 `assertEquals(#TDS…#->toString(), $result.values->at(0)…->toString())`.
  - 235 are compared with a string literal or a list of them.
  - 98 are compared with a concatenation of string literals.
  - 28 are 14 asserts with a print on both sides (`toCSV` against `toCSV`).
  - 8 are compared with a non-literal, non-print expression.
- The only tail after a C1 print is `->replace(lit, lit)` (41 sites, all `toCSV($result.values)->replace('\n', ',')`); the other 583 have no tail.
- C1 asserts are `assertEquals` 604 and `assertSameElements` 19.

## 2. C1/C2 prints that come from `let x = execute(...)`
- **C1/C2 core sites:** 629. Of these, 474 derive from a let-bound `execute(...)` (C1 471, C2 3), in 440 tests. The rest are 127 `#TDS` literals, 18 `validate()`, and 10 other producers. No inline `execute(...).values->print()` exists.
- **By print function** (earlier run, before the `.csv` site was added): `makeString` 171, `toCSV` 164, `toString` 129, `joinStrings` 9.

| Is the execute variable used again? | sites | (test, variable) pairs | tests |
|---|---|---|---|
| no, only this one print | 117 | 114 | 104 (plus 7 tests with one variable of each kind) |
| yes, non-print uses | 347 | 352 | 329 |
| yes, non-print uses and another print | 10 | (inside the 352) | |

- The 116 "only this print" sites in the earlier run were `toString` 85 (relation tests), `toCSV` 23, `makeString` 7, `joinStrings` 1.
- **What the other uses are**, over 465 pairs (earlier run):
  - SQL text only: 210
  - SQL text and size: 122
  - nothing else: 113
  - size only: 7
  - SQL text, size and something else: 5
  - a second print with SQL text, size or other: 7
  - other only: 1

Top 10 other-use shapes (`$R` is the execute variable; occurrences / tests):
| occurrences | tests | shape |
|---|---|---|
| 176 | 171 | `assertEquals('…', $R->sqlRemoveFormatting())` |
| 78 | 74 | `assertSameSQL('…', $R)` |
| 60 | 55 | `assertSize($R.values.columns, n)` |
| 59 | 59 | `assertSize($R.values.rows, n)` |
| 44 | 44 | `assertEquals(…, $R->sql())` |
| 31 | 31 | `assertEqualsH2Compatible(…, $R->sqlRemoveFormatting())` |
| 5 | 4 | `assertSize($R.values, n)` |
| 5 | 5 | `assert($R->sqlRemoveFormatting()->toLower()->contains('…'))` |
| 5 | 4 | `assertFalse($R->sqlRemoveFormatting()->toLower()->contains('…'))` |
| 4 | 4 | `assertEquals(n, $R.values.rows->size())` |

- After those: `assert($R->sql()->contains(lit))` 4, `assertEquals(…, $R.values.columns.type)` 3, `println($R)` 2, `println($R.activities)` 2.
- **All execute-bound variables in runnable tests, any print position:** 2204 variables in 1979 tests.
  - 1399 are never printed in the test body.
  - 805 feed a print; 181 of those are used by prints only (135 by one print, 46 by two), and 624 also have non-print uses.
- **C3 only:** of 403 (test, execute variable) pairs printed per row, 382 also have non-print uses.

## 3. Examples

### C1: print is directly an assert argument
- `tests/mapping/distinct/testDistinct.pure:52`
  `assertEquals('name,code\n' + 'IF 1,1001\n' +  'IF 2,1002\n' + 'IF 2,1022\n', $result.values->toOne()->toCSV());`
- `calendarAggregation/tests/testCalendarFunctions.pure:827`
  `assertEquals(['hireType,annualized,Campus,9.812850678733032,Lateral,9.267692307692307,'], toCSV($result.values)->replace('\n', ','));`
- `tests/mapping/modelJoin/testModelJoinAdvanced.pure:90-96`
  `assertEquals(` / `#TDS` / `'Legal Name', 'Employee First Name'` / `Apple, Alice` / `Google, Bob` / `#->toString(),` / `$result.values->at(0)->sort([~'Legal Name'->ascending(), ~'Employee First Name'->ascending()])->toString());`
- `tds/tests/testTDSRestrictDistinct.pure:32-34`; the same `$res1` is used three times:
  `assertSize($res1.values.columns, 1);`
  `assertEquals('Anthony,David,Fabrice,John,Oliver,Peter', $res1.values.rows.values->sort()->makeString(','));`
  `assertEquals('select distinct "root".FIRSTNAME as "firstName" from personTable as "root"', $res1->sqlRemoveFormatting());`
- `postprocessor/cteExtractionPostProcessor.pure:152`
  `assertEquals($resultWithoutCTEPP.values->toCSV(), $result.values->toCSV());`
- `tests/mapping/enumeration/testEnumerationMapping.pure:162-166`
  `assertEquals('name, dateOfHire, type, active, firm, role\n'+ 'Alice,1983-03-15,CONTRACT,YES,FIRM_A,JUNIOR\n'+ ... , $result.values->cast(@meta::pure::metamodel::relation::TDS<Any>).csv);`

### C2: `let` then assert (5 core sites runnable, 16 in all 2761)
- `tests/advanced/testQueryStructure.pure:127-129`
  `assertSize($result.values.rows, 4);`
  `let res = $result.values.rows->map(a|$a.getString('name1')+' '+$a.getString('name2')+' '+$a.getString('name3')+' '+$a.getString('name4')+' '+$a.getString('name5'))->makeString(',');`
  `assertEquals('Peter Smith Smith Smith Smith,Peter Smith Smith Smith Hill,...', $res);`
- `functions/tests/testFetchDbMetaData.pure:176-177`
  `let primaryKeyInPersonTable = fetchDbPrimaryKeysMetaData($connection, [], 'FETCHDBMETADATAPERSONTABLE1').rows.values->at(3)->toOne()->toString();   //COLUMN_NAME`
  `assertEq($primaryKeyInPersonTable, 'ID');`
- `milestoning/tests/testGetAllForEachDate.pure:52-53`
  `let actualValue = $result.values->toCSV();` / `assertSameElements($expectedValue, $actualValue);`
  This file holds 11 such tests, all `<<test.Test, test.ExcludeAlloy>>`, so outside the 2613.
- `functions/tests/testObjectReferenceIn.pure:315`
  `let objReferences = $result.values->parseJSON()->cast(@JSONArray).values->map(je | ...decodeObjectReferencesAndGetPkMap('vX_X_X', ...))->j...` (ends in `joinStrings`, then asserted).
- extra: `executionPlan/tests/executionPlanTest.pure:187`
  `let result = $generatedPlan->planToString(meta::relational::extension::relationalExtensions());` then compared with `let expected = 'Sequence\n'+ ...`.

### C3: print inside a per-element lambda
All 516 runnable sites sit in a `map` lambda.
- `functions/tests/projection/testFilters.pure:81-83`
  `let tds = $result.values->at(0);`
  `assertEquals(['Firm X', 'Firm A', 'Firm B', 'Firm C'],$tds.rows->map(r|$r.values->makeString(',')));`
  `assertEquals('select case when false then concat(...', ...` (SQL text of the same `$result`).
- `tests/advanced/testForcedInjection.pure:38-39`
  `assertEquals(4, $tds.rows->size());`
  `assertSameElements(['Trade 1 prod 1 V2','Trade 2 prod 2 V1','Trade 3 prod 3 V1','Trade 4 prod 4 V2'], $tds.rows->map(r|$r.get('a')->toString()+' '+$r.get('b')->toString()));`
- `functions/tests/projection/testAggregation.pure:35`
  `assertSameElements(['19.75Firm X', '32.0Firm B', '34.0Firm A', '35.0Firm C'], $result.values.rows->map(r|$r.getFloat('a')->toString()+$r.getString('b')));`
- `functions/tests/projection/testAggregation.pure:187`
  `assertSameElements(['FirmA->2013-12-04', 'FirmB->2013-12-02', 'FirmC->2013-12-01'], $result.values.rows->map(r | $r.getString('legalName') + '->' + $r.getStrictDate('birthDateOfOldestEmployee')->toString()));`
- `tests/mapping/union/testSpecialUnionMapping.pure:77-78`
  `assertEquals(3, $res.values.rows->size());`
  `assertEquals('5,2|6,2|7,1', $res.values.rows->map(r | $r.values->map(x | $x->toString())->joinStrings(','))->joinStrings('|'));`
- `postprocessor/tests/testPostProcessor.pure:413`
  `assertSameElements(['Account 1,100.0', 'Account 2,350.0'], $result.values->map(p|[$p.account->toOne().name, $p.pnl]->makeString(',')));`

Top 10 exact C3 statement shapes (431 distinct statements; `$_` is any variable; `lit` is a literal or a list of literals):
| count | shape |
|---|---|
| 91 | `assertEquals(lit, $_.rows->map(x|$_.values->makeString(lit)))` |
| 55 | `assertSameElements(lit, $_.values.rows->map(x|$_.values->makeString(lit)))` |
| 52 | `assertEquals(lit, $_.values.rows->map(x|$_.values->makeString(lit))->makeString(lit))` |
| 23 | `assertEquals(lit, $_.rows->map(x|$_.values->makeString(lit))->makeString(lit))` |
| 18 | `assertSameElements(lit, $_.rows->map(x|$_.get(lit)->toString() + lit + $_.get(lit)->toString()))` |
| 17 | `assertEquals(lit, $_.values.rows->map(x|$_.values->makeString(lit)))` |
| 13 | `assertSameElements(lit, $_.rows->map(x|$_.getString(lit) + lit + $_.getString(lit)))` |
| 8 | `assertSameElements(lit, $_.rows->map(x|$_.getString(lit) + lit + $_.getInteger(lit)->toString()))` |
| 6 | `assertSameElements(lit, $_.values.rows->map(x|$_.values->makeString(lit))->makeString(lit))` |
| 6 | `assertSameElements(lit, $_.values->at(lit).rows->map(x|$_.values->makeString(lit)))` |

- Five more shapes also occur 6 times each, among them `assertEquals(lit, $_.rows->map(x|$_.values->makeString(lit))->sort())` and `println($_.columns->map(x|$_.name + lit + $_.type->toOne()->elementToPath())->joinStrings(lit))`.
- **Lambda-body shapes per site:** `$_.values->makeString(lit)` 299; `$_.get(lit)->toString()` 42; `$_.getString(lit) + lit + $_.getString(lit)` 25; `$_.getInteger(lit)->toString()` 18; `$_.get(lit)->toString() + lit + $_.get(lit)->toString()` 18; `$_.getString(lit) + lit + $_.getInteger(lit)->toString()` 12; `$_.getString(lit)->toString()` 11; `$_.getFloat(lit)->toString()` 4.
- **What happens to the mapped list** (431 statements):
  - compared as a list by `assertSameElements`: 157
  - compared as a list by `assertEquals`: 133
  - joined into one text by an outer `makeString`/`joinStrings`, then asserted: 126 (that outer join is also counted as a C1 site)
  - bound by `let`: 9
  - passed to `println`: 4
- Of the 115 C3 `+` sites, 65 have a print operand and the rest are cell-getter concatenations such as `getString + ' ' + getString`.

### C4: print inside the query sent to the store
- `tests/query/testWithFunction.pure:228`
  `let result = execute(|Trade.all()->filter(t | $t.id->toString() == '1'), simpleRelationalMapping, ...testRuntime(), ...relationalExtensions());`
- `tests/mapping/relation/tests.pure:1157`
  `|Trade.all()->project(~[id:t|$t.id, date:t|$t.date->toString()]),` (the result is then compared with `#TDS id, date / 1, 2014-12-01 ...`).
- `functions/tests/projection/testFunction.pure:98`
  `let result = execute(|Person.all()->project([#/Person/lastName#, p|$p.firstName+'|'+$p.lastName], ['lastName','fullName']), ...);`
- `functions/tests/projection/testAggregation.pure:98-100`
  `let result = execute(|Firm.all()->project([` / `f|$f.legalName,` / `f|$f.employees.firstName->joinStrings('^')`
- `executionPlan/tests/executionPlanTest.pure:1693`
  `let result = executionPlan({|Person.all()->filter(p|($p.firstName == 'John' && $p.lastName == 'Johnson') || $p.lastName == 'Allen').lastName->makeString(', ')}`
- `router/tests/testRouting.pure:55` (ExcludeAlloy)
  `let result = execute(|Person.all()->filter(p|$p.firstName == 'John')->mapNestedInUserFunction(e|$e.lastName)->makeString(', ') + ' | ' + Person.all()->filter(p|$p.firstName == 'Anthony')->toOne().lastName->toOne(), ...`
- **C4 by function** (runnable, sites / tests): `joinStrings` 43/33, `+` 34/24, `toString` 2/2, `makeString` 1/1, `format` 1/1; extra `serialize` 181/177.
- 163 of the `serialize` sites are in a `let query = {|...->graphFetch($tree)->serialize($tree)}` later passed to a query function.

### C5: everything else (all distinct shapes, runnable; sites / tests)
| sites / tests | shape (detail) | example |
|---|---|---|
| 92 / 46 | `let dateRange = [ …->min()->toOne()->toString(), …->max()->toOne()->toString() ]`, then `assertEquals([lit…], $dateRange)` (in a list, let, assert) | `calendarAggregation/tests/testCalendarFunctions.pure:256-259` |
| 26 / 26 | `pathToElement('meta::protocols::pure::' + $clientVersion + '::…')` (argument of `pathToElement`, `+`) | `testDataGeneration/tests/testDataGeneration.pure:1853` |
| 13 / 5 | `println($result->toCSV());` | `tds/tests/testTdsExtension.pure:33` |
| 6 / 2 | `println($rawTradeDate.columns->map(c|$c.name + ':' + $c.type->toOne()->elementToPath())->joinStrings(','));` | `tds/tests/testTdsExtension.pure:96` |
| 6 / 3 (and 8 / 7 for the `+`) | assert failure-message thunk: `assert($expected->forAll(e|$results->contains($e)),| 'expected: '+$expected->joinStrings('[',',',']')+' actual:'+$results->joinStrings('[',',',']'));` | `functions/tests/projection/testFunctionVariables.pure:75` |
| (in the 8 / 7 above) | `assert($planString->contains('…'), |'expected plain equals in plan, got: ' + $planString);` | `tests/query/testLegacyNullUnsafeEquals.pure:35` |
| 2 / 1 | print, then `->cast(@String)`, then assert: `assertEquals( 'Amount | Rate | Amount-Sum | Rate-Average',$results2.values.columns->map(r|$r.name->makeString(','))->makeString(' | ')->cast(@String) );` | `functions/tests/projection/testGroupWithWindowSubset.pure:69` |
| 2 / 1 (plus 2 extra) | `assert( or( $result->toJSONStringStream([],true)->makeString() == '{"values": …', $result->toJSONStringStream([],true)->makeString() == '…'))` | `tests/relationalSetUp.pure:1603-1605` |
| 1 / 1 (plus 1 extra) | `println($result->toJSONStringStream([],true)->makeString());` | `tests/relationalSetUp.pure:1601` |
| 1 extra | `$result->planToString(...)->println();` | `executionPlan/tests/executionPlanTest.pure:1881` |
| 1 extra | `let v = executionPlan(...)->planToString(...)` never used | `modelToModelToRelational/m2m2rShowcase.pure:23` |

The first row is effectively a C2 variant (text in a list, bound by `let`, list asserted); it is all 46 calendar tests and explains H2's 57 C5 tests on its fail roster.

## 4. Limits of the parser
- **No types.** `toString`/`makeString` on a String is counted like any other; `joinStrings` is always text to text but counted as asked; `+` cannot tell string from number, so 118 of 183 `+` sites have no print operand and may be plain string concatenation. "What is printed" labels come from receiver text (`.rows`, `.values`, `getFloat(`, `cast(@StrictDate)`).
- **Prints inside helpers are not attributed to the calling test.**
  - Confirmed by reading: `testFirmAgg` (`tests/mapping/relation/aggregation/testRelationFunctionAggregation.pure:29-38`, 9 tests) and `testTds` (`tests/mapping/union/relation/testRelationUnion.pure:323-332` and its `…Advanced` twin, 2 tests) both run `execute($query, …).values->at(0)` and then `assertSameElements($expectedRows, $result.rows->map(r|$r.values->makeString(' | ')))`, which is a C3 print done for the test.
  - `toCSV` itself prints inside `helperFunctions/helperFunctions.pure:208-233` (`joinStrings`, `toString()`, `format` for dates); it is counted as one site.
  - A name-based census (simple name only, so false positives such as `join` 95 and `scanRelations` 49): 167 runnable tests call an in-tree non-test function whose body both asserts and prints, and 109 of them have no print site of their own; 144 call one that prints without asserting, 87 with no own print site.
- **Tests with no assert in their own body:** 36 runnable (43 of 2761); 31 of the 36 call an asserting in-tree helper by name. 1019 runnable tests call some in-tree asserting helper (`assertSameSQL` 437, `assertEqualsH2Compatible` 323, `assertTestData` 59, …); any call whose name starts with `assert` is treated as an assert.
- **C4 depends on a callee list.** It is every callee seen with a lambda first argument (`execute` 2051, `toSQLString` 82, `executionPlan` 65, `validate` 41, `executeInternal` 16, …) plus let-bound lambdas later passed to one of them. Queries passed by function reference are invisible. Two sites are C4 only because they sit in a let-bound lambda not passed to a known query function (`router/tests/testRouting.pure:224`, `tests/testRelationalExtension.pure:146`).
- **C3 means** inside a lambda with at least one parameter whose owner is not `if`, `match`, `eval` or `mayExecuteAlloyTest`. A C3 inner print and a C1 outer join in the same statement are counted as two sites.
- **C1 strictness.** Only the tails you listed are allowed; one shape falls to C5 for that reason (`->cast(@String)`). No C1 argument concatenates a print with other text.
- **Reuse follows only `let x = execute(...)` or `executeInternal(...)`**, plus lets that are a single chain rooted at such a variable (`let tds = $result.values->at(0)`). Lambda parameters shadowing a variable name are excluded. `validate()` results (18 C1 sites) and other producers are not followed. Lets inside `mayExecuteAlloyTest({…|…})` are resolved by name, first definition wins.
- **Shape normalisation is lossy:** `$_` is any variable, call arguments are elided to `lit`, `…` or a lambda body, and sort keys and literals are dropped.

## Files are in `/tmp/claude-0/-home-user-legend-lite/a081db72-2095-5e3c-b0e3-e111003021e6/scratchpad/mine-corpus/`
- `sites.tsv` has 1754 data rows plus a header: 1254 core, 192 plus, 308 extra. Columns are the eight you asked for (file, line, test, position, print_function, printed_from, reused_elsewhere, snippet), then detail, shape, in_lite_corpus(runnable), duckdb_fail_roster, print_set.
- `mine.py` is the parser and classifier; `report.py` writes the TSV and maps the roster; `stats.py` produces the counts (saved in `stats.out`); `helpers.py` is the helper census.
