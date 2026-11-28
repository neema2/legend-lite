# PCT sort (r2): M4's buckets 3, 4 and 5, and M6's Pure-mode failures

Research only, 2026-11-25. Nothing built, nothing run; every class below is read from the measurements (M4, M6), the PCT
test functions in legend-pure's and legend-engine's pinned sources, legend-engine's DuckDB translation, and lite's code
and ledgers at `/home/user/s2` (`da231244`, the commit M4 and M6 measured). Where a mechanism is inferred and not read or
run, the row says **unverified**.

Inputs: `/home/user/s1/docs/records/edition-homework/r1/{m4-engine-parity-duckdb,m4_tables,m6-pure-parity,m6_tables}.md`,
`scratchpad/r1/{m4,m6}.json`; rulings `v2-amendments-ruled-2026-11-25.md` (items 4, 7, 11, 12, 13); the engine lane's pinned
tables `/home/user/s2/pct/engine_lane/duckdb_*.tsv`; channel B's recordings
`/home/user/s2/pct/src/test/resources/edge/channel_b_*_outcomes.tsv` and ledger
`/home/user/s2/pct/src/test/resources/org/finos/legend/lite/pct/channelb/ledger.tsv`.

Abbreviations used in citations:

- **ENG** = `/root/.cache/bazel/_bazel_root/a6663fbd60fad5804a563aa83422814f/external/+http_archive+legend_engine_src`;
  **PURE** = the sibling `+http_archive+legend_pure_src`.
- **DX** = ENG `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-dbExtension/legend-engine-xt-relationalStore-duckdb/legend-engine-xt-relationalStore-duckdb-pure/src/main/resources/core_relational_duckdb/relational/sqlQueryToString/duckdbExtension.pure`
- **ED** = ENG `legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/sqlQueryToString/extensionDefaults.pure`
- **Manifest `<Suite>:<line>`** = ENG `.../legend-engine-xt-relationalStore-duckdb-PCT/src/main/resources/pct-manifests/relational-duckdb/<Suite>Functions_manifest.json`, the line of the test's row.
- **Lite paths** are relative to `/home/user/s2`.

## Summary

**A. Bucket 3 (215: legend-engine fails on DuckDB, lite passes).**

| class | count | what lite should do in engine mode |
|---|---:|---|
| 1. refused by legend-engine (its own words) | 102 | answer (ruled: lite may answer where legend-engine refuses) |
| 2. a database error from legend-engine's SQL | 15 | answer Pure's way (each is in legend-engine's own manifest as a known failure; proposed reading (3)) |
| 3. a legend-engine translation bug (a wrong value) | 20 | answer Pure's way (known failures, reading (3)) |
| 4. a presentation or type difference | 24 | answer Pure's typed value (reproducing would mean copying legend-engine's listed failures) |
| **5. lite does something legend-engine deliberately does not -- FLAGGED** | **7** | **needs the user's word (below)** |
| 6. other: legend-engine faults (planner and run-time) | 47 | answer (a crash is a refusal in effect) |

- The flagged class is the one ruling 12's homework calls wrong ("lite added a check"); I read the task's "flag the
  last class" as this one. "Other" needs no ruling: it is legend-engine crashing.
- **No bucket-3 test passes because of a check lite adds.** None of the 215 test functions calls `assertError` or
  `assertFail` (checked over every body). Lite's checks only show in buckets 2b and 4 (below), where with the user's
  item-13 lean they would turn passes.
- The 7 flagged: **(E1, beyond both references) `testLargePlus`, `testLargeMinus`, `testLargeTimes`**: lite casts
  an integer literal near the INT64 edge to HUGEINT (`Scalars.hugeWiden`, `core/src/main/java/com/legend/lowering/Scalars.java:2871-2883`), matching
  interpreted legend-pure's BigInteger; legend-engine (DuckDB raises INT64 overflow) and compiled legend-pure (its own
  manifest lists all three) both hold 64-bit Integers. **(E2, depends on how far ruling 7 reaches)
  `testEqEnum`, `testEqualEnum`** (legend-engine evidently compares two enumerations' values by their names (inferred), so
  `TestEnum1.FIRST == TestEnum2.FIRST` is true) and **`testContainsPrimitive`, `testInPrimitive`** (legend-engine sends a
  mixed-type list to DuckDB, which cannot unify it): lite keeps each value's type; ruling 7 and the 2026-11-24 ruling
  ("strict typing is lite's alone") say legend-engine's door takes the database's answer for comparisons across types.
- M4's own classes moved: 6 "database errors" are legend-engine's own refusal `DuckDB doesn't support YEAR and
  YEAR-MONTH` (DX:150-152; M4's probe tested for the word "DuckDB" before the refusal words); 3 "other" are DuckDB's
  `list_reduce` binder error stored without its prefix; 4 YEAR-MONTH tests crash with `Date has no day` because the same
  guard lets YEAR-MONTH through.

**B. Bucket 4 (25, both fail differently).** 16 are channel A's own boundary on lite's side (error column, instance
identity, a class's toString, `deactivate`) against a legend-engine refusal or fault: nothing to match. **4 are cheap**:
`testAdjustBy{Days,Hours,Months,Weeks}BigNumber` fail on the same DuckDB error and differ only by legend-engine's
`java.sql.SQLException: ` prefix, an envelope the engine door's error presenter could write. 3 (`testRemError`,
`testSquareRootError`, `testBitShiftLeft_MoreThan62Bits`) are lite's ENGINE_UNCHECKED raises; with lite's checks kept in
engine mode (item 13) they **pass**, as would bucket 2b's `testSliceError`, `testRangeStepError` and
`testBitShiftRight_MoreThan62Bits`. 2 (`testArcCosineError`, `testArcSineError`) come from lite's DuckDB dialect turning
DuckDB's domain error into NaN (`DuckDb.java:730-735`, "as the engine's H2"), which neither legend-engine on DuckDB nor
Pure does.

**C. `testBigFloatAbs`.** legend-engine passes it: its PCT runs on **compiled** legend-pure, where the expected literal
`123456789123456789.99` is a Java double, and its answer comes back as the same double (the DECIMAL typing of the bare
literal changes nothing). Lite fails only because its channel A, the engine lane included, runs on **interpreted**
legend-pure, whose literal is a 20-digit BigDecimal; channel B passes. S120's facts are right (legend-engine holds a
double), but the conclusion drawn from them in M4's bucket 5 is not: it is a harness artifact, not a difference from
legend-engine. Deleting the DECIMAL Float literal does not interact (lite's parser already holds the double).

**D. M6, Pure mode (reference passes, lite fails).**

| database | lite fails | database limitation | lite bug or gap | ruled divergence | channel-A-only artifact |
|---|---:|---:|---:|---:|---:|
| DuckDB | 24 | 4 | 0 | 2 (S119) | 18 |
| SQLite | 28 | 8 | 0 | 2 (S119) | 18 |
| H2 (Relation) | 20 | 16 | 4 | 0 | 0 |

The user's framing holds on DuckDB once channel A's artifacts are set aside: channel B on DuckDB fails 6 tests, 4 of them
DuckDB's date range. The other 2 (`testArcCosineError`, `testArcSineError`) are **not** a database limitation: S119
(ruled, IEEE NaN) rests on compiled legend-pure holding a double, but compiled legend-pure **raises** `Unable to compute
acos of 2.0` (PURE `legend-pure-runtime/legend-pure-runtime-java-engine-compiled/src/main/java/org/finos/legend/pure/runtime/java/compiled/CoreHelper.java:141-148`, asin 126-133),
as for sqrt, which lite already raises. Under ruling 4 (pure = compiled legend-pure) S119 should be reopened for these
two. On H2 the 4 LATERAL tests are a lite gap (each lateral is a correlated filter, so an inner join), not H2's.
The lite fix list is at the end of section D.

## A. M4's bucket 3: legend-engine fails on DuckDB, lite passes (215)

Each test is in one class. `#` is the test's position in M4's bucket 3 (the order of `scratchpad/r1/m4.json`); the suite
is abbreviated (Ess, Gra, Rel, Sta, Unc). The last column is legend-engine's own manifest row that records the failure.
The engine lane's pinned class for every row is in `pct/engine_lane/duckdb_<suite>.tsv` (REFUSES / DEFECT.{DATABASE_ERROR,
PLANNER_FAULT, RUNTIME_FAULT, WRONG_ANSWER}); this sort agrees with it except where the reason says otherwise.

How the classes were decided:

1. **Refused**: legend-engine's own words that it does not do this (`No SQL translation exists`, `not supported`,
   `not managed yet`, `isn't a literal`, `is supported only`, `only supports`, `does not support`, `doesn't support`,
   `[unsupported-api]`, `No relational processor matched`).
2. **Database error**: DuckDB rejected the SQL legend-engine wrote (DuckDB's error text).
3. **Translation bug**: legend-engine answered, with a wrong value, and the cause is in its translation (or, once, its
   read-back); legend-engine's manifest lists the test.
4. **Presentation or type**: the same quantity, a different type or precision (DateTime precision, Integer `2` versus
   Float `2.0`, Decimal versus Float, a Decimal's scale).
5. **Lite does something legend-engine deliberately does not (FLAGGED)**: lite passes only because it goes beyond what
   legend-engine does by design (a type distinction, a widening), not because legend-engine has a bug. E1: beyond both
   references; E2: depends on how far ruling 7 reaches.
6. **Other**: legend-engine crashed in its own planning or after its SQL (an exception that is neither its refusal nor
   DuckDB's). Lite answering is in effect answering a refusal.

Changes against M4's message classes (`m4-engine-parity-duckdb.md` §3's bucket-3 table: wrong value 46, refusal 96,
database error 23, other 50): M4's 23 database errors are 6 refusals (`DuckDB doesn't support YEAR and YEAR-MONTH`, DX:152
`fail(...)`), 5 flagged (the 2 mixed IN lists, the 3 INT64 overflows) and 12 database errors; M4's 50 other are 47 other
plus 3 database errors (DuckDB's `list_reduce` binder message without its prefix); M4's 46 wrong values are 20
translation bugs, 24 presentation or type and 2 flagged (the enumeration equalities).

| class | Essential | Grammar | Relation | Standard | Unclassified | total |
|---|---:|---:|---:|---:|---:|---:|
| 1. Refused by legend-engine | 71 | 28 | 1 | 2 | 0 | **102** |
| 2. A database error from legend-engine's SQL | 5 | 1 | 0 | 6 | 3 | **15** |
| 3. A legend-engine translation bug | 15 | 0 | 0 | 0 | 5 | **20** |
| 4. A presentation or type difference | 7 | 3 | 0 | 14 | 0 | **24** |
| 5. Lite does something legend-engine deliberately does not | 1 | 5 | 0 | 1 | 0 | **7** |
| 6. Other: legend-engine faults before or after its SQL | 27 | 1 | 0 | 19 | 0 | **47** |
| **total** | 126 | 38 | 1 | 42 | 8 | **215** |

### 5. Lite does something legend-engine deliberately does not (FLAGGED) -- 7

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 8 | `collection::tests::contains::testContainsPrimitive` (Ess) | DuckDB 'Conversion Error: Unimplemented type for cast (INTEGER -> DATE)' on `1 in (1, 2, 5, 2, 'a', true, DATE '2014-02-01', 'c')`: legend-engine sends the mixed list to the database and the database's type unification fails. Lite passes by keeping each element's kind (its tagged mixed-list carrier). Under ruling 7 ('comparisons under engine take the database's promotion, including types known only at run time (Any, Number)') the database's answer here is this error. [E2: depends on ruling 7's reach] | Essential:28 |
| 129, 134 | `boolean::tests::equality::eq::testEqEnum` (Gra)<br>`boolean::tests::equality::equal::testEqualEnum` (Gra) | legend-engine answers `Assert failed`; the first assert that can fail is the one across two enumerations, `eq(TestEnum1.FIRST, TestEnum2.FIRST)` (PURE `.../grammar/functions/boolean/equality/eq.pure:74`; `equal.pure:105`): legend-engine's SQL carries an enumeration value as its name, so the two values compare equal (inferred from the failure, SQL not seen, unverified). Lite keeps the enumeration's type and answers false (Pure's). Under the 2026-11-24 ruling ('strict typing is lite's alone', `core/src/main/java/com/legend/edge/EngineComparisons.java:13-19`) and ruling 7, legend-engine's door answers a comparison across types the database's way, which here is true. [E2: depends on ruling 7's reach; by proposed reading (3) it is a translation bug lite corrects] | Grammar:13, Grammar:33 |
| 154, 158, 161 | `math::tests::minus::testLargeMinus` (Gra)<br>`math::tests::plus::testLargePlus` (Gra)<br>`math::tests::times::testLargeTimes` (Gra) | DuckDB 'Out of Range Error: Overflow ... of INT64': the answer (testLargePlus `9223372036854775807 + 3 + ...`, testLargeTimes `2 * 9223372036854775807`) or the literal itself (testLargeMinus `9223372036854775898 - 132`, which legend-engine's compiled Pure already wrapped to -9223372036854775718) needs more than 64 bits. legend-engine's Integer is a Java long by design (compiled TypeProcessor.java:196-198) and compiled legend-pure fails these three too (pct_grammar_compile_native.json:5,9,13, 'Assert failure'). Lite passes only because its lowering casts an integer literal near the INT64 edge to HUGEINT (`Scalars.hugeWiden`, Scalars.java:2871-2883, used at 315/331/355/373/383): a literal-magnitude special case that matches interpreted legend-pure's BigInteger, which ruling 4 does not follow. [E1: beyond both references, in both modes] | Grammar:137, Grammar:153, Grammar:165 |
| 171 | `collection::tests::in::testInPrimitive` (Sta) | same as testContainsPrimitive (row 8): DuckDB cannot unify the mixed IN list; lite keeps each element's kind. [E2: depends on ruling 7's reach] | Standard:25 |

### 1. Refused by legend-engine (its own words) -- 102

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 1 | `collection::tests::add::testAddWithOffset` (Ess) | No SQL translation exists for add(T[*],Integer,T) | Essential:4 |
| 2, 3, 4, 127 | `collection::tests::at::testAt` (Ess)<br>`collection::tests::at::testAtOtherScenario` (Ess)<br>`collection::tests::at::testAtWithVariable` (Ess)<br>`boolean::tests::conjunctions::not::testNotInCollection` (Gra) | '->at(...) function is supported only after direct access of 1->MANY properties' | Essential:16, Essential:10, Essential:13, Grammar:5 |
| 5, 6 | `collection::tests::concatenate::testConcatenateMixedType` (Ess)<br>`collection::tests::concatenate::testConcatenateTypeInference` (Ess) | 'Any is not managed yet!' (a mixed-type list) | Essential:19, Essential:22 |
| 7 | `collection::tests::contains::testContainsNonPrimitive` (Ess) | 'Parameter to IN operation isn't a literal!' (a list of class instances) | Essential:25 |
| 15 | `collection::tests::fold::testFoldEmptyListAndEmptyIdentity` (Ess) | 'Any is not managed yet!' (fold over []->cast(@Any)) | Essential:52 |
| 21, 22, 23 | `collection::tests::forall::testforAllOnEmptySet` (Ess)<br>`collection::tests::forall::testforAllOnNonEmptySetIsFalse` (Ess)<br>`collection::tests::forall::testforAllOnNonEmptySetIsTrue` (Ess) | No SQL translation exists for forAll | Essential:73, Essential:76, Essential:79 |
| 28, 29, 30, 32 | `collection::tests::removeDuplicates::testRemoveDuplicatesEmptyListExplicit` (Ess)<br>`collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveNonStandardFunction` (Ess)<br>`collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit` (Ess)<br>`collection::tests::removeDuplicatesBy::testRemoveDuplicatesByPrimitive` (Ess) | No SQL translation exists for removeDuplicates with key/equality functions | Essential:97, Essential:100, Essential:103, Essential:109 |
| 31 | `collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionMixedTypes` (Ess) | 'Any is not managed yet!' (mixed-type list) | Essential:106 |
| 33 | `collection::tests::sort::testMixedSortNoComparator` (Ess) | 'Not supported: Number' (sort of a mixed Number list) | Essential:115 |
| 34, 35, 36, 37 | `collection::tests::sort::testSimpleSort` (Ess)<br>`collection::tests::sort::testSimpleSortReversed` (Ess)<br>`collection::tests::sort::testSimpleSortWithFunctionVariables` (Ess)<br>`collection::tests::sort::testSimpleSortWithKey` (Ess) | No SQL translation exists for sort with key/comparator | Essential:127, Essential:118, Essential:121, Essential:124 |
| 38, 40, 42, 43, 45, 46 | `collection::tests::zip::testZipBothListsAreOfPairs` (Ess)<br>`collection::tests::zip::testZipBothListsSameLength` (Ess)<br>`collection::tests::zip::testZipFirstListLonger` (Ess)<br>`collection::tests::zip::testZipFirstListsIsOfPairs` (Ess)<br>`collection::tests::zip::testZipSecondListLonger` (Ess)<br>`collection::tests::zip::testZipSecondListsIsOfPairs` (Ess) | No SQL translation exists for zip | Essential:130, Essential:136, Essential:142, Essential:145, Essential:151, Essential:154 |
| 50, 52, 60, 71 | `date::tests::testAdjustByYears` (Ess)<br>`date::tests::testDateDiffYears` (Ess)<br>`date::tests::testDatePartYearOnly` (Ess)<br>`date::tests::testYear` (Ess) | its own refusal 'DuckDB doesn't support YEAR and YEAR-MONTH' (DX:150-152, fail() in convertDateToSqlStringDuckDB; M4 filed it as a database error because of the word DuckDB) | Essential:178, Essential:184, Essential:208, Essential:256 |
| 53, 54, 55, 58 | `date::tests::testDateFromHour` (Ess)<br>`date::tests::testDateFromMinute` (Ess)<br>`date::tests::testDateFromMonth` (Ess)<br>`date::tests::testDateFromYear` (Ess) | No SQL translation exists for date(...) of 1, 2, 4 or 5 parts | Essential:187, Essential:190, Essential:193, Essential:202 |
| 61, 62, 63, 64, 65, 66, 67 | `date::tests::testHasDay` (Ess)<br>`date::tests::testHasHour` (Ess)<br>`date::tests::testHasMinute` (Ess)<br>`date::tests::testHasMonth` (Ess)<br>`date::tests::testHasSecond` (Ess)<br>`date::tests::testHasSubsecond` (Ess)<br>`date::tests::testHasSubsecondWithAtLeastPrecision` (Ess) | No SQL translation exists for has{Day,Hour,Minute,Month,Second,Subsecond,SubsecondWithAtLeastPrecision} | Essential:214, Essential:217, Essential:220, Essential:223, Essential:226, Essential:232, Essential:229 |
| 72 | `lang::tests::match::testMatch` (Ess) | 'type not supported: ...MA_GeographicEntityType' | Essential:298 |
| 73 | `lang::tests::match::testMatchManyWithMany` (Ess) | 'Match only supports operands with multiplicity [1]..!' | Essential:259 |
| 74, 75, 76, 81, 82, 83, 84 | `lang::tests::match::testMatchOneWith` (Ess)<br>`lang::tests::match::testMatchOneWithMany` (Ess)<br>`lang::tests::match::testMatchOneWithZeroOne` (Ess)<br>`lang::tests::match::testMatchWithFunctionsAsParamManyMatch` (Ess)<br>`lang::tests::match::testMatchWithFunctionsManyMatch` (Ess)<br>`lang::tests::match::testMatchZeroWithMany` (Ess)<br>`lang::tests::match::testMatchZeroWithZero` (Ess) | 'Match does not support Non-Primitive return type..! Current return type : Any' | Essential:268, Essential:262, Essential:265, Essential:277, Essential:283, Essential:292, Essential:295 |
| 77, 78 | `lang::tests::match::testMatchWithExtraParam` (Ess)<br>`lang::tests::match::testMatchWithExtraParamsAndFunctionsAsParam` (Ess) | No SQL translation exists for match with extra parameters | Essential:271, Essential:274 |
| 90, 91, 92, 93, 94, 95, 96, 97, 100, 101, 102 | `string::tests::format::testFormatBoolean` (Ess)<br>`string::tests::format::testFormatDate` (Ess)<br>`string::tests::format::testFormatFloat` (Ess)<br>`string::tests::format::testFormatFloatWithRounding` (Ess)<br>`string::tests::format::testFormatFloatWithTruncation` (Ess)<br>`string::tests::format::testFormatFloatWithZeroPadding` (Ess)<br>`string::tests::format::testFormatInteger` (Ess)<br>`string::tests::format::testFormatIntegerWithZeroPadding` (Ess)<br>`string::tests::format::testFormatRepr` (Ess)<br>`string::tests::format::testFormatString` (Ess)<br>`string::tests::format::testSimpleFormatDate` (Ess) | No SQL translation exists for format(String, Any[*]) | Essential:328, Essential:331, Essential:343, Essential:334, Essential:337, Essential:340, Essential:349, Essential:346, Essential:358, Essential:361, Essential:364 |
| 103 | `string::tests::indexOf::testFromIndex` (Ess) | No SQL translation exists for indexOf(String, String, Integer) | Essential:367 |
| 106, 107 | `string::tests::parseBoolean::testParseFalse` (Ess)<br>`string::tests::parseBoolean::testParseTrue` (Ess) | '[unsupported-api] The function 'parseBoolean' ... is not supported yet' | Essential:376, Essential:379 |
| 108, 109, 110, 111 | `string::tests::parseDate::testParseDate` (Ess)<br>`string::tests::parseDate::testParseDateTypes` (Ess)<br>`string::tests::parseDate::testParseDateWithTimezone` (Ess)<br>`string::tests::parseDate::testParseDateWithZ` (Ess) | '[unsupported-api] The function 'toTimestamp' ... is not supported yet' (parseDate) | Essential:391, Essential:382, Essential:385, Essential:388 |
| 128, 133 | `boolean::tests::equality::eq::testEqDate` (Gra)<br>`boolean::tests::equality::equal::testEqualDateStrictYear` (Gra) | its own refusal 'DuckDB doesn't support YEAR and YEAR-MONTH' (DX:150-152, fail() in convertDateToSqlStringDuckDB; M4 filed it as a database error because of the word DuckDB) (equality of year-only dates) | Grammar:9, Grammar:29 |
| 130, 131, 132, 135, 136, 137 | `boolean::tests::equality::eq::testEqNonPrimitive` (Gra)<br>`boolean::tests::equality::eq::testEqPrimitiveExtension` (Gra)<br>`boolean::tests::equality::eq::testEqVarIdentity` (Gra)<br>`boolean::tests::equality::equal::testEqualNonPrimitive` (Gra)<br>`boolean::tests::equality::equal::testEqualPrimitiveExtension` (Gra)<br>`boolean::tests::equality::equal::testEqualVarIdentity` (Gra) | 'Filter expressions are only supported for Primitives and Enums' | Grammar:17, Grammar:21, Grammar:25, Grammar:37, Grammar:41, Grammar:45 |
| 139, 148, 162 | `collection::tests::map::testMapInstance` (Gra)<br>`lang::tests::letFn::testAssignNewInstance` (Gra)<br>`string::tests::plus::testMultiPlusWithPropertyExpressions` (Gra) | 'type not supported: ...GeographicEntityType' | Grammar:61, Grammar:113, Grammar:169 |
| 140, 141, 142, 143, 144, 145, 146, 147 | `lang::tests::compare::testBooleanCompare` (Gra)<br>`lang::tests::compare::testCompareDecimalAndLongTypes` (Gra)<br>`lang::tests::compare::testCompareMixedTypes` (Gra)<br>`lang::tests::compare::testDateCompare` (Gra)<br>`lang::tests::compare::testFloatCompare` (Gra)<br>`lang::tests::compare::testIntegerCompare` (Gra)<br>`lang::tests::compare::testNumberCompare` (Gra)<br>`lang::tests::compare::testStringCompare` (Gra) | No SQL translation exists for compare | Grammar:81, Grammar:85, Grammar:89, Grammar:93, Grammar:97, Grammar:101, Grammar:105, Grammar:109 |
| 149, 150, 151, 152 | `lang::tests::letFn::testLetAsLastStatement` (Gra)<br>`lang::tests::letFn::testLetChainedWithAnotherFunction` (Gra)<br>`lang::tests::letFn::testLetInsideIf` (Gra)<br>`lang::tests::letFn::testLetWithParam` (Gra) | No SQL translation exists for letFunction | Grammar:117, Grammar:121, Grammar:125, Grammar:129 |
| 156, 159 | `math::tests::minus::testSingleMinusType` (Gra)<br>`math::tests::plus::testSinglePlusType` (Gra) | No SQL translation exists for genericType | Grammar:141, Grammar:157 |
| 163 | `string::tests::plus::testPlusInCollect` (Gra) | No SQL translation exists for at(T[*], Integer) | Grammar:173 |
| 164 | `string::tests::plus::testPlusInIterate` (Gra) | 'No relational processor matched function fold' (its catch-all) | Grammar:177 |
| 165 | `relation::tests::in::testSimpleIn_Negated` (Rel) | 'Negating in(value, Relation) is not supported on relational stores. Use not exists instead...': a deliberate refusal with a suggested rewrite | Relation:7 |
| 170 | `collection::tests::in::testInNonPrimitive` (Sta) | 'Parameter to IN operation isn't a literal!' | Standard:21 |
| 185 | `math::hashCode::tests::testHashCodeAggregate` (Sta) | '[unsupported-api] The function 'hashAgg' ... is not supported yet' | Standard:81 |

### 2. A database error from legend-engine's SQL -- 15

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 14, 19, 20 | `collection::tests::fold::testFoldCollectionAccumulator` (Ess)<br>`collection::tests::fold::testFoldWithEmptyAccumulator` (Ess)<br>`collection::tests::fold::testFoldWithSingleValue` (Ess) | DuckDB's list_reduce binder error 'The initial value type must be the same as the list child type or a common super type' (the manifest stores DuckDB's text without the `java.sql.SQLException: Binder Error:` prefix that row 17 carries; the engine lane files it PLANNER_FAULT for that reason): legend-engine maps fold to list_reduce, which cannot fold into an accumulator of another type | Essential:49, Essential:64, Essential:67 |
| 17 | `collection::tests::fold::testFoldMixedAccumulatorTypes` (Ess) | DuckDB 'Binder Error: The initial value type must be the same as the list child type...': fold(['one','two'], {v,a\|$a+$v->length()}, 1) mapped to list_reduce with an Integer accumulator over a String list | Essential:58 |
| 112 | `string::tests::parseDecimal::testParseDecimal` (Ess) | DuckDB 'Conversion Error: Could not convert string "3.14159d" to DECIMAL(18,3)': legend-engine's parseDecimal is a plain cast (DX:256) and does not accept Pure's `d` suffix | Essential:397 |
| 155 | `math::tests::minus::testSingleMinus` (Gra) | DuckDB 'Parser Error: SELECT clause without selection list': legend-engine writes `--1` / `---1` unparenthesized, which SQL reads as a comment (inferred from the message, SQL not seen) | Grammar:145 |
| 194, 200 | `math::tests::maxBy::testMaxBy` (Sta)<br>`math::tests::minBy::testMinBy` (Sta) | DuckDB 'Binder Error: No function matches max_by(INTEGER, INTEGER, INTEGER, INTEGER)': DX:246-247 join every argument into one MAX_BY(...) call, so the list form gets four scalars | Standard:125, Standard:149 |
| 204 | `math::tests::percentile::testPercentile` (Sta) | DuckDB 'Binder Error: No function matches quantile_cont(BIGINT[], DECIMAL(3,2))': percentile over a list mapped to the aggregate | Standard:165 |
| 205 | `math::tests::percentile::testPercentile_Relation_Window` (Sta) | DuckDB 'Catalog Error: Aggregate Function with name percentile_cont does not exist!' (window form) | Standard:169 |
| 206, 207 | `math::tests::toDegrees::testToDegrees` (Sta)<br>`math::tests::toRadians::testToRadians` (Sta) | DuckDB 'Out of Range Error: Overflow in multiplication of DECIMAL(18)': legend-engine writes the Float literal 3.141592653589793 bare (ED:134), DuckDB types it DECIMAL, and the degree/radian arithmetic overflows DECIMAL(18). This is the register row ruling 11 creates (legend-engine on DuckDB computes Float literal arithmetic as DECIMAL); lite answers IEEE and passes before and after the deletion (M1: engine lanes 0 moves) | Standard:173, Standard:177 |
| 208 | `string::tests::base64::testDecodeBase64NoPadding` (Unc) | DuckDB 'Conversion Error: Could not decode string ... as base64: length must be a multiple of 4': `decode(from_base64(%s))` (DX:219) needs padding; Pure decodes unpadded text | Unclassified:4 |
| 209, 214 | `string::tests::lpad::testLpadEmptyChar` (Unc)<br>`string::tests::rpad::testRpadEmptyChar` (Unc) | DuckDB 'Invalid Input Error: Insufficient padding in LPAD/RPAD': `lpad(%s,%s,%s)` (DX:243, 271) with an empty pad string; Pure answers the string unchanged | Unclassified:7, Unclassified:22 |

### 3. A legend-engine translation bug (a wrong value) -- 20

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 27 | `collection::tests::indexof::testIndexOfOneElement` (Ess) | `'c'->indexOf('c')` answers 1: DX:237 maps indexOf to 1-based `instr`, with legend-engine's own TODO: 'pure uses 0-based indexing ... keeping this as H2 also returns 1-based currently, many user tests need to be fixed' (a known bug kept for its users) | Essential:94 |
| 48 | `date::tests::testAdjustByMinutesBigNumber` (Ess) | wrong value read back, not wrong SQL: Pure `%-21457-01-08T20:48:00`, legend-engine `%21459-07-29T20:48:00` -- the BC timestamp DuckDB computes loses its era and shifts by the Julian/Gregorian gap in legend-engine's Java read-back (mechanism inferred from the shape of the error, unverified) | Essential:166 |
| 51 | `date::tests::testDateDiffWeeks` (Ess) | `datediff('week', a, b)` (DX:213, unit map DX:478-483): DuckDB counts Monday-based week boundaries, Pure Sunday-based (2015-07-04 Sat to 07-05 Sun is 1 in Pure, 0 in DuckDB) | Essential:181 |
| 68 | `date::tests::testHour` (Ess) | `%2015-04-15T17->hour()` answers 0: the DuckDB literal processor's optional time section (DX:151, `yyyy-MM-dd?[ HH:mm:ss?[.S*]]`) drops the time of an hour-precision DateTime, so the literal reaches SQL as midnight (read from the format, not run) | Essential:238 |
| 69 | `date::tests::testMinute` (Ess) | `%2015-04-15T17:09->minute()` answers 0: same literal-processor cause as testHour (minute precision) | Essential:244 |
| 85 | `math::tests::pow::testNumberPow` (Ess) | `{a:Number[1], b:Number[1] \| pow($a,$b)}->eval(3.0, 2)` answers 27.0, not 9.0: the lambda's arguments are mis-bound in planning (27 = 3^3 suggests both parameters took 3.0; unverified) | Essential:301 |
| 87, 88 | `math::tests::round::testNegativeFloatRoundHalfEvenUp` (Ess)<br>`math::tests::round::testPositiveFloatRoundHalfEvenDown` (Ess) | half-even rounding: DX:865-871 writes `cast(round(%s, 0) as BIGINT)`, DuckDB rounds half away from zero (round(16.5) = 17, Pure 16) | Essential:310, Essential:313 |
| 104 | `string::tests::indexOf::testSimple` (Ess) | string indexOf answers 1-based (5 for 4): DX:237 `instr`, legend-engine's own TODO keeps it (see row 27) | Essential:370 |
| 115, 116 | `string::tests::substring::testStart` (Ess)<br>`string::tests::substring::testStartEnd` (Ess) | 1-based `substring` passthrough (DX:277); legend-engine's fix is behind its CORRECT_SQL_SUBSTRING_INDEXING feature flag (executionPlanFeature.pure:26, pureToSQLQuery.pure:369), off in PCT. Ruling 12 already names this a bug | Essential:406, Essential:403 |
| 118, 119, 126 | `string::tests::toString::testDateTimeToString` (Ess)<br>`string::tests::toString::testDateTimeWithTimezoneToString` (Ess)<br>`string::tests::toString::testSimpleDateToString` (Ess) | toString of a DateTime is `cast(%s as varchar)` (DX:284), DuckDB's text ('2014-01-01 00:00:00'), not Pure's ('2014-01-01T00:00:00.000+0000'): the String value is wrong | Essential:415, Essential:418, Essential:439 |
| 125 | `string::tests::toString::testPersonToString` (Ess) | `$str->startsWith('Anonymous_') \|\| $str->startsWith('@_')` answers false: startsWith is a LIKE with the `_` escaped (ED:288, 800-806); the escape does not hold on DuckDB as written (mechanism unverified) | Essential:436 |
| 210, 211, 212, 213 | `string::tests::regexpLike::testRegexpLike_CaseInsensitive_Multiline` (Unc)<br>`string::tests::regexpLike::testRegexpLike_CaseInsensitive_Multiline_NonNewlineSensitive` (Unc)<br>`string::tests::regexpLike::testRegexpLike_Multiline` (Unc)<br>`string::tests::regexpLike::testRegexpLike_Multiline_NonNewlineSensitive` (Unc) | regexpLike with MULTILINE: the Pure-to-DuckDB flag mapping (`regexpParametersToPatternFlagsForDuckDB`, DX:412-415) does not give `^` per-line anchoring, so `'abc\ndef'->regexpLike('^def', MULTILINE)` is false (flag semantics not run, unverified) | Unclassified:10, Unclassified:13, Unclassified:16, Unclassified:19 |
| 215 | `string::tests::splitPart::testSplitPartEmptyToken` (Unc) | `'Hello World'->splitPart('', 0)` answers 'H': `split_part(%s, %s, %s)` (DX:276) splits on the empty token into characters; Pure answers the whole string | Unclassified:25 |

### 4. A presentation or type difference -- 24

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 47 | `date::tests::testAdjustByMicrosecondsBigNumber` (Ess) | precision: Pure keeps the literal's 7 sub-second digits (`.4990000`), legend-engine answers `.499` | Essential:163 |
| 56, 57 | `date::tests::testDateFromSecond` (Ess)<br>`date::tests::testDateFromSubSecond` (Ess) | precision: date(y,m,d,h,mi,s) is `make_timestamp` (DX:504-511) read back at nanosecond precision (`.000000000`); Pure's answer has second (or the literal's sub-second) precision | Essential:196, Essential:199 |
| 86 | `math::tests::rem::testRemWithDecimals` (Ess) | kind: rem(3.14, 1.5d) is a Decimal in Pure (0.14d), a Float in legend-engine (0.14) | Essential:307 |
| 89 | `math::tests::toDecimal::testIntToDecimal` (Ess) | scale: toDecimal(8) is `8D` in Pure; legend-engine's `cast(%s as decimal)` (DX:282) gives a scaled DECIMAL read back as `8.0D` | Essential:319 |
| 113, 114 | `string::tests::parseDecimal::testParseDecimalWithPrecisionScale` (Ess)<br>`string::tests::parseDecimal::testParseZero` (Ess) | Decimal scale: Pure `123.12300D` / `0.000D`, legend-engine `123.123D` / `0.0D` (its parseDecimal ignores the precision and scale arguments) | Essential:394, Essential:400 |
| 153, 157, 160 | `math::tests::minus::testDecimalMinus` (Gra)<br>`math::tests::plus::testDecimalPlus` (Gra)<br>`math::tests::times::testDecimalTimes` (Gra) | Decimal versus Float kind of a mixed Decimal/Integer/Float arithmetic result (Pure: Decimal; legend-engine: Float). Same ruling-7 note as the Number-kind rows | Grammar:133, Grammar:149, Grammar:161 |
| 166, 168, 172, 174, 178, 179, 182, 183 | `collection::tests::greatest::testGreatest_DateTime` (Sta)<br>`collection::tests::greatest::testGreatest_Single` (Sta)<br>`collection::tests::least::testLeast_DateTime` (Sta)<br>`collection::tests::least::testLeast_Single` (Sta)<br>`date::tests::max::testMax_DateTime` (Sta)<br>`date::tests::max::testMax_DateTimeArray` (Sta)<br>`date::tests::min::testMin_DateTime` (Sta)<br>`date::tests::min::testMin_DateTimeArray` (Sta) | DateTime precision: legend-engine reads DuckDB's TIMESTAMP back at nanosecond precision (`...T20:10:20.000000000`), Pure's literal has second precision | Standard:5, Standard:13, Standard:29, Standard:37, Standard:57, Standard:53, Standard:73, Standard:69 |
| 167, 173, 192, 193, 198, 199 | `collection::tests::greatest::testGreatest_Number` (Sta)<br>`collection::tests::least::testLeast_Number` (Sta)<br>`math::tests::max::testMax_Numbers` (Sta)<br>`math::tests::max::testMax_NumbersArray` (Sta)<br>`math::tests::min::testMin_Numbers` (Sta)<br>`math::tests::min::testMin_NumbersArray` (Sta) | Number kind: Pure keeps the winning element's own kind (Integer 2, Decimal 1.0D); legend-engine lets DuckDB promote the mixed list (DOUBLE) and answers a Float. The user's own example of a type difference. Note: would move to class 5 if ruling 7 (the database's promotion for runtime-typed Number) is read to cover a result's kind | Standard:9, Standard:33, Standard:121, Standard:117, Standard:145, Standard:141 |

### 6. Other: legend-engine faults before or after its SQL (planner and run-time faults) -- 47

| # | tests | why | legend-engine manifest (relational-duckdb) |
|---:|---|---|---|
| 9 | `collection::tests::contains::testContainsWithFunction` (Ess) | planner fault: legend-engine's own Pure parser rejects the expression it generated ('no viable alternative at input ...contains(...comparator...)') | Essential:31 |
| 10 | `collection::tests::exists::testExists` (Ess) | run-time fault 'Cannot cast a collection of size 0 to multiplicity [1]': legend-engine's SQL answered no value (likely a translation defect; SQL not inspected, unverified) | Essential:34 |
| 11, 12 | `collection::tests::find::testFindLiteral` (Ess)<br>`collection::tests::find::testFindLiteralFromVar` (Ess) | planner fault 'Function does not exist find(String[n], LambdaFunction...)': its compiler has no find() for a literal list of known size | Essential:43, Essential:40 |
| 13, 16, 18 | `collection::tests::fold::testFold` (Ess)<br>`collection::tests::fold::testFoldFiltering` (Ess)<br>`collection::tests::fold::testFoldToMany` (Ess) | planner fault 'Function does not exist copy(FO_Person[1],String[1],KeyExpression[1])': copy-with-update (^$p2(lastName=...)) inside the fold lambda | Essential:70, Essential:55, Essential:61 |
| 24, 25, 26 | `collection::tests::head::testHeadOnEmptySet` (Ess)<br>`collection::tests::head::testHeadOnOneElement` (Ess)<br>`collection::tests::head::testHeadSimple` (Ess) | run-time fault 'Cannot cast a collection of size 0 to multiplicity [1]': legend-engine's SQL answered no value (likely a translation defect; SQL not inspected, unverified) (`head` of a literal list) | Essential:85, Essential:88, Essential:91 |
| 39, 41, 44 | `collection::tests::zip::testZipBothListsEmpty` (Ess)<br>`collection::tests::zip::testZipFirstListEmpty` (Ess)<br>`collection::tests::zip::testZipSecondListEmpty` (Ess) | planner fault 'The system is trying to get an element at offset 0 where the collection is of size 0' (zip of an empty list) | Essential:133, Essential:139, Essential:148 |
| 49, 59, 70 | `date::tests::testAdjustByMonths` (Ess)<br>`date::tests::testDatePartYearMonthOnly` (Ess)<br>`date::tests::testMonthNumber` (Ess) | 'Date has no day': a YEAR-MONTH literal passes the guard `hasMonth() \|\| hasDay()` (DX:150) meant to refuse it, then the `yyyy-MM-dd` format fails (the YEAR-MONTH refusal leaking as a crash) | Essential:172, Essential:205, Essential:247 |
| 79, 80 | `lang::tests::match::testMatchWithFunctions` (Ess)<br>`lang::tests::match::testMatchWithFunctionsAsParam` (Ess) | planner fault 'Cast exception: Literal cannot be cast to SemiStructuredPropertyAccess' | Essential:286, Essential:280 |
| 98, 99 | `string::tests::format::testFormatList` (Ess)<br>`string::tests::format::testFormatPair` (Ess) | run-time fault 'Cannot cast a collection of size 0 to multiplicity [1]': legend-engine's SQL answered no value (likely a translation defect; SQL not inspected, unverified) (format of a List/Pair) | Essential:352, Essential:355 |
| 105 | `string::tests::joinStrings::testJoinStringsNoStrings` (Ess) | planner fault 'Match failure: null' (joinStrings over an empty tail) | Essential:373 |
| 117 | `string::tests::toString::testClassToString` (Ess) | planner fault 'Match failure: ClassInstanceHolderObject instanceOf ClassInstanceHolder' (toString of a class) | Essential:409 |
| 120 | `string::tests::toString::testDateToString` (Ess) | 'Date has no day': a YEAR-MONTH literal passes the guard `hasMonth() \|\| hasDay()` (DX:150) meant to refuse it, then the `yyyy-MM-dd` format fails (the YEAR-MONTH refusal leaking as a crash) (toString of %2014-01) | Essential:421 |
| 121 | `string::tests::toString::testEnumerationToString` (Ess) | planner fault 'Match failure: ClassInstanceHolderObject' (toString of an enumeration) | Essential:424 |
| 122, 124 | `string::tests::toString::testListToString` (Ess)<br>`string::tests::toString::testPairToString` (Ess) | run-time fault 'Cannot cast a collection of size 0 to multiplicity [1]': legend-engine's SQL answered no value (likely a translation defect; SQL not inspected, unverified) (toString of a List/Pair) | Essential:427, Essential:433 |
| 123 | `string::tests::toString::testPairCollectionToString` (Ess) | run-time fault 'Cannot cast a collection of size 2 to multiplicity [1]' (toString over a list of Pairs) | Essential:430 |
| 138 | `collection::tests::getAll::testBasic` (Gra) | planner fault 'Cast exception: InstanceValue cannot be cast to StoreMappingRoutedValueSpecification' (Class.all()) | Grammar:57 |
| 169 | `collection::tests::in::testInIsEmpty` (Sta) | run-time fault 'NullPointer exception' (`[]->in(['a','b'])`) | Standard:17 |
| 175, 176 | `collection::tests::max::testMax` (Sta)<br>`collection::tests::min::testMin` (Sta) | run-time fault 'Cannot cast a collection of size 0 to multiplicity [1]': legend-engine's SQL answered no value (likely a translation defect; SQL not inspected, unverified) (max/min with a comparator lambda) | Standard:41, Standard:45 |
| 177, 180, 181, 184 | `date::tests::max::testMax_DateArray` (Sta)<br>`date::tests::max::testMax_StrictDateArray` (Sta)<br>`date::tests::min::testMin_DateArray` (Sta)<br>`date::tests::min::testMin_StrictDateArray` (Sta) | planner fault 'Unused format args' building DuckDB's aggregate template over a literal list (Java String.format in its plan generation) (max/min of a Date list) | Standard:49, Standard:61, Standard:65, Standard:77 |
| 186, 187, 188, 189, 190, 191, 195, 196, 197, 201, 202, 203 | `math::tests::average::testAverage_Floats` (Sta)<br>`math::tests::average::testAverage_Integers` (Sta)<br>`math::tests::average::testAverage_Numbers` (Sta)<br>`math::tests::corr::testCorr` (Sta)<br>`math::tests::covarPopulation::testCovarPopulation` (Sta)<br>`math::tests::covarSample::testCovarSample` (Sta)<br>`math::tests::median::testMedian_Floats` (Sta)<br>`math::tests::median::testMedian_Integers` (Sta)<br>`math::tests::median::testMedian_Numbers` (Sta)<br>`math::tests::mode::testMode_Float` (Sta)<br>`math::tests::mode::testMode_Integer` (Sta)<br>`math::tests::mode::testMode_Number` (Sta) | planner fault 'Unused format args' building DuckDB's aggregate template over a literal list (Java String.format in its plan generation) (average/corr/covar/median/mode) | Standard:85, Standard:89, Standard:93, Standard:105, Standard:109, Standard:113, Standard:129, Standard:133, Standard:137, Standard:153, Standard:157, Standard:161 |

## B. M4's bucket 4: both fail, differently (25)

Messages are M4's (`m4-engine-parity-duckdb.md` §5). "Cheap to match" means: lite's engine door could give
legend-engine's failure without copying a legend-engine bug, a crash, or a test-harness wrapper.

| # | test | why legend-engine fails | why lite fails (engine route, channel A) | cheap to match? |
|---:|---|---|---|---|
| 1 | `collection::tests::at::testAtError` | refuses: `->at(...) function is supported only after direct access of 1->MANY properties` (manifest Essential:7) | lite raises Pure's message but channel A cannot recover the column of an error raised in the database (`Execution error column mismatch. Actual: 23 where expected: 37`; ledger rationale `Test_LegendLite_EssentialFunctions_PCT.java:37-42`). Channel B passes (`channel_b_duckdb_essential_outcomes.tsv:4`) | no: matching means refusing `at` on a literal list, which loses bucket 3's three `at` passes |
| 2 | `collection::tests::find::testFindInstance` | its compiled harness cannot evaluate the expression (`Error dynamically evaluating value specification`, Essential:37) | instance identity across channel A's wire (`assertIs` wants the interpreter's original object); channel B passes | no: a crash; lite's side is a harness artifact |
| 3 | `collection::tests::find::testFindUsingVarForFunction` | same as 2 (Essential:46) | same as 2 | no |
| 4 | `collection::tests::head::testHeadComplex` | `Cannot cast a collection of size 0 to multiplicity [1]` (Essential:82) | same as 2 | no |
| 5 | `date::tests::testAdjustByDaysBigNumber` | DuckDB: `java.sql.SQLException: Invalid Input Error: Type INT64 with value 12345678912 can't be cast ... INT32` (Essential:157) | the same DuckDB error, without the `java.sql.SQLException: ` prefix | **yes**: the engine door's error presenter writes a native database error as legend-engine's envelope does (a re-spelling, tenet 3). The test then fails exactly as legend-engine's manifest says (bucket 2); it still cannot pass (the answer lies past DuckDB's date range) |
| 6 | `date::tests::testAdjustByHoursBigNumber` | DuckDB: `Out of Range Error: Interval value 12345678912 hours out of range` with the prefix (Essential:160) | same, no prefix | **yes**, as 5 |
| 7 | `date::tests::testAdjustByMonthsBigNumber` | DuckDB INT64 to INT32 cast, with the prefix (Essential:169) | same, no prefix | **yes**, as 5 |
| 8 | `date::tests::testAdjustByWeeksBigNumber` | DuckDB INT64 to INT32 cast, with the prefix (Essential:175) | same, no prefix | **yes**, as 5 |
| 9 | `date::tests::testDayOfMonthError` | refuses: `DuckDB doesn't support YEAR and YEAR-MONTH` (DX:152; Essential:211; M4 filed it as a database error) | lite raises Pure's message; channel A's column cannot be recovered (`Actual: 23 where expected: 36`); channel B passes | no: matching means refusing year-only dates, which loses six bucket-3 passes (`testYear`, `testAdjustByYears`, `testDateDiffYears`, `testDatePartYearOnly`, `testEqDate`, `testEqualDateStrictYear`) |
| 10 | `date::tests::testHourError` | same refusal (Essential:235) | same as 9 | no |
| 11 | `date::tests::testMinuteError` | same refusal (Essential:241) | same as 9 | no |
| 12 | `date::tests::testNewDateError` | refuses: `No SQL translation exists for ... date_Integer_1__Integer_1__Date_1_` (Essential:250) | column mismatch as 9 (`expected: 29`); channel B passes | no |
| 13 | `date::tests::testSecondError` | same refusal as 9 (Essential:253) | same as 9 | no |
| 14 | `lang::tests::match::testMatchWithMixedReturnType` | refuses: `type not supported: ...LA_GeographicEntityType` (Essential:289) | lite refuses in channel A: `scalar lowering not yet implemented for TypedDeactivate` (A executes the let eagerly; `Test_LegendLite_EssentialFunctions_PCT.java:129-138`); channel B passes | no: both refuse, in their own words |
| 15 | `math::tests::rem::testRemError` | no error (Essential:304): `rem` is `mod(%s,%s)` (ED:267) and DuckDB answers `mod(5, 0)` without raising (NULL; unverified) | DIVIDE_BY_ZERO is one of the ten ENGINE_UNCHECKED raises (`core/src/main/java/com/legend/edge/PureText.java:169-174`), so lite sends plain `mod` too; the empty answer then fails channel A's interpreted harness (`Cannot cast a collection of size 0 to multiplicity [1]`) | not needed: the difference is the two harnesses' handling of an empty `[1]` answer. **With lite's check kept in engine mode (item 13) lite raises `Cannot divide 5 by zero` and passes** (it passes in Pure mode, M6) |
| 16 | `math::tests::testSquareRootError` | the database's error inside legend-engine's harness wrapper `Unexpected error executing function with params [Anonymous_Lambda]` (Essential:316; `sqrt(%s)`, ED:287) | SQRT_OF_NEGATIVE unchecked: DuckDB's own `Out of Range Error: cannot take square root of a negative number` | the wrapper is legend-engine's test harness's, not a product message: no. **With item 13, lite passes** |
| 17 | `math::tests::trigonometry::testArcCosineError` | the harness wrapper again (Essential:322); bare `acos(%s)` (ED:190), so DuckDB raises its domain error (the ledger comment's reading, `Test_LegendLite_EssentialFunctions_PCT.java:139-151`; the manifest keeps only the wrapper, so unverified) | lite's DuckDB dialect writes `CASE WHEN x BETWEEN -1 AND 1 THEN acos(x) ELSE 'NaN'::DOUBLE END` (`core/src/main/java/com/legend/sql/dialect/DuckDb.java:730-735`, "NaN outside the domain, as the engine's H2"); channel A's boundary refuses the NaN (`the database answered NaN for a Float, which has no NaN in Pure`) | **cheap but a product question**: legend-engine on DuckDB lets DuckDB raise; lite's NaN guard copies legend-engine's H2 and is a correction legend-engine's DuckDB does not make. Dropping it in engine mode matches legend-engine; adding Pure's raise (as compiled legend-pure does, section D) makes the test pass in both modes. Touches S119 |
| 18 | `math::tests::trigonometry::testArcSineError` | as 17 (Essential:325; `asin(%s)`, ED:195) | as 17 | as 17 |
| 19 | `string::tests::toString::testComplexClassToString` | refuses: `type not supported: ...ErrorType` (Essential:412) | channel A's adapter-composed model drops the class's own `toString()` qualifier, so lite prints the default `@_o1` (`Test_LegendLite_EssentialFunctions_PCT.java:77-85`); channel B passes | no |
| 20 | `collection::tests::filter::testFilterInstance` | `Error dynamically evaluating value specification ... error compiling generated Java code` (Grammar:49) | instance identity (channel A); channel B passes | no |
| 21 | `collection::tests::first::testFirstComplex` | `Expected at most one object, but found many` (Grammar:53) | instance identity; channel B passes | no |
| 22 | `collection::tests::map::testMapRelationshipFromManyToMany` | `Error dynamically evaluating value specification` (Grammar:65) | instance identity; channel B passes | no |
| 23 | `collection::tests::map::testMapRelationshipFromManyToOne` | same (Grammar:69) | instance identity; channel B passes | no |
| 24 | `collection::tests::map::testMapRelationshipFromOneToOne` | `Error during dynamic reactivation` (Grammar:73) | instance identity; channel B passes | no |
| 25 | `math::tests::bitShiftLeft::testBitShiftLeft_MoreThan62Bits` | DuckDB's overflow inside the harness wrapper (Standard:97; `(cast(%s as BIGINT) << cast(%s as BIGINT))`, DX:199) | SHIFT_OUT_OF_RANGE unchecked: DuckDB's own `Out of Range Error: Overflow in left shift (1 << 63)` | no (the wrapper). **With item 13, lite passes** (lite's bound check, `core/src/main/java/com/legend/lowering/Scalars.java:426-450`) |

Counts: 16 are lite's channel-A boundary against a legend-engine refusal or crash (1-4, 9-14, 19-24); 4 are the same
DuckDB error under a different envelope (5-8, cheap); 3 are unchecked raises (15, 16, 25) that pass with lite's checks;
2 are lite's DuckDB NaN guard (17, 18).

Bucket 2b, for the same item-13 question: `testSliceError` (SLICE_BOUNDS_INVERTED), `testRangeStepError`
(RANGE_STEP_ZERO) and `testBitShiftRight_MoreThan62Bits` (SHIFT_OUT_OF_RANGE) fail now exactly as legend-engine does
("No error was thrown", manifest Essential:112, Grammar:77, Standard:101) because lite leaves those raises plain on the
engine route; with lite's checks kept they pass, as they do in Pure mode. So keeping lite's checks in engine mode moves 6
tests from "fails like legend-engine" to "passes" and none the other way.

## C. `testBigFloatAbs`, M4's one bucket-5 test

The test (PURE `legend-pure-core/legend-pure-m3-core/src/main/resources/platform/pure/essential/math/operation/abs.pure:76-79`):
`assertEq(123456789123456789.99, $f->eval(|abs(-123456789123456789.99)))`.

**Does legend-engine pass it on DuckDB? Yes.** The `relational-duckdb` Essential manifest has no `abs::` row at all, and
the mechanism agrees:

1. legend-engine's DuckDB PCT runs on **compiled** legend-pure: `Test_Relational_DuckDB_PCT.java` (ENG
   `.../legend-engine-xt-relationalStore-duckdb-PCT/src/test/java/.../pct/Test_Relational_DuckDB_PCT.java`) sets
   `PLATFORM = "compiled"` (:42) and builds with `PureTestBuilderCompiled.buildPCTSurveyorSuite` (:80).
2. In compiled legend-pure a Float is a Java `double` (PURE `.../compiled/generation/processors/type/TypeProcessor.java:200-202`),
   so the expected literal `123456789123456789.99` is the double 1.2345678912345678E17 before the test starts.
3. legend-engine's literal processor writes a Float bare: `pair(Float, ^LiteralProcessor(format = '%s', transform =
   toString...))` (ED:134; DX's DuckDB map, DX:133-142, does not override Float). DuckDB types a bare decimal literal as
   DECIMAL, so the database computes `abs` exactly on whatever digits arrive. Those digits are already the double's
   when the value in the compiled graph is a double (compiled `primitiveToString(double)` prints it with
   `DECIMAL_FORMAT`, PURE `.../compiled/generation/processors/support/CompiledSupport.java:1271-1274`): unverified which
   text reaches the SQL, and it does not matter, because:
4. the answer is read back as a Float, a double, and compared with the expected double. Compiled `eq` of a
   `BigDecimal` and a `Double` is false (`CompiledSupport.java:994-1001`), so legend-engine's pass shows it does read
   the result back as a double. Both sides lose the same digits; `assertEq` holds.

So "legend-engine loses the digits too" (S120, `docs/SEMANTICS_REGISTER.md:155`; `docs/DIFFERENCES_FROM_LEGEND_ENGINE.md:396-397`)
is true, but it loses them on **both** sides of the assertion, which is why it passes. The DECIMAL typing of the bare
literal is irrelevant to the outcome.

**Why lite fails.** Lite's channel A, and the engine lane M4 measured with it, run on **interpreted** legend-pure:
`Test_LegendLite_EssentialFunctions_PCT.java:14` imports `PureTestBuilderInterpreted`, `:25` sets `platform =
"interpreted"`; `Test_LegendLite_Engine_PCT.java:20,36,67` the same. The interpreter holds a Float literal as a
`BigDecimal` with all twenty digits (S119's and S120's own text), so the harness's expected value is
`123456789123456789.99`, while lite answers the double, printed `123456789123456780.0`. The ledger row says exactly
this (`Test_LegendLite_EssentialFunctions_PCT.java:54-55`: "the interpreter holds a BigDecimal, lite a double"). Channel
B, where both sides are doubles in the database, passes (`pct/src/test/resources/edge/channel_b_duckdb_essential_outcomes.tsv:184`;
SQLite too, `channel_b_sqlite_essential_outcomes.tsv:184`).

So M4's bucket 5 is a harness artifact, not a difference from legend-engine: lite and legend-engine agree on the value
(the double). The engine lane's `LITE_BEHIND` pin (`pct/engine_lane/duckdb_essential.tsv:186`) compares lite under an
interpreted harness with legend-engine under a compiled one. Under ruling 4 (`pure` = compiled legend-pure) the
reference passes this test too, so in Pure mode it is the same artifact (section D).

**Does deleting the DECIMAL Float literal (ruling 11) interact? No.** Since S20 (2026-11-17) lite's parser keeps no
digits a double cannot hold (S120's text), so the literal is the double before any SQL is written; whether the edge
spells it as a DECIMAL or a DOUBLE literal, `abs` answers the same double and the interpreted harness still holds
twenty digits. M1 measured the deletion: engine lanes 0 moves, PCT A DuckDB no outcome moves. The new register row
(legend-engine on DuckDB computes Float literal arithmetic as DECIMAL) does not bite here: `abs` of one literal rounds
nothing. Where it does bite in this sort is `testToDegrees` and `testToRadians` (bucket 3, class 2): legend-engine's
DECIMAL arithmetic overflows DECIMAL(18); lite's IEEE answer passes before and after the deletion.

What to correct: S120's sentence on legend-engine (and the DIFFERENCES page line) should say legend-engine passes the
PCT test because both sides are doubles; M4's bucket 5 should read 0 (a harness difference). Running channel A on
compiled legend-pure would remove the artifact in both modes.

## D. M6's Pure-mode failures: the reference passes, lite fails

The reference is compiled legend-pure (ruling 4). Classes:

- **database limitation**: a range, precision or feature the database lacks, named;
- **lite bug or gap**: lite could fix it;
- **ruled divergence**: a register row the user ruled;
- **channel-A-only artifact**: channel A's boundary (the interpreted harness, the wire between it and lite), shown by
  channel B passing the same test on the same database.

Ledger rows are lite's (`pct/src/test/java/org/finos/legend/lite/pct/Test_LegendLite_<Db>_<Suite>Functions_PCT.java:<line>`, DuckDB's
without `<Db>_`); channel B rows are `pct/src/test/resources/edge/channel_b_<db>_<suite>_outcomes.tsv:<line>`.

### D1. DuckDB (24): database limitation 4, lite gap 0, ruled 2, channel-A-only 18

| class | tests | why | lite ledger | channel B |
|---|---|---|---|---|
| database limitation | `testAdjustByDaysBigNumber`, `testAdjustByHoursBigNumber`, `testAdjustByMonthsBigNumber`, `testAdjustByWeeksBigNumber` | the expected dates lie at years 1.4M to 800M; DuckDB's DATE/TIMESTAMP range is 290309 BC to 294246 AD, and its interval constructors take INT32 (the user's own example) | Essential:125-128 | fails too, `Conversion Error: date field value out of range` (essential:112, 114, 122, 126) |
| ruled divergence, **premise contradicted** | `testArcCosineError`, `testArcSineError` | S119 (`docs/SEMANTICS_REGISTER.md:154`, ruled 2026-11-03, "IEEE it is then"): lite's Float is IEEE, NaN is a value. Lite's DuckDB dialect even turns DuckDB's own domain error into NaN (`DuckDb.java:730-735`), and channel A's boundary then refuses the NaN. But S119's reason that compiled legend-pure "maps Float to Java double" does not carry to these functions: compiled legend-pure raises `Unable to compute acos of 2.0` / `asin` when the result is NaN (PURE `.../compiled/CoreHelper.java:141-148`, `126-133`), exactly as it does for `sqrt` (`:515-520`), which lite raises already (`core/src/main/java/com/legend/error/ErrorKind.java:40`, SQRT_OF_NEGATIVE). Under ruling 4 these two are a lite gap unless the user keeps S119 for them | Essential:152-153 | fails too, "No error was thrown" (essential:254, 256; channel B ledger `channelb/ledger.tsv:29`) |
| channel-A-only | `testAtError`, `testDayOfMonthError`, `testHourError`, `testMinuteError`, `testNewDateError`, `testSecondError` | the error's column: lite raises Pure's message and line, but the column of a call that fails inside the database cannot cross channel A's wire | Essential:107-112 (rationale :37-42) | passes (essential:4, 149, 158, 160, 162, 164) |
| channel-A-only | `testFindInstance`, `testFindUsingVarForFunction`, `testHeadComplex`, `testFilterInstance`, `testFirstComplex`, `testMapRelationshipFromManyToMany`, `testMapRelationshipFromManyToOne`, `testMapRelationshipFromOneToOne` | instance identity: `assertIs` wants the interpreter's original object back | Essential:97, 98, 104; Grammar:44, 50, 60, 61, 64 | passes (essential:26, 29, 45; grammar:56, 60, 72-74) |
| channel-A-only | `testBigFloatAbs` | the interpreted harness holds the expected literal as a 20-digit BigDecimal; compiled legend-pure (the reference) and lite both hold the double (section C). Register S120 is "the user to confirm", not ruled | Essential:55 | passes (essential:184) |
| channel-A-only | `testComplexClassToString` | the adapter-composed model drops the class's own `toString()` qualifier, so lite prints `@_o1` | Essential:85 | passes (essential:324) |
| channel-A-only | `testMatchWithMixedReturnType` | A executes the `let` eagerly, so `deactivate` reaches the lowering (`scalar lowering not yet implemented for TypedDeactivate`) | Essential:138 | passes (essential:181) |
| channel-A-only | `testVariantArrayColumn_joinStrings` | the TDS text on the wire cannot carry an empty-string cell (`4,'null',` arrives as `null`); legend-engine's DuckDB manifest pins the same failure (Relation:4) | Relation:45 | passes (relation:47) |

### D2. SQLite (28): database limitation 8, lite gap 0, ruled 2, channel-A-only 18

| class | tests | why | lite ledger | channel B |
|---|---|---|---|---|
| database limitation | `testAdjustByDaysBigNumber`, `testAdjustByHoursBigNumber`, `testAdjustByMinutesBigNumber`, `testAdjustByMonthsBigNumber`, `testAdjustByWeeksBigNumber` | SQLite's dates are text of the years 0000-9999 (`a date past the years 0000-9999 has no SQLite value`); testAdjustByMinutesBigNumber needs years 25489 and -21457, the rest as on DuckDB | SQLite_Essential:48-51, 73 | fails too (essential:112-126; `channelb/ledger.tsv:63-67`) |
| database limitation | `testFormatDate` | `%t{[EST]...}` / `%t{[CET]...}`: SQLite has no time zones but UTC | SQLite_Essential:75 | fails too (essential:268) |
| database limitation | `TestJoin_CurrentUserId`, `testCurrentUserId` | an embedded database has no session user (`CURRENT_USER_FN reached SQLite`) | SQLite_Relation:38; SQLite_Unclassified:35 | fails too (relation:8, unclassified:20) |
| ruled divergence, premise contradicted | `testArcCosineError`, `testArcSineError` | as DuckDB; SQLite cannot even hold the NaN, so lite words its own refusal (`acos of a value outside [-1, 1] is NaN, which this database cannot hold`). With Pure's raise this database limitation disappears too | SQLite_Essential:66-67 | fails too (essential:254, 256) |
| channel-A-only | `testAtError`, `testDayOfMonthError`, `testHourError`, `testMinuteError`, `testNewDateError`, `testSecondError` | the error's column, as DuckDB | SQLite_Essential:42, 52-56 | passes |
| channel-A-only | `testFindInstance`, `testFindUsingVarForFunction`, `testHeadComplex`, `testFilterInstance`, `testFirstComplex`, `testMapRelationshipFromManyToMany`, `testMapRelationshipFromManyToOne`, `testMapRelationshipFromOneToOne` | instance identity, as DuckDB | SQLite_Essential:37-39; SQLite_Grammar:36-39, 48 | passes |
| channel-A-only | `testBigFloatAbs` | as DuckDB. The ledger's older note ("SQLite has no exact decimal ... so the digits are the database's limit", SQLite_Essential:58-60) is not the cause; its last line (:64, "the interpreter holds a BigDecimal, lite a double") is: compiled legend-pure loses the same digits, and channel B passes on SQLite | SQLite_Essential:65 | passes (essential:184) |
| channel-A-only | `testComplexClassToString`, `testMatchWithMixedReturnType`, `testVariantArrayColumn_joinStrings` | as DuckDB | SQLite_Essential:70, 57; SQLite_Relation:36 | passes |

Not in the 28 but worth a line: `testLargeMinus`, `testLargePlus`, `testLargeTimes` fail on SQLite (`an integer past 64
bits has no SQLite value`, SQLite_Grammar:53-55) and on compiled legend-pure, so M6 counts them "both fail". On DuckDB
lite passes them where the compiled reference fails (bucket 3's flagged E1: lite's HUGEINT widening).

### D3. H2, Relation only (20): database limitation 16, lite gap 4

legend-engine's own H2 fails all 20 as well (its `relational-h2` RelationFunctions manifest lines 25, 28, 31, 34, 37, 43,
46, 58, 61, 85, 88, 97, 103, 109, 112, 115, 118, 157, 160, 163), so none is a gap against legend-engine.

| class | tests | why | lite ledger (H2_Relation) | channel B |
|---|---|---|---|---|
| database limitation | `testVariantArrayColumn_joinStrings`, `testVariantArrayColumn_reverse`, `testVariantArrayColumn_sort`, `testVariantColumn_contains`, `testVariantColumn_distinct_removeDuplicates`, `testVariantColumn_functionComposition`, `testVariantColumn_indexOf`, `testVariantColumn_filter`, `testVariantColumn_fold`, `testVariantColumn_map`, `testVariantColumn_filterOutputFromLambda`, `testFlatten_LateralJoin_Nested_Extend` (12) | an operation over each element of a row's JSON array (filter, map, sort, reverse, position, membership, fold, join). H2 has no list functions over a value and no correlated table-function arguments (no LATERAL, no `UNNEST(t.col)`): lite's probed capability notes, `core/src/main/java/com/legend/sql/dialect/H2.java:25-30`, and `CAPS`, `:227-229`. legend-engine's 2.1.214 reaches these through Java functions loaded into the database, a route lite's H2 dialect bans (`H2.java:29-30`); so this is a limitation of H2's SQL, not of H2 with Java in it | :47-50, 53-54, 56-61 (:55 is `testVariantColumn_slice`, which the compiled reference fails too) | fails too (h2_relation:47-51, 55, 56, 160, 161, 164, 171, 465) |
| database limitation | `testFlatten_LateralJoin`, `testFlatten_LateralJoin_Nested`, `testVariantMapColumn_keys_LateralFlatten`, `testVariantMapColumn_values_LateralFlatten` (4) | flatten of a row's JSON array or map: a correlated UNNEST, which H2 has no placement for | :63-66 | fails too (h2_relation:62, 63, 463, 464) |
| **lite gap** | `testLateralJoin`, `testLateralJoinAreInnerJoins`, `testLateralJoin_Chained`, `testLateralJoin_CorrelatedColumnNameAlsoOnInner` (4) | H2 has no LATERAL (`Function "LATERAL" not found`), but none of these needs it: each body is `$t->lateral(x \| $t2->filter(y \| $y.k == $x.k))` (plus a `select` in one), a correlated filter, i.e. an inner join on that predicate (ENG `.../core_functions_relation/relation/functions/transformation/lateral.pure:25, 65, 121, 162`). Rewriting such a lateral to a join rearranges a shape and computes nothing (the world-map tenet), and works on every database. Medium effort; the chained case is two joins | :71-74 | fails too (h2_relation:246-249) |

### D4. Where channel A and channel B disagree (59 = M6's 36 + 23)

**Channel A fails, B passes (36): all channel-A-only artifacts**, the same 18 on DuckDB and 18 on SQLite as above:
the error's column (6 + 6), instance identity (8 + 8), `testBigFloatAbs` (1 + 1), `testComplexClassToString` (1 + 1),
`testMatchWithMixedReturnType` (1 + 1), `testVariantArrayColumn_joinStrings` (1 + 1).

**Channel A passes, B fails (23): channel-B-only.** Channel B plans the whole test body, including the harness's own
checking code that channel A leaves to the interpreter (for example `$res->extend(~newColSorted : c |
$c.newCol->toOne()->chunk(1)->sort()->joinStrings(''))` after `$f->eval`, ENG `.../core_functions_relation/relation/functions/transformation/groupBy.pure:87`).

| db | tests | class | why | channel B ledger |
|---|---|---|---|---|
| SQLite | `testSimpleGroupBy_{SingleSingle, SingleSingle_MultipleExpressions, MultipleSingle, MultipleSingle_MultipleExpressions, SingleMultiple, SingleMultiple_MultipleExpressions, MultipleMultiple, MultipleMultiple_MultipleExpressions}`, `testOLAPAggStringWithPartitionAndUnboundedWindow`, `testJoinStrings_GroupBy_Unordered` (10) | **lite gap, cheap** | the checking code's `chunk(1)`: lite lowers `chunk(s, n)` to `regexp_extract_all(s, '.{1,' \|\| CAST(n AS VARCHAR) \|\| '}')` even when `n` is a literal (`core/src/main/java/com/legend/lowering/Scalars.java:1809-1820`), and SQLite's dialect takes only a literal pattern (`a regexp_extract_all pattern that is not a literal reached SQLite`). A literal `n` gives a literal pattern (folding a constant is allowed) | `channelb/ledger.tsv:82-91` |
| SQLite | `testDateDiffWithDifferentTimeZones` | lite gap, probable (ledgered as SQLite having no time zones) | `parseDate('2014-12-31T23:00:00.000-0500')` runs in SQLite in channel B and answers nothing (`expected: [0], actual: []`). An offset is not a time zone: SQLite's date functions accept a `[+-]HH:MM` suffix, so `-0500` needs only its colon (unverified) | `channelb/ledger.tsv:68-69` |
| H2 | `testSimpleGroupBy_*` (8), `testOLAPAggStringWithPartitionAndUnboundedWindow`, `testJoinStrings_GroupBy_Unordered`, `testExtendJoinStringOnNull` (11) | database limitation | the checking code splits one value into a list and sorts it (`chunk(1)` / `split(':')`, `sort`, `joinStrings`): a per-row list, which H2 cannot hold or iterate (as D3) | `channelb/ledger.tsv:110-135` |
| H2 | `testCoalesceInPreFilter` | lite gap, unverified | `LIST_GET reached a dialect without a list encoding`; channel A passes this test on H2 through `CarrierStrategies.filteredLiteralLength` (the H2 ledger's note, `Test_LegendLite_H2_RelationFunctions_PCT.java:51-52`), so channel B takes another path to the same literal list | `channelb/ledger.tsv:140` |

### D5. Lite fix list for Pure mode

In order of value for cost:

1. **`acos`/`asin` raise Pure's error** (`Unable to compute acos of 2.0`), as compiled legend-pure does
   (`CoreHelper.java:141-148`, `126-133`) and as lite already does for `sqrt` (ErrorKind SQRT_OF_NEGATIVE): 2 tests on
   DuckDB, 2 on SQLite, both channels. Needs the user to reopen S119 for these two functions (its premise, compiled
   legend-pure holding a double, is right; its conclusion for acos/asin is not). Also decide `DuckDb.java:730-735`'s NaN
   guard, which copies legend-engine's H2 and matches neither Pure nor legend-engine on DuckDB. Cheap.
2. **Run channel A on compiled legend-pure** (ruling 4): today channel A and the engine lane use
   `PureTestBuilderInterpreted`. Removes `testBigFloatAbs` on every lane, M4's bucket 5, and any other
   interpreted-versus-compiled difference. Medium (harness). The 17 other channel-A artifacts per lane (column, identity,
   toString qualifier, `deactivate`, the TDS empty string) are the wire's limits; channel B is the measure for those.
3. **H2: rewrite a correlated-filter lateral into an inner join**: 4 tests (D3). Medium.
4. **SQLite channel B: a literal `chunk` size gives a literal pattern** (`Scalars.java:1813-1820`): 10 channel-B rows.
   Cheap.
5. **SQLite channel B: `parseDate` with a `+HHMM` offset**: 1 row (`testDateDiffWithDifferentTimeZones`). Cheap,
   unverified.
6. **H2 channel B: `testCoalesceInPreFilter`** takes the `LIST_GET` path channel A avoids. Unverified.
7. Not a fix, a consistency question for ruling 4: on DuckDB lite passes `testLargePlus`, `testLargeMinus`,
   `testLargeTimes`, which compiled legend-pure fails (its manifest, `pct_grammar_compile_native.json:5, 9, 13`),
   through a literal-magnitude special case (`Scalars.java:2871-2883`). Keep it, or follow compiled Pure's 64 bits.

With 1 and 2 done, every Pure-mode failure left on DuckDB is DuckDB's date range (4), as the user expected; on SQLite
its date range, time zones and session user (8); on H2 its missing list and LATERAL support (16).

## Method and limits

- Bucket memberships are M4's and M6's, unchanged (`scratchpad/r1/m4.json`, `m6.json`); the A-versus-B list is recomputed
  from `m6.json` and the committed channel-B recordings and gives M6's 36 + 23 exactly.
- Every bucket-3 test body was read (215 of 215 found in PURE or ENG) and checked for `assertError`/`assertFail`: none.
- legend-engine's SQL was not run. Where a reason gives a mechanism the manifest's message does not show, it is read
  from DX/ED or inferred, and inferred ones say **unverified**: rows 48 (BC read-back), 85 (`pow` argument binding),
  125 (`startsWith` escaping), 129/134 (which enumeration assert fails), 155 (`--1` as a comment), 210-213 (regexp
  flags), the run-time faults' "no value", bucket 4's #15-18 (what DuckDB answered under the harness wrapper), and
  D4's two channel-B gaps.
- "Database limitation" for H2 relies on lite's own probed capability notes (`H2.java:25-30`), not on a new probe.
- Working files: `scratchpad/r2/b3j.tsv` (bucket 3 joined with the engine lane), `b3_sorted.json` (this sort, one
  record per test), `ab.tsv` (the 59 A/B disagreements), `manloc.json` (manifest line per test); scripts in
  `scratchpad/r2tools/`.
