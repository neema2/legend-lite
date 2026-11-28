# Slice 7: the probe's tables (2026-11-28)

*The full output behind `slice-7-PLAN-2026-11-28.md`, section 1. Produced by the temporary probe
`slice-7-probe-2026-11-28.patch.txt` (never committed to a product branch) on `claude/step-6b-next` at `4be6626b`.
Columns: the door's table a call was lowered with (`EngineTable.TESTS` is legend-engine's tests' door, the relational
corpus; `EngineTable.DOOR` its API, pure/v1; `EngineTable.PCT` its PCT run; `PureTable` legend-pure's door;
`LiteEditionTable` lite's own, or a lowerer built with the product's style); the comparison setting the
type-checker held when it typed the call; who made the call; and whether lite's rule would write the same SQL record
as the door's rule did for the same call and lowered arguments ("same"), a different one (`DIFF[...]`: `R` the rule's
own output differs, `E` the empty side differs, `V` lite's "same values" instruction would apply), or would refuse
(`LITE-RULE-THROWS`). "(reverse)" is the same question asked the other way, for a call typed under a door's setting
and lowered with lite's table. A star marks a call the type-checker's hook rebuilt (its mark is on the call it
rebuilt).*

## 1. Comparisons reaching the lowering, by suite


### The relational corpus, DuckDB (bazel run //spec:corpus_one -- duckdb host)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| EngineTable.TESTS | - | store resolver: AssociationJoins.pkEqualityCond | same | 137 | `meta::relational::tests::advanced::forcedselfjoin::isolationTest` |
| EngineTable.TESTS | - | store resolver: CastReRoot.reRoot | same | 20 | `meta::pure::executionPlan::tests::testDatabaseConnectionSQLPopulation` |
| EngineTable.TESTS | - | store resolver: ChainNormalizer.identityEquality | same | 26 | `meta::relational::tests::mapping::include::testStoreSubstitution` |
| EngineTable.TESTS | - | store resolver: ChainNormalizer.keyIdentity | same | 4 | `meta::relational::tests::mapping::include::testStoreSubstitution` |
| EngineTable.TESTS | - | store resolver: ClassSources.wholeSourceStep | same | 10 | `meta::pure::graphFetch::tests::XStoreUnion::inMemoryAndRelational::testNestedUnionCrossStore` |
| EngineTable.TESTS | - | store resolver: ElementReferences.elementRowByKey | same | 911 | `meta::relational::functions::toPostgresModel::tests::testConvertAlias` |
| EngineTable.TESTS | - | store resolver: ObjectReferenceArms.runtimeRefMembership | same | 1 | `meta::relational::graphFetch::tests::simple::testObjectReferenceInUsingResultReferences` |
| EngineTable.TESTS | - | store resolver: Substitution.pkMembership | same | 41 | `meta::relational::tests::functions::objectReferenceIn::testObjectReferenceInEmbeddedMapping` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteCallArms | same | 48 | `meta::relational::tests::advanced::resultSourcing::relationalResultSourcingOfDateList` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteMembershipExists | same | 31 | `meta::relational::tests::mapping::enumeration::testEnumMappings` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteTdsContains | same | 40 | `meta::relational::tests::tds::tdsContains::testTDSContainsType1` |
| EngineTable.TESTS | - | store resolver: SyntheticHeads.positionalRows | same | 1 | `meta::relational::functions::toPostgresModel::tests::testConvertTableAliasColumn` |
| EngineTable.TESTS | - | store resolver: TemporalFrame.cmpCall | same | 759 | `meta::relational::tests::mapping::union::testChainedUnions` |
| EngineTable.TESTS | - | store resolver: TemporalFrame.dateCmpCall | same | 9188 | `ational::tests::milestoning::businessdate::testBusinessDateInjectionFromVarReferenceWithProject` |
| EngineTable.TESTS | Engine | type-checker: BODY | LITE-RULE-THROWS:ModelException | 17 | `meta::relational::tests::advanced::contractmoneyscenario::test1` |
| EngineTable.TESTS | Engine | type-checker: BODY | same | 6075 | `meta::relational::tests::mapping::enumeration::testEnumTheSame` |
| EngineTable.TESTS | Engine | type-checker: GENERIC_BODY | LITE-RULE-THROWS:ModelException | 4 | `meta::pure::tds::tests::extensions::iqrClassifyTest` |
| EngineTable.TESTS | Engine | type-checker: GENERIC_BODY | same | 166 | `meta::pure::tds::tests::extensions::columnValueDifferenceTest` |
| EngineTable.TESTS | Engine | type-checker: QUERY | DIFF[E] | 42 | `meta::pure::executionPlan::tests::testOptionalEnumParameterEqualsClassProp` |
| EngineTable.TESTS | Engine | type-checker: QUERY | LITE-RULE-THROWS:ModelException | 6 | `meta::relational::tests::projection::filter::dates::now::testNow` |
| EngineTable.TESTS | Engine | type-checker: QUERY | same | 6425 | `meta::relational::tests::projection::filter::in::testContains` |
| EngineTable.TESTS | Engine | type-checker: UNREAD | same | 23212 | `meta::external::store::relational::modelJoins::test::testJoinWithConstantDate` |
| LiteEditionTable | AS_WRITTEN | type-checker: BODY | n/a | 1 | `meta::pure::lineage::scanColumns::test::testQualifier` |
| LiteEditionTable | AS_WRITTEN | type-checker: UNREAD | n/a | 9 | `meta::pure::lineage::scanColumns::test::testQualifier` |
| LiteEditionTable | Engine | type-checker: BODY | n/a | 4620 | `meta::relational::tests::mapping::enumeration::testEnumTheSame` |
| LiteEditionTable | Engine | type-checker: GENERIC_BODY | n/a | 8 | `meta::pure::tds::tests::extensions::zScoreTest` |
| LiteEditionTable | Engine | type-checker: QUERY | n/a | 561 | `meta::relational::tests::tds::tdsJoin::testTableToTDSWithQuotesGroupBy` |

total comparison calls: 52363

### The relational corpus, H2 (bazel run //spec:corpus_one -- h2 host)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| EngineTable.TESTS | - | store resolver: AssociationJoins.pkEqualityCond | same | 137 | `meta::relational::tests::advanced::forcedselfjoin::isolationTest` |
| EngineTable.TESTS | - | store resolver: CastReRoot.reRoot | same | 20 | `meta::pure::executionPlan::tests::testDatabaseConnectionSQLPopulation` |
| EngineTable.TESTS | - | store resolver: ChainNormalizer.identityEquality | same | 26 | `meta::relational::tests::mapping::include::testStoreSubstitution` |
| EngineTable.TESTS | - | store resolver: ChainNormalizer.keyIdentity | same | 4 | `meta::relational::tests::mapping::include::testStoreSubstitution` |
| EngineTable.TESTS | - | store resolver: ClassSources.wholeSourceStep | same | 10 | `meta::pure::graphFetch::tests::XStoreUnion::inMemoryAndRelational::testNestedUnionCrossStore` |
| EngineTable.TESTS | - | store resolver: ElementReferences.elementRowByKey | same | 901 | `meta::relational::tests::mapping::enumeration::testEnumMappings` |
| EngineTable.TESTS | - | store resolver: Substitution.pkMembership | same | 35 | `meta::relational::tests::functions::objectReferenceIn::testObjectReferenceInEmbeddedMapping` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteCallArms | same | 48 | `meta::relational::tests::advanced::resultSourcing::relationalResultSourcingOfDateList` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteMembershipExists | same | 31 | `meta::relational::tests::mapping::enumeration::testEnumMappings` |
| EngineTable.TESTS | - | store resolver: Substitution.rewriteTdsContains | same | 40 | `meta::relational::tests::tds::tdsContains::testTDSContainsType1` |
| EngineTable.TESTS | - | store resolver: TemporalFrame.cmpCall | same | 747 | `meta::relational::tests::mapping::union::testChainedUnions` |
| EngineTable.TESTS | - | store resolver: TemporalFrame.dateCmpCall | same | 9064 | `ational::tests::milestoning::businessdate::testBusinessDateInjectionFromVarReferenceWithProject` |
| EngineTable.TESTS | Engine | type-checker: BODY | LITE-RULE-THROWS:ModelException | 17 | `meta::relational::tests::advanced::contractmoneyscenario::test1` |
| EngineTable.TESTS | Engine | type-checker: BODY | same | 3890 | `meta::relational::tests::mapping::enumeration::testEnumTheSame` |
| EngineTable.TESTS | Engine | type-checker: GENERIC_BODY | LITE-RULE-THROWS:ModelException | 2 | `meta::pure::tds::tests::extensions::iqrClassifyTest` |
| EngineTable.TESTS | Engine | type-checker: GENERIC_BODY | same | 161 | `meta::pure::tds::tests::extensions::columnValueDifferenceTest` |
| EngineTable.TESTS | Engine | type-checker: QUERY | DIFF[E] | 42 | `meta::pure::executionPlan::tests::testOptionalEnumParameterEqualsClassProp` |
| EngineTable.TESTS | Engine | type-checker: QUERY | LITE-RULE-THROWS:ModelException | 6 | `meta::relational::tests::projection::filter::dates::now::testNow` |
| EngineTable.TESTS | Engine | type-checker: QUERY | same | 6339 | `meta::relational::tests::projection::filter::in::testContains` |
| EngineTable.TESTS | Engine | type-checker: UNREAD | same | 23004 | `meta::external::store::relational::modelJoins::test::testJoinWithConstantDate` |
| LiteEditionTable | AS_WRITTEN | type-checker: BODY | n/a | 1 | `meta::pure::lineage::scanColumns::test::testQualifier` |
| LiteEditionTable | AS_WRITTEN | type-checker: UNREAD | n/a | 9 | `meta::pure::lineage::scanColumns::test::testQualifier` |
| LiteEditionTable | Engine | type-checker: BODY | n/a | 2417 | `meta::relational::tests::mapping::enumeration::testEnumTheSame` |
| LiteEditionTable | Engine | type-checker: QUERY | n/a | 554 | `meta::relational::tests::tds::tdsJoin::testTableToTDSWithQuotesGroupBy` |

total comparison calls: 47505

### PCT channel A, legend-pure's door, DuckDB (//pct:pct_duckdb_*)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| PureTable | - | type-checker, a function named as a value (no hook) | same | 6 | `?` |
| PureTable | - | type-checker: BODY* | same | 2 | `?` |
| PureTable | Pure | type-checker: BODY | same | 1 | `?` |
| PureTable | Pure | type-checker: QUERY | same | 372 | `?` |

total comparison calls: 381

### PCT channel A, legend-pure's door, H2 (//pct:pct_h2_*)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| PureTable | - | type-checker, a function named as a value (no hook) | same | 5 | `?` |
| PureTable | - | type-checker: BODY* | same | 2 | `?` |
| PureTable | Pure | type-checker: BODY | same | 1 | `?` |
| PureTable | Pure | type-checker: QUERY | same | 356 | `?` |

total comparison calls: 364

### PCT channel B, legend-pure's door (//pct:pct_channel_b_*, //pct:pct_channel_b_h2_relation)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| LiteEditionTable | - | type-checker: BODY* | n/a | 16 | `meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition` |
| LiteEditionTable | Pure | type-checker: QUERY | same (reverse) | 1716 | `meta::pure::functions::relation::tests::joinStrings::testJoinStrings_GroupBy_ForcingSubSelect` |
| PureTable | - | type-checker, a function named as a value (no hook) | same | 6 | `ons::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit` |
| PureTable | - | type-checker: BODY* | same | 18 | `meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition` |
| PureTable | Pure | type-checker: BODY | same | 1 | `meta::pure::functions::lang::tests::letFn::testLetChainedWithAnotherFunction` |
| PureTable | Pure | type-checker: QUERY | same | 1342 | `meta::pure::functions::boolean::tests::equality::eq::testEqInteger` |

total comparison calls: 3099

### PCT, lite's own lanes, channel A (//pct:pct_duckdb_lite_*)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| LiteEditionTable | - | type-checker, a function named as a value (no hook) | n/a | 6 | `?` |
| LiteEditionTable | - | type-checker: BODY* | n/a | 2 | `?` |
| LiteEditionTable | Lite | type-checker: BODY | n/a | 1 | `?` |
| LiteEditionTable | Lite | type-checker: QUERY | n/a | 345 | `?` |

total comparison calls: 354

### PCT, lite's own lanes, channel B (//pct:pct_channel_b_lite_*)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| LiteEditionTable | - | type-checker, a function named as a value (no hook) | n/a | 24 | `ons::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit` |
| LiteEditionTable | - | type-checker: BODY* | n/a | 128 | `meta::pure::functions::relation::tests::composition::testVariantColumn_functionComposition` |
| LiteEditionTable | Lite | test planning (an assert as its comparison), typed through comparedAsInAQuery | n/a | 3140 | `meta::pure::functions::flow::test::coalesce::coalesce_NotEmpty` |
| LiteEditionTable | Lite | type-checker: BODY | n/a | 4 | `meta::pure::functions::lang::tests::letFn::testLetChainedWithAnotherFunction` |
| LiteEditionTable | Lite | type-checker: QUERY | n/a | 6800 | `meta::pure::functions::relation::tests::composition::testVariantColumn_contains` |

total comparison calls: 10096

### Core's edge tests (//core:core_edge_tests)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| EngineTable.DOOR | - | store resolver: ElementReferences.elementRowByKey | same | 2 | `m.legend.server.PureV1SqlTextTest#anOrdinaryRequestIsUntouchedAndASqlTextRequestAnswersLitesSql` |
| EngineTable.DOOR | Engine | type-checker: BODY | same | 6 | `ningEngineTest#aBodyComparingAcrossTypesIsAdmittedOnEveryRoute_litesStrictnessIsLitesDoorsAlone` |
| EngineTable.DOOR | Engine | type-checker: QUERY | DIFF[E] | 1 | `com.legend.edge.UpstreamTiesTest#anEmptySideIsAnsweredAsEachAudienceAnswersIt` |
| EngineTable.DOOR | Engine | type-checker: QUERY | DIFF[R] | 8 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| EngineTable.DOOR | Engine | type-checker: QUERY | LITE-RULE-THROWS:ModelException | 8 | `om.legend.edge.UpstreamTiesTest#everyComparisonTheProductRefusesOverColumnsIsTheDoorsOwnAtADoor` |
| EngineTable.DOOR | Engine | type-checker: QUERY | same | 1019 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| EngineTable.DOOR | Engine | type-checker: UNREAD | same | 10 | `m.legend.server.PureV1SqlTextTest#anOrdinaryRequestIsUntouchedAndASqlTextRequestAnswersLitesSql` |
| EngineTable.PCT | Engine | type-checker: QUERY | DIFF[R] | 4 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| EngineTable.PCT | Engine | type-checker: QUERY | LITE-RULE-THROWS:ModelException | 8 | `om.legend.edge.UpstreamTiesTest#everyComparisonTheProductRefusesOverColumnsIsTheDoorsOwnAtADoor` |
| EngineTable.PCT | Engine | type-checker: QUERY | same | 2 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| EngineTable.TESTS | Engine | type-checker: QUERY | same | 21 | `com.legend.edge.EngineTestsStyleTest#aTestsLetReadInsideAQueryIsPures` |
| LiteEditionTable | - | store resolver: ElementReferences.elementRowByKey | n/a | 477 | `com.legend.testplan.BodyRouterParityTest#consumedElementReferencePrograms` |
| LiteEditionTable | - | store resolver: TemporalFrame.dateCmpCall | n/a | 48 | `com.legend.testplan.BodyRouterParityTest#framesUnderTheDatabaseJudge` |
| LiteEditionTable | - | type-checker: BODY* | n/a | 52 | `com.legend.testplan.BodyRouterParityTest#lineagePrograms` |
| LiteEditionTable | - | type-checker: BODY, answered at its call* | n/a | 1 | `com.legend.edge.UpstreamTiesTest#aFunctionWrittenForNumberAtLegendPuresDoor` |
| LiteEditionTable | Engine | type-checker: BODY | same (reverse) | 2 | `edge.EngineTablePlacementTest#aCheckedGraphFetchsConstraintAndItsQualifiedPropertyAreEachPlaced` |
| LiteEditionTable | Engine | type-checker: QUERY | same (reverse) | 24 | `.PureV1ApiTest#e8_execute_aParameterWithNoValue_isRefused_aStrayValueIgnored_asTheEngineAnswers` |
| LiteEditionTable | Engine | type-checker: UNREAD | same (reverse) | 14 | `com.legend.server.PureV1ApiTest#e8_execute_anEnumerationColumn_isAStringInTheBuilder` |
| LiteEditionTable | Lite | test planning (an assert as its comparison), typed through comparedAsInAQuery | n/a | 274 | `com.legend.testplan.BodyRouterParityTest#assertProgramsUnderTheDatabaseJudge` |
| LiteEditionTable | Lite | type-checker: QUERY | n/a | 875 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| LiteEditionTable | Lite | type-checker: UNREAD | n/a | 1230 | `com.legend.planner.QueryDoorTest#anInlineLazyExecuteReadIsPlanned` |
| PureTable | - | store resolver: ElementReferences.elementRowByKey | same | 216 | `com.legend.testplan.BodyRouterParityTest#systemRoutedEngineTextPrograms` |
| PureTable | - | type-checker: BODY, answered at its call* | same | 3 | `com.legend.edge.UpstreamTiesTest#aFunctionWrittenForNumberAtLegendPuresDoor` |
| PureTable | Engine | type-checker: QUERY | same | 45 | `com.legend.DecimalKindsTest#floatEqualityIsExactOnEveryRoute` |
| PureTable | Lite | type-checker: QUERY | same | 81 | `com.legend.integration.PureOnlyTextTest#anInBodyAssertRaisesItsKindOnBothRoads` |
| PureTable | Lite | type-checker: UNREAD | same | 1778 | `com.legend.testplan.BodyRouterParityTest#framesUnderTheDatabaseJudge` |
| PureTable | Pure | type-checker: QUERY | DIFF[R] | 8 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |
| PureTable | Pure | type-checker: QUERY | LITE-RULE-THROWS:ModelException | 8 | `om.legend.edge.UpstreamTiesTest#everyComparisonTheProductRefusesOverColumnsIsTheDoorsOwnAtADoor` |
| PureTable | Pure | type-checker: QUERY | same | 5 | `com.legend.edge.UpstreamTiesTest#mixedNumbersKeepThreeTypesAtTheCompatibilityDoors` |

total comparison calls: 6230

### The edge net and the engine lane (//gates:edge_net, //pct:update_engine_lane_*; build actions, so no test names)

| Lowered with | Typed under | Made by | Lite's rule against the door's | Calls | A test |
|---|---|---|---|---:|---|
| EngineTable.PCT | - | type-checker, a function named as a value (no hook) | DIFF[E] | 4 | `?` |
| EngineTable.PCT | - | type-checker, a function named as a value (no hook) | same | 7 | `?` |
| EngineTable.PCT | Engine | type-checker: BODY | same | 5 | `?` |
| EngineTable.PCT | Engine | type-checker: QUERY | same | 550 | `?` |
| LiteEditionTable | - | type-checker, a function named as a value (no hook) | n/a | 12 | `?` |
| LiteEditionTable | - | type-checker: BODY* | n/a | 36 | `?` |
| LiteEditionTable | Pure | type-checker: BODY | same (reverse) | 1 | `?` |
| LiteEditionTable | Pure | type-checker: QUERY | same (reverse) | 3291 | `?` |
| PureTable | - | type-checker, a function named as a value (no hook) | same | 36 | `ons::collection::tests::removeDuplicates::testRemoveDuplicatesPrimitiveStandardFunctionExplicit` |
| PureTable | - | type-checker: BODY* | same | 44 | `?` |
| PureTable | Pure | type-checker: BODY | same | 4 | `?` |
| PureTable | Pure | type-checker: QUERY | same | 3520 | `?` |

total comparison calls: 7510

## 2. The other rows of the doors' tables that answered (all suites together)

Every call was made by the type-checker; none by the resolver.

| Rule the table named | Function | Table | Made by | Typed as | Stamp | Calls | A test |
|---|---|---|---|---|---|---:|---|
| `CAST_TO_BARE_DECIMAL` | `toDecimal/1` | EngineTable.DOOR | type-checker | QUERY | inside | 3 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `CAST_TO_BARE_DECIMAL` | `toDecimal/1` | EngineTable.PCT | type-checker | QUERY | inside | 3 | `?` |
| `CAST_TO_BARE_DECIMAL` | `toDecimal/1` | EngineTable.TESTS | type-checker | QUERY | inside | 2 | `meta::relational::tests::functions::sqlstring::testToSQLStringToNumericCasts` |
| `DATE_DIFF_AS_GIVEN` | `dateDiff/3` | EngineTable.DOOR | type-checker | QUERY | inside | 3 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `DATE_DIFF_AS_GIVEN` | `dateDiff/3` | EngineTable.PCT | type-checker | QUERY | inside | 129 | `?` |
| `DATE_DIFF_AS_GIVEN` | `dateDiff/3` | EngineTable.TESTS | type-checker | QUERY | inside | 56 | `ts::functions::sqlstring::testGenerateDateDiffExpressionForH2ForDifferenceInDays` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `greatest/1` | EngineTable.DOOR | type-checker | QUERY | outside | 6 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `greatest/1` | EngineTable.PCT | type-checker | QUERY | inside | 64 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `greatest/1` | EngineTable.TESTS | type-checker | QUERY | inside | 4 | `meta::relational::tests::functions::sqlstring::testGreatestLeast` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `greatest/1` | PureTable | type-checker | QUERY | inside | 351 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `least/1` | EngineTable.DOOR | type-checker | QUERY | outside | 4 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `least/1` | EngineTable.PCT | type-checker | QUERY | inside | 62 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `least/1` | EngineTable.TESTS | type-checker | QUERY | inside | 4 | `meta::relational::tests::functions::sqlstring::testGreatestLeast` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `least/1` | PureTable | type-checker | QUERY | inside | 349 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/1` | EngineTable.DOOR | type-checker | QUERY | inside | 3 | `com.legend.edge.UpstreamTiesTest#theRowsAreNeverRefusedByDatabase` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/1` | EngineTable.DOOR | type-checker | QUERY | outside | 9 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/1` | EngineTable.PCT | type-checker | QUERY | inside | 24 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/1` | EngineTable.TESTS | type-checker | QUERY | outside | 2 | `com.legend.edge.UpstreamTiesTest#theCorpussOwnCodeKeepsUpstreamsTies` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/1` | PureTable | type-checker | QUERY | inside | 103 | `com.legend.edge.UpstreamTiesTest#theCorpussOwnCodeKeepsUpstreamsTies` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/2` | EngineTable.DOOR | type-checker | QUERY | inside | 1 | `edge.UpstreamTiesTest#anIntegerAndAFloatColumnsExtremeIsAFloatAtLegendEnginesApi` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/2` | EngineTable.DOOR | type-checker | QUERY | outside | 6 | `legend.edge.UpstreamTiesTest#ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/2` | EngineTable.PCT | type-checker | QUERY | inside | 21 | `legend.edge.UpstreamTiesTest#ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/2` | EngineTable.TESTS | type-checker | QUERY | outside | 2 | `com.legend.edge.UpstreamTiesTest#theCorpussOwnCodeKeepsUpstreamsTies` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `max/2` | PureTable | type-checker | QUERY | inside | 92 | `com.legend.edge.UpstreamTiesTest#theCorpussOwnCodeKeepsUpstreamsTies` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/1` | EngineTable.DOOR | type-checker | QUERY | outside | 6 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/1` | EngineTable.PCT | type-checker | QUERY | inside | 20 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/1` | PureTable | type-checker | QUERY | inside | 97 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/2` | EngineTable.DOOR | type-checker | QUERY | outside | 4 | `legend.edge.UpstreamTiesTest#ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/2` | EngineTable.PCT | type-checker | QUERY | inside | 21 | `legend.edge.UpstreamTiesTest#ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst` |
| `EXTREME_KEEPS_THE_FOLDS_TIE` | `min/2` | PureTable | type-checker | QUERY | inside | 91 | `legend.edge.UpstreamTiesTest#ofSeveralArgumentsTheCompatibilityDoorsKeepTheFirst` |
| `EXTREME_OF_TWO_IN_THE_DATABASES_TYPE` | `max/2` | EngineTable.DOOR | type-checker | QUERY | inside | 2 | `edge.UpstreamTiesTest#anIntegerAndAFloatColumnsExtremeIsAFloatAtLegendEnginesApi` |
| `EXTREME_OF_TWO_IN_THE_DATABASES_TYPE` | `min/2` | EngineTable.DOOR | type-checker | QUERY | inside | 1 | `edge.UpstreamTiesTest#anIntegerAndAFloatColumnsExtremeIsAFloatAtLegendEnginesApi` |
| `ROUND_AS_GIVEN` | `round/1` | EngineTable.DOOR | type-checker | QUERY | inside | 4 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `ROUND_AS_GIVEN` | `round/1` | EngineTable.PCT | type-checker | QUERY | inside | 30 | `?` |
| `ROUND_AS_GIVEN` | `round/1` | EngineTable.TESTS | type-checker | QUERY | inside | 8 | `meta::relational::tests::query::function::round::testFilterUsingRoundFunction` |
| `ROUND_AS_GIVEN` | `round/1` | EngineTable.TESTS | type-checker | UNREAD | inside | 30 | `meta::relational::tests::mapping::sqlFunction::round::testFilter` |
| `ROUND_AS_GIVEN` | `round/2` | EngineTable.DOOR | type-checker | QUERY | inside | 4 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `ROUND_AS_GIVEN` | `round/2` | EngineTable.PCT | type-checker | QUERY | inside | 14 | `?` |
| `ROUND_AS_GIVEN` | `round/2` | EngineTable.TESTS | type-checker | QUERY | inside | 28 | `relational::tests::query::function::round::testFilterUsingRoundFunctionWithScale` |
| `SORT_TIES_AS_WRITTEN` | `sort/1` | EngineTable.DOOR | type-checker | QUERY | outside | 13 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/1` | EngineTable.PCT | type-checker | QUERY | inside | 25 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/1` | EngineTable.TESTS | type-checker | QUERY | outside | 166 | `l::tests::advanced::failures::moveFilterInOnClause::failMoveFilterInOnClauseJoin` |
| `SORT_TIES_AS_WRITTEN` | `sort/1` | EngineTable.TESTS | type-checker | rebuilt* | outside | 2 | `meta::relational::tests::mapping::dates::strictdate::testProject` |
| `SORT_TIES_AS_WRITTEN` | `sort/1` | PureTable | type-checker | QUERY | inside | 258 | `com.legend.edge.UpstreamTiesTest#theCorpussOwnCodeKeepsUpstreamsTies` |
| `SORT_TIES_AS_WRITTEN` | `sort/2` | EngineTable.DOOR | type-checker | QUERY | outside | 2 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/2` | EngineTable.PCT | type-checker | QUERY | inside | 8 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/2` | PureTable | type-checker | QUERY | inside | 38 | `com.legend.lowering.ToOneLaneTest#sortDescRaises` |
| `SORT_TIES_AS_WRITTEN` | `sort/3` | EngineTable.DOOR | type-checker | QUERY | outside | 2 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/3` | EngineTable.PCT | type-checker | QUERY | inside | 6 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SORT_TIES_AS_WRITTEN` | `sort/3` | PureTable | type-checker | QUERY | inside | 20 | `com.legend.edge.UpstreamTiesTest#oneTextAtTheThreeDoors` |
| `SPLIT_PART_AS_GIVEN` | `splitPart/3` | EngineTable.DOOR | type-checker | QUERY | inside | 1 | `gineTablePlacementTest#splitPartIsARowOnlyWhereLegendEngineWritesTheDatabasesOwn` |
| `SPLIT_PART_AS_GIVEN` | `splitPart/3` | EngineTable.PCT | type-checker | QUERY | inside | 10 | `?` |
| `SPLIT_PART_AS_GIVEN` | `splitPart/3` | EngineTable.TESTS | type-checker | QUERY | inside | 8 | `meta::relational::tests::functions::sqlstring::testToSQLStringSplitPart` |
| `SQL_POSITION` | `indexOf/2` | EngineTable.DOOR | type-checker | BODY | inside | 4 | `dge.EngineTablePlacementTest#codeLegendEngineSendsToTheDatabaseIsTheDatabasesOwn` |
| `SQL_POSITION` | `indexOf/2` | EngineTable.DOOR | type-checker | QUERY | inside | 22 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `SQL_POSITION` | `indexOf/2` | EngineTable.PCT | type-checker | QUERY | inside | 4 | `?` |
| `SQL_POSITION` | `indexOf/2` | EngineTable.TESTS | type-checker | BODY | inside | 38 | `meta::pure::executionPlan::tests::testModelConnectionAgg` |
| `SUBSTRING_AS_GIVEN` | `substring/2` | EngineTable.DOOR | type-checker | QUERY | inside | 28 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `SUBSTRING_AS_GIVEN` | `substring/2` | EngineTable.PCT | type-checker | QUERY | inside | 4 | `?` |
| `SUBSTRING_AS_GIVEN` | `substring/3` | EngineTable.DOOR | type-checker | BODY | inside | 8 | `ePlacementTest#aCheckedGraphFetchsConstraintAndItsQualifiedPropertyAreEachPlaced` |
| `SUBSTRING_AS_GIVEN` | `substring/3` | EngineTable.DOOR | type-checker | QUERY | inside | 74 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `SUBSTRING_AS_GIVEN` | `substring/3` | EngineTable.PCT | type-checker | QUERY | inside | 9 | `?` |
| `SUBSTRING_AS_GIVEN` | `substring/3` | EngineTable.TESTS | type-checker | BODY | inside | 38 | `meta::pure::executionPlan::tests::testModelConnectionAgg` |
| `SUBSTRING_AS_GIVEN` | `substring/3` | EngineTable.TESTS | type-checker | QUERY | inside | 47 | `meta::pure::executionPlan::tests::testSubstringIndexingCorrectedByFeatureFlag` |
| `SUBSTR_AS_GIVEN` | `substr/2` | EngineTable.DOOR | type-checker | QUERY | inside | 2 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `SUBSTR_AS_GIVEN` | `substr/2` | EngineTable.PCT | type-checker | QUERY | inside | 8 | `?` |
| `SUBSTR_AS_GIVEN` | `substr/2` | EngineTable.TESTS | type-checker | QUERY | inside | 8 | `elational::tests::functions::sqlstring::testToSQLStringSubstrFoldsLiteralIndexes` |
| `SUBSTR_AS_GIVEN` | `substr/3` | EngineTable.DOOR | type-checker | QUERY | inside | 9 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `SUBSTR_AS_GIVEN` | `substr/3` | EngineTable.PCT | type-checker | QUERY | inside | 8 | `?` |
| `SUBSTR_AS_GIVEN` | `substr/3` | EngineTable.TESTS | type-checker | QUERY | inside | 4 | `elational::tests::functions::sqlstring::testToSQLStringSubstrFoldsLiteralIndexes` |
| `SUBSTR_AS_GIVEN` | `substring/2` | EngineTable.DOOR | type-checker | QUERY | inside | 1 | `dge.EngineTablePlacementTest#codeLegendEngineSendsToTheDatabaseIsTheDatabasesOwn` |
| `SUBSTR_AS_GIVEN` | `substring/3` | EngineTable.DOOR | type-checker | QUERY | inside | 3 | `dge.EngineTablePlacementTest#codeLegendEngineSendsToTheDatabaseIsTheDatabasesOwn` |
| `SUBSTR_AS_GIVEN` | `substring/3` | EngineTable.TESTS | type-checker | QUERY | inside | 4 | `meta::pure::executionPlan::tests::testSubstringIndexingCorrectedByFeatureFlag` |
| `TEXT_CAST_TO_FLOAT` | `parseFloat/1` | EngineTable.DOOR | type-checker | QUERY | inside | 5 | `com.legend.edge.EngineTablePlacementTest#eachRowsRuleLowersEachOverloadItMaps` |
| `TEXT_CAST_TO_FLOAT` | `parseFloat/1` | EngineTable.PCT | type-checker | QUERY | inside | 10 | `?` |
| `TEXT_CAST_TO_FLOAT` | `parseFloat/1` | EngineTable.TESTS | type-checker | UNREAD | inside | 22 | `meta::relational::tests::mapping::sqlFunction::stringToFloat::testFilter` |
| `TEXT_CAST_TO_INTEGER` | `parseInteger/1` | EngineTable.DOOR | type-checker | QUERY | inside | 11 | `com.legend.server.PureV1EdgeGoldenTest#everyRequestsAnswer` |
| `TEXT_CAST_TO_INTEGER` | `parseInteger/1` | EngineTable.PCT | type-checker | QUERY | inside | 15 | `?` |
| `TEXT_CAST_TO_INTEGER` | `parseInteger/1` | EngineTable.TESTS | type-checker | BODY | inside | 8 | `::XStore::ordered::testOrderedCrossStoreGraphFetchWithComplexQualifierExpression` |
| `TEXT_CAST_TO_INTEGER` | `parseInteger/1` | EngineTable.TESTS | type-checker | QUERY | inside | 8 | `meta::relational::tests::query::function::testFilterUsingParseIntegerFunction` |
| `TEXT_CAST_TO_INTEGER` | `parseInteger/1` | EngineTable.TESTS | type-checker | UNREAD | inside | 34 | `meta::relational::tests::mapping::sqlFunction::castInteger::testFilter` |
| `TEXT_HAS_NO_CASED_LETTER` | `isNumeric/1` | EngineTable.TESTS | type-checker | UNREAD | inside | 18 | `meta::relational::tests::mapping::sqlFunction::isNumeric::testFilter` |
| `TEXT_IS_PLAIN_DECIMAL` | `isNumeric/1` | EngineTable.DOOR | type-checker | QUERY | inside | 3 | `eTablePlacementTest#isNumericIsARowByDatabase_refusedWhereLegendEnginesIsNotRead` |
| `TEXT_IS_PLAIN_DECIMAL` | `isNumeric/1` | EngineTable.TESTS | type-checker | UNREAD | inside | 18 | `meta::relational::tests::mapping::sqlFunction::isNumeric::testFilter` |
| `TRUST_ONE` | `toOne/1` | EngineTable.DOOR | type-checker | QUERY | inside | 10 | `com.legend.server.PureV1EdgeGoldenTest#everyRequestsAnswer` |

## 3. The raise checks asked of a table (all suites together)

| Table | Raise | Function | Typed as | Stamp | Answer | Times asked |
|---|---|---|---|---|---|---:|
| EngineTable.DOOR | `DIVIDE_BY_ZERO` | `divide` | QUERY | inside | PLAIN | 8 |
| EngineTable.DOOR | `DIVIDE_BY_ZERO` | `divide` | QUERY | outside | checked | 2 |
| EngineTable.DOOR | `DIVIDE_BY_ZERO` | `rem` | QUERY | inside | PLAIN | 2 |
| EngineTable.DOOR | `DIVIDE_BY_ZERO` | `rem` | QUERY | outside | checked | 2 |
| EngineTable.DOOR | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | inside | PLAIN | 6 |
| EngineTable.DOOR | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | outside | checked | 2 |
| EngineTable.DOOR | `SUBSTRING_OUT_OF_RANGE` | `substr` | QUERY | outside | checked | 1 |
| EngineTable.DOOR | `SUBSTRING_OUT_OF_RANGE` | `substring` | BODY | outside | checked | 9 |
| EngineTable.DOOR | `SUBSTRING_OUT_OF_RANGE` | `substring` | QUERY | outside | checked | 5 |
| EngineTable.PCT | `DIVIDE_BY_ZERO` | `divide` | QUERY | inside | PLAIN | 4 |
| EngineTable.PCT | `DIVIDE_BY_ZERO` | `rem` | QUERY | inside | PLAIN | 16 |
| EngineTable.PCT | `INVERSE_TRIG_OUT_OF_DOMAIN` | `acos` | QUERY | inside | PLAIN | 8 |
| EngineTable.PCT | `INVERSE_TRIG_OUT_OF_DOMAIN` | `asin` | QUERY | inside | PLAIN | 8 |
| EngineTable.PCT | `RANGE_STEP_ZERO` | `range` | QUERY | inside | PLAIN | 29 |
| EngineTable.PCT | `SHIFT_OUT_OF_RANGE` | `bitShiftLeft` | QUERY | inside | PLAIN | 13 |
| EngineTable.PCT | `SHIFT_OUT_OF_RANGE` | `bitShiftRight` | QUERY | inside | PLAIN | 13 |
| EngineTable.PCT | `SIZE_NOT_MULTIPLICITY` | `toOne` | QUERY | inside | PLAIN | 2 |
| EngineTable.PCT | `SLICE_BOUNDS_INVERTED` | `(no call)` | - | - | PLAIN | 6 |
| EngineTable.PCT | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | inside | PLAIN | 8 |
| EngineTable.TESTS | `DIVIDE_BY_ZERO` | `rem` | QUERY | inside | PLAIN | 8 |
| EngineTable.TESTS | `INVERSE_TRIG_OUT_OF_DOMAIN` | `acos` | QUERY | inside | PLAIN | 8 |
| EngineTable.TESTS | `INVERSE_TRIG_OUT_OF_DOMAIN` | `acos` | UNREAD | inside | PLAIN | 30 |
| EngineTable.TESTS | `INVERSE_TRIG_OUT_OF_DOMAIN` | `asin` | QUERY | inside | PLAIN | 8 |
| EngineTable.TESTS | `INVERSE_TRIG_OUT_OF_DOMAIN` | `asin` | UNREAD | inside | PLAIN | 30 |
| EngineTable.TESTS | `SIZE_NOT_MULTIPLICITY` | `toOne` | QUERY | outside | checked | 17 |
| EngineTable.TESTS | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | inside | PLAIN | 8 |
| EngineTable.TESTS | `SQRT_OF_NEGATIVE` | `sqrt` | UNREAD | inside | PLAIN | 30 |
| EngineTable.TESTS | `SUBSTRING_OUT_OF_RANGE` | `substring` | QUERY | outside | checked | 7 |
| LiteEditionTable | `DIVIDE_BY_ZERO` | `divide` | QUERY | inside | checked | 156 |
| LiteEditionTable | `DIVIDE_BY_ZERO` | `rem` | QUERY | inside | checked | 57 |
| LiteEditionTable | `INVERSE_TRIG_OUT_OF_DOMAIN` | `acos` | QUERY | inside | checked | 27 |
| LiteEditionTable | `INVERSE_TRIG_OUT_OF_DOMAIN` | `asin` | QUERY | inside | checked | 26 |
| LiteEditionTable | `RANGE_STEP_ZERO` | `range` | QUERY | inside | checked | 173 |
| LiteEditionTable | `SHIFT_OUT_OF_RANGE` | `bitShiftLeft` | QUERY | inside | checked | 76 |
| LiteEditionTable | `SHIFT_OUT_OF_RANGE` | `bitShiftRight` | QUERY | inside | checked | 76 |
| LiteEditionTable | `SIZE_NOT_MULTIPLICITY` | `toOne` | QUERY | inside | checked | 108 |
| LiteEditionTable | `SLICE_BOUNDS_INVERTED` | `(no call)` | - | - | checked | 92 |
| LiteEditionTable | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | inside | checked | 31 |
| LiteEditionTable | `SUBSTRING_OUT_OF_RANGE` | `substr` | QUERY | inside | checked | 55 |
| LiteEditionTable | `SUBSTRING_OUT_OF_RANGE` | `substring` | BODY | inside | checked | 12 |
| LiteEditionTable | `SUBSTRING_OUT_OF_RANGE` | `substring` | QUERY | inside | checked | 138 |
| PureTable | `DIVIDE_BY_ZERO` | `divide` | QUERY | inside | checked | 57 |
| PureTable | `DIVIDE_BY_ZERO` | `rem` | QUERY | inside | checked | 72 |
| PureTable | `INVERSE_TRIG_OUT_OF_DOMAIN` | `acos` | QUERY | inside | checked | 34 |
| PureTable | `INVERSE_TRIG_OUT_OF_DOMAIN` | `asin` | QUERY | inside | checked | 34 |
| PureTable | `RANGE_STEP_ZERO` | `range` | QUERY | inside | checked | 180 |
| PureTable | `SHIFT_OUT_OF_RANGE` | `bitShiftLeft` | QUERY | inside | checked | 89 |
| PureTable | `SHIFT_OUT_OF_RANGE` | `bitShiftRight` | QUERY | inside | checked | 88 |
| PureTable | `SIZE_NOT_MULTIPLICITY` | `toOne` | QUERY | inside | checked | 42 |
| PureTable | `SLICE_BOUNDS_INVERTED` | `(no call)` | - | - | checked | 40 |
| PureTable | `SQRT_OF_NEGATIVE` | `sqrt` | QUERY | inside | checked | 38 |
| PureTable | `SUBSTRING_OUT_OF_RANGE` | `substr` | QUERY | inside | checked | 64 |
| PureTable | `SUBSTRING_OUT_OF_RANGE` | `substring` | QUERY | inside | checked | 132 |
