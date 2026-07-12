# Relational corpus scoreboard (real legend-engine core_relational)

RUN-as-data over the local legend-engine checkout; row equality is the
contract, golden SQL is advisory. SHAPE = test body/assert form the
runner does not yet recognize (accounted, not skipped silently).

## Failed seed statements (3)

- `insert into calendar ("calendar name", "date", "fiscal week start", "fiscal week end", "fiscal day", "fiscal week", "fiscal year", "fiscal year start", "fiscal year end", "fiscal day of week") => Parser Error: syntax error at end of input`
- `select * from DATA_WITH_TIMESTAMPS_KEYS ; => Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result`
- `insert into FirmTable ("id", "legalName", "addressId") values (8, 'No Employees', 11); => Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result`

| family | tests | pass | fail | error | shape |
|---|---|---|---|---|---|
| query | 99 | 11 | 1 | 63 | 24 |
| mapping/association | 24 | 1 | 0 | 17 | 6 |
| mapping/join | 29 | 14 | 1 | 13 | 1 |
| mapping/embedded | 70 | 0 | 0 | 65 | 5 |
| mapping/enumeration | 27 | 0 | 0 | 14 | 13 |
| mapping/distinct | 18 | 1 | 7 | 9 | 1 |
| mapping/groupBy | 10 | 0 | 0 | 10 | 0 |
| mapping/filter | 10 | 3 | 0 | 6 | 1 |
| mapping/inheritance | 51 | 0 | 0 | 46 | 5 |
| mapping/selfJoin | 3 | 0 | 0 | 2 | 1 |
| mapping/boolean.pure | 3 | 0 | 0 | 3 | 0 |
| mapping/dates.pure | 7 | 0 | 0 | 4 | 3 |
| **total** | 351 | **30** | 9 | 252 | 60 |

### mapping walls (dropped at assembly)

- meta::pure::mapping::modelToModel::test::simple::PersonPureMappingSub => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::PersonView' is not a known primitive, class, or enum
- meta::pure::mapping::modelToModel::test::simple::OrderContactPureMapping => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::OrderContactView' is not a known primitive, class, or enum
- tests/mapping/association meta::relational::tests::mapping::association::embedded::associationMappingInlinedEmbedded => [2617:1] AssociationMapping join 'Firm_Organizations' not found in db 'myDB'; association='meta::relational::tests::model::simple::FirmOrganizations', mapping=meta::relational::tests::mapping::association::embedded::associationMappingInlinedEmbedded
- tests/mapping/inheritance meta::relational::tests::model::inheritance::milestoned::MilestonedInheritanceMapping => [3341:4] Association mappings cannot be marked root (the leading '*' is only valid for class mappings)
- tests/mapping/inheritance meta::relational::tests::mapping::subType::SubTypeMappingValidWhenMappedExplicitly => [3508:3] clean-sheet mapping binding for 'MyProduct' has an empty body
- tests/mapping/inheritance meta::relational::tests::mapping::subType::MyMapping => [3511:14] expected integer or string inside '[...]' index, got VALID_STRING ('meta_relational_tests_model_simple_Trade')
- tests/mapping/inheritance meta::relational::tests::mapping::subType::MyMappingWithIds => [3499:13] expected integer or string inside '[...]' index, got VALID_STRING ('meta_relational_tests_mapping_subType_MyProduct')

### top error buckets

- 25x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::simpleRelationalMapping' failed to normalize this class: Join 'Trade_TradeEventViewMaxTradeEventDate' targets view 'tradeEventViewMaxTradeEventDate'; views as JOIN TARGETS are a roadmap feature (the view must expand as a relation at the join hop). mapping=meta::relational::tests::simpleRelationalMapping
- 17x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::embedded::model::mapping::testMappingEmbedded' failed to normalize this class: Embedded sub-PM 'employees' on 'firm' is a class-typed Join; nested Join PMs inside a value-position Embedded are not supported (would need a hoisted navigate keyed to the embedded slot). Mapping=meta::relational::tests::mapping::embedded::model::mapping::testMappingEmbedded
- 12x Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist! | Did you mean "Org"? |  | LINE 2: FROM PRODUCT_DENORM AS t0 |              ^
- 10x class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- 10x project expects ~[…] column specifications
- 9x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::inheritance::inheritanceMain' failed to normalize this class: property 'cars' of class 'meta::relational::tests::model::inheritance::Person' routes to NON-root mapping set 'map1' — multi-set target routing is a roadmap feature (navigating the root set instead would be wrong rows)
- 7x 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 7x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::groupBy::model::mapping::testMapping' failed to normalize this class: PropertyMapping 'Join' for property 'product' is not supported under ~groupBy (only Column, Expression, JoinTerminalColumn, and aggregate Expression PMs are allowed). Mapping=meta::relational::tests::mapping::groupBy::model::mapping::testMapping
- 6x [1:95] expected ']' to close collection literal
- 6x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exactly one
- 5x serialize expects (classCollection, #{Class{…}}#)
- 5x mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- 4x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::association::inheritence::childMapping' failed to normalize this class: property 'vehicles' of class 'meta::relational::tests::model::inheritance::Person' routes to NON-root mapping set 'map2' — multi-set target routing is a roadmap feature (navigating the root set instead would be wrong rows)
- 4x no overload of 'agg' matches 3 argument(s) of these shapes
- 4x class-typed property '$p.bondDetails' used as a whole value is graph output (Phase H4)
- 4x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::enumeration::model::domain::Employee' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::enumeration::model::mapping::employeeTestMapping' failed to normalize this class: enum-mapped constant/expression source for property 'role' is not resolvable yet
- 4x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::mapping::inheritance::relational::union::inheritanceUnion' failed to normalize this class: property 'vehicles' of class 'meta::relational::tests::model::inheritance::Person' routes to NON-root mapping set 'map2' — multi-set target routing is a roadmap feature (navigating the root set instead would be wrong rows)
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Account' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::simpleRelationalMapping' failed to normalize this class: Join 'AccountPnlView_Account' targets view 'accountOrderPnlView'; views as JOIN TARGETS are a roadmap feature (the view must expand as a relation at the join hop). mapping=meta::relational::tests::simpleRelationalMapping
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- 3x [1:97] expected ']' to close collection literal
- 3x class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- 3x multi-hop navigation holder.address.name through an embedded/slot head is not supported yet
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::MyProduct' (of 1 candidates); class-query dispatch needs exactly one
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Interaction' (of 1 candidates); class-query dispatch needs exactly one; 'meta::relational::tests::simpleRelationalMapping' failed to normalize this class: Join references view 'interactionViewMaxTime' with ~filter semantics as its source side; joins over non-plain views are a roadmap feature. mapping=meta::relational::tests::simpleRelationalMapping
- 2x unbound variable '$var'
- 2x property 'name' of class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::simpleRelationalMapping'
- 2x unknown function 'nameWithTitle'
- 2x mapping pipeline for 'meta::relational::tests::model::simple::OrderPnl' has TypedDistinct above join slot(s); H3-pending
- 2x unbound variable '$input'
- 2x class query under TypedNativeCall is not resolvable yet (H2 vocabulary)

### per-test outcomes (non-passing)

- SHAPE testGroupBy [query]: no execute(|...) call
- SHAPE testInFlowGroupBy_noDatePath [query]: no execute(|...) call
- SHAPE testPlanGroupBy_noDatePath [query]: no execute(|...) call
- SHAPE testGroupByWithFilterFunction_noDatePath [query]: no execute(|...) call
- SHAPE testGroupByWithFilterFunction [query]: no execute(|...) call
- SHAPE testGroupByWithRelativeDateFunctions [query]: no execute(|...) call
- SHAPE testGroupByWithRelativeDateFunctions_noDatePath [query]: no execute(|...) call
- SHAPE testGroupByWithRelativeDateFunctionsWithPathFilter [query]: no execute(|...) call
- ERROR testProjectWithSeparateGroupBy [query]: unbound variable '$startDate'
- ERROR testDayOfWeek [query]: unknown function 'mostRecentDayOfWeek'
- SHAPE testAssociationMixed [query]: multiple execute() calls
- SHAPE testAssociationMixedDeep [query]: multiple execute() calls
- ERROR testAssociationToMany [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testAssociationToManyNotExists [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testAssociationToManyWithBoolean [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testTwoAssociationsToManyDeep [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testAssociationToManyWithConstantPredicate [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testTwoAssociationsToManyDeepWithOr [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testAssociationToManyWithTwoSeparateExists [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testTwoAssociationsToOneDeep [query]: multi-hop navigation firm.address.name through an embedded/slot head is not supported yet
- ERROR testGroupOpenVariable [query]: unbound variable '$other'
- ERROR testExistsOpenVariable [query]: unbound variable '$var'
- ERROR testExistsOpenVariableClass [query]: unbound variable '$var'
- ERROR testExistsAndBooleanOpenVariables [query]: unbound variable '$var1'
- ERROR testNoParameters [query]: property 'name' of class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::simpleRelationalMapping'
- ERROR testNoParametersThroughAssociation [query]: property 'name' of class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::simpleRelationalMapping'
- ERROR testWithParameter [query]: unknown function 'nameWithTitle'
- ERROR testWithParameterUsedWithinExists [query]: unknown function 'nameWithTitle'
- SHAPE testWithParameterToClassNestedSelect [query]: multiple execute() calls
- SHAPE testExistsWithQualifierOnleftSide [query]: multiple execute() calls
- ERROR testAssociationSpecifiedQualifiedProperty [query]: unknown function 'synonymByType'
- ERROR testNonTrivialQualifierWithDataTypeReturnTypeAsFunctionInput [query]: unknown function 'employeeByLastNameFirstName'
- ERROR testViewAll [query]: mapping pipeline for 'meta::relational::tests::model::simple::OrderPnl' has TypedDistinct above join slot(s); H3-pending
- ERROR testViewWithJoinsAndDistinct [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDistinctOnlyIncludesTopLevelColumns [query]: in function 'meta::relational::tests::TestViewWithDistinctAndJoins$class$meta::relational::tests::model::simple::Person': unknown table 'FirstNameAddress' in da
- ERROR testViewSimpleFilter [query]: mapping pipeline for 'meta::relational::tests::model::simple::OrderPnl' has TypedDistinct above join slot(s); H3-pending
- ERROR testAllWithJoinToView [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Order' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testViewWithGroupBy [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::AccountPnl' (of 1 candidates); class-query dispatch needs exactly on
- ERROR testAssnToViewWithGroupBy [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Account' (of 1 candidates); class-query dispatch needs exactly one; 
- ERROR testViewSimpleExists [query]: in function 'meta::relational::tests::query::view::relationalMappingWithViewAndInnerJoin$class$meta::relational::tests::model::simple::Order': unknown table 'Pe
- ERROR testViewPropertyFilterWithPrimaryKey [query]: in function 'meta::relational::tests::query::view::EmployeeMappingWithViewAndInnerJoin$class$meta::relational::tests::model::simple::Employee': unknown table 'O
- SHAPE testPushDownProject [query]: no execute(|...) call
- SHAPE testPushDownProjectWithParameter [query]: no execute(|...) call
- ERROR testFilterEnumOnClassProp [query]: unbound variable '$eType'
- ERROR testFilterThroughAssociationUsingPlusFunction [query]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- SHAPE testFilterUsingStartsWithFunction [query]: multiple execute() calls
- SHAPE testFilterUsingIsAlphaNumericFunction [query]: multiple execute() calls
- SHAPE testFilterUsingMatchesFunction [query]: multiple execute() calls
- FAIL testFilterUsingSubstringFunction [query]: size: expected 2, got 0; lastName: expected [Johnson, Hill], got []
- ERROR testFilterUsingFunctionWithVariable [query]: unbound variable '$input'
- ERROR testFilterUsingFunctionWithClassAttribute [query]: unbound variable '$input'
- SHAPE testFilterUsingContainsFunction [query]: multiple execute() calls
- SHAPE testFilterUsingEndsWithFunction [query]: multiple execute() calls
- ERROR testFilterUsingIsEmptyFunction [query]: class-typed property '$f.synonyms' used as a whole value is graph output (Phase H4)
- SHAPE testFilterUsingIfFunction [query]: partial: 2/3 asserts recognized (recognized ones hold)
- SHAPE testMostRecentDayOfWeek [query]: no execute(|...) call
- ERROR testFilterUsingParseIntegerFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Account' (of 1 candidates); class-query dispatch needs exactly one; 
- ERROR testFilterUsingParseDecimalFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Account' (of 1 candidates); class-query dispatch needs exactly one; 
- ERROR testFilterUsingToStringFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingFirstDayOfThisYearH2 [query]: unknown function 'firstDayOfThisYear'
- ERROR testFilterUsingFirstDayOfThisQuarter [query]: unknown function 'firstDayOfThisQuarter'
- ERROR testFilterUsingRoundFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingRoundFunctionWithScale [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingCeilingFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingFloorFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingPowFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingExpFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingLogFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingLog10Function [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingsinFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingCosFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingCotFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingTanFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingArcSinFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingArcCosFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingArcTanFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingArcTan2Function [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingSqrtFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingSignFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingModFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testFilterUsingRemFunction [query]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- SHAPE testFilterTimesWithManyOperands [query]: multiple execute() calls
- SHAPE testFilterUsingQuarterNumberFunction [query]: multiple execute() calls
- ERROR testCollectionDistinctFunction [query]: class query under TypedNativeCall is not resolvable yet (H2 vocabulary)
- SHAPE testDivideFunctionPrecision [query]: multiple execute() calls
- ERROR testJoinStringFunction [query]: no overload of 'meta::pure::functions::string::joinStrings' accepts 1 argument(s)
- ERROR testDayOfWeekFunction [query]: a bare lambda has no type outside a call position (lambdas type against their call's signature)
- ERROR testDayOfWeekNumberFunction [query]: a bare lambda has no type outside a call position (lambdas type against their call's signature)
- ERROR testPersonToOrganisations [mapping/association]: mapping 'meta::relational::tests::mapping::association::embedded::associationMapping' includes unknown mapping 'meta::relational::tests::mapping::embedded::mode
- ERROR testFirmToOrganisations [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Firm' (of 1 candidates); class-query dispatch needs exactly one
- ERROR testPersonToOrganisationsInlineEmbedded [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- ERROR testPersonToFirmAddressNestedInlineEmbedded [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- ERROR testPersonToFirmLocationsInlineEmbedded [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- SHAPE testPersonToFirm [mapping/association]: partial: 1/2 asserts recognized (recognized ones hold)
- SHAPE testFirmToEmployees [mapping/association]: partial: 1/2 asserts recognized (recognized ones hold)
- SHAPE testPersonToFirmWithDefaults [mapping/association]: partial: 1/2 asserts recognized (recognized ones hold)
- SHAPE testFirmToEmployeesWithDefaults [mapping/association]: partial: 1/2 asserts recognized (recognized ones hold)
- ERROR testFirmToEmployeesIncludes [mapping/association]: association 'meta::relational::tests::model::simple::Employment' is not mapped in mapping 'meta::relational::tests::mapping::association::associationMappingWith
- ERROR testPersonToFirmIncludes [mapping/association]: association 'meta::relational::tests::model::simple::Employment' is not mapped in mapping 'meta::relational::tests::mapping::association::associationMappingWith
- ERROR testProjectTwoLambdas [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGroupBy [mapping/association]: no overload of 'agg' matches 3 argument(s) of these shapes
- ERROR testBuilderRoutingOfAggFunctionParameters [mapping/association]: [1:126] expected ')' to close argument list
- ERROR testQuery [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProject [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProjectBooleanInFilter [mapping/association]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- SHAPE testGetAllFilterWithAssociation [mapping/association]: multiple execute() calls
- SHAPE testSubTypeFilter [mapping/association]: multiple execute() calls
- ERROR testSubTypeProjectWithAssociation [mapping/association]: [1:97] expected ']' to close collection literal
- ERROR testSubTypeProjectSharedNonDirectlyRouted [mapping/association]: [1:95] expected ']' to close collection literal
- ERROR testSubTypeProjectSharedNonDirectlyRoutedWithFilter [mapping/association]: [1:138] expected ']' to close collection literal
- ERROR testSubTypeInColumnProjectionsWithInlineMappings [mapping/association]: [1:93] expected ')' to close argument list
- ERROR testFilterOnSimpleTypePropertyDeepWithJoinInMapping [mapping/join]: class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- ERROR testFilterOnSimpleTypePropertyDeepWithJoinInMappingNotUsed [mapping/join]: class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- ERROR testFilterDeepWithJoinInMappingInMiddle [mapping/join]: class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- FAIL testMultipleJoinsInPropertyMappingWithDatesInClass [mapping/join]: inDate: expected [DateExpected[iso=1900-01-01T00:00:00.000000000], DateExpected[iso=1900-01-01T00:00:00.000000000], DateExpected[iso=1900-01-01T00:00:00.0000000
- ERROR testMultipleJoinsInPropertyMappingWithDateInJoin [mapping/join]: in function 'meta::relational::tests::mapping::join::model::mapping::advancedRelationalMapping2$class$meta::relational::tests::mapping::join::model::domain::Typ
- ERROR testConstraintTargetingMultipleJoinsInPropertyMapping [mapping/join]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- ERROR testConstraintTargetingMultipleJoinsInPropertyMappingNoJoinProperty [mapping/join]: class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- SHAPE testChainedOuterJoinsMerge [mapping/join]: partial: 2/3 asserts recognized (recognized ones hold)
- ERROR testChainedInnerJoinsMerge [mapping/join]: in function 'meta::relational::tests::mapping::join::model::mapping::chainedJoinsInner$class$meta::relational::tests::model::simple::Firm': unknown function 'ca
- ERROR testChainedInnerJoinsWithFilterMerge [mapping/join]: in function 'meta::relational::tests::mapping::join::model::mapping::chainedJoinsInner$class$meta::relational::tests::model::simple::Firm': unknown function 'ca
- ERROR testConvertToStringH2 [mapping/join]: in function 'meta::relational::tests::mapping::join::model::mapping::MappingForAccountAndTrade$class$meta::relational::tests::model::simple::Trade': unknown fun
- ERROR testChainedOuterJoinsWithFilterInproject [mapping/join]: object-space expression node TypedFilter is not substitutable yet (H2 vocabulary)
- ERROR testChainedOuterJoinsWithQualifierInproject [mapping/join]: unknown function 'employeesByAge'
- ERROR testChainedInnerJoinsWithQualifierInGroupBy [mapping/join]: legacy groupBy expects (source, [keys], [aggs], ['aliases'])
- ERROR testIsNotEmptyCheckWithoutRowExplosion [mapping/join]: class-typed property '$x.employees' used as a whole value is graph output (Phase H4)
- ERROR testDenormMappingOneToManyProjectUsingPaths [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProject [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProjectLambdaSyntaxWithMap [mapping/embedded]: class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::mapping::embedded::model::mapping::testMappingEmbedded
- ERROR testDenormMappingOneToManyProjectWithFilter [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProjectWithComplexFilter [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProjectWithEnum [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProjectWithFilterOnEnumLeft [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingOneToManyProjectWithFilterOnEnumRight [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testGroupByEmbeddedProperty [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testProjectToEmbedded [mapping/embedded]: class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::mapping::embedded::model::mapping::testMappingEmbedded
- ERROR testDenormMappingOneToManyProjectEmbeddedQualifier [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testDenormMappingWithQualifierWithIfAndEquals [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testFilterWithEmbeddedQualifier [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testExists [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testIsEmpty [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testMapEmbeddedQualifierWithIfTwoEmbeddedProperties [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testGetter [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testGetterTwoJoinTraversal [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- SHAPE testRoutingQualifiedPropertySameVariableNames [mapping/embedded]: no execute(|...) call
- ERROR testOptionalPropertyEmbedded [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one; '
- ERROR testProjectionOtherwise [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestFilter [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- SHAPE otherwiseTestGetter [mapping/embedded]: multiple execute() calls
- SHAPE otherwiseTestGetterDeepTraversal [mapping/embedded]: multiple execute() calls
- ERROR testProjectionOtherwiseDeepTraversal [mapping/embedded]: multi-hop navigation bondDetails.holder.name through an embedded/slot head is not supported yet
- ERROR testProjectionOtherwiseNonPrimitive [mapping/embedded]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::embedded::advanced::model::Product' (of 1 candidates); class-query dispatc
- ERROR otherwiseTestProjection [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestDenormWithComplexFilter [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestGroupBy [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestGroupByComplexAgg [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestGroupByComplexExpressionEmbeddedAndJoin [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestQualifierProperty [mapping/embedded]: property 'duration' of class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' is not mapped in mapping 'meta::relational::tests::mappin
- ERROR otherwiseTestQualifierPropertyConstantExpression [mapping/embedded]: property 'isBond' of class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' is not mapped in mapping 'meta::relational::tests::mapping:
- ERROR otherwiseTestQualifierPropertyExpressionWithPropertyInJoinOnly [mapping/embedded]: unknown function 'durationStartsWith'
- ERROR otherwiseTestQualifierPropertyExpressionWithEmbeddedProperty [mapping/embedded]: unknown function 'prefixedDescription'
- ERROR otherwiseTestQualifierPropertyExpressionWithEmbeddedPropertyandJoinProperty [mapping/embedded]: property 'fullName' of class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' is not mapped in mapping 'meta::relational::tests::mappin
- ERROR otherwiseTestComplexExpression [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR otherwiseTestComplexExpressionWithEnumMapping [mapping/embedded]: property 'type' of class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' is not mapped in mapping 'meta::relational::tests::mapping::e
- ERROR otherwiseTestEmbeddedToEmbedded [mapping/embedded]: multi-hop navigation bondDetails.issuer.name through an embedded/slot head is not supported yet
- ERROR otherwiseTestFilterExistsOnEmbeddedProperty [mapping/embedded]: class-typed property '$p.bondDetails' used as a whole value is graph output (Phase H4)
- ERROR otherwiseTestProjectExistsOnEmbeddedProperty [mapping/embedded]: class-typed property '$p.bondDetails' used as a whole value is graph output (Phase H4)
- ERROR otherwiseTestFilterExistsOnOtherwiseProperty [mapping/embedded]: class-typed property '$p.bondDetails' used as a whole value is graph output (Phase H4)
- ERROR otherwiseTestProjectExistsOnOtherwiseProperty [mapping/embedded]: class-typed property '$p.bondDetails' used as a whole value is graph output (Phase H4)
- ERROR testProjection [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testFilter [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testDenormWithComplexFilter [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testGroupBy [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testGroupByComplexAgg [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testQualifierProperty [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testInlineEmbeddedMappingWithAssociationFromRootMapping [mapping/embedded]: 'Product' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- ERROR testProjection [mapping/embedded]: multi-hop navigation issuer.address.name through an embedded/slot head is not supported yet
- SHAPE testFilter [mapping/embedded]: multiple execute() calls
- ERROR testDenormWithComplexFilter [mapping/embedded]: multi-hop navigation issuer.address.name through an embedded/slot head is not supported yet
- ERROR testGroupBy [mapping/embedded]: multi-hop navigation holder.address.name through an embedded/slot head is not supported yet
- ERROR testGroupByComplexAgg [mapping/embedded]: multi-hop navigation holder.address.name through an embedded/slot head is not supported yet
- ERROR testQualifierProperty [mapping/embedded]: multi-hop navigation issuer.address.description through an embedded/slot head is not supported yet
- ERROR testInlineInEmbedded [mapping/embedded]: multi-hop navigation holder.address.name through an embedded/slot head is not supported yet
- ERROR testInlineInEmbeddedGraphFetch [mapping/embedded]: serialize expects (classCollection, #{Class{…}}#)
- ERROR testMilestonedEmbeddedGraphFetch [mapping/embedded]: serialize expects (classCollection, #{Class{…}}#)
- ERROR testMilestonedEmbeddedInlineGraphFetch [mapping/embedded]: serialize expects (classCollection, #{Class{…}}#)
- ERROR testMilestonedExtendsEmbeddedGraphFetch [mapping/embedded]: serialize expects (classCollection, #{Class{…}}#)
- ERROR testMilestonedInlineGraphFetchWithEnumProperty [mapping/embedded]: serialize expects (classCollection, #{Class{…}}#)
- ERROR testProjection [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR testSubType [mapping/embedded]: [1:113] expected ']' to close collection literal
- ERROR testSubTypeOnPropertyMappedToNonRootInlineSetImpl [mapping/embedded]: [1:113] expected ']' to close collection literal
- SHAPE testFilter [mapping/embedded]: multiple execute() calls
- ERROR testDenormWithComplexFilter [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR testGroupBy [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR testGroupByComplexAgg [mapping/embedded]: Invalid Input Error: Attempting to execute an unsuccessful or closed pending query result | Error: Catalog Error: Table with name PRODUCT_DENORM does not exist!
- ERROR testQualifierProperty [mapping/embedded]: property 'description' of embedded 'issuer' on class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' is not mapped in mapping 'meta::r
- SHAPE testEnumTheSame [mapping/enumeration]: no execute(|...) call
- SHAPE testMapping [mapping/enumeration]: multiple execute() calls
- ERROR testEnumInRelation [mapping/enumeration]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::enumeration::model::domain::Employee' (of 1 candidates); class-query dispa
- SHAPE testQueryWithEnum [mapping/enumeration]: multiple execute() calls
- SHAPE testEnumMappings [mapping/enumeration]: no execute(|...) call
- SHAPE testEnumMappingsWithInclude [mapping/enumeration]: no execute(|...) call
- ERROR testProjectionWithEnum [mapping/enumeration]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::enumeration::model::domain::Employee' (of 1 candidates); class-query dispa
- ERROR testProjectionWithEnumUsingLambda [mapping/enumeration]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::enumeration::model::domain::Employee' (of 1 candidates); class-query dispa
- ERROR testProjectionWithEnumAndFunctionsUsingLambda [mapping/enumeration]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::enumeration::model::domain::Employee' (of 1 candidates); class-query dispa
- SHAPE testInQueryWithEnum [mapping/enumeration]: multiple execute() calls
- ERROR testAggregationFunctionWithEnum [mapping/enumeration]: legacy groupBy expects (source, [keys], [aggs], ['aliases'])
- SHAPE testProjectionWithInheritedEnum [mapping/enumeration]: no recognizable assertions
- ERROR testProjectionWithEnumThroughAssociation [mapping/enumeration]: object-space expression node TypedMap is not substitutable yet (H2 vocabulary)
- ERROR testProjectionWithEnumQualifierParameter [mapping/enumeration]: [2:237] navigation path segment 'synonymsByType(meta::relational::tests::mapping::enumeration::model::domain::ProductSynonymType.CUSIP)' uses an unsupported pat
- ERROR testFilterWithEnumQualifierParameter [mapping/enumeration]: unknown function 'synonymsByType'
- SHAPE testProjectWithIfWhereOneSideIsEnumLiteral [mapping/enumeration]: no recognizable assertions
- ERROR testProjectWithIfWhereOneSideIsEnumLiteral2 [mapping/enumeration]: unbound variable '$value'
- SHAPE testProjectWithIfWhereBothSidesUseTheSameEnumMapping [mapping/enumeration]: no recognizable assertions
- SHAPE testProjectWithIfWhereEnumEqualsClassProp [mapping/enumeration]: partial: 1/3 asserts recognized (recognized ones hold)
- ERROR testTdsProjectWithSingleStringEnumEqualityComparison [mapping/enumeration]: project expects ~[…] column specifications
- ERROR testTdsProjectWithMultiStringEnumEqualityComparison [mapping/enumeration]: project expects ~[…] column specifications
- ERROR testTdsProjectWithEnumInequalityComparison [mapping/enumeration]: project expects ~[…] column specifications
- ERROR testTdsProjectWithEnumToStringEqualityComparison [mapping/enumeration]: project expects ~[…] column specifications
- ERROR testTdsProjectWithEnumsInInClause [mapping/enumeration]: project expects ~[…] column specifications
- SHAPE testTdsProjectWithEnumVarEquality [mapping/enumeration]: no execute(|...) call
- SHAPE testEnumValueReturnedInIfExp [mapping/enumeration]: no execute(|...) call
- SHAPE testEnumValueReturnedInIfExpNotDistinctTransformers [mapping/enumeration]: no execute(|...) call
- FAIL testDistinctMappingSelectAll [mapping/distinct]: name: expected [IF 1, IF 2, IF 2], got [IF 2, IF 1, IF 2, IF 2, IF 2]
- FAIL testDistinctMappingSelectAllWithFilter [mapping/distinct]: name: expected [IF 2], got [IF 2, IF 2, IF 2]; code: expected [1002], got [1002, 1002, 1002]
- FAIL testDistinctMappingSimpleProjectSelectOneOfTheDistinctProperties [mapping/distinct]: toCSV: expected <name
IF 1
IF 2
IF 2
>, got <name
IF 1
IF 2
>
- FAIL testDistinctMappingSimpleProjectDistinct [mapping/distinct]: toCSV: expected <name
IF 1
IF 2
>, got <name
IF 2
IF 1
>
- FAIL testDistinctMappingWithFilterSelectAll [mapping/distinct]: name: expected [IF 1, IF 2, IF 2], got [IF 2, IF 2, IF 2, IF 1]
- FAIL testDistinctMappingWithFilterSelectOneProperty [mapping/distinct]: toCSV: expected <name
IF 1
IF 2
IF 2
>, got <name
IF 1
IF 2
>
- ERROR testDistinctMappingWithJoinSelectAll [mapping/distinct]: mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- FAIL testDistinctMappingWithJoinProject [mapping/distinct]: toCSV: expected <IfName
IfName1
IfName2

>, got <IfName

IfName2
IfName1
>
- ERROR testProjectDistinctMappingWithDistinctInJoin [mapping/distinct]: mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- ERROR testProjectDistinctMappingWithDistinctInJoinWithDup [mapping/distinct]: mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- ERROR testDistinctMappingWithDistinctInJoinWithFilter [mapping/distinct]: mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- ERROR testDistinctMappingWithDistinctInJoinWithFilterOnJoin [mapping/distinct]: mapping pipeline for 'meta::relational::tests::mapping::distinct::model::domain::IncomeFunction' has TypedDistinct above join slot(s); H3-pending
- SHAPE testDistinctMappingWithFullDenormSelfJoins [mapping/distinct]: multiple execute() calls
- ERROR testDistinctMappingWithFullDenormSelfJoinsWithFilterOnJoin [mapping/distinct]: store-only navigate (class-extent target) reached the lowerer — resolver bug
- ERROR testDistinctMappingWithFullDenormSelfJoinsWithTwoFiltersOnJoin [mapping/distinct]: store-only navigate (class-extent target) reached the lowerer — resolver bug
- ERROR testDistinctMappingWithCaseStatement [mapping/distinct]: [1:335] navigation path segment 'meta::relational::tests::mapping::distinct::model::domain::Classification' uses an unsupported path feature (only plain propert
- ERROR testDistinctMappingWithSize [mapping/distinct]: class query under TypedNativeCall is not resolvable yet (H2 vocabulary)
- ERROR testGroupByMapping [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingWithFilter [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingWithFilterOnAggregate [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingWithFilterOnAggregateWithJoin [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingWithFilterOnAggregateWithProject [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingProject [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingProjectWithJoin [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingProjectWithJoinAndTableFilter [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR testGroupByMappingProjectWithGroupByInJoin [mapping/groupBy]: class 'meta::relational::tests::mapping::groupBy::model::domain::Position' is not mapped in mapping 'meta::relational::tests::mapping::groupBy::model::mapping::
- ERROR testGroupByMappingProjectWithMultipleGroupBys [mapping/groupBy]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch 
- ERROR filterMappingWithJoinInFilterAndPropertyGetAll [mapping/filter]: mapping ~filter for 'meta::relational::tests::model::simple::Person' reads through a join slot; join-mediated mapping filters are H3-pending
- ERROR testFilterMappingWithJoin [mapping/filter]: mapping ~filter for 'meta::relational::tests::mapping::filter::model::domain::Org' reads through a join slot; join-mediated mapping filters are H3-pending
- ERROR testFilterMappingWithProjection [mapping/filter]: project expects ~[…] column specifications
- ERROR testFilterMappingWithProjectionOverlapp [mapping/filter]: multi-hop navigation parent.parent.name through an embedded/slot head is not supported yet
- ERROR testFilterMappingWithProjectionAndJoin [mapping/filter]: project expects ~[…] column specifications
- SHAPE testGetterWithTargetFilter [mapping/filter]: no recognizable assertions
- ERROR testFilterMappingWithProjectionAndJoinAndQuery [mapping/filter]: mapping ~filter for 'meta::relational::tests::mapping::filter::model::domain::Org' reads through a join slot; join-mediated mapping filters are H3-pending
- SHAPE testAssociation [mapping/inheritance]: multiple execute() calls
- ERROR testGroupBy [mapping/inheritance]: no overload of 'agg' matches 3 argument(s) of these shapes
- ERROR testProject [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- ERROR testProjectAssociation [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testProjectAssociationTdsV2 [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testProjectTwoLambdas [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProject [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testProjectTwoLambdasWithAutomap [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testProjectTwoLambdas [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGroupBy [mapping/inheritance]: no overload of 'agg' matches 3 argument(s) of these shapes
- ERROR testQuery [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProject [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProjectBooleanInFilter [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGetAll [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- ERROR testGetAllFilter [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- SHAPE testGetAllFilterWithAssociation [mapping/inheritance]: multiple execute() calls
- SHAPE testSubTypeFilter [mapping/inheritance]: multiple execute() calls
- ERROR testSubTypeProjectWithAssociation [mapping/inheritance]: [1:97] expected ']' to close collection literal
- ERROR testSubTypeProjectDirect [mapping/inheritance]: [1:95] expected ']' to close collection literal
- ERROR testSubTypeProjectShared [mapping/inheritance]: [1:103] expected ')' to close argument list
- ERROR testSubTypeProjectSharedNonDirectlyRouted [mapping/inheritance]: [1:95] expected ']' to close collection literal
- ERROR testSubTypeGroupBy [mapping/inheritance]: [2:66] expected ')' to close argument list
- ERROR testSubTypeGroupByThroughMap [mapping/inheritance]: [2:70] expected ')' to close argument list
- ERROR testFilteringOnColumnsNotInProject [mapping/inheritance]: project expects ~[…] column specifications
- ERROR testFilteringOnColumnsNotInProjectSingleChildStructure [mapping/inheritance]: project expects ~[…] column specifications
- ERROR testProjectQualifiedPropertyFromUnmappedSuperClass [mapping/inheritance]: project expects ~[…] column specifications
- ERROR testEmbeddMappingInSubTypes [mapping/inheritance]: [1:93] expected ']' to close collection literal
- ERROR testMilestonedSubTyping [mapping/inheritance]: [1:110] expected ']' to close collection literal
- ERROR testMilestonedSubTypingWithDifferentDates [mapping/inheritance]: [1:123] expected ']' to close collection literal
- ERROR testProjectAssociation [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testSubTypeProjectDirect [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- ERROR testForcedSubTypeProjectDirect [mapping/inheritance]: [1:122] expected ']' to close collection literal
- ERROR testSubTypeProjectSharedNonDirectlyRouted [mapping/inheritance]: [1:95] expected ']' to close collection literal
- ERROR testProjectTwoLambdas [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGroupBy [mapping/inheritance]: no overload of 'agg' matches 3 argument(s) of these shapes
- ERROR testQuery [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProject [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testFilterProjectBooleanInFilter [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGetAll [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- ERROR testGetAllFilter [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exac
- SHAPE testGetAllFilterWithAssociation [mapping/inheritance]: multiple execute() calls
- SHAPE testSubTypeFilter [mapping/inheritance]: multiple execute() calls
- ERROR testSubTypeProjectWithAssociation [mapping/inheritance]: [1:97] expected ']' to close collection literal
- ERROR testSubTypeProjectDirect [mapping/inheritance]: [1:95] expected ']' to close collection literal
- ERROR testSubTypeProjectShared [mapping/inheritance]: [1:103] expected ')' to close argument list
- ERROR testSubTypeProjectSharedNonDirectlyRouted [mapping/inheritance]: [1:95] expected ']' to close collection literal
- ERROR testProject [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::MyProduct' (of 1 candidates); class-query dispatch needs exactly 
- ERROR testProjectWithIds [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::MyProduct' (of 1 candidates); class-query dispatch needs exactly 
- ERROR testObjectQuery [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::MyProduct' (of 1 candidates); class-query dispatch needs exactly 
- ERROR testProjectSubtype [mapping/inheritance]: [1:87] expected ']' to close collection literal
- ERROR testSubTypeMappingValidWhenMappedExplicitly [mapping/inheritance]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::CreditRating' (of 1 candidates); class-query dispatch needs exact
- SHAPE testSelfJoinPropertyMapping [mapping/selfJoin]: no recognizable assertions
- ERROR testSelfJoinPropertyMappingOverlap [mapping/selfJoin]: multi-hop navigation parent.parent.name through an embedded/slot head is not supported yet
- ERROR testSelfJoinPropertyMappingWithDynaFunction [mapping/selfJoin]: multi-hop navigation parent.parent.parent.name through an embedded/slot head is not supported yet
- ERROR testGet [mapping/boolean.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Interaction' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testQuery [mapping/boolean.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Interaction' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testProject [mapping/boolean.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Interaction' (of 1 candidates); class-query dispatch needs exactly o
- ERROR testGet [mapping/dates.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- SHAPE testQuery [mapping/dates.pure]: multiple execute() calls
- ERROR testProject [mapping/dates.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- ERROR testGet [mapping/dates.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- SHAPE testQuery [mapping/dates.pure]: multiple execute() calls
- ERROR testProject [mapping/dates.pure]: runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one; 'm
- SHAPE retrieveDateWithTimeZone [mapping/dates.pure]: multiple execute() calls
