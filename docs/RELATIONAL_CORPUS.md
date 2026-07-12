# Relational corpus scoreboard (real legend-engine core_relational)

RUN-as-data over the local legend-engine checkout; row equality is the
contract, golden SQL is advisory. SHAPE = test body/assert form the
runner does not yet recognize (accounted, not skipped silently).

| family | tests | pass | fail | error | shape |
|---|---|---|---|---|---|
| query | 99 | 12 | 1 | 63 | 23 |
| mapping/association | 24 | 5 | 0 | 17 | 2 |
| mapping/join | 29 | 0 | 0 | 29 | 0 |
| mapping/embedded | 70 | 0 | 0 | 65 | 5 |
| mapping/enumeration | 27 | 0 | 0 | 17 | 10 |
| mapping/distinct | 18 | 5 | 3 | 9 | 1 |
| mapping/groupBy | 10 | 0 | 0 | 10 | 0 |
| mapping/filter | 10 | 0 | 3 | 6 | 1 |
| mapping/inheritance | 51 | 0 | 0 | 46 | 5 |
| mapping/selfJoin | 3 | 0 | 0 | 3 | 0 |
| mapping/boolean.pure | 3 | 0 | 0 | 3 | 0 |
| mapping/dates.pure | 7 | 0 | 0 | 4 | 3 |
| **total** | 351 | **22** | 7 | 272 | 50 |

### mapping walls (dropped at assembly)

- meta::pure::mapping::modelToModel::test::simple::PersonPureMappingSub => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::PersonView' is not a known primitive, class, or enum
- meta::pure::mapping::modelToModel::test::simple::OrderContactPureMapping => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::OrderContactView' is not a known primitive, class, or enum
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/query/datePeriods.pure meta::relational::tests::groupBy::datePeriods::mapping::myMapping => [2301:0] unsupported top-level keyword: GREATER_THAN ('>')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/association/testAssociationEmbedded.pure meta::relational::tests::mapping::association::embedded::associationMappingInlinedEmbedded => [2322:1] AssociationMapping join 'Firm_Organizations' not found in db 'myDB'; association='meta::relational::tests::model::simple::FirmOrganizations', mapping=meta::relational::tests::mapping::association::embedded::associationMappingInlinedEmbedded
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbeddedOtherwise => Unknown type: 'Party' is not a known primitive, class, or enum
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbeddedOtherwise2 => Unknown type: 'BondDetail' is not a known primitive, class, or enum
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbeddedOtherwise3 => Unknown type: 'Party' is not a known primitive, class, or enum
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testEmbeddedOtherwiseMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbeddedOtherwiseWithUnion => Unknown type: 'BondDetail' is not a known primitive, class, or enum
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testInlineEmbeddedMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbedded => [2495:4] expected identifier, got PAREN_OPEN
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testInlineEmbeddedMapping.pure meta::relational::tests::mapping::embedded::advanced::mapping::testMappingEmbeddedParent => [2495:4] expected identifier, got PAREN_OPEN
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/embedded/testInlineEmbeddedMapping.pure meta::relational::tests::mapping::embedded::advanced::model::testMilestonedEmbeddedInlineMapping => [2495:4] expected identifier, got PAREN_OPEN
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/enumeration/testEnumerationMappingDomain.pure meta::relational::tests::mapping::enumeration::model::mapping::employeeTestMapping => [2522:78] Missing table or alias for column 'type'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/enumeration/testEnumerationMappingDomain.pure meta::relational::tests::mapping::enumeration::model::mapping::employeeTestMappingWithFunction => [2545:59] Missing table or alias for column 'type'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/enumeration/testTransposeEnumrationMapping.pure meta::relational::tests::mapping::enumeration::model::mapping::employeeTestMappingWithTransposeFunction => [2308:68] Missing table or alias for column 'skills'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/distinct/testDistinct.pure meta::relational::tests::mapping::distinct::model::mapping::testMappingWithCase => [2547:36] Missing table or alias for column 'IF_NAME'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/groupBy/testGroupBy.pure meta::relational::tests::mapping::groupBy::model::mapping::testMapping => [2401:28] Missing table or alias for column 'QTY'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/groupBy/testGroupBy.pure meta::relational::tests::mapping::groupBy::model::mapping::testMappingWithFilter => [2402:28] Missing table or alias for column 'QTY'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/groupBy/testGroupBy.pure meta::relational::tests::mapping::groupBy::model::mapping::testMappingWithTwoGroupBysAndFilters => [2402:28] Missing table or alias for column 'QTY'
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inheritance/testInheritanceRelationalMilestoned.pure meta::relational::tests::model::inheritance::milestoned::MilestonedInheritanceMapping => [2314:4] expected identifier, got PAREN_OPEN
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inheritance/testSubtypeMapping.pure meta::relational::tests::mapping::subType::SubTypeMappingValidWhenMappedExplicitly => [2336:3] clean-sheet mapping binding for 'MyProduct' has an empty body
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inheritance/testSubtypeMapping.pure meta::relational::tests::mapping::subType::MyMapping => [2339:14] expected integer or string inside '[...]' index, got VALID_STRING ('meta_relational_tests_model_simple_Trade')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inheritance/testSubtypeMapping.pure meta::relational::tests::mapping::subType::MyMappingWithIds => [2328:13] expected integer or string inside '[...]' index, got VALID_STRING ('meta_relational_tests_mapping_subType_MyProduct')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inClause/testInClauseForJoinsAndFilters.pure meta::relational::tests::mapping::in::inClauseMapping0 => [2273:0] unsupported top-level keyword: VALID_STRING ('meta')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inClause/testInClauseForJoinsAndFilters.pure meta::relational::tests::mapping::in::inClausePrefixMapping1 => [2273:0] unsupported top-level keyword: VALID_STRING ('meta')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inClause/testInClauseForJoinsAndFilters.pure meta::relational::tests::mapping::in::inClausePrefixMapping2 => [2273:0] unsupported top-level keyword: VALID_STRING ('meta')
- /Users/neema/legend/legend-engine/legend-engine-xts-relationalStore/legend-engine-xt-relationalStore-generation/legend-engine-xt-relationalStore-pure/legend-engine-xt-relationalStore-core-pure/src/main/resources/core_relational/relational/tests/mapping/inClause/testInClauseForJoinsAndFilters.pure meta::relational::tests::mapping::in::filterMapping => [2273:0] unsupported top-level keyword: VALID_STRING ('meta')

### top error buckets

- 28x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- 25x in function 'meta::relational::tests::simpleRelationalMapping$class$meta::relational::tests::model::simple::Trade': unknown table 'tradeEventViewMaxTradeEventDate' in database 'meta::relational::tests::db'
- 21x class meta::relational::tests::model::simple::Product has no property 'bondDetails'
- 11x 'BondDetail' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 10x project expects ~[…] column specifications
- 9x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::groupBy::model::domain::Position' (of 1 candidates); class-query dispatch needs exactly one
- 8x class-typed property '$f.employees' used as a whole value is graph output (Phase H4)
- 8x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Firm' (of 1 candidates); class-query dispatch needs exactly one
- 7x [2495:4] expected identifier, got PAREN_OPEN
- 6x [1:95] expected ']' to close collection literal
- 6x mapping 'meta::relational::tests::mapping::inheritance::cross::inheritanceMappingCross' includes unknown mapping 'meta::relational::tests::mapping::inheritance::inheritanceMain' (a silently-unresolved include hid class bindings)
- 5x serialize expects (classCollection, #{Class{…}}#)
- 5x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::RoadVehicle' (of 1 candidates); class-query dispatch needs exactly one
- 4x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::inheritance::Person' (of 1 candidates); class-query dispatch needs exactly one
- 4x no overload of 'agg' matches 3 argument(s) of these shapes
- 4x class meta::relational::tests::model::simple::Employee has no property 'name'
- 4x in function 'meta::relational::tests::mapping::inheritance::inheritanceMain$class$meta::relational::tests::model::inheritance::Person': expected meta::relational::tests::model::inheritance::Vehicle, got (ID:Integer[1], db_bic_wheelCount:Integer[0..1], b_Description:String[0..1], b_PersonID:Integer[0..1])
- 4x in function 'meta::relational::tests::mapping::inheritance::relational::union::inheritanceUnion$class$meta::relational::tests::model::inheritance::Person': expected meta::relational::tests::model::inheritance::Vehicle, got (ID:Integer[1], db_bic_wheelCount:Integer[0..1], b_Description:String[0..1], b_PersonID:Integer[0..1])
- 3x in function 'meta::relational::tests::simpleRelationalMapping$class$meta::relational::tests::model::simple::Account': unknown table 'accountOrderPnlView' in database 'meta::relational::tests::db'
- 3x [1:97] expected ']' to close collection literal
- 3x class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- 3x class meta::relational::tests::model::simple::Product has no property 'description'
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::mapping::subType::MyProduct' (of 1 candidates); class-query dispatch needs exactly one
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Interaction' (of 1 candidates); class-query dispatch needs exactly one
- 2x [2301:0] unsupported top-level keyword: GREATER_THAN ('>')
- 2x unbound variable '$var'
- 2x property 'name' of class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::tests::simpleRelationalMapping'
- 2x unknown function 'nameWithTitle'
- 2x mapping pipeline for 'meta::relational::tests::model::simple::OrderPnl' has TypedDistinct above join slot(s); H3-pending
- 2x unbound variable '$input'
