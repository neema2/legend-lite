# Relational corpus scoreboard (real legend-engine core_relational)

RUN-as-data over the local legend-engine checkout; row equality is the
contract, golden SQL is advisory. SHAPE = test body/assert form the
runner does not yet recognize (accounted, not skipped silently).

| family | tests | pass | fail | error | shape |
|---|---|---|---|---|---|
| query | 99 | 0 | 0 | 76 | 23 |
| mapping/association | 24 | 0 | 0 | 22 | 2 |
| mapping/join | 29 | 0 | 0 | 29 | 0 |
| mapping/embedded | 70 | 0 | 0 | 65 | 5 |
| mapping/enumeration | 27 | 0 | 0 | 18 | 9 |
| mapping/distinct | 18 | 0 | 0 | 17 | 1 |
| mapping/groupBy | 10 | 0 | 0 | 10 | 0 |
| mapping/filter | 10 | 0 | 0 | 10 | 0 |
| mapping/inheritance | 51 | 0 | 0 | 46 | 5 |
| mapping/selfJoin | 3 | 0 | 0 | 3 | 0 |
| mapping/boolean.pure | 3 | 0 | 0 | 3 | 0 |
| mapping/dates.pure | 7 | 0 | 0 | 4 | 3 |
| **total** | 351 | **0** | 0 | 303 | 48 |

### mapping walls (dropped at assembly)

- meta::relational::tests::MappingWithInnerJoinAndEmbeddedMappingSub => [1183:1] ColumnRef references table 'firmTable' not in scope; available=[personTable]
- meta::pure::mapping::modelToModel::test::simple::PersonPureMappingSub => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::PersonView' is not a known primitive, class, or enum
- meta::pure::mapping::modelToModel::test::simple::OrderContactPureMapping => Unknown type: 'meta::pure::mapping::modelToModel::test::shared::dest::OrderContactView' is not a known primitive, class, or enum
- meta::relational::tests::simpleRelationalMappingIncWithStoreFilter => [1274:1] ColumnRef references table 'PersonView' not in scope; available=[personTable]
- meta::relational::tests::simpleRelationalMappingInc => [1358:1] PropertyMapping 'firm' references property not declared on class 'meta::relational::tests::model::simple::PersonWithConstraints'; mapping=meta::relational::tests::simpleRelationalMappingInc
- meta::relational::tests::simpleRelationalMappingWithEnumConstant => enum-mapped constant/expression source for property 'type' is not resolvable yet
- meta::relational::tests::simpleRelationalMapping => Join 'OrderPnlView_Order' references multiple non-source tables [orderPnlView, orderTable]; multi-table joins not supported. owner=order, hop 1, mapping=meta::relational::tests::simpleRelationalMapping
- meta::relational::tests::simpleRelationalMappingWithClassFilterAndEmbeddedProperty => Join 'Firm_Person' references multiple non-source tables [firmTable, personTable]; multi-table joins not supported. owner=firm, hop 1, mapping=meta::relational::tests::simpleRelationalMappingWithClassFilterAndEmbeddedProperty
- meta::relational::tests::simpleRelationalMappingWithConcat => Relational mapping without ~mainTable, sourceUrl, or an inferable column binding; class=meta::relational::tests::model::simple::Order, mapping=meta::relational::tests::simpleRelationalMappingWithConcat. See docs/MAPPING_LEGACY_TO_FUNCTION.md §5.2.3.
- meta::relational::tests::TestMappingOfSubtypeClass => [1560:1] PropertyMapping 'firm' references property not declared on class 'meta::relational::tests::model::simple::PersonExtension'; mapping=meta::relational::tests::TestMappingOfSubtypeClass
- meta::relational::tests::TestMappingWithViewJoins => [1560:1] Join 'personViewWithFirmTable' not found in db 'meta::relational::tests::db'; PM='null', mapping=meta::relational::tests::TestMappingWithViewJoins

### top error buckets

- 48x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Person' (of 1 candidates); class-query dispatch needs exactly one
- 28x class meta::relational::tests::model::simple::Product has no property 'bondDetails'
- 27x project expects ~[…] column specifications
- 24x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Trade' (of 1 candidates); class-query dispatch needs exactly one
- 22x class meta::relational::tests::model::simple::Person has no property 'roadVehicles'
- 20x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Firm' (of 1 candidates); class-query dispatch needs exactly one
- 11x 'BondDetail' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 7x 'Org' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 6x [1:50] expected ']' to close collection literal
- 6x 'RoadVehicle' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 5x [1:52] expected ']' to close collection literal
- 5x serialize expects (classCollection, #{Class{…}}#)
- 5x 'IncomeFunction' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 4x 'Position' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 3x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Account' (of 1 candidates); class-query dispatch needs exactly one
- 3x class query under TypedPropertyAccess is not resolvable yet (H2 vocabulary)
- 3x class meta::relational::tests::model::simple::Employee has no property 'name'
- 3x class meta::relational::tests::model::simple::Product has no property 'description'
- 3x 'MyProduct' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 2x unbound variable '$var'
- 2x unknown function 'nameWithTitle'
- 2x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::OrderPnl' (of 1 candidates); class-query dispatch needs exactly one
- 2x runtime 'rcorpus::Rt' has 0 mappings binding class 'meta::relational::tests::model::simple::Order' (of 1 candidates); class-query dispatch needs exactly one
- 2x unbound variable '$input'
- 2x a bare lambda has no type outside a call position (lambdas type against their call's signature)
- 2x 'TypeBuiltOutOfMultipleJoins' is not a known class, mapping, runtime, connection, or database — user elements in a query need a fully qualified name
- 2x legacy groupBy expects (source, [keys], [aggs], ['aliases'])
- 2x [2:179] navigation path segment 'name!address' uses an unsupported path feature (only plain property segments desugar): #/meta::relational::tests::model::simple::Person/address/name!address#
- 2x [2:179] navigation path segment 'type!address' uses an unsupported path feature (only plain property segments desugar): #/meta::relational::tests::model::simple::Person/address/type!address#
- 2x [1:108] navigation path segment 'name!incomeFunction' uses an unsupported path feature (only plain property segments desugar): #/Classification/incomeFunctions/name!incomeFunction#
