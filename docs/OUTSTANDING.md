# OUTSTANDING — feature-keyed ledger of non-passing corpus tests

A FROZEN HISTORY: written by tmp/outstanding.py (no longer run, and no build runs it) over docs/RELATIONAL_CORPUS.md
+ the corpus sources (Bazel workplan P2-18: no file claims to be generated without a producer and a diff test).
Primary key = family dir + defining file + test-name tokens (the FEATURE).
`stage` = the CURRENT wall only — sequential walls hide behind it.

## Pivot: family dir x current stage

-   33  executionPlan/tests  ::  rows-differ
-   11  pureToSQLQuery/tests  ::  harness-shape
-   11  tests  ::  harness-shape
-   10  functions/tests  ::  rows-differ
-    9  executionPlan/tests  ::  other
-    9  lineage/scanRelations  ::  harness-shape
-    9  milestoning/tests  ::  harness-shape
-    8  functions/tests  ::  other
-    8  tds/tests  ::  harness-shape
-    7  executionPlan/tests  ::  harness-shape
-    7  functions/tests  ::  platform-surface
-    7  transform/fromPure/tests  ::  harness-shape
-    6  graphFetch/tests  ::  rows-differ
-    5  aggregationAware/test/rewrite/NOP  ::  execute(DuckDB)
-    5  functions/tests/projection  ::  other
-    5  functions/tests/projection  ::  rows-differ
-    5  tds/tests  ::  other
-    5  tests/query  ::  other
-    4  graphFetch/tests  ::  typer
-    4  graphFetch/tests  ::  platform-surface
-    4  postprocessor/tests  ::  harness-shape
-    4  testDataGeneration/tests  ::  rows-differ
-    4  tests/advanced  ::  other
-    4  tests/mapping/inheritance  ::  normalize(mapping)
-    3  functions/tests/projection  ::  resolve
-    3  functions/tests/projection  ::  typer
-    3  functions/tests/projection  ::  harness-shape
-    3  graphFetch/tests  ::  normalize(mapping)
-    3  modelToModelToRelational/milestoned  ::  typer
-    3  postprocessor/tests  ::  other
-    3  sqlQueryToString/DDL  ::  harness-shape
-    3  tds/tests  ::  rows-differ
-    3  tests  ::  rows-differ
-    3  tests/mapping  ::  rows-differ
-    3  tests/mapping/association  ::  normalize(mapping)
-    3  tests/mapping/classMappingFilterWithInnerJoin  ::  normalize(mapping)
-    3  tests/mapping/embedded  ::  other
-    3  tests/mapping/embedded  ::  resolve
-    3  tests/mapping/enumeration  ::  harness-shape
-    3  tests/mapping/enumeration  ::  rows-differ
-    3  tests/mapping/inheritance  ::  platform-surface
-    3  tests/mapping/modelJoin  ::  normalize(mapping)
-    3  tests/mapping/relation  ::  rows-differ
-    3  tests/mapping/union  ::  normalize(mapping)
-    3  transform/fromPure/tests  ::  render(DB2)
-    3  transform/fromPure/tests  ::  other
-    3  validation/tests  ::  other
-    2  functions/tests  ::  resolve
-    2  functions/tests  ::  execute(DuckDB)
-    2  functions/tests  ::  harness-shape
-    2  functions/tests/projection  ::  lower
-    2  helperFunctions/tests  ::  harness-shape
-    2  lineage/scanColumns  ::  other
-    2  milestoning/tests  ::  other
-    2  milestoning/tests  ::  resolve
-    2  milestoning/tests  ::  rows-differ
-    2  modelJoins  ::  rows-differ
-    2  modelToModelToRelational/milestoned  ::  normalize(mapping)
-    2  modelToModelToRelational/milestoned  ::  harness-shape
-    2  router/tests  ::  harness-shape
-    2  router/tests  ::  typer
-    2  tds/relation  ::  harness-shape
-    2  tds/tests  ::  resolve
-    2  tests/advanced  ::  rows-differ
-    2  tests/advanced  ::  harness-shape
-    2  tests/injection  ::  other
-    2  tests/mapping/association  ::  resolve
-    2  tests/mapping/enumeration  ::  other
-    2  tests/mapping/inheritance  ::  other
-    2  tests/mapping/join  ::  rows-differ
-    2  tests/mapping/modelJoin  ::  resolve
-    2  tests/mapping/selfJoin  ::  rows-differ
-    2  tests/mapping/tree  ::  rows-differ
-    2  tests/query  ::  typer
-    2  transform/fromPure/tests  ::  rows-differ
-    1  autogeneration/tests  ::  harness-shape
-    1  executionPlan/tests  ::  platform-surface
-    1  executionPlan/tests  ::  typer
-    1  functions/tests/loadCsvToDbTable  ::  typer
-    1  functions/tests/projection  ::  normalize(mapping)
-    1  functions/tests/projection  ::  execute(DuckDB)
-    1  graphFetch/domain  ::  rows-differ
-    1  graphFetch/tests  ::  harness-shape
-    1  graphFetch/tests  ::  resolve
-    1  graphFetch/tests/union  ::  rows-differ
-    1  graphFetch/tests/union  ::  other
-    1  lineage/scanColumns  ::  normalize(mapping)
-    1  modelJoins  ::  harness-shape
-    1  postprocessor/tests  ::  typer
-    1  router/tests  ::  rows-differ
-    1  router/tests  ::  resolve
-    1  sqlQueryToString  ::  harness-shape
-    1  sqlQueryToString/testSuite  ::  harness-shape
-    1  tds/tests  ::  platform-surface
-    1  tds/tests  ::  lower
-    1  testDataGeneration/tests  ::  resolve
-    1  testDataGeneration/tests  ::  normalize(mapping)
-    1  testDataGeneration/tests  ::  other
-    1  testDataGeneration/tests  ::  harness-shape
-    1  tests/advanced  ::  resolve
-    1  tests/datatype  ::  rows-differ
-    1  tests/datatype  ::  platform-surface
-    1  tests/mapping  ::  lower
-    1  tests/mapping/association  ::  other
-    1  tests/mapping/classMappingFilterWithInnerJoin  ::  execute(DuckDB)
-    1  tests/mapping/embedded  ::  rows-differ
-    1  tests/mapping/embedded  ::  normalize(mapping)
-    1  tests/mapping/filter  ::  rows-differ
-    1  tests/mapping/include  ::  harness-shape
-    1  tests/mapping/join  ::  other
-    1  tests/mapping/join  ::  resolve
-    1  tests/mapping/modelJoin  ::  rows-differ
-    1  tests/mapping/multigrain  ::  resolve
-    1  tests/mapping/sqlFunction  ::  execute(DuckDB)
-    1  tests/mapping/sqlFunction  ::  other
-    1  tests/mapping/union  ::  resolve
-    1  tests/mapping/union  ::  execute(DuckDB)
-    1  tests/mapping/union  ::  typer
-    1  tests/mapping/union  ::  rows-differ
-    1  tests/query  ::  resolve
-    1  tests/query  ::  harness-shape
-    1  tests/query  ::  execute(DuckDB)
-    1  transform/fromPure/tests  ::  platform-surface
-    1  validation/tests  ::  resolve

## Pivot: intent x status

-   64  row-assert  ::  ERROR
-   44  row-assert  ::  SHAPE
-   35  golden-sql+row-assert  ::  ERROR
-   28  row-assert  ::  FAIL
-   27  row-assert+graph  ::  ERROR
-   21  row-assert+plan-assert  ::  SHAPE
-   20  golden-sql+row-assert  ::  SHAPE
-   16  golden-sql+row-assert  ::  FAIL
-   13  row-assert+plan-assert  ::  ERROR
-   12  ?  ::  SHAPE
-    9  row-assert+plan-assert  ::  FAIL
-    9  row-assert+plan-assert+graph  ::  SHAPE
-    9  row-assert+lineage  ::  SHAPE
-    5  row-assert+plan-assert+graph  ::  FAIL
-    4  golden-sql+row-assert+plan-assert  ::  FAIL
-    4  row-assert+graph  ::  SHAPE
-    4  row-assert+constraints  ::  ERROR
-    3  plan-assert  ::  SHAPE
-    3  row-assert+plan-assert+graph  ::  ERROR
-    3  golden-sql  ::  FAIL
-    3  golden-sql  ::  SHAPE
-    2  ?  ::  ERROR
-    2  golden-sql+plan-assert  ::  SHAPE
-    2  graph  ::  SHAPE
-    2  row-assert+graph  ::  FAIL
-    2  golden-sql+row-assert+plan-assert  ::  SHAPE
-    2  golden-sql  ::  ERROR
-    1  ?  ::  FAIL
-    1  plan-assert  ::  FAIL
-    1  golden-sql+row-assert+plan-assert+graph  ::  ERROR
-    1  row-assert+lineage  ::  ERROR
-    1  row-assert+lineage  ::  FAIL
-    1  row-assert+lineage+graph  ::  SHAPE
-    1  row-assert+lineage+plan-assert  ::  SHAPE
-    1  row-assert+printer  ::  FAIL
-    1  row-assert+printer  ::  SHAPE
-    1  golden-sql+row-assert+plan-assert  ::  ERROR

## Pivot: stereotype

-  274  Test
-   43  Test, AlloyOnly
-   25  meta::pure::profiles::Test
-   10  meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly
-    5  ?
-    1  meta::pure::profiles::Test, AlloyOnly

## Ledger (one row per test)

| status | family | file | test | stage | intent | stereo | wall |
|---|---|---|---|---|---|---|---|
| ERROR | aggregationAware/test/rewrite/NOP | nonAggregationAware.pure | testRewriteFilter | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| ERROR | aggregationAware/test/rewrite/NOP | nonAggregationAware.pure | testRewriteGetAllQuery | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| ERROR | aggregationAware/test/rewrite/NOP | nonAggregationAware.pure | testRewriteProjectFunction | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| ERROR | aggregationAware/test/rewrite/NOP | nonAggregationAware.pure | testRewriteProjectFunctionMulti | execute(DuckDB) | row-assert | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| ERROR | aggregationAware/test/rewrite/NOP | nonAggregationAware.pure | testRewriteTDSOperation | execute(DuckDB) | row-assert | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| SHAPE | autogeneration/tests | relationalToPure.pure | testClassesAssociationsAndMappingFromDatabase | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::extension] — wall: unknown class 'meta::protocols::p |
| SHAPE | executionPlan/tests | ? | testEnumPushDownWithExternalFormat | rows-differ | ? | ? | assert form 'assertEquals/2' is not supported yet — plan wall: unknown function 'meta::external::for |
| FAIL | executionPlan/tests | ? | testMultiExpressionWithPlatformAndFromFunction | rows-differ | ? | ? | assertEquals: expected Sequence\n(\n  type = Class[impls=(meta::relational::tests::model::simple::Pe |
| SHAPE | executionPlan/tests | ? | testRelationalProjectionWithExternalFormat | rows-differ | ? | ? | assert form 'assertEquals/2' is not supported yet — plan wall: unknown function 'meta::external::for |
| SHAPE | executionPlan/tests | executionPlanExecutionTest.pure | testPureExecutionStrategyForCreateAndPopulateTempTableExecutionNode | harness-shape | plan-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: assert form 'assertE |
| SHAPE | executionPlan/tests | executionPlanExecutionTest.pure | testPureExecutionStrategyForRelationalInstantiationExecutionNode | harness-shape | plan-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: assert form 'assertE |
| SHAPE | executionPlan/tests | executionPlanTest.pure | inheritance | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: plan: no class mapping for 'meta::rel |
| SHAPE | executionPlan/tests | executionPlanTest.pure | relationalTDSTypeForColumnsAndQuoting | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: planToString: no getAll root (multi-n |
| ERROR | executionPlan/tests | executionPlanTest.pure | tdsJoinOneDBOneExpression | other | row-assert+plan-assert | Test | Index 0 out of bounds for length 0 |
| ERROR | executionPlan/tests | executionPlanTest.pure | tdsJoinTwoDBExtend | other | row-assert+plan-assert | Test | ArrayIndexOutOfBoundsException |
| ERROR | executionPlan/tests | executionPlanTest.pure | tdsJoinTwoDBWithColumnMappedViaJoins | other | row-assert+plan-assert | Test | ArrayIndexOutOfBoundsException |
| ERROR | executionPlan/tests | executionPlanTest.pure | tdsTwoJoinThreeDB | other | row-assert+plan-assert | Test | ArrayIndexOutOfBoundsException |
| ERROR | executionPlan/tests | executionPlanTest.pure | testCrossDbPlanGenerationWithFromWithoutExternalMapping | other | row-assert+plan-assert | Test | ArrayIndexOutOfBoundsException |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testCrossDbPlanGenerationWithRelationFromWithOnlyRuntimes | harness-shape | row-assert+plan-assert | Test | no execute(\|...) call [calls meta::relational::extension] — wall: assert form 'assertEquals/2' is n |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testCrossDbPlanGenerationWithRelationUsesCorrectColumnTypes | harness-shape | row-assert+plan-assert | Test | no execute(\|...) call [calls meta::relational::extension] — wall: assert form 'assertEquals/2' is n |
| ERROR | executionPlan/tests | executionPlanTest.pure | testDatabaseConnectionSQLPopulation | other | row-assert+plan-assert | Test | class meta::relational::mapping::SQLExecutionNode has no property 'connection' |
| ERROR | executionPlan/tests | executionPlanTest.pure | testDatabaseConnectionSQLPopulationLegacy | other | row-assert+plan-assert | Test | class meta::relational::mapping::SQLExecutionNode has no property 'connection' |
| FAIL | executionPlan/tests | executionPlanTest.pure | testExecutionPLanGenerationForFromInAllocation | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Allocation\n(\n  type = Class[impls=(meta::pure::mapping::modelToModel::test: |
| FAIL | executionPlan/tests | executionPlanTest.pure | testExecutionPlanGenerationForInWithVarAndConstantInputs | rows-differ | row-assert+plan-assert | Test | assertEquals: expected RelationalBlockExecutionNode(type=TDS[(fullName,String,VARCHAR(1000),"")](Fun |
| FAIL | executionPlan/tests | executionPlanTest.pure | testExecutionPlanGenerationForLambdaFromWithEnumMapping | rows-differ | plan-assert | Test | assert did not hold (false) |
| FAIL | executionPlan/tests | executionPlanTest.pure | testExecutionPlanGenerationForMultipleInWithTwoCollectionInputs | rows-differ | row-assert+plan-assert | Test | assertEquals: expected RelationalBlockExecutionNode(type=TDS[(fullName,String,VARCHAR(1000),"")](Fun |
| FAIL | executionPlan/tests | executionPlanTest.pure | testFilterInWithResultSorcedFromAnExpression | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Sequence(type=TDS[(firm,String,VARCHAR(200),"")](FunctionParametersValidation |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testGraphFetchH2TempTableStrategy | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class meta::pure::graphFetch::executi |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testGraphFetchH2TempTableStrategyWithQuoteIdentifiers | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class meta::pure::graphFetch::executi |
| FAIL | executionPlan/tests | executionPlanTest.pure | testGroupByWithOpenVariableInAgg | rows-differ | golden-sql+row-assert+plan-assert | Test | assertEquals: expected Sequence\n(\n  type = TDS[(Sales Division, String, VARCHAR(30), ""), (Income  |
| FAIL | executionPlan/tests | executionPlanTest.pure | testGroupByWithTwoOpenVariablesInAggAndFilter | rows-differ | golden-sql+row-assert+plan-assert | Test | assertEquals: expected Sequence\n(\n  type = TDS[(Sales Division, String, VARCHAR(30), ""), (Income  |
| FAIL | executionPlan/tests | executionPlanTest.pure | testMapWithOpenVariable | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Sequence\n(\n  type = Integer\n  resultSizeRange = *\n  (\n    Allocation\n   |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testMapWithOpenVariableOutsideBlock | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: object-space expression node TypedNew |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testModelConnectionAgg | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: model-to-model binding of 'meta::pure |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testModelConnectionDeepFunction | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: model-to-model binding of 'meta::pure |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testModelConnectionJoin | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class 'meta::pure::mapping::modelToMo |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testModelConnectionMultipleAgg | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: model-to-model binding of 'meta::pure |
| ERROR | executionPlan/tests | executionPlanTest.pure | testPlanForExecutionOption | platform-surface | row-assert+plan-assert | Test | Unknown type: 'PlanVarPlaceHolder' is not a known primitive, class, or enum |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testPlanGenerationForMultipleExpressionsWithPropertyPath | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: plan: struct extraction has no engine |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testPlanWithLocalH2ConnectionWithSQL | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class meta::relational::mapping::SQLE |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testPreprocessFunctionOnRuntime | harness-shape | row-assert+plan-assert | Test | no execute(\|...) call [calls meta::pure::executionPlan] — wall: assert form 'assertEquals/2' is not |
| FAIL | executionPlan/tests | executionPlanTest.pure | testQuoteIdentifiersFlagWithGraphFetch | rows-differ | row-assert+plan-assert+graph | Test | assertEquals: expected PureExp(type=Stringexpression=->serialize(#{meta::relational::tests::model::s |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testRoutingContextBuilderFunctions | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class meta::pure::metamodel::type::An |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testSQLCommentsInPlan | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: class meta::relational::mapping::SQLE |
| ERROR | executionPlan/tests | executionPlanTest.pure | testSupportStreamFlagFromSimple | typer | row-assert+plan-assert+graph | Test | no overload of 'executionPlan' matches the argument types |
| ERROR | executionPlan/tests | executionPlanTest.pure | testSupportStreamFlagWithGraphFetchAndFrom | other | row-assert+plan-assert+graph | Test | graphFetch expects (classCollection, #{Class{…}}#) |
| FAIL | executionPlan/tests | executionPlanTest.pure | testSupportStreamFlagWithSupportedAndUnSupportedUsages | rows-differ | row-assert+plan-assert | Test | assertEquals: expected true, got false |
| FAIL | executionPlan/tests | executionPlanTest.pure | testSupportStreamFlagithTdsJoinForTwoDB | rows-differ | row-assert+plan-assert+graph | Test | assertEquals: expected true, got false |
| FAIL | executionPlan/tests | executionPlanTest.pure | testTemporalDateVariableInFunctionExpressionWithPropagation | rows-differ | golden-sql+row-assert+plan-assert | Test | assertEquals: expected select "productexchangetable_0".name as "exchangeName" from ProductTable as " |
| FAIL | executionPlan/tests | executionPlanTest.pure | testTwoMappingsOneRuntime | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Relational\n(\n  type = TDS[(legalName, String, VARCHAR(200), ""), (legalName |
| FAIL | executionPlan/tests | executionPlanTest.pure | testTwoMappingsOneRuntimeWithoutExternalMapping | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Relational\n(\n  type = TDS[(legalName, String, VARCHAR(200), ""), (legalName |
| SHAPE | executionPlan/tests | executionPlanTest.pure | testViewToTDS | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: in function 'meta::pure::tds::viewToT |
| SHAPE | executionPlan/tests | executionPlanTest.pure | twoDBRenameColumns | harness-shape | row-assert+plan-assert | Test | no verifying assertions |
| ERROR | executionPlan/tests | executionPlanTest.pure | withPlatform | other | row-assert+plan-assert | Test | LIST_AGG reached a dialect without a list encoding |
| SHAPE | executionPlan/tests | m2m2rExecutionPlanTests.pure | executeProjectWithNestedDerivedProperty | harness-shape | row-assert+plan-assert+graph | meta::pure::profiles::Test | no execute(\|...) call — wall: no overload of 'meta::pure::executionPlan::m2m2r::tests::generateAndE |
| SHAPE | executionPlan/tests | m2m2rExecutionPlanTests.pure | planGraphFetchWithDerivedProperty | rows-differ | row-assert+plan-assert+graph | meta::pure::profiles::Test | assert form 'assertEquals/2' is not supported yet — plan wall: class query under TypedGraphFetch is  |
| SHAPE | executionPlan/tests | m2m2rExecutionPlanTests.pure | planGraphFetchWithNestedDerivedProperty | rows-differ | row-assert+plan-assert+graph | meta::pure::profiles::Test | assert form 'assertEquals/2' is not supported yet — plan wall: class query under TypedGraphFetch is  |
| FAIL | functions/tests | testConcatenate.pure | testConcatenateFlatWithOtherProperty | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [1, 1, 2, 2], got [1, 2] |
| ERROR | functions/tests | testConcatenate.pure | testConcatenateInQualifierWithComplexReturnType | other | golden-sql+row-assert | Test | class-typed property '$p.address' used as a whole value is graph output (Phase H4) |
| ERROR | functions/tests | testConcatenate.pure | testQualifierConcatenateTwoSimilarJoins | other | golden-sql+row-assert | Test | extend/project columns [Trade ID, OE] reference names unresolvable even after isolation [col='OE' re |
| ERROR | functions/tests | testConcatenate.pure | testQualifierConcatenateTwoSimilarJoinsEmbedded | other | golden-sql+row-assert | Test | class-typed property 'oe' of association target 'meta::relational::tests::projection::function::conc |
| ERROR | functions/tests | testExists.pure | testAssociationWithProjectionHandlingDups | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: subqueries in lambda expressions are not supported |
| SHAPE | functions/tests | testExists.pure | testComplexOrExistsToManyProperty | other | golden-sql+row-assert | Test | statement 'map' failed through the pipeline: class query under TypedMap is not resolvable yet (H2 vo |
| FAIL | functions/tests | testExists.pure | testDupsFilterProject | rows-differ | golden-sql+row-assert | Test | assertEquals: expected Firm X, got [Firm X, Yes] |
| ERROR | functions/tests | testExists.pure | testExistsWithEmbeddedWithPostProcessor | other | golden-sql+row-assert | Test | in function 'meta::relational::postProcessor::postprocess': in call to 'meta::relational::postProces |
| ERROR | functions/tests | testExists.pure | testNestedExistsWithExistsInAbstractProperty | other | golden-sql+row-assert | Test | exists/forAll predicate references column 'firm_employees', unresolvable even after isolation [param |
| ERROR | functions/tests | testFilters.pure | testSelectChainOfAndOrOperators | other | row-assert | Test | runtime 'rcorpus::Rt' has 2 mappings binding class 'meta::relational::tests::model::simple::Person'  |
| SHAPE | functions/tests | testFrom.pure | testFromWithMapping | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: unknown function 'wi |
| SHAPE | functions/tests | testFrom.pure | testFromWithMappingAndIntermediateFuncCall | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: unknown function 'wi |
| FAIL | functions/tests | testIn.pure | testInExecutionWithTempTableForDateTimesWithTz | rows-differ | row-assert | Test, AlloyOnly | assertSize: expected 5, got 0 |
| ERROR | functions/tests | testIsEmpty1.pure | testInputNotIsolatedWhenPropertyPathIsToOne | resolve | golden-sql+row-assert | Test | emptiness check over a toOne()-pierced navigation through the ~filter-mapped set of 'firm' needs the |
| FAIL | functions/tests | testIsEmpty1.pure | testIsEmptyOnCollection | rows-differ | row-assert+plan-assert | Test | assertEquals: expected Sequence(type=TDS[(name,String,VARCHAR(200),"")](FunctionParametersValidation |
| FAIL | functions/tests | testMap.pure | testSequenceMapWithConfusingSetImplementation | rows-differ | row-assert | Test | assertEquals: expected [ROOT, ok, TDSNull], got [Firm X, ok, ROOT] |
| FAIL | functions/tests | testMap.pure | testSubAggregationMultiLevel | rows-differ | row-assert | Test | assertSameElements: expected [12.0, 22.0, 22.0, 23.0, 32.0, 34.0, 35.0], got [23, 22, 12, 22, 34, 32 |
| FAIL | functions/tests | testModelGroupBy.pure | testGroupByWithJoinDB2 | rows-differ | golden-sql+row-assert | Test | assertEquals: expected select "root".LEGALNAME as "legalName", "personTable_d#4_d_m1".FIRSTNAME as " |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInEmbeddedMapping | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInSimple | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInWithBiTemporalMilestoning | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInWithEmptyLists | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInWithMilestonedProperty | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferenceInWithObjReferenceOutput | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testObjectReferenceIn.pure | testObjectReferneceInWithMilestonedRootClass | platform-surface | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | unknown function 'generateObjectReferences' — no function of this name in the native or user catalog |
| ERROR | functions/tests | testSimple.pure | testAll | resolve | row-assert+plan-assert | Test | scalar lowering not yet implemented for TypedSerializeGraph |
| ERROR | functions/tests | testSimple.pure | testSQLComments | execute(DuckDB) | ? | Test | Binder Error: No function matches the given name and argument types 'struct_extract(VARCHAR, STRING_ |
| SHAPE | functions/tests | testSliceTakeLimitDrop.pure | testFilterLimitInSequenceForTableAccessor | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: planToString: no getAll root (multi-n |
| SHAPE | functions/tests | testSliceTakeLimitDrop.pure | testLimitFilterInSequenceForTableAccessor | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: planToString: no getAll root (multi-n |
| FAIL | functions/tests | testSort.pure | testSortByLambdaAndGraphFetchDeep | rows-differ | row-assert+plan-assert+graph | Test, AlloyOnly | assertJsonStringsEqual: FIRST DIFF at $[0].address expected null, got {name=Hoboken} \| expected [{a |
| ERROR | functions/tests | testSort.pure | testSortByLambdaDeepOptional | other | row-assert+plan-assert+graph | Test | zip over inputs that are not two scalar projections of the SAME class chain has no relational shape |
| ERROR | functions/tests/loadCsvToDbTable | testLoadCsv.pure | testLoadCsv | typer | row-assert | Test | in function 'meta::relational::metamodel::execute::loadCsvToDbTable': no overload of 'meta::relation |
| ERROR | functions/tests/projection | testAggregation.pure | testSubAggregationWithDeepAndOverlap | lower | row-assert | Test | no scalar lowering registered for resolved overload 'meta::pure::functions::collection::count' with  |
| ERROR | functions/tests/projection | testAggregation.pure | testSubAggregationWithDeepAndOverlap_WithColVar | lower | row-assert | Test | project expects ~[…] column specifications |
| ERROR | functions/tests/projection | testAssociationToMany.pure | testQualifiedPropertyUsingColumnProtocol | other | golden-sql+row-assert | Test | object-space expression node TypedFilter is not substitutable yet (H2 vocabulary): TypedFilter[sourc |
| FAIL | functions/tests/projection | testDateFilters.pure | testAdjustWithMicroseconds | rows-differ | golden-sql+row-assert | Test | assertSameElements: expected 2014-12-04 15:22:23.123456, got 2014-12-04 15:22:23.123456789 |
| FAIL | functions/tests/projection | testDateFilters.pure | testMostRecentDayOfWeek | rows-differ | golden-sql+row-assert | Test | assertEquals: expected select "root".tradeDate as "date" from tradeTable as "root" where "root".trad |
| ERROR | functions/tests/projection | testExists.pure | testExistsAsNullWithSubType | resolve | row-assert | Test | in function 'meta::relational::tests::projection::exists::mappingForMultipleSubTypes$class$meta::rel |
| ERROR | functions/tests/projection | testExists.pure | testSimpleExists | other | row-assert | Test | class-typed property '$p.address' used as a whole value is graph output (Phase H4) |
| ERROR | functions/tests/projection | testFilter.pure | testChainedFiltersQuery | normalize(mapping) | golden-sql+row-assert | Test | property 'locations' of class 'meta::relational::tests::model::simple::Person' is not mapped in mapp |
| SHAPE | functions/tests/projection | testFilter.pure | testFilterAfterJoinInRelation | harness-shape | golden-sql+plan-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | functions/tests/projection | testFilter.pure | testFilterAfterJoinInRelationWithExtendedPrimitives | harness-shape | golden-sql+plan-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | functions/tests/projection | testFilter.pure | testParametrizedEnumFilter | typer | golden-sql+row-assert+plan-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| FAIL | functions/tests/projection | testFilters.pure | testIsolatioWhereNoConstaintsAndInnerJoin | rows-differ | row-assert | Test | assertEquals: expected [Firm X, UK, Firm X, Europe, Firm X, Europe, Firm X, Europe, Firm A, Europe,  |
| ERROR | functions/tests/projection | testFilters.pure | testIsolationOfFiltersWithoutAlias | other | row-assert | Test | Invalid Input Error: More than one row returned by a subquery used as an expression - scalar subquer |
| ERROR | functions/tests/projection | testFunctionVariables.pure | testVariableReferenceInFilterWithSameNameAsThatInParentProject | resolve | row-assert | Test | store resolution left getAll(meta::relational::tests::model::simple::Person) unresolved — the query  |
| ERROR | functions/tests/projection | testFunctionVariables.pure | testVariableReferenceInMapWithNestedFilter | typer | row-assert | Test | expected at most one value, got many ([*]) |
| ERROR | functions/tests/projection | testFunctionVariables.pure | testVariableReferenceInMapWithSameNameAsThatInParentProject | resolve | row-assert | Test | store resolution left getAll(meta::relational::tests::model::simple::Person) unresolved — the query  |
| FAIL | functions/tests/projection | testFunctionVariables.pure | testVariableReferenceWithNestedFilterMultiple | other | row-assert | Test | h2-advisory divergence: golden SQL on H2 gave 7 row(s) [Allen\|<null>, Harris\|<null>, Hill\|<null>, |
| ERROR | functions/tests/projection | testGroupWithWindowSubset.pure | testGroupByWithWindowSubset | typer | golden-sql+row-assert | Test | no overload of 'groupByWithWindowSubset' matches 6 argument(s) of these shapes (no candidates at all |
| SHAPE | functions/tests/projection | testIn.pure | H2Test | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::metamodel::execute] — wall: Values of types "BOOLEAN |
| ERROR | functions/tests/projection | testIn.pure | testInWithDynaFunction | execute(DuckDB) | row-assert | Test | Conversion Error: Could not convert string 'something' to BOOL \|  \| LINE 3: ... = 'Y' THEN 'true'  |
| ERROR | functions/tests/projection | testIn.pure | testQualifierWithInThroughJoin | other | row-assert | Test | derived property 'accountCategory' over a [0..1] receiver has a body outside the null-strict whiteli |
| FAIL | functions/tests/projection | testQualifier.pure | testSimpleBoolean | rows-differ | row-assert | Test | assertEquals: expected false, got [] |
| FAIL | functions/tests/projection | testQualifier.pure | testTwoQualifiersUsingSameJoinWithNoUserParams | rows-differ | golden-sql+row-assert | Test | assertSize: expected 1, got 4 |
| SHAPE | graphFetch/domain | domainManagementTests.pure | testGraphFetch | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: 'Domain' is not a known class, mappin |
| SHAPE | graphFetch/tests | ? | testCrossStoreWithCSVDataSource | rows-differ | ? | ? | assert form 'assertEquals/2' is not supported yet — plan wall: from() argument 2 must be a mapping o |
| ERROR | graphFetch/tests | testCrossDatabaseGraphFetch.pure | testCrossMappingWithRelOpWithJoinKeys | normalize(mapping) | row-assert+graph | Test, AlloyOnly | association 'meta::relational::graphFetch::tests::crossDatabase::EmploymentAssociation' is not mappe |
| ERROR | graphFetch/tests | testCrossStoreGraphFetch.pure | testCrossMappingJsonToDBWithExplosion | normalize(mapping) | row-assert+graph | Test, AlloyOnly | class 'meta::pure::graphFetch::tests::XStore::inMemoryAndRelational::T_Trade' is not mapped in mappi |
| ERROR | graphFetch/tests | testCrossStoreGraphFetchMilestoning.pure | CrossStoreGraphFetchWithRelationalMilestoned | typer | row-assert+graph | meta::pure::profiles::Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| ERROR | graphFetch/tests | testCrossStoreGraphFetchMilestoning.pure | CrossStoreGraphFetchWithRelationalMilestonedAllversions | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| ERROR | graphFetch/tests | testCrossStoreGraphFetchMilestoning.pure | CrossStoreGraphFetchWithRelationalMilestonedFlowDown | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| ERROR | graphFetch/tests | testCrossStoreGraphFetchMilestoning.pure | CrossStoreGraphFetchWithRelationalMilestonedFlowDownM2M | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| SHAPE | graphFetch/tests | testCrossStoreGraphFetchMilestoning.pure | testCrossStoreGraphFetchWithRelationalDatePropagationForMilestonedPropertyConstraint | harness-shape | graph | Test, AlloyOnly | no execute(\|...) call [calls meta::legend] — wall: [151:2] expected COLON but found VALID_STRING (' |
| ERROR | graphFetch/tests | testGraphFetchChain.pure | testRelationalChainExecutionNested | resolve | row-assert+graph | Test, AlloyOnly | serialize leaf 'managers' references column 'manager', unresolvable in the envelope source |
| SHAPE | graphFetch/tests | testGraphFetchEmbeddedOtherwise.pure | testMilestonedRootAndMilestonedProperty | rows-differ | row-assert+graph | Test, AlloyOnly | assert form 'assertJsonStringsEqual/2' is not supported yet |
| SHAPE | graphFetch/tests | testGraphFetchEmbeddedOtherwise.pure | testMilestonedRootAndMilestonedProperty | rows-differ | row-assert+graph | Test, AlloyOnly | assert form 'assertJsonStringsEqual/2' is not supported yet |
| FAIL | graphFetch/tests | testGraphFetchMilestoning.pure | testMilestonedProperty | rows-differ | row-assert+plan-assert+graph | Test, AlloyOnly | assertEquals: expected PureExp\n(\n  type = String\n  expression =  -> serialize(#{meta::relational: |
| FAIL | graphFetch/tests | testSimpleRelationalGraphFetch.pure | testCheckedWithCircularConstraints | rows-differ | row-assert+plan-assert+graph | Test, AlloyOnly | assertJsonStringsEqual: FIRST DIFF at $[2].defects expected 1 element(s), got 0 \| expected [{defect |
| FAIL | graphFetch/tests | testSimpleRelationalGraphFetch.pure | testGraphFetchWithTableMapperPostProcessor | rows-differ | row-assert+graph | Test, AlloyOnly | assertJsonStringsEqual: FIRST DIFF at $[0].employees expected 0 element(s), got 4 \| expected [{lega |
| ERROR | graphFetch/tests | testSimpleRelationalGraphFetch.pure | testObjectReferenceInUsingResultReferences | platform-surface | row-assert+graph | Test, AlloyOnly | unknown function 'alloyConfig' — no function of this name in the native or user catalog (unported pl |
| ERROR | graphFetch/tests | testSimpleRelationalGraphFetch.pure | testQualifierInsideQualifier | normalize(mapping) | row-assert+graph | Test, AlloyOnly | property 'initiator' of class 'meta::relational::tests::model::simple::Trade' is not mapped in mappi |
| ERROR | graphFetch/tests | testSimpleRelationalGraphFetch.pure | testRelationalGraphFetchWithAlloySerializationConfig | platform-surface | row-assert+graph | Test, AlloyOnly | unknown function 'alloyConfig' — no function of this name in the native or user catalog (unported pl |
| ERROR | graphFetch/tests | testSubTypeGraphFetch.pure | testInheritanceMappingWithoutSubType | platform-surface | row-assert+graph | Test, AlloyOnly | unknown function 'parseJSON' — no function of this name in the native or user catalog (unported plat |
| ERROR | graphFetch/tests | testSubTypeGraphFetch.pure | testSubTypeAtRootLevelWithInheritanceMapping | platform-surface | row-assert+graph | Test, AlloyOnly | unknown function 'parseJSON' — no function of this name in the native or user catalog (unported plat |
| FAIL | graphFetch/tests/union | testUnionPropertyLevel_Relational.pure | test6 | rows-differ | row-assert+graph | Test, AlloyOnly | assertJsonStringsEqual: FIRST DIFF at $[0].legalName expected Firm B, got Firm X \| expected [{legal |
| ERROR | graphFetch/tests/union | testUnionRootLevel_relational.pure | testSpecialUnion_m2m2r | other | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | from() argument 2 must be a mapping or runtime reference, got TypedNewInstance |
| SHAPE | helperFunctions/tests | testDdlGeneration.pure | dropAndCreateTempTable | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: unknown class 'meta: |
| SHAPE | helperFunctions/tests | testDdlGeneration.pure | testCreateTempTableStatement | harness-shape | row-assert | Test | no execute(\|...) call — wall: eval expects a lambda, a function reference, ~col, or a function-type |
| SHAPE | lineage/scanColumns | scanColumnsTests.pure | testNonDataTypeProperty | other | row-assert+lineage | meta::pure::profiles::Test | scanColumns query: class-typed property '$p.address' used as a whole value is graph output (Phase H4 |
| ERROR | lineage/scanColumns | scanColumnsTests.pure | testSubType | normalize(mapping) | row-assert+lineage | meta::pure::profiles::Test | class 'meta::relational::tests::model::inheritance::Vehicle' is not mapped in mapping 'meta::relatio |
| FAIL | lineage/scanColumns | scanColumnsTests.pure | testView | other | row-assert+lineage | meta::pure::profiles::Test | scanColumns: expected [firmTable.ID <JoinTreeNode>, personTable.AGE <JoinTreeNode>, personTable.FIRM |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testSameRelationsAtSameLevel | harness-shape | row-assert+lineage+graph | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTableToTdsWithCrossJoin | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTableToTdsWithJoin | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTableToTdsWithJoinAndUnion | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTableToTdsWithJoinToSameTable | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTableToTdsWithOLAPGroupBy | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testTdsJoinConcatenateAndJoin | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testUnionToSameTableWithDiffKeys | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | lineage/scanRelations | scanRelationsTests.pure | testUnionWithJoinToOneTable | harness-shape | row-assert+lineage | meta::pure::profiles::Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | milestoning/tests | testApplyMilestoningFilters.pure | testMilestoningFilterApplicationOnSemiStructuredRelationalOperationElements | harness-shape | golden-sql+row-assert+plan-assert | Test | no execute(\|...) call [calls meta::relational::extension] — wall: Unknown type: 'Operation' is not  |
| ERROR | milestoning/tests | testBusinessDateMilestoning.pure | testBusinessDateInjectionFromVarReferenceInProjectUsingExternalFunction | resolve | golden-sql+row-assert | Test | milestoned property access 'product' on a NESTED navigation is not supported yet |
| SHAPE | milestoning/tests | testBusinessDateMilestoning.pure | testBusinessDatePropagationInColFunction_asQueryParam | rows-differ | golden-sql+row-assert+plan-assert | Test | assert form 'assertEqualsH2Compatible/3' is not supported yet — plan wall: no overload of 'cast' mat |
| SHAPE | milestoning/tests | testBusinessDateMilestoning.pure | testDateFunctionInMilestonedProperty | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | milestoning/tests | testBusinessDateMilestoning.pure | testDateFunctionInMilestonedPropertyWithMilestonedEntity | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| FAIL | milestoning/tests | testBusinessDateMilestoning.pure | testExecutionPlanForQueryWithVariableRundateWithinLambda | rows-differ | golden-sql+row-assert+plan-assert | Test | assertEquals: expected Sequence\n(\n  type = Class[impls=(meta::relational::tests::milestoning::Prod |
| FAIL | milestoning/tests | testBusinessDateMilestoning.pure | testMilestoningQueryWithMilestoneFilterAndDifferentDatesOnTypeWithLatestDateOnProperty | other | golden-sql+row-assert | Test | sql-text: expected select "root".id as "pk_0", "root".name as "pk_1", "root".id as "id", "root".name |
| SHAPE | milestoning/tests | testBusinessDateMilestoning.pure | testQueryOfMilestonedTypeUsingLatestWithFilterInMapping | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | milestoning/tests | testBusinessDateMilestoning.pure | testViewChainsWithBusinessDate | harness-shape | golden-sql+row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: sql-only: 1 advisory |
| SHAPE | milestoning/tests | testLatestDateMilestoning.pure | testLatestIgnoredForNonMilestonedMappedBiTemporalClassesAllQuery | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | milestoning/tests | testLatestDateMilestoning.pure | testLatestIgnoredForNonMilestonedMappedClassesAllQuery | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| FAIL | milestoning/tests | testMilestoningContextPropagation.pure | testIsolationOfMilestoningFiltersUsedOnIntermediateJoinInOR | other | golden-sql+row-assert | Test | sql-text: expected select "root".id as "pk_0", "root".name as "pk_1", "root".id as "id", "cancelacti |
| SHAPE | milestoning/tests | testMilestoningContextPropagation.pure | testLatestMilestoneDateMappedTableDateDoesNotOverrideLatestDateFromChildPropertyInPropogation | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | milestoning/tests | testMilestoningContextPropagation.pure | testLatestMilestoneDatePropogationFromTypeQueryDoesNotOverrideThatSpecifiedAsArgToMilestonedQpInFilter | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | milestoning/tests | testMilestoningContextPropagation.pure | testMilestoningContextIsPropogatedThroughSubType | resolve | golden-sql+row-assert | Test | multi-hop navigation product.stc_meta__relational__tests__milestoning__Product___classification.desc |
| SHAPE | modelJoins | testModelJoinsToRelationalJoins.pure | testModelJoinForNonRelationalConcepts | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: in function 'meta::external::store::r |
| SHAPE | modelJoins | testModelJoinsToRelationalJoins.pure | testPersonToFirmUsingFromProject | rows-differ | row-assert+plan-assert+graph | Test | assert form 'assertEquals/2' is not supported yet — plan wall: plan walk: executionPlan argument sha |
| SHAPE | modelJoins | testModelJoinsToRelationalJoins.pure | testPersonToFirmUsingProject | harness-shape | row-assert+plan-assert | Test | no verifying assertions |
| ERROR | modelToModelToRelational/milestoned | milestonedSourceToNonMilestonedTargetProperty.pure | testFlatten_ViaAllVersionsMapping | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| ERROR | modelToModelToRelational/milestoned | milestonedSourceToNonMilestonedTargetProperty.pure | testFlatten_ViaHardcodedDateMapping | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| SHAPE | modelToModelToRelational/milestoned | milestonedSourceToNonMilestonedTargetProperty.pure | testFlatten_ViaNoArgMapping | harness-shape | row-assert+graph | Test, AlloyOnly | no execute(\|...) call [calls meta::pure::graphFetch::tests::m2m2r::milestoning::milestonedSourceToN |
| SHAPE | modelToModelToRelational/milestoned | milestonedSourceToNonMilestonedTargetProperty.pure | testFlatten_ViaNoArgMapping_ViaAssociation | harness-shape | row-assert+graph | Test, AlloyOnly | no execute(\|...) call [calls meta::pure::graphFetch::tests::m2m2r::milestoning::milestonedSourceToN |
| ERROR | modelToModelToRelational/milestoned | milestonedSourceToNonMilestonedTargetProperty.pure | test_ViaAllVersionsMapping | typer | row-assert+graph | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 4 argument(s) of these shapes (no candidat |
| ERROR | modelToModelToRelational/milestoned | nonMilestonedSourceToMilestonedTargetProperty.pure | testWithHardcodedDate | normalize(mapping) | row-assert+graph | Test, AlloyOnly | class 'meta::relational::tests::milestoning::TargetProductMilestoned' is not mapped in mapping 'meta |
| ERROR | modelToModelToRelational/milestoned | nonMilestonedSourceToMilestonedTargetProperty.pure | testWithHardcodedDate | normalize(mapping) | row-assert+graph | Test, AlloyOnly | class 'meta::relational::tests::milestoning::TargetProductMilestoned' is not mapped in mapping 'meta |
| SHAPE | postprocessor/tests | testPostProcessor.pure | testDb2ColumnRename | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::functions::sqlQueryToString] — wall: Unknown type: ' |
| SHAPE | postprocessor/tests | testPostProcessor.pure | testPostProcessTransformJoinOp | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: Unknown type: 'SQLQu |
| SHAPE | postprocessor/tests | testPostProcessor.pure | testPushFiltersDownToJoinsPostProcessorToSQL | harness-shape | golden-sql+row-assert | Test | no execute(\|...) call [calls meta::relational::functions::sqlQueryToString] — wall: Unknown type: ' |
| FAIL | postprocessor/tests | testPostProcessor.pure | testReplaceTablePostProcessorWithExists | other | golden-sql | Test | sql-text: expected select "root".ID as "pk_0", "root".LEGALNAME as "legalName" from firmTable as "ro |
| ERROR | postprocessor/tests | testPostProcessor.pure | testReplaceTablePostProcessorWithSubQueries | typer | golden-sql | Test | in function 'meta::relational::tests::postProcessor::nonExecutable::runtimeWithNonExecutable': no ov |
| FAIL | postprocessor/tests | testPostProcessor.pure | testReplaceTablePostProcessorWithView | other | golden-sql | Test | sql-text: expected select "root".ID as "pk_0", "root".ID as "id", "root".quantity as "quantity", "ro |
| FAIL | postprocessor/tests | testPostProcessor.pure | testReplaceTablesPostProcessor | other | golden-sql | Test | h2-advisory divergence: golden SQL on H2 gave 0 row(s) [], our pipeline gave 7 row(s) [Firm A\|Fabri |
| SHAPE | postprocessor/tests | testPostProcessor.pure | testToSqlStringReplaceTablesPostProcessor | harness-shape | golden-sql | Test | no execute(\|...) call [calls meta::relational::functions::sqlstring] — wall: sql-only: 1 advisory g |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | addDriverTablePkForProject | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: class 'meta::relatio |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | simpleFunctionExpressionTranslationAdjust | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: Unknown type: 'SQLQu |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | simpleFunctionExpressionTranslationNow | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: Unknown type: 'SQLQu |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | tesIsToOneDataTypeFunctionExpressionSequence | harness-shape | ? | Test | no execute(\|...) call — wall: a non-let intermediate statement in a bare lambda literal is not supp |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | tesIsToOneDataTypeFunctionExpressionSequenceWithCastExpressions | harness-shape | row-assert | Test | no execute(\|...) call — wall: a non-let intermediate statement in a bare lambda literal is not supp |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | tesIsToOneDataTypeFunctionExpressionSequenceWithQualifiers | harness-shape | ? | Test | no execute(\|...) call — wall: a non-let intermediate statement in a bare lambda literal is not supp |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | testFindAliasMappingBySchemaName | harness-shape | row-assert | Test | no execute(\|...) call — wall: unknown function 'relation' — no function of this name in the native  |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | testFindFunctionSequenceMultiplicity | harness-shape | row-assert | Test | no execute(\|...) call — wall: 'ZeroMany' is not a known class, mapping, runtime, connection, or dat |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | testImportDataFlow | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: Unknown type: 'SQLQu |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | testMergeOldAliasToNewAlias | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::functions::pureToSqlQuery::mergeOldAli |
| SHAPE | pureToSQLQuery/tests | testPureToSql.pure | testReAliasMergedJoinOperations | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::tests::functions::pureToSqlQuery::buil |
| SHAPE | router/tests | testPreeval.pure | testPrerouting42 | rows-differ | graph | Test | assert form 'assertRoundTrip/3' is not supported yet |
| SHAPE | router/tests | testRouting.pure | testCompositionInMultiStatementPureExpressions | harness-shape | row-assert | Test | no execute(\|...) call — wall: no overload of 'meta::relational::tests::query::routing::routeInterna |
| ERROR | router/tests | testRouting.pure | testPlatformExpressionDependencyOnAFromExpression | typer | row-assert | Test | no overload of 'routeFunction' matches 4 argument(s) of these shapes (no candidates at all) |
| ERROR | router/tests | testRouting.pure | testPlatformExpressionDependencyOnAFromExpression2 | typer | row-assert | Test | no overload of 'routeFunction' matches 4 argument(s) of these shapes (no candidates at all) |
| SHAPE | router/tests | testRouting.pure | testRoutingOfSimpleQualifiedProperty | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::external::store::relational::tests] — wall: no overload of 'rout |
| ERROR | router/tests | testRouting.pure | testRoutingWithSubtypePropagation | resolve | row-assert | Test | multi-hop navigation employees.stc_meta__relational__tests__model__simple__PersonExtension___manager |
| SHAPE | sqlQueryToString | extensionDefaults.pure | testProcessIdentifierWithQuoteChar | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::functions::sqlQueryToString::h2::v2_1_214] — wall: U |
| SHAPE | sqlQueryToString/DDL | testDDL.pure | testSetupDataSqlGeneration | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::alloy::service::execution] — wall: in call to 'meta::alloy::serv |
| SHAPE | sqlQueryToString/DDL | testDDL.pure | testSetupDataSqlGenerationWithColumnValueHasDelimiterAndQuotes | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::alloy::service::execution] — wall: in function 'meta::relational |
| SHAPE | sqlQueryToString/DDL | testDDL.pure | testSetupDataSqlGenerationWithDataAsString | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::alloy::service::execution] — wall: in function 'meta::relational |
| SHAPE | sqlQueryToString/testSuite | testTempTableSqlStatements.pure | testTempTableSqlStatementsForH2 | harness-shape | ? | Test | no execute(\|...) call [calls meta::relational::functions::sqlQueryToString::tests] — wall: in funct |
| SHAPE | tds/relation | testTdsToRelation.pure | testJoinFunc | harness-shape | ? | Test | no execute(\|...) call [calls meta::relational::extension] — wall: 'TestClass' is not a known class, |
| SHAPE | tds/relation | testTdsToRelation.pure | testJoinUsing | harness-shape | ? | Test | no execute(\|...) call [calls meta::relational::extension] — wall: 'TestClass' is not a known class, |
| SHAPE | tds/tests | testCanRouteWrappedFunctions.pure | testExecutionPlanGeneration | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: no overload of 'meta::pure::functions |
| SHAPE | tds/tests | testSliceTakeLimitDrop.pure | testSimpleSliceZeroSameAsTake | harness-shape | row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | tds/tests | testSort.pure | testSortQuotes | platform-surface | row-assert | Test | unknown function 'enumValues' — no function of this name in the native or user catalog (unported pla |
| ERROR | tds/tests | testSort.pure | testTableToTDSWithQuotes | other | row-assert | Test | in call to 'meta::pure::tds::desc', argument 1: expected ColSpec<T>, got String |
| ERROR | tds/tests | testTDSConcatenate.pure | testMultiConcatenate | lower | row-assert | Test | lowering not yet implemented for TypedCollection |
| SHAPE | tds/tests | testTDSExtend.pure | testDecimal | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::functions::sqlstring] — wall: sql-only: 1 advisory g |
| SHAPE | tds/tests | testTDSExtend.pure | testParseDate | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::functions::sqlstring] — wall: no overload of 'meta:: |
| FAIL | tds/tests | testTDSFilter.pure | testFilterOnEnum | rows-differ | row-assert | Test | assertEquals: expected CITY, got [New York, CITY] |
| ERROR | tds/tests | testTDSJoin.pure | testJoinWithExtendWithDigestOnColumnsOnBothQueries | other | row-assert | Test, AlloyOnly | unbound variable '$_nr2' |
| ERROR | tds/tests | testTDSRestrict.pure | testRestrictWithPostProcessor | other | row-assert | Test | in function 'meta::relational::postProcessor::postprocess': in call to 'meta::relational::postProces |
| FAIL | tds/tests | testTDSRestrictDistinct.pure | testRestrictDistinct_NoOptimization_WindowColumns | rows-differ | row-assert | Test | assertEquals: expected select distinct "root".LASTNAME as "lastName", "root".FIRSTNAME as "firstName |
| ERROR | tds/tests | testTdsExtension.pure | columnValueDifferenceTest | resolve | row-assert | Test | store resolution left getAll(meta::relational::tests::model::simple::Trade) unresolved — the query s |
| ERROR | tds/tests | testTdsExtension.pure | columnValueDifferenceWithoutPrevalTest | resolve | row-assert | Test, AlloyOnly | store resolution left getAll(meta::relational::tests::model::simple::Trade) unresolved — the query s |
| SHAPE | tds/tests | testTdsExtension.pure | iqrClassifyTest | harness-shape | row-assert | Test | no execute(\|...) call — wall: no overload of 'meta::pure::functions::relation::join' structurally m |
| ERROR | tds/tests | testTdsExtension.pure | rowValueDifferenceTest | other | row-assert | Test | cannot access 'name' on String |
| SHAPE | tds/tests | testTdsExtension.pure | testExtendDigest_InMemory | harness-shape | row-assert | Test | no execute(\|...) call — wall: cannot access 'name' on String |
| ERROR | tds/tests | testTdsExtension.pure | testExtendDigest_Relational | other | row-assert | Test, AlloyOnly | cannot access 'name' on String |
| SHAPE | tds/tests | testTdsExtension.pure | testFirstNotNull | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::pure::tds::extensions] — wall: unresolved type variable T reache |
| SHAPE | tds/tests | testTdsExtension.pure | zScoreTest | harness-shape | row-assert | Test | no execute(\|...) call — wall: no overload of 'meta::pure::functions::relation::join' structurally m |
| SHAPE | tds/tests | testTdsSchema.pure | resolveSchemaTest | harness-shape | ? | Test | no execute(\|...) call [calls meta::relational::functions::database] — wall: assert form 'meta::pure |
| SHAPE | testDataGeneration/tests | testDataGeneration.pure | testAlloyTestDatGenForNestedViews | harness-shape | ? | meta::pure::profiles::Test | no verifying assertions |
| SHAPE | testDataGeneration/tests | testDataGeneration.pure | testAlloyTestDatGenWithQuotedColumnsForViews | rows-differ | row-assert+lineage+plan-assert | meta::pure::profiles::Test | assert form 'assertEquals/2' is not supported yet |
| SHAPE | testDataGeneration/tests | testDataGeneration.pure | testErrorDueToNoSeedForRoot | rows-differ | row-assert+plan-assert | meta::pure::profiles::Test | assert form 'assertEquals/2' is not supported yet |
| ERROR | testDataGeneration/tests | testDataGeneration.pure | testInheritanceMultipleLevel | resolve | row-assert | meta::pure::profiles::Test | multi-hop navigation vehicles#f1.stc_meta__relational__tests__model__inheritance__Bicycle___person.n |
| SHAPE | testDataGeneration/tests | testDataGeneration.pure | testTableToTdsWithJoinAndUnion | other | row-assert | meta::pure::profiles::Test | scanRelations: tableToTDS join side is not a single table source |
| ERROR | testDataGeneration/tests | testDataGeneration.pure | testUnionToUnion | normalize(mapping) | row-assert | meta::pure::profiles::Test | class 'meta::relational::tests::model::simple::Firm' is not mapped in mapping 'meta::relational::tes |
| FAIL | testDataGeneration/tests | testDataGeneration.pure | testUnionViewOnView | rows-differ | row-assert | meta::pure::profiles::Test | assertSize(sqls): expected 14, got 12 |
| FAIL | testDataGeneration/tests | testDataGeneration.pure | testViewEmbeddedInChainedJoin | rows-differ | row-assert | meta::pure::profiles::Test | assertSize(sqls): expected 5, got 4 |
| SHAPE | tests | relationalSetUp.pure | testResultToJsonStream | harness-shape | plan-assert | Test | no execute(\|...) call — wall: unknown enumeration 'GeographicEntityType' |
| SHAPE | tests | testRelationalExtension.pure | testConnectionEqualityAllButOnePropertySame | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::metamodel::execute::tests::runRelation |
| SHAPE | tests | testRelationalExtension.pure | testConnectionEqualityAllSameStatic | harness-shape | ? | Test | no execute(\|...) call — wall: in function 'meta::relational::metamodel::execute::tests::runRelation |
| SHAPE | tests | testRelationalExtension.pure | testConnectionEqualityTypeDiff | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::metamodel::execute::tests::runRelation |
| SHAPE | tests | testRelationalExtension.pure | testConnectionEqualityTypeSameSpecDiff | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::metamodel::execute::tests::runRelation |
| SHAPE | tests | testRelationalExtension.pure | testConnectionEqualityTypeSpecSameAuthDiff | harness-shape | row-assert | Test | no execute(\|...) call — wall: in function 'meta::relational::metamodel::execute::tests::runRelation |
| FAIL | tests | testRelationalExtension.pure | testDynaComplexInference2 | rows-differ | row-assert+printer | Test | assertEquals: expected VARCHAR(400), got VARCHAR(200) |
| SHAPE | tests | testRelationalExtension.pure | testExecuteInDbToTDS | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::metamodel::execute] — wall: let-bound setup: Normali |
| SHAPE | tests | testRelationalExtension.pure | testExtractDBsWithSubstituition | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::runtime] — wall: in function 'meta::relational::runt |
| SHAPE | tests | testRelationalExtension.pure | testJoinStringsTypeInference | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::functions::typeInference] — wall: expected at most o |
| SHAPE | tests | testRelationalExtension.pure | testSQLNullWithinCaseTypeInference1 | harness-shape | row-assert+printer | Test | no execute(\|...) call [calls meta::relational::functions::typeInference] — wall: expected at most o |
| SHAPE | tests | testRelationalExtension.pure | testTranslateDbType | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::metamodel::datatype] — wall: unknown class 'meta::re |
| FAIL | tests | testRelationalMapper.pure | testRelationalMapperTwoDBs | rows-differ | row-assert | Test | assertEquals: expected select "root".NAME as "name", "synonymtable_0".NAME as "cusip" from snDB.prod |
| FAIL | tests | testRelationalMapper.pure | testRelationalMapperWithJoin | rows-differ | row-assert | Test | assertEquals: expected select "addresstable_0".NAME as "address" from snDBDefault.default.firmTableN |
| FAIL | tests/advanced | testForced.pure | testFilterMappingWithProjectionOverlappForcedCorrelated | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [ROOT, TDSNull, TDSNull], got [Federation, Firm X, ROOT] |
| FAIL | tests/advanced | testForced.pure | testFilterMappingWithProjectionOverlappForcedOnClause | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [ROOT, TDSNull, TDSNull], got [Federation, Firm X, ROOT] |
| ERROR | tests/advanced | testForcedSelfJoin.pure | isolationTest | resolve | row-assert | Test | multi-hop navigation employees.group.children#f0.name through an embedded/slot head is not supported |
| SHAPE | tests/advanced | testQueryStructure.pure | testForcedIsolationFilterOnTop | harness-shape | golden-sql | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | tests/advanced | testQueryStructure.pure | testLiteralConditionsForcedIsolation | harness-shape | golden-sql | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | tests/advanced | testQueryStructure.pure | testQualifierWithIsolation | other | row-assert | Test | extend/project columns [firm] reference names unresolvable even after isolation [col='firm' ref='fir |
| ERROR | tests/advanced | testQueryStructure.pure | testQualifierWithIsolationXX | other | row-assert | Test | extend/project columns [firm] reference names unresolvable even after isolation [col='firm' ref='fir |
| ERROR | tests/advanced | testRelationalResultSourcing.pure | relationalResultSourcingOfDateList | other | row-assert+plan-assert | meta::pure::profiles::Test | object-space expression node TypedLimit is not substitutable yet (H2 vocabulary): TypedLimit[source= |
| ERROR | tests/advanced | testRelationalResultSourcing.pure | relationalResultSourcingOfListExecutionPlan | other | row-assert+plan-assert | meta::pure::profiles::Test | UNNEST reached a dialect without an unnest placement |
| FAIL | tests/datatype | testDataTypeMapping.pure | testSimpleTypeMappingNulls | rows-differ | row-assert | Test | assertEquals: expected [], got null |
| ERROR | tests/datatype | testDataTypeMapping.pure | testSimpleTypeMappingProjectNulls | platform-surface | row-assert | Test | unknown function 'toJSON' — no function of this name in the native or user catalog (unported platfor |
| ERROR | tests/injection | testInjection.pure | testProjectThroughAssociation | other | row-assert | Test | auto-map mapper body node TypedFilter is not inlinable yet |
| ERROR | tests/injection | testInjection.pure | testProjectThroughAssociationAutoMap | other | row-assert | Test | auto-map mapper body node TypedFilter is not inlinable yet |
| FAIL | tests/mapping | boolean.pure | testGet | rows-differ | row-assert | Test | assertSize: expected 1, got 0 |
| ERROR | tests/mapping | boolean.pure | testProject | lower | row-assert | Test | lowering not yet implemented for TypedNativeCall ('meta::pure::functions::collection::sort' in relat |
| FAIL | tests/mapping | boolean.pure | testQuery | rows-differ | row-assert | Test | assertSize(result.values): expected 1, got 2 (TDS = one carrier; collections splat) |
| FAIL | tests/mapping | dates.pure | retrieveDateWithTimeZone | rows-differ | row-assert | Test | assertEquals: expected 2016-02-05 21:00:00.123456, got 2016-02-05 21:00:00.123456789 |
| ERROR | tests/mapping/association | testAssociationEmbedded.pure | testPersonToFirmLocationsInlineEmbedded | resolve | row-assert | Test | multi-hop navigation firm.address.location.place through an embedded/slot head is not supported yet  |
| ERROR | tests/mapping/association | testAssociationEmbedded.pure | testPersonToOrganisations | resolve | row-assert | Test | multi-hop navigation firm.organizations.name through an embedded/slot head is not supported yet [ass |
| ERROR | tests/mapping/association | testAssociationMappingInheritance.pure | testBuilderRoutingOfAggFunctionParameters | normalize(mapping) | row-assert | Test | association 'meta::relational::tests::model::inheritance::VehicleOwnerVehicle' is not mapped in mapp |
| ERROR | tests/mapping/association | testAssociationMappingInheritance.pure | testGetAllFilterWithAssociation | normalize(mapping) | row-assert | Test | association 'meta::relational::tests::model::inheritance::Driver' is not mapped in mapping 'meta::re |
| ERROR | tests/mapping/association | testAssociationMappingInheritance.pure | testSubTypeFilter | other | row-assert | Test | class-typed property '$p.roadVehicles' used as a whole value is graph output (Phase H4) |
| ERROR | tests/mapping/association | testAssociationMappingInheritance.pure | testSubTypeInColumnProjectionsWithInlineMappings | normalize(mapping) | row-assert | Test | class 'meta::relational::tests::model::inheritance::Vehicle' is not mapped in mapping 'meta::relatio |
| ERROR | tests/mapping/classMappingFilterWithInnerJoin | testClassMappingFilterWithInnerJoin.pure | TestClassMappingsWithInnerFilterJoinedWithMilestoningDepthTwoNestedGeneration | normalize(mapping) | row-assert | Test | class 'meta::relational::tests::model::simple::TemporalProduct' is not mapped in mapping 'meta::rela |
| ERROR | tests/mapping/classMappingFilterWithInnerJoin | testClassMappingFilterWithInnerJoin.pure | testChainedJoinsWithUnionsAndIsolationWithProjectionQueryTableFilter | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: Referenced table "t5" not found! \| Candidate tables: "t4" \|  \| LINE 16:     SELECT  |
| ERROR | tests/mapping/classMappingFilterWithInnerJoin | testClassMappingFilterWithInnerJoin.pure | testSourceViewPropertyQueryWithInnerJoinClassMappingViewFilter | normalize(mapping) | row-assert | Test | class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::t |
| ERROR | tests/mapping/classMappingFilterWithInnerJoin | testClassMappingFilterWithInnerJoin.pure | testSourceViewRootQueryWithInnerJoinClassMappingViewFilter | normalize(mapping) | row-assert | Test | class 'meta::relational::tests::model::simple::Person' is not mapped in mapping 'meta::relational::t |
| ERROR | tests/mapping/embedded | ? | testProjectionOtherwiseNonPrimitive | resolve | ? | ? | multi-hop navigation bondDetails.bondClassification.type through an embedded/slot head is not suppor |
| ERROR | tests/mapping/embedded | testEmbeddedMapping.pure | testDenormMappingWithQualifierWithIfAndEquals | other | golden-sql+row-assert | Test | derived property 'isFirmX' over a [0..1] receiver has a body outside the null-strict whitelist — emp |
| ERROR | tests/mapping/embedded | testEmbeddedMapping.pure | testExists | other | golden-sql+row-assert | Test | class-typed property '$p.firm' used as a whole value is graph output (Phase H4) |
| FAIL | tests/mapping/embedded | testEmbeddedMapping.pure | testIsEmpty | rows-differ | row-assert | Test | assertEquals: expected name,firm\n\n, got [] |
| ERROR | tests/mapping/embedded | testEmbeddedOtherwiseMapping.pure | otherwiseTestComplexExpressionWithEnumMapping | normalize(mapping) | golden-sql+row-assert | Test | property 'type' of class 'meta::relational::tests::mapping::embedded::advanced::model::BondDetail' i |
| ERROR | tests/mapping/embedded | testInlineEmbeddedNested.pure | testInlineInEmbeddedGraphFetch | resolve | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | resolver bug: graph child 'address' is not a property of 'meta::relational::tests::mapping::embedded |
| ERROR | tests/mapping/embedded | testInlineEmbeddedNested.pure | testMilestonedEmbeddedInlineGraphFetch | resolve | row-assert+graph | meta::pure::profiles::Test, meta::pure::profiles::AlloyOnly | resolver bug: graph child 'unit' is not a property of 'meta::relational::tests::mapping::embedded::a |
| ERROR | tests/mapping/embedded | testInlineEmbeddedTargetIds.pure | testSubType | other | golden-sql+row-assert | Test | property 'stc_meta__relational__tests__mapping__embedded__advanced__model__Party___name' of embedded |
| ERROR | tests/mapping/enumeration | testEnumerationMapping.pure | testEnumInRelation | other | row-assert | Test, AlloyOnly | class meta::pure::metamodel::relation::TDS has no property 'csv' |
| SHAPE | tests/mapping/enumeration | testEnumerationMapping.pure | testEnumMappings | harness-shape | row-assert | Test | no execute(\|...) call — wall: unknown function 'enumerationMappingByName' — no function of this nam |
| SHAPE | tests/mapping/enumeration | testEnumerationMapping.pure | testEnumMappingsWithInclude | harness-shape | row-assert | Test | no execute(\|...) call — wall: unknown function 'enumerationMappingByName' — no function of this nam |
| SHAPE | tests/mapping/enumeration | testEnumerationMapping.pure | testEnumTheSame | harness-shape | row-assert | Test | no execute(\|...) call — wall: unknown enumeration 'meta::relational::tests::mapping::enumeration::m |
| ERROR | tests/mapping/enumeration | testEnumerationMapping.pure | testMapping | other | row-assert | Test | runtime 'rcorpus::Rt' has 2 mappings binding class 'meta::relational::tests::mapping::enumeration::m |
| FAIL | tests/mapping/enumeration | testEnumerationMapping.pure | testProjectWithIfWhereBothSidesUseTheSameEnumMapping | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [My Product, GS_NUMBER], got [My Product 2, CUSIP] |
| FAIL | tests/mapping/enumeration | testEnumerationMapping.pure | testProjectWithIfWhereOneSideIsEnumLiteral | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [My Product, GS_NUMBER], got [My Product 2, GS_NUMBER] |
| FAIL | tests/mapping/enumeration | testEnumerationMapping.pure | testProjectionWithEnumThroughAssociation | rows-differ | row-assert | Test | assertEquals: expected [GS_NUMBER, GS_NUMBER, false], got [CUSIP, CUSIP, true] |
| FAIL | tests/mapping/filter | testFilterMappingTree.pure | testFilterMappingWithProjectionOverlapp | rows-differ | row-assert | Test | assertEquals: expected [ROOT, TDSNull, TDSNull], got [Federation, Firm X, ROOT] |
| SHAPE | tests/mapping/include | testStoreSubstitution.pure | testStoreSubstitution | harness-shape | ? | Test | no execute(\|...) call — wall: assert form 'assertIs/2' is not supported yet |
| ERROR | tests/mapping/inheritance | testInheritanceRelational.pure | testEmbeddMappingInSubTypes | normalize(mapping) | row-assert | Test | class 'meta::relational::tests::model::inheritance::Vehicle' is not mapped in mapping 'meta::relatio |
| ERROR | tests/mapping/inheritance | testInheritanceRelational.pure | testGetAll | platform-surface | row-assert | Test | unknown function 'genericType' — no function of this name in the native or user catalog (unported pl |
| ERROR | tests/mapping/inheritance | testInheritanceRelational.pure | testGetAll | platform-surface | row-assert | Test | unknown function 'genericType' — no function of this name in the native or user catalog (unported pl |
| ERROR | tests/mapping/inheritance | testInheritanceRelational.pure | testSubTypeFilter | other | row-assert | Test | class-typed property '$p.roadVehicles' used as a whole value is graph output (Phase H4) |
| ERROR | tests/mapping/inheritance | testInheritanceRelational.pure | testSubTypeFilter | other | row-assert | Test | class-typed property '$p.roadVehicles' used as a whole value is graph output (Phase H4) |
| ERROR | tests/mapping/inheritance | testInheritanceRelationalMilestoned.pure | testMilestonedSubTyping | normalize(mapping) | row-assert | Test | association 'meta::relational::tests::model::inheritance::milestoned::Vehicle_VehicleOwner' is not m |
| ERROR | tests/mapping/inheritance | testInheritanceRelationalMilestoned.pure | testMilestonedSubTypingWithDifferentDates | normalize(mapping) | row-assert | Test | association 'meta::relational::tests::model::inheritance::milestoned::Vehicle_VehicleOwner' is not m |
| ERROR | tests/mapping/inheritance | testInheritanceRelationalMultiJoins.pure | testForcedSubTypeProjectDirect | normalize(mapping) | row-assert | Test | property 'person' of class 'meta::relational::tests::model::inheritance::RoadVehicle' is not mapped  |
| ERROR | tests/mapping/inheritance | testSubtypeMapping.pure | testSubTypeMappingValidWhenMappedExplicitly | platform-surface | golden-sql+row-assert | Test | unknown function '_classMappingByClass' — no function of this name in the native or user catalog (un |
| ERROR | tests/mapping/join | testMappingAssociationToAdvancedJoin.pure | testChainedInnerJoinsWithQualifierInGroupBy | resolve | golden-sql+row-assert | Test | filtered-navigation leaf 'extraInformation' reads a join slot of 'meta::relational::tests::model::si |
| ERROR | tests/mapping/join | testMappingAssociationToAdvancedJoin.pure | testMultipleJoinsInPropertyMappingWithDateInJoin | other | golden-sql+row-assert | Test | in function 'meta::relational::tests::mapping::join::model::mapping::advancedRelationalMapping2$clas |
| FAIL | tests/mapping/join | testMappingAssociationToAdvancedJoin.pure | testMultipleJoinsInPropertyMappingWithDatesInClass | rows-differ | golden-sql+row-assert | Test | assertSameElements: expected [Row1, Row2, Row3, Row1, Row2, Row3], got [Row1, Row2, Row3] |
| FAIL | tests/mapping/join | testMappingAssociationToAdvancedJoin.pure | testSameTableNameDifferentSchema1 | rows-differ | golden-sql+row-assert | Test | assertEquals: expected [Peter B, John B, John B, Anthony B, Oliver B, null, null], got [Peter B, Joh |
| FAIL | tests/mapping/modelJoin | testModelJoinAdvanced.pure | testChainedTwoHops | rows-differ | row-assert | Test, AlloyOnly | assertEquals: expected [Apple, null, Apple, ProjectY, Apple, ProjectX, Google, ProjectZ], got [Apple |
| ERROR | tests/mapping/modelJoin | testModelJoinAdvanced.pure | testFilterWithInnerJoinOnTarget | normalize(mapping) | row-assert | Test, AlloyOnly | class 'meta::relational::tests::mapping::modelJoin::domain::Person' is not mapped in mapping 'meta:: |
| ERROR | tests/mapping/modelJoin | testModelJoinAdvanced.pure | testNestedModelJoinCompoundInnerCondition | normalize(mapping) | row-assert | Test, AlloyOnly | association 'meta::relational::tests::mapping::modelJoin::domain::Person_Firm' is not mapped in mapp |
| ERROR | tests/mapping/modelJoin | testModelJoinAdvanced.pure | testQualifiedPropertyInQuery | resolve | row-assert | Test, AlloyOnly | nested navigation 'address.city' inside an exists/isEmpty predicate is not supported yet |
| ERROR | tests/mapping/modelJoin | testModelJoinAdvanced.pure | testSubFilter | resolve | row-assert | Test, AlloyOnly | nested navigation 'address.city' inside an exists/isEmpty predicate is not supported yet |
| ERROR | tests/mapping/modelJoin | testModelJoinSimple.pure | testDerivedPropertyInCondition | normalize(mapping) | row-assert | Test, AlloyOnly | association 'meta::relational::tests::mapping::modelJoin::domain::Person_Firm' is not mapped in mapp |
| ERROR | tests/mapping/multigrain | testMultiGrainTableMappings.pure | testToManyWithQualifierWithFilterOnJoin | resolve | golden-sql+row-assert | Test | multi-hop navigation account.incomeFunctionSplits#f0.incomeFunction.Classification.name through an e |
| FAIL | tests/mapping/relation | tests.pure | testMappingWithWindowColumn | rows-differ | row-assert | Test, AlloyOnly | assertEquals: expected [David, Group D, 1, Fabrice, Group C, 1, John, Group A, 2, Oliver, Group C, 2 |
| FAIL | tests/mapping/relation | tests.pure | testMixedMappingWithFilterInProject | rows-differ | row-assert | Test, AlloyOnly | assertEquals: expected [David, null, Fabrice, null, John, John, Oliver, Fabrice, Oliver, Oliver], go |
| FAIL | tests/mapping/relation | tests.pure | testSimpleMappingQueryWithFilterInProject | rows-differ | row-assert | Test, AlloyOnly | assertEquals: expected [David, null, Fabrice, null, John, John, Oliver, Fabrice, Oliver, Oliver], go |
| FAIL | tests/mapping/selfJoin | selfJoin.pure | testSelfJoinPropertyMappingOverlap | rows-differ | row-assert | Test | assertEquals: expected [ROOT, TDSNull, TDSNull], got [Federation, Firm X, ROOT] |
| FAIL | tests/mapping/selfJoin | selfJoin.pure | testSelfJoinPropertyMappingWithDynaFunction | rows-differ | row-assert | Test | assertEquals: expected [ROOT, TDSNull, TDSNull, true], got [Banking_c1_c1, Firm X, ROOT, false] |
| ERROR | tests/mapping/sqlFunction | boolean.pure | testProject | execute(DuckDB) | row-assert | Test | Binder Error: No function matches the given name and argument types 'len(DOUBLE)'. You might need to |
| SHAPE | tests/mapping/sqlFunction | testSqlFunctionsInMapping.pure | testAdjustDateTranslationInMappingAndQuery | other | golden-sql+row-assert | Test | statement 'map' failed through the pipeline: class query under TypedMap is not resolvable yet (H2 vo |
| FAIL | tests/mapping/tree | tree.pure | testJoinIsolationDeeperTwoIsolations_LeftOuterLeftOuterThenInner | rows-differ | row-assert | Test | assertEquals: expected [11, Alex, OrgName3, OrgName2], got [11, Alex, OrgName3, null] |
| FAIL | tests/mapping/tree | tree.pure | testJoinIsolationDeeper_LeftOuterLeftOuterThenInner | rows-differ | row-assert | Test | assertEquals: expected [11, OrgName3], got [11, OrgName3] |
| SHAPE | tests/mapping/union | testUnion.pure | testEnumFilterWithUnionMappingPlanGeneration | rows-differ | row-assert+plan-assert | Test | assert form 'assertEquals/2' is not supported yet — plan wall: plan: alias 't2' not resolvable to a  |
| ERROR | tests/mapping/union | testUnion.pure | testPksWithImportDataFlow | typer | golden-sql+row-assert+plan-assert | Test | no overload of 'execute' matches the argument types |
| ERROR | tests/mapping/union | testUnion.pure | testProjectAndFilterSamePropertySameJoinInUnion | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: Table "t0" does not have a column named "firstName" \|  \| Candidate bindings: : "last |
| ERROR | tests/mapping/union | testUnion.pure | testUnionToUnionJoinSequenceWithMultipleChildrenInUnionSourceTree | resolve | golden-sql | Test | resolver bug: undemanded navigation — consumed expression reads STRIPPED join slot 'PersonSet1Person |
| ERROR | tests/mapping/union | testUnionPartial.pure | testPartialUnionMappingOfSubTypePrimitiveProperties_EmbeddedMapping | normalize(mapping) | golden-sql+row-assert | Test | property 'ext1Address' of class 'meta::relational::tests::mapping::union::partial::PersonBase' is no |
| ERROR | tests/mapping/union | testUnionWithExtends.pure | testAdvancedEmbeddedInMappingQuery | normalize(mapping) | golden-sql+row-assert | Test | class 'meta::relational::tests::model::simple::Firm' is not mapped in mapping 'meta::relational::tes |
| ERROR | tests/mapping/union | testUnionWithExtends.pure | testAdvancedEmbeddedInMappingQuery | normalize(mapping) | golden-sql+row-assert | Test | class 'meta::relational::tests::mapping::union::extend::Firm' is not mapped in mapping 'meta::relati |
| ERROR | tests/query | datePeriods.pure | testGroupByWithFilterFunction_noDatePath | other | golden-sql+row-assert | Test | object-space expression node TypedFilter is not substitutable yet (H2 vocabulary): TypedFilter[sourc |
| ERROR | tests/query | testQualifier.pure | testWithParameterToClassNestedSelect | resolve | golden-sql+row-assert | Test | store resolution left getAll(meta::relational::tests::model::simple::Product) unresolved — the query |
| SHAPE | tests/query | testView.pure | testViewSimpleExists | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | tests/query | testWithEnumPushDown.pure | testPushDownProjectWithParameter | typer | row-assert+plan-assert | Test, AlloyOnly | no overload of 'meta::legend::executeLegendQuery' matches 3 argument(s) of these shapes (no candidat |
| ERROR | tests/query | testWithFunction.pure | testCollectionDistinctFunction | execute(DuckDB) | golden-sql+row-assert | Test | Binder Error: subqueries in lambda expressions are not supported |
| ERROR | tests/query | testWithFunction.pure | testDayOfWeekNumberFunction | typer | row-assert | Test | no overload of 'meta::pure::functions::date::dayOfWeekNumber' accepts 2 argument(s) |
| FAIL | tests/query | testWithFunction.pure | testFilterTimesWithManyOperands | other | golden-sql+row-assert | Test | h2-advisory divergence: golden SQL on H2 gave 12 row(s) [Allen\|6556, Firm B\|4690, Harris\|4900, Hi |
| ERROR | tests/query | testWithFunction.pure | testFilterUsingArcCosFunction | other | golden-sql+row-assert | Test | Invalid Input Error: Unable to compute acos of 1.1 |
| ERROR | tests/query | testWithFunction.pure | testFilterUsingArcSinFunction | other | golden-sql+row-assert | Test | Invalid Input Error: Unable to compute asin of 1.1 |
| ERROR | tests/query | testWithFunction.pure | testJoinStringFunction | other | golden-sql+row-assert | Test | LIST_AGG reached a dialect without a list encoding |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testCbrt | render(DB2) | golden-sql+row-assert | Test | per-driver golden loop declares DatabaseType.Composite — only the H2/DB2 renderers are built |
| ERROR | transform/fromPure/tests | testToSQLString.pure | testGreatestLeast | other | row-assert | Test | LIST_GET reached a dialect without a list encoding |
| ERROR | transform/fromPure/tests | testToSQLString.pure | testHashFunctions | other | row-assert | Test, AlloyOnly | LIST_AGG reached a dialect without a list encoding |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testIsDistinctSQLGeneration | harness-shape | golden-sql+row-assert | Test | sql-only: 2 advisory golden-SQL assert(s), no row verification |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testNonExecutableSQLString | harness-shape | golden-sql+row-assert | Test | no execute(\|...) call [calls meta::relational::extension] — wall: sql-only: 1 advisory golden-SQL a |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testPad | render(DB2) | golden-sql+row-assert | Test | per-driver golden loop declares DatabaseType.Composite — only the H2/DB2 renderers are built |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testSqlGenerationDivide_AllDBs | harness-shape | golden-sql+row-assert | Test | sql-only: 2 advisory golden-SQL assert(s), no row verification |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testSqlGenerationForAdjustStrictDateUsageInFiltersForH2 | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testSqlGenerationForAdjustStrictDateUsageInProjectionForH2 | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| ERROR | transform/fromPure/tests | testToSQLString.pure | testToSQLStringForTDSStringJoin | other | row-assert | Test | LIST_AGG reached a dialect without a list encoding |
| FAIL | transform/fromPure/tests | testToSQLString.pure | testToSQLStringJoinStrings | rows-differ | golden-sql+row-assert | Test | assertEquals: expected select "root".LEGALNAME as "legalName", listagg("personTable_d#4_d_m1".FIRSTN |
| ERROR | transform/fromPure/tests | testToSQLString.pure | testToSQLStringWithAbs | platform-surface | golden-sql+row-assert | Test | 'meta::pure::tds::groupBy_TabularDataSet_1__String_MANY__AggregateValue_MANY__TabularDataSet_1_' is  |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testToSQLStringWithAggregation | harness-shape | row-assert | Test | no execute(\|...) call [calls meta::relational::tests::functions::sqlstring] — wall: 'meta::pure::td |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testToSQLStringWithCodeBlock | harness-shape | golden-sql+row-assert | Test | sql-only: 1 advisory golden-SQL assert(s), no row verification |
| FAIL | transform/fromPure/tests | testToSQLString.pure | testToSQLStringWithPosition | rows-differ | row-assert | Test | assertEquals: expected select substring("root".FULLNAME, 0, locate(',', "root".FULLNAME) - 1) as "fi |
| SHAPE | transform/fromPure/tests | testToSQLString.pure | testTrim | render(DB2) | row-assert | Test | per-driver golden loop declares DatabaseType.Composite — only the H2/DB2 renderers are built |
| ERROR | validation/tests | testComplexValidations.pure | validateComplexValidation2 | other | row-assert+constraints | Test | object-space expression node TypedFilter is not substitutable yet (H2 vocabulary): TypedFilter[sourc |
| ERROR | validation/tests | testComplexValidations.pure | validateComplexValidation3 | other | row-assert+constraints | Test | object-space expression node TypedFilter is not substitutable yet (H2 vocabulary): TypedFilter[sourc |
| ERROR | validation/tests | testComplexValidations.pure | validateComplexValidation5 | other | row-assert+constraints | Test | object-space expression node TypedGroupBy is not substitutable yet (H2 vocabulary): TypedGroupBy[sou |
| ERROR | validation/tests | testComplexValidations.pure | validateComplexValidation6 | resolve | row-assert+constraints | Test | filtered-navigation leaf 'locationStreet' reads a join slot of 'meta::relational::validation::comple |

## Declared platform gaps (F2.6, hand-maintained)

15 platform gaps invisible to the corpus scoreboard. Each was an empty
@Disabled("GAP: ...") test in RelationalMappingIntegrationTest until 2026-10-05,
when the stubs were deleted (Bazel workplan P3-17: an empty body asserts
nothing); a gap closed is a test written then, not a stub un-disabled. Two more were RETIRED
2026-08-16 — 'XStore not in grammar' and 'AggregationAware not in grammar'
were stale (both implemented; the un-disabled tests pass). NOTE: this file's
generated body is script-owned; this section is appended by hand until the
generator learns it (FOUNDATIONS_PLAN §9 script backlog).

- GAP: AggregationAware + join
- GAP: Database filter + mapping filter stacking
- GAP: Database filters not extracted
- GAP: Extends + filter inheritance
- GAP: Local property + join + filter
- GAP: Local property + prefix semantics lost
- GAP: Mapping include + join navigation
- GAP: Relation class mapping not in grammar
- GAP: Scope block + embedded + filter
- GAP: Set IDs + filter disambiguation
- GAP: Store substitution + query
- GAP: View + join + filter
- GAP: extends clause ignored by builder
- GAP: scope keyword in lexer but no grammar rule
- GAP: store substitution not visited by builder
