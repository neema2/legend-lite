"""THE EDGE'S TESTS (Phase 7's build half, 2026-11-17): the core test files that reach an edge class (com.legend.edge),
compiled in core_edge_tests_lib against the edge; core_tests_lib, the product's tests, compiles without it, so a product
test cannot reach the edge (Bazel's strict deps; ArchitectureTest.productTestsReachNoEdge reads this list too). A file
joins only when it tests the edge's own behaviour.

Measured at the split (2026-11-17, A5): test cases per family before (f-guards e422d7035) and after, each target
green -- host 6335 = 5429 product + 906 edge; judges 9788 = 8168 + 1620; the h2 lane 2322 = 1956 + 366; the sqlite
lane 2322 = 1956 + 366; guardrails 208 = 61 + 147. Nothing runs twice, nothing dropped."""

EDGE_TEST_SRCS = [
    "src/test/java/com/legend/ArchitectureTest.java",
    "src/test/java/com/legend/AssertErrorNativeTest.java",
    "src/test/java/com/legend/AssertVerdictSpliceTest.java",
    "src/test/java/com/legend/AuditRound3Test.java",
    "src/test/java/com/legend/CellTextDirectTest.java",
    "src/test/java/com/legend/DecimalKindsTest.java",
    "src/test/java/com/legend/EdgeSeparationTest.java",
    # no guard names what is not there (the edge audit's K16, 2026-11-17): it reads every jar the boundary guards read
    "src/test/java/com/legend/GuardNamesExistTest.java",
    "src/test/java/com/legend/HookRulingTest.java",
    # the presentation program's guards, report-only from S0 (2026-11-18): G6, today's mode branches counted; G1, the
    # text-producing MIR the edge builds or reaches
    "src/test/java/com/legend/ModeBranchCensusTest.java",
    "src/test/java/com/legend/OnePlanGuardTest.java",
    # the two hook registries agree (the GUARD stream, 2026-11-17): it reads HookRulingTest and EdgeSeparationTest
    "src/test/java/com/legend/HookRulingsAgreeTest.java",
    # the open compatibility classes pinned by member set (the GUARD stream, C.4, 2026-11-17): it reads the census
    "src/test/java/com/legend/OpenClassMembersTest.java",
    # the open classes marked @OpenLine, held equal to the census's list (the GUARD stream, C.7, 2026-11-17)
    "src/test/java/com/legend/OpenLineMarkedTest.java",
    # the plan style's surface: KINDS, criterion (a) on TWO_FORMS, its methods and overriders (the GUARD stream, C.9)
    "src/test/java/com/legend/PlanStyleSurfaceTest.java",
    # the ServiceLoader seams pinned by type, and every service file (the GUARD stream, C.10, 2026-11-17)
    "src/test/java/com/legend/ServiceLoaderSitesTest.java",
    # every SqlFn sorted PRODUCT or EDGE_ONLY, the other guards' lists held to it (the GUARD stream, C.13, 2026-11-17)
    "src/test/java/com/legend/SqlFnSortedTest.java",
    # hooks implemented by lambdas, and twoForm's callers, pinned (the GUARD stream, C.12, 2026-11-17)
    "src/test/java/com/legend/HookLambdasTest.java",
    # L-A (the orchestrator's order, 2026-11-18): every product form calls each function it is handed -- it reads
    # HookLambdasTest's pinned product forms
    "src/test/java/com/legend/ProductFormsUseTheirArgumentsTest.java",
    # the edge's package a boundary: no dotless prefix check, no edge class in a product jar (the GUARD stream, C.14)
    "src/test/java/com/legend/EdgePackageBoundaryTest.java",
    # a public product member the edge alone uses is counted (J40 of the edge audit, 2026-11-18), and its planted
    # product class and edge caller
    "src/test/java/com/legend/EdgeOnlyMembersTest.java",
    "src/test/java/com/legend/PlantedEdgeOnlyApi.java",
    "src/test/java/com/legend/edge/PlantedEdgeOnlyCaller.java",
    # L-B (the orchestrator's order, 2026-11-18): every piece of the plans' and the MIR's data has a real product use,
    # and its planted shapes (J28's and J31's), a product-package node and its edge maker
    "src/test/java/com/legend/EdgeOnlyDataTest.java",
    # K2's shipped-artifact roots (2026-11-18): it reads the genqueries guardrails_edge declares, and EdgeSeparationTest
    "src/test/java/com/legend/ShippedArtifactRootsTest.java",
    "src/test/java/com/legend/sql/PlantedEdgeOnlyMir.java",
    # L-B's planted rewriter (2026-11-18, train 18): a read to decide is a use, a read to rebuild a copy
    "src/test/java/com/legend/sql/PlantedDecidingRewriter.java",
    "src/test/java/com/legend/edge/PlantedEdgeOnlyMirMaker.java",
    "src/test/java/com/legend/PlanStyleFacetsTest.java",
    # the edge's extension records walked as data (census C2, 2026-11-17: the appeals moved to com.legend.edge)
    "src/test/java/com/legend/PlanTypesAreDataTest.java",
    # upstream's JSON grammar, the edge's form of JsonGrammar (census C1, 2026-11-17)
    "src/test/java/com/legend/testplan/JudgeJsonTest.java",
    # the edge's refusal of a non-constant text side, recorded by its kind (train 17, 2026-11-18: NotAConstant moved to
    # com.legend.edge, raised by its text collector)
    "src/test/java/com/legend/testplan/FailureKindsTest.java",
    "src/test/java/com/legend/PlannerSurfaceTest.java",
    # census C1's K14 (the orchestrator, 2026-11-17): upstream's behaviour tested on the edge's door
    "src/test/java/com/legend/RelationCountAssertsEdgeTest.java",
    "src/test/java/com/legend/testplan/CompiledPureNumbersEdgeTest.java",
    "src/test/java/com/legend/testplan/RenderedTextEdgeTest.java",
    # G10 (2026-11-17): an assert whose sides both raise, under the compatibility rules on the edge's door too
    "src/test/java/com/legend/testplan/HostSideOrderTest.java",
    # census C1 (2026-11-17): a run whose style and hook disagree, refused by name
    "src/test/java/com/legend/testexec/MixedRunRefusalTest.java",
    "src/test/java/com/legend/testexec/EqualityCompatEdgeTest.java",
    "src/test/java/com/legend/ReferenceIntegrityTest.java",
    "src/test/java/com/legend/ResultWritersTest.java",
    "src/test/java/com/legend/compiler/spec/MultiplicityStrictnessTest.java",
    "src/test/java/com/legend/edge/EngineSpansTest.java",
    # the net under the harness's H2 sessions (the audit's J22, 2026-11-17)
    "src/test/java/com/legend/edge/EdgeH2SessionTest.java",
    # legend-engine's template parameters, the edge's own table (the audit's J31, 2026-11-18)
    "src/test/java/com/legend/edge/EngineParamsTest.java",
    # a dynamic pivot's text refused where the edge would compose it (J40, 2026-11-18)
    "src/test/java/com/legend/edge/DynamicPivotTextRefusalTest.java",
    # a String[*] parameter's in(...) on the plan-text route, refused by name (A2's reviewer, merged into the stage-A batch
    # 2026-11-19: nothing pinned InProtocol's conditional refusal once its FreeMarker splice was deleted)
    "src/test/java/com/legend/edge/InCollectionSpliceRefusalTest.java",
    "src/test/java/com/legend/edge/PresenterTest.java",
    "src/test/java/com/legend/edge/PureMeaningCheckTest.java",
    "src/test/java/com/legend/edge/PureDatesArrowTest.java",
    "src/test/java/com/legend/edge/SiteWordsRecorderTest.java",
    "src/test/java/com/legend/exec/ExecuteInDbDuckDbEdgeTest.java",
    "src/test/java/com/legend/exec/ExecutionTraceTest.java",
    "src/test/java/com/legend/exec/InterimEdgeSitesTest.java",
    "src/test/java/com/legend/exec/SeamBenchmark.java",
    "src/test/java/com/legend/exec/SeamBenchmarkDuckDbTest.java",
    "src/test/java/com/legend/exec/SeamBenchmarkH2Test.java",
    "src/test/java/com/legend/exec/TaggedListEdgeTest.java",
    "src/test/java/com/legend/integration/CatalogTurnFailureTest.java",
    "src/test/java/com/legend/integration/CsvJsonColumnTest.java",
    "src/test/java/com/legend/integration/DataDeliveryMatrixTest.java",
    "src/test/java/com/legend/integration/DataReleaseTest.java",
    "src/test/java/com/legend/integration/DeclaredQuotedTableH2Test.java",
    "src/test/java/com/legend/integration/EdgeLiveSourceTest.java",
    "src/test/java/com/legend/integration/EdgeRefereeLoadTest.java",
    "src/test/java/com/legend/integration/EdgeSpellingHookTest.java",
    "src/test/java/com/legend/integration/EmbeddedDataKindsTest.java",
    "src/test/java/com/legend/integration/FloatLiteralKindTest.java",
    "src/test/java/com/legend/integration/FloatTextTest.java",
    "src/test/java/com/legend/integration/GraphKeyTextTest.java",
    "src/test/java/com/legend/integration/JoinedTextTest.java",
    # the edge golden net's missing cases, EDGE-1 to EDGE-3 on DuckDB and H2 under both edge styles (S0 of the
    # presentation program, 2026-11-18)
    "src/test/java/com/legend/integration/EdgeCasesGoldenTest.java",
    "src/test/java/com/legend/integration/PureOnlyTextTest.java",
    "src/test/java/com/legend/integration/RelationToOneGuardTest.java",
    "src/test/java/com/legend/integration/RenderCsvIntegrationTest.java",
    "src/test/java/com/legend/integration/ServiceTestProvisionKeyTest.java",
    "src/test/java/com/legend/integration/StressServiceSuitesH2Test.java",
    "src/test/java/com/legend/integration/StressServiceSuitesTest.java",
    "src/test/java/com/legend/integration/StressSuites.java",
    "src/test/java/com/legend/integration/StressTool.java",
    "src/test/java/com/legend/integration/TextPlanSourceTest.java",
    "src/test/java/com/legend/integration/TypeInferenceIntegrationTest.java",
    "src/test/java/com/legend/integration/WritePlansH2Test.java",
    # the engine execute()'s values root, the edge's fold beside the product's array (the audit's J3, 2026-11-17)
    "src/test/java/com/legend/lowering/ReadThroughExecuteValuesTest.java",
    "src/test/java/com/legend/lowering/AggOrderNullPlacementTest.java",
    "src/test/java/com/legend/lowering/OptionalCollectionNullDropTest.java",
    "src/test/java/com/legend/lowering/ToOneLaneTest.java",
    "src/test/java/com/legend/plan/RelationTypeJsonTest.java",
    "src/test/java/com/legend/planner/BoundaryLiteExportsTest.java",
    "src/test/java/com/legend/planner/BoundaryRequestsAreClosedTest.java",
    "src/test/java/com/legend/planner/DuckDbOutputPlanTest.java",
    "src/test/java/com/legend/planner/DuckDbSnapCopyTest.java",
    "src/test/java/com/legend/planner/HostOperationsTakeNoSqlTest.java",
    "src/test/java/com/legend/planner/PlantedBoundary.java",
    "src/test/java/com/legend/planner/PlantedCompatReach.java",
    # the repointed V3 and S144 rules' planted violator (the edge audit's K16, 2026-11-17)
    "src/test/java/com/legend/planner/PlantedVerdictReach.java",
    # the CODE guard reads the sql package (the edge audit's K16, 2026-11-17): its planted reads, arm and dispatch
    "src/test/java/com/legend/sql/PlantedSqlReach.java",
    "src/test/java/com/legend/sql/dialect/PlantedSqlRendererArm.java",
    "src/test/java/com/legend/sql/dialect/PlantedRendererNest.java",
    # review F of the guard line (2026-11-18, SHOULD 1): a varargs or array keeper, an ordinal or a name kept; one shape
    # a class, and the number or the name handed to a keeper (the review of the guard line, G, SHOULD 1, 2026-11-18)
    "src/test/java/com/legend/sql/dialect/PlantedLaunderVarargs.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderArrayKeeper.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderOrdinal.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderName.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderOrdinalHanded.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderNameHanded.java",
    "src/test/java/com/legend/sql/dialect/PlantedRendererByInterface.java",
    "src/test/java/com/legend/sql/dialect/PlantedWordedRaise.java",
    "src/test/java/com/legend/sql/dialect/PlantedBoundRaise.java",
    "src/test/java/com/legend/sql/dialect/PlantedDialectBase.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderReturn.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderLambda.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderBuild.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderField.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderArray.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderAdd.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderThrow.java",
    "src/test/java/com/legend/sql/dialect/PlantedLaunderSetter.java",
    "src/test/java/com/legend/sql/PlantedSqlSwitch.java",
    "src/test/java/com/legend/sql/PlantedSqlSwitchNamed.java",
    "src/test/java/com/legend/planner/PlantedRendererHide.java",
    "src/test/java/com/legend/planner/PlantedEdgeReach.java",
    "src/test/java/com/legend/planner/ProductStatementsTest.java",
    "src/test/java/com/legend/planner/QueryDoorTest.java",
    "src/test/java/com/legend/planner/SnapCopyTransactionTest.java",
    "src/test/java/com/legend/server/ConnectionLeaseTest.java",
    "src/test/java/com/legend/server/CreateClassTest.java",
    "src/test/java/com/legend/server/EdgeServerTest.java",
    "src/test/java/com/legend/server/ServerBootTest.java",
    "src/test/java/com/legend/server/EngineAnswerRefusedBeforeTheRunTest.java",
    "src/test/java/com/legend/server/LegendHttpServerIntegrationTest.java",
    "src/test/java/com/legend/server/PureV1ApiTest.java",
    "src/test/java/com/legend/server/PureV1EdgeGoldenTest.java",
    # the SQL text a legend-engine request asks for, after A2 (the compatibility program's stage A, 2026-11-19)
    "src/test/java/com/legend/server/PureV1SqlTextTest.java",
    "src/test/java/com/legend/server/PureV1HttpTest.java",
    "src/test/java/com/legend/server/SavedQueriesTest.java",
    "src/test/java/com/legend/server/TestDataRoutesTest.java",
    "src/test/java/com/legend/sql/dialect/BinaryCollateTest.java",
    "src/test/java/com/legend/sql/dialect/BoundaryRequestFieldsFuzzTest.java",
    "src/test/java/com/legend/sql/dialect/CapabilityMatrix.java",
    # legend-pure's words of an assert's failure, the edge's (C5's step B, 2026-11-17: edge.EdgePureWords)
    "src/test/java/com/legend/testexec/PureAssertsTest.java",
    "src/test/java/com/legend/sql/dialect/CapabilityReach.java",
    "src/test/java/com/legend/sql/dialect/CatalogGridTest.java",
    "src/test/java/com/legend/sql/dialect/DialectVocabularyTest.java",
    "src/test/java/com/legend/sql/dialect/EngineSeedRowTest.java",
    "src/test/java/com/legend/sql/dialect/IdentifierEscapingTest.java",
    # LegacyTextTest.java (legend-engine's legacy text of windows and aggregates) went 2026-11-19 with the printers (A2)
    "src/test/java/com/legend/sql/dialect/PreQuotedNamesTest.java",
    "src/test/java/com/legend/sql/dialect/RaiseTextTest.java",
    "src/test/java/com/legend/sql/dialect/SchemaDdlTextTest.java",
    "src/test/java/com/legend/sql/dialect/SqlWriterTest.java",
    "src/test/java/com/legend/sql/dialect/StoreWritesTest.java",
    "src/test/java/com/legend/sql/dialect/StoredReadsTest.java",
    "src/test/java/com/legend/sql/dialect/TempTableAsTest.java",
    "src/test/java/com/legend/sql/dialect/TranslateTest.java",
    "src/test/java/com/legend/sql/dialect/UpstreamProtocolFieldsTest.java",
    "src/test/java/com/legend/testdatagen/GeneratedTestDataPrintingTest.java",
    "src/test/java/com/legend/testdatagen/GeneratedTestDataProgramTest.java",
    "src/test/java/com/legend/testdatagen/GeneratorSpliceTest.java",
    "src/test/java/com/legend/testdatagen/TestDataPlannerTest.java",
    "src/test/java/com/legend/testing/LaneFloorTest.java",
    "src/test/java/com/legend/testplan/BodyRouterParityTest.java",
    "src/test/java/com/legend/testplan/DatabaseHalfDe5Test.java",
    "src/test/java/com/legend/testplan/ExecEnvCatalogTest.java",
    "src/test/java/com/legend/testplan/TestPlanJsonRoundTripTest.java",
    # a host's planted edge reach (the GUARD stream's C.5 review, 2026-11-17): it names an edge type
    "src/test/java/planner/PlantedHostEdgeReach.java",
    # (JudgingRulesReachTest and its planted violators: deleted 2026-11-18, train 17, with the judging rules)
]

# a file whose class is not its name
_CLASS_OF = {
    "src/test/java/com/legend/integration/JoinedTextTest.java": "com.legend.integration.CellTextsTest",
}

def edge_test_classes(packages = None, root_besides = None):
    """The edge's test classes (each EDGE_TEST_SRCS file named *Test), as --select-class arguments: those in
    `packages` (com.legend.<p>, its subpackages included) and, with `root_besides`, the root target's (com.legend less
    the packages `root_besides` lists, as core_tests_root selects); every one when
    `packages` is None. The edge's test targets select them by class, so a product test never runs there twice and
    the product's test targets, which carry no edge library, never see one (F's census line A5, 2026-11-17)."""
    out = []
    names = [_CLASS_OF.get(src, src[len("src/test/java/"):-len(".java")].replace("/", ".")) for src in EDGE_TEST_SRCS]
    for name in names:
        if not name.endswith("Test"):
            continue
        parts = name[len("com.legend."):].split(".")
        top = parts[0] if len(parts) > 1 else None
        if packages != None and not (top in packages or (root_besides != None and top not in root_besides)):
            continue
        out.append("--select-class=" + name)
    return out

def product_tests_closure(name, extra = []):
    """THE PRODUCT'S TEST TARGETS' CLOSURE (F's census line A5, the orchestrator's word of 2026-11-17: "provable"
    means the product's tests build and pass with no edge target in their closure): every testonly java_test and
    java_binary of this package not tagged "edge_test", and `extra`, with all they reach -- a genquery
    //tools/deps:edge_separation_closure_test meets with the edge's targets. Call it last in the BUILD file, so every
    target is seen: a new product test target is covered without a list to keep."""
    targets = sorted([
        "//core:" + r["name"]
        for r in native.existing_rules().values()
        if (r["kind"] == "java_test" or (r["kind"] == "java_binary" and r.get("testonly"))) and
           "edge_test" not in r.get("tags", [])
    ] + extra)
    native.genquery(
        name = name,
        testonly = True,
        expression = "deps(set(%s))" % " ".join(targets),
        opts = ["--noimplicit_deps"],
        scope = targets,
        visibility = ["//tools/deps:__pkg__"],
    )
