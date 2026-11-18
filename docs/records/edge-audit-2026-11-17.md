# Edge audit, 2026-11-17 (read-only, d-stage c6b20f3bd)

The user (16:50): "Should we start an audit / hunt now for every possible thing left in the wrong package or places we
might have missed? ... Do we have provable separation of the edge? And all doors closed so things can't come back?"

Four read-only auditors. The orchestrator spot-checked each auditor's top claims against the code; all held. Paths are
relative to the repository root.

## Verdict (auditor 3, guards)

"Zero" today proves that no product code names the edge or a listed compatibility item. It does NOT prove that the
product holds no compatibility. Guards match closed lists of names, and several areas are not scanned at all.

## A. Hosts, build, artifacts (auditor 2): product features run through legend-engine's routes

Owner: a new HOST stream (three workers), once the user rules.

| # | Finding | Where |
|---|---|---|
| H1 | Every shipped artifact is rooted at an edge entry: the server (EdgeServer), //wasm:planner (TabExports), the query-store page, //native:compiler via :edge_entry, and the app bundles. A4 and its extensions were ruled by the orchestrator, not the user | core/BUILD.bazel:559, wasm/BUILD.bazel:113, query-store/BUILD.bazel:34, native/BUILD.bazel:113 |
| H2 | DataCube's product tab planner parses, prints and types queries through the edge's pure/v1 router; lite's 4 A1 exports have no caller | datacube/src/lite-planner.ts:632,681; engine-client/src/legend/planner-answer.ts:57 |
| H3 | Studio's and Query's in-tab execute is legend-engine's execute (UPSTREAM dialect, Presenter tokens) | engine-client/src/legend/browser-engine.ts:64-72; studio/src/app/app.ts:126; query/demo/main.ts:156 |
| H4 | Python's public parse/print/relation_type are pure/v1 through edge native entries; LegendError.kind is legend-engine's errorType | python/legend_lite/compiler.py:66-97,273-300 |
| H5 | Python's engine answers in upstream bytes through edge entries; product tests pin edge output | python/legend_lite/engine.py:1040,1798-1838; python/tests/test_engine.py:191,592 |
| H6 | The saved queries and pages use the edge's /pure/v1/query, though /api/lite/v1/query exists | query-store/src/client.ts:90; datacube/src/cube-store.ts:119; python/legend_lite/page.py:3688+ |
| H7 | The apps cannot run against lite's own server without the edge | studio/src/app/app.ts:97-107; datacube/demo/planners.ts:63-75; engine-client/src/legend/planning-client.ts:64-141 |
| H8 | A G7 fallback: lite's compile answers 404, so legend-engine's compile is asked instead | engine-client/src/legend/planning-client.ts:105-128 |
| H9 | The product run side prefers the plan's `upstream` words | engine-client/src/raised.ts:67; python/legend_lite/_raised.py:95 |
| H10 | The S23 TinyInt workaround for legend-engine is in the product boundary and records | wasm/src/main/java/planner/Boundary.java:647,670; engine-client/src/relation-type.ts:49-65 |
| H11 | TypeScript has no edge layer: engine-client:server is a direct dep of the apps; legend-engine shapes are in :records | tools/deps/ts-layers.txt |

## B. Java product: compat code no map lists (auditor 1)

Owners are proposed. Rulings needed are marked RULE.

| # | Finding | Where | Owner |
|---|---|---|---|
| J1 | RULE: parseDecimal of a column is legend-engine H2's DECIMAL(5,2), on every database | lowering/Scalars.java:2433-2440 | E-SQL |
| J2 | RULE: parseJSON accepts the goldens' '}{' typo (REGEXP_REPLACE), which corrupts real strings; the same in sql/Json | lowering/JsonLane.java:31-42; sql/Json.java:23-27 | E-SQL |
| J3 | BUG vs the user's 2026-11-03 ruling: JsonBuilder's one-object-bare cardinality applies with no gate | compiler/spec/ResultEnvelopeSplice.java:639-673; Lowerer.java:842,4480 | E-SQL |
| J4 | legend-engine plan printers in the product package com.legend.plan (PlanText, PlanSupportFunctions, PlanEnumForm, PreciseTypes, RelationalMapperRenames); the product PlanAllocations composes engine text; the HOOKS_RULED row is untrue | plan/*; planner/PlanAllocations.java:70-104 | E-P5 |
| J5 | AggregationAwareActivity in legend-engine's routed-query text on every product execute | planner/AggAwareActivities.java; PlanAllocations.java:277 | E-P5 |
| J6 | legend-engine's planTestDataGeneration shape in product testdatagen; the base renderer writes FreeMarker for VarSetPlaceholder | testdatagen/TestDataGenerator.java:430-620; sql/dialect/AnsiSqlRenderer.java:683 | E-P5 |
| J7 | legend-engine test-data texts (row identifiers sentence, seed string) on the product path | testdatagen/TestDataPlanner.java:500-516,843-870 | E-P5 |
| J8 | "Invalid provided parameter(s)" in product exec | exec/PlanParameters.java:89 | E-P5 |
| J9 | Saved queries and cubes answer lite's own route in legend-engine's sentences and envelope | queries/SavedQueries.java, SavedCubes.java | E-P5 |
| J10 | Compile/model sentences in upstream's words (Duplicated class mappings, Duplicated mapping include, PropertyValidator) | normalizer/*, compiler/element/ModelIntegrity.java | E-P5 |
| J11 | The 2-ULP tolerance is composed in product lowering and values; callers use predicates the guard cannot see | lowering/VerdictSql.java:771-846,3229; values/FloatTolerance.java | E-Java (C1) |
| J12 | legend-engine's H2 test connection JSON is in every product object reference, whatever the database | model/AsorRef.java:31-51; resolver/GraphEmission.java:3207 | E-SQL |
| J13 | ExecuteOptions.withEngineCompat: a public compat switch no guard sees | ExecuteOptions.java:76-84 | E-Java (C1) |
| J14 | Precision.INSTANT is "spelled as legend-engine's TDS"; published as legend.precision in product Arrow | executionplan/ExecutionPlan.java:680; result/ArrowWriter.java:113 | E-P5 |
| J15 | testexecutionplan/Edge.java: an unpinned edge hook | testexecutionplan/Edge.java:16 | E-Java |
| J16 | G7: the product's by-name refusal is swallowed as "no rows" | planner/PlanAllocations.java:128-145 | E-P5 |
| J17 | PureAsserts (legend-pure's assert format) is named by no map; C5 could read closed while it stands | testexec/PureAsserts.java | E-P5 (step B) |
| J18 | lite's own route answers in legend-engine's error envelope and errorType words | server/LiteV1Api.java:371-420 | E-P5 |
| J19 | RULE: the product lowers EXISTS with legend-engine's "second exists emission" | lowering/ExistsJoinForm.java; planner/ProductStatements.java:401 | E-SQL |
| J20 | RULE: substring/indexOf (S130/S134, ruled before the program) and isNumeric pinned to engine H2 | lowering/Scalars.java:1489,2501,2546 | E-SQL |
| J21 | RULE: GrammarLevel.LEGEND_ENGINE branches product parse semantics; PmcdParser always uses it; ProtocolEmitter | parser/*, protocol/ProtocolEmitter.java | E-P5 |
| J22 | RULE: product H2 sessions use legend-engine's settings (MODE=LEGACY, DEFAULT_NULL_ORDERING=HIGH) | sql/dialect/H2.java:44-52; exec/H2Connector.java:25 | E-SQL |
| J23 | Live misses from auditor 3: LoweringShape's edge-only flags (viewsAsDerivedTables, elideWireCasts), with B5 counted closed | lowering/LoweringShape.java; CastPolicy.java:51; edge/EnginePlans.java:213 | E-SQL |
| J24 | A rules check without the constant | testexec/IsolatedRun.java:104 | E-Java (C1) |
| J25 | A legend-pure sentence in //base, raised by product exec | base/src/main/java/com/legend/base/DataSize.java:36 | E-P5 |
| J26 | NITs: LiteV1Api's unused import, PureV1Handler's name, Styled's dead written text, RelationTexts' style substitution | | E-P5 |
| J27 | legend-engine's empty-cell text as a Pure body the platform ships: meta::pure::tds::TDSNull's toString(){'TDSNull'} is copied verbatim from upstream's TDS. It is B10's spelling, and it breaks the world-map rule that the platform ships declarations and no other Pure bodies. GUARD's C.6 found it (2026-11-17 19:37); no guard saw it before. Design first: the product's text for an empty cell is lite's form or a refusal by name; legend-engine's 'TDSNull' is the edge's; upstream's definition is read from the pinned sources | core/src/main/resources/com/legend/builtin/prelude.pure:5029-5036 | F |

## C. Guard holes (auditor 3): what it takes for zero to be provable

Owner: a new GUARD stream (one worker), mostly in new test files; it lands after wave 2.

1. Structure over names: every product member whose only callers are com.legend.edge is counted or ruled (reachedEntries over every product package).
2. HOOKS_RULED cross-checked against HookRulingTest.RULED. Terminal register rows (STAYS/RULED/HOOK-RULED) frozen as an exact set with ruling ids.
3. The JudgingRules TYPE flagged, not just COMPILED_PURE_COMPAT.
4. Open classes pinned by member set or access sites; FACADES list members instead of `#*`.
5. The style, dialect, spelling-door, reflection and hook rules run over the hosts' doors; the stale planner.Boundary exemption deleted.
6. A constant-pool scan of the product jars (catches concatenation and borrowed constants); per-class pins.
7. Open-line classes marked by an annotation, not by name, so a rename cannot close them.
8. Scans of resources, the hosts' Java, Python and TypeScript; TypeScript and Python edge modules with layer rules.
9. KINDS, TWO_FORMS and PlanStyle's methods pinned exactly; criterion (a) applied to TWO_FORMS; the overrider set pinned.
10. ServiceLoader call sites pinned by SPI type.
11. Bazel roots and edge lists derived from one source; shrink-only enforced in CI by a diff against the merge-base.
12. Single-method hooks (lambdas) and twoForm's callers pinned.
13. One exact map from SqlFn.values() to PRODUCT or EDGE_ONLY.
14. The product jars hold no com.legend.edge class; the package prefix check has its dot.

## D. Closures checked against the tenets (auditor 4, the whole program since 2026-11-08)

**Method:**
- About 300 guard removals were enumerated, and every vocabulary or pattern change was read in full.
- About 135 entries were code-read.
- The 168 removed CSG spellings were re-scanned over EVERY main .java file. None survives outside the edge, except the FLOAT_PRINT rename.

**Result:**

| Period | Real | Dodges | Closed by redefinition | Partial |
|---|---|---|---|---|
| Before 11-16 | all | 0 | | (the 15 FLOAT_PRINT relabels started B9's chain) |
| 11-16 | all | 0 | | W9's PlanAllocations |
| 11-17 | about 140 | 37: B9 rename 36, B5 withEngineText 1 | 7 (A1-A3: the shipped artifacts declared edge) | B7 |

| # | Finding | Where | Owner |
|---|---|---|---|
| K1 | B9 was closed by a RENAME. The orchestrator approved it at 12:52 as "lite's canonical Float text", but the node's emitters are compat-only, and the new name is in no vocabulary. **REOPEN** until the product's own Float text provably uses it, or until it moves to the edge | sql/SqlFn.java:133-146; sql/dialect/FloatShortestText.java | E-SQL |
| K2 | A1-A3 were closed by declaring the shipped artifacts edge (A4 extended by the orchestrator). Goes to the user with the HOST stream | core/BUILD.bazel; query-store, native, wasm BUILD | HOST |
| K3 | = J4: legend-engine's plan printer and model are in the product com.legend.plan. UpstreamPlans' signature returns com.legend.plan.PlanNode | plan/*; edge hooks | E-P5 |
| K4 | = J16: the product's by-name refusal is swallowed (G7). HookRulingTest's (b) is tautological for facet hooks | planner/PlanAllocations.java:128-144,349 | E-P5 / GUARD |
| K5 | Product flags choose upstream branches: LoweringShape's two edge-only flags; Styled.plain's "omit the check" branches | Lowerer.java:1250; CastPolicy.java:51; Scalars.java:433,3027 | E-SQL |
| K6 | Implicit "no edge" defaults: a Lowerer field defaults to Styled.PRODUCT; TestObserver's default method returns NoEdgeRun.NONE | Lowerer.java:279; test/TestObserver.java:50 | E-SQL / E-Java |
| K7 | testexecutionplan.Edge is a nullable hook; null selects the judging rules (against the 16:40 ruling) | ExecuteOptions.java:33; PlanningOptions.java:36; TestDoor.java:140; TestRunner.java:200 | E-Java (C1) |
| K8 | The product's format('%r') bypasses twoForm through a PRODUCT DateTimeTexts that writes DATETIME_PRINT; HookRulingTest pins no implementers | Scalars.java:3569 | F (B12 2b) / GUARD |
| K9 | BUG: a stale class name in a plan kind turns a planned refusal into a crash (SqlTextVerdicts$NotAConstant moved to EngineTestText) | testexecutionplan/Failure.java:60 | E-Java |
| K10 | Exec picks upstream or lite words by null: Site.upstream is an edge-only field in the product plan | executionplan/ExecutionPlan.java:826-838; RaisedErrors.java | E-P5 |
| K11 | = J18: B7 keeps legend-engine's errorType words and status map under lite names | server/LiteV1Api.java:305,373 | E-P5 |
| K12 | The legend-engine route branch stays in product testdatagen (B13), and so does the S300 sentence | testdatagen/TestDataPlanner.java:148-185,499-515,620-635 | E-P5 |
| K13 | The product writer prints legend-pure's Map form `{k=v}` (and a list as `[a, b]`), against the user's 2026-11-10 ProductListTexts ruling | result/CellText.java:75-88 | E-SQL |
| K14 | Product tests carry COMPILED_PURE_COMPAT; no guard scans test sources | EqualityWorldsConformanceTest, PureAssertsTest, JudgeJsonTest | E-Java (C1) |
| K15 | DATETIME_TO_TEXT carries an edge-only operand in product MIR and plans | Styled.java:33; SqlFn.java:149 | E-SQL |
| K16 | NITs: sql.* field accesses are skipped; raiseMessage's product default is unpinned; CanonicalDivergence; stale text; PureText's edge-side fallback to the product dialect | | GUARD / various |

**Direction:** clean. No product main source names an edge type. There is no reflection. The one ServiceLoader seam loads EdgeCodec from the edge jar only. Plan tags are enum names, apart from K9's class-name string.

## Confirmation audit at d-stage fdf001bee (2026-11-18 01:00)

One fresh read-only auditor. Its brief: product Java that exists only for legend-engine or legend-pure compatibility and is named by no census line or audit item. Its verdict: separation is complete at the dependency level (the guards hold, and no product class reaches the edge), but not at the level of text and behaviour. The orchestrator spot-checked J29, J30 and J32 against the code.

| Id | Finding | Where | Owner |
|---|---|---|---|
| J29 | The judges write legend-pure's assert sentences in product code. A boolean (FailureWords/HostForms/TestPlan.upstreamWords) chooses between those and lite's words, and some product paths default to legend-pure's (the `lite=false` overloads). Wider than J17 | lowering/VerdictSql.java (438, 574, 580, 640, 658, 2010-2030; overloads 409, 557, 561, 602, 611, 1405, 1983), testexec/AssertErrorRuns.java:66-118, testexec/HostJudgments.java:608,647, testplan/AssertVerdicts.java:1116,1486, testplan/DatabaseJudge.java:593,636,1039 | E-Java |
| J30 | legend-engine's parser bug (RELATIONAL_ARITH_MISASSOCIATION) applies at LEGEND_LITE, so `1 < 2 + 3 * 4` parses as `1 < 20`; Pure's precedence gives `1 < 14` | parser/EngineQuirks.java:53, parser/OperatorParts.java:117-120 | E-P5 |
| J31 | The MIR carries legend-engine's FreeMarker template fields, set and read only by the edge | sql/SqlExpr.java:783-814 (PlanParam.enumMapFn, Kind.RAW), planner/QueryParameters.java:41-51, executionplan/MirJson.java:510,989, sql/dialect/AnsiSqlRenderer.java:2172-2179 | E-SQL |
| J32 | legend-engine's setUpDataSQLs text generator sits in the product; its only callers are edge/EnginePlans | setup/Ddl.java:94-375 | GUARD |
| J33 | The referee and SQL-text arm's residue, called only by edge/SqlTextRuns and SqlTextVerdicts | testexecutionplan/Verdicts.java:83-107, compiler/spec/VerdictQueries.java:46,97,123-171,434 | F |
| J34 | legend-engine's EqualToJson rules (TestAssertions, Equality.serviceJson/serviceDiff), used only by the edge. Possibly already closed by 17cdf06e72 | test/TestAssertions.java, testexec/Equality.java:467-545 | E-Java (verify) |
| J35 | The product's JSON parse doors keep legend-engine 4.145.0's span quirk (withoutSpans/keptByTheEngine) | protocol/SourceInformation.java:41-58,157-170, parser/SpecParser.java:342-345, parser/PmcdParser.java:152-155 | E-P5 |
| J36 | Unpinned hooks (CellText.Forms, HostForms, RenderedTexts); ArrowWriter constructed only by the edge; CellText.number public only for Presenter | result/CellText.java:49-60, result/ArrowWriter.java | GUARD |
| J37 | The product builds legend-engine's AggregationAwareActivity row (J5's residue) | planner/PlanAllocations.java:282-290 | E-P5 |
| J38 | The product's JSON reader tolerates typos from legend-engine's goldens. Wider than J2 | sql/Json.java:23-27,52-58,113-115 | E-SQL |

Suspected but unverified, for GUARD to check and route:
- The PlatformTypes DDL/TO_SQL_STRING constants, read only by the edge.
- AssertVerdicts.DatabaseGate.ENGINE_TEXT and ENGINE_TEXT_READ.
- Product public statics called only by the edge: BodyPlanner.pushDownEnums, AggregationAwareRouting.mainSetId, ColumnFacts.store, PlanJson.readColumns/writeColumns/loadStepJson, LiteV1Api.head/runtimeOf, TestDataPlanner.declaredName/seedJson/pureName.
- PlanStyle.graphKeyDate and graphKeyString.
- protocol/OlderSpecReader.
- PureDateLiteral.toEngineString's name.

## GUARD's suspect list, verified (2026-11-18 03:44)

| Id | Finding | Owner / ruling |
|---|---|---|
| J40 | Product public members only the edge calls: BodyPlanner.pushDownEnums, AggregationAwareRouting.mainSetId; PlanJson.readColumns/writeColumns/loadStepJson; LiteV1Api.head/runtimeOf; TestDataPlanner.declaredName/seedJson/pureName; PlatformTypes.TO_SQL_STRING/_PRETTY; RowDecode.taggedSlots. Each moves to the edge, or is pinned as a product API with a reason. The only-edge-callers guard measures the full set. | F (pushDownEnums, mainSetId); E-P5 (LiteV1Api, PlanJson, TestDataPlanner); E-Java (TO_SQL_STRING*); GUARD (taggedSlots, with J36) |
| J41 | AssertVerdicts.DatabaseGate.ENGINE_TEXT/_READ: product branches reached only when a host judgment is EdgeJudged | F |
| J42 | protocol/OlderSpecReader: every route accepts legend-engine 4.145.0's older value-spec shapes | **STAYS**, by the user's requirement (2026-11-18 ~01:5x): "We absolutely need to be able to parse legend-pure and legend-engine projects as-is" |
| J43 | PureDateLiteral.toEngineString: the product's own canonical date text, misnamed | E-P5 (rename, last) |

Not findings: ColumnFacts.store (the product reads it); PlanStyle.graphKeyDate/graphKeyString (style hooks, the overriders pinned). J32 was done by GUARD (0b84c036a0, edge.EngineSeedText).
