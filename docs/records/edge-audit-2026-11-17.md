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

## D. Closures checked against the tenets (auditor 4, the whole program)

Pending.
