# The separation census: what stands between the product and "the edge fully separated" (F, 2026-11-17)

Phase 7's first step, read-only, measured on d-stage `885328d0` (step 2a merged; step 3 not yet). One burndown list.
Each line: the evidence (file:line under `core/src/main/java/com/legend/` unless a Bazel label), the proposed owner,
whether a guard pins it today, and IN-FLIGHT where a running stream already covers it.
Guards cited: **CSG** = `CompatSpellingGuardTest` (shrink-only register of literal spellings and symbols);
**ESH** = `EdgeSpellingHookTest` (which dialect hooks an edge dialect may override); **AT** = `ArchitectureTest`;
**LAY** = `tools/deps/core-layers.txt` (each core library's exact deps); **PSF** = `PlanStyleFacetsTest`.

## A. Build: product targets that depend on an edge target

Query: `rdeps(//..., //core:edge + :edge_server + :edge_service_tests + :edge_store_words)`, non-test library and
binary rules. `//core:core` and `//core:plan_side` hold none of the four (LAY pins every core library).

| # | Target -> edge target | Owner | Guard | State |
|---|---|---|---|---|
| A1 | `//wasm:boundary` -> `//core:edge` (wasm/BUILD.bazel:49: pure/v1's upstream routes in the tab and Python) | D (pure/v1 build) | AT `theHostsDoorsReachNoEdgeButTheBoundarysD1Exception` (a named exception) | IN-FLIGHT (D-1: clients to lite's own routes) |
| A2 | `//native:entry` -> `//core:edge_store_words` (native/BUILD.bazel:38: upstream's /api/pure/v1/query words) | D | AT, same rule (named exception `SavedQueriesHome`) | open |
| A3 | `//query-store:page_entry` -> `//core:edge_store_words` (query-store/BUILD.bazel:21) | D | **NONE** at class level: `QueriesPage` is not in `authored_door_jars`, which the AT rule imports | open |
| A4 | `//core:server` (the shipped binary), `main_class = com.legend.edge.EdgeServer` (core/BUILD.bazel:~575) | the user's D1.0b ruling | none (a binary) | ruled? to confirm it stays the shipped root |
| A5 | `//core:core_tests_lib`, `//core:test_cases` -> `:edge`, `:edge_server` (product TESTS run with the edge) | E-Java (Phase 7 build half) | none yet (`productTestsReachNoEdge` planned) | IN-FLIGHT |
| A6 | `//core:seam_benchmark`, `//core:data_delivery_matrix_postgres_test` -> edge | E-Java with A5 | none | open |
| A7 | Record only, not separation (E-P5's A1 follow-up, docs/records/e-p5-a1-2026-11-17.md): O7's lazily loaded TypeScript compatibility module (the tab's engine clients off the module's `pure/v1` and execute exports, legend-engine's answers from the TS module; then `//wasm:edge_tab` returns to a product entry and `//wasm:edge_boundary` leaves the bundle), and Python onto lite's own plan exports (off `lite_pure_v1`, `lite_execute_plan`, `lite_execute_json_plan`, `lite_execute_json_answer`; then `EngineEntries` goes) | D (a later product step) | - | recorded only |

Transitively through A1/A2: `//datacube:*`, `//site:*`, `//studio:serve`, `//query:serve`, `//python:*`,
`//native:compiler`, `//wasm:planner`/`tab` (each clears when A1/A2 do).

## B. Code: product-package members that name legend-engine or legend-pure behaviour

| # | Member (evidence) | What it is | Owner | Guard | State |
|---|---|---|---|---|---|
| B1 | `database/PlanStyle` members `engineText`:29, `engineTestText`:38, `engineTestPlanText`:41, `engineTestConnection`:45, `activitySql`:50, `graphKeyDate`:55, `graphKeyString`:59, `errorWords`:66, `siteWords`:74, `facet`:81 | the hook the edge implements; PRODUCT answers or refuses each | D (Phase 7 review: each member kept as a ruled seam or moved behind `facet`) | AT `onlyTheEdgeImplementsTheHooksUpstreamSide`; CSG `EngineTextDialect` x8 (HOOK) | open |
| B2 | `planner/UpstreamPlans` (a facet: planToString, toSQLString, $plan rows, activity SQL) | upstream plan text seam | E-SQL | PSF; CSG | IN-FLIGHT (Phase 6) |
| B3 | `sql/dialect/EngineTextDialect` (interface, product package) | legend-engine's text dialect type | E-SQL | CSG D1.7 | IN-FLIGHT (Phase 6) |
| B4 | `RawSqlBoundary`: `sql/dialect/RawSqlBoundary.java`, `AnsiSqlRenderer`, `DuckDb`, `Postgres` | engine raw-SQL boundary | E-SQL | CSG D1.7 x5 | IN-FLIGHT (Phase 6) |
| B5 | `lowering/Lowerer.withEngineText`:238; `lowering/JsonEmission` `tdsBuilder`:74; `plan/PlanNode` executionTraceID:29 | engine plan/JSON text in the product's lowering and plan | E-SQL | CSG D1.7 | IN-FLIGHT (Phase 6) |
| B6 | `plan/PurePrint` (engine's let-source printer); `plan/UpstreamRelationType` (pure/v1's relation-type JSON) | upstream text shapes in `plan` | E-SQL (PurePrint, Phase 6); D (UpstreamRelationType, pure/v1) | **NONE** | PurePrint IN-FLIGHT (Phase 6); UpstreamRelationType open |
| B7 | `server/PureV1Api` + the `//core:pure_v1` target inside `_PLAN_SIDE_TARGETS` | legend-engine's pure/v1 API in a product package and the plan side | D (pure/v1 build) | LAY pins it as product; **no rule** says it is compat | IN-FLIGHT (D's "pure/v1 matches engine" build) |
| B8 | SqlFn edge-only nodes, each a product-refused or edge-composed form: `FLOAT_PRINT`:133, `DATETIME_PRINT`:150, `FORMAT_PADDED`:161, `INSTANCE_TEXT`:166, `RELATION_TEXT`:172, `REPRESENTATION_TEXT`:179, `TEXT_FORMS`:185 (legend-pure's arm), `TRACE_TEXT`:191 (engine's arm); emitted at `lowering/Scalars` 2850 (TEXT_FORMS), 2928 (INSTANCE_TEXT), 3455 (FORMAT_PADDED), 1724 (REPRESENTATION_TEXT), `lowering/TaggedLists`:564 (TEXT_FORMS), `lowering/Lowerer` 2817 (TRACE_TEXT), 4194 (RELATION_TEXT) | the lowering writes a node only the edge spells | D (decide: the edge's lowering variants, kind B, or stay as refused-by-name nodes) | FLOAT_PRINT: CSG (JUDGE rows x14); **the other seven: NONE** | FLOAT_PRINT/DATETIME_PRINT arms: wait on 4b/5 |
| B9 | `AnsiSqlRenderer.spellingPasses`:302, `spellsFloatPrint`:288, `bindsFloatPrint`:294, `raiseMessage`:1145; `DuckDb.scanOrderPasses`:723; `MongoDb.dateTimeFormat`:832; `sql/dialect/FloatPrintText`, `DateTimePrintText` | the renderer's hooks and passes for the edge | D | ESH (which hooks an edge dialect may override); CSG JUDGE rows | FloatPrintText/DateTimePrintText wait on 4b/5 |
| B10 | `"TDSNull"` literals: `lowering/Render.java`:58/788 (`renderTdsNull`, the CSV text), `lowering/CanonicalRenderSql`:487/499/594, `lowering/Scalars`:2214, `lowering/VerdictSql`:2752 | legend-engine's empty-cell text in product lowering | E-Java (judges' ones, 4b); D (Render, Scalars) | **NONE** (CSG says "TDSNull is not listed yet") | judges' IN-FLIGHT (4b) |
| B11 | `+0000`: `lowering/LiteralSpelling` x7, `testdatagen/TestDataPlanner` x1, `testexecutionplan/Verdicts` x1 | legend-pure's date zone text | D / E-Java (Verdicts) | CSG D1.9/D1.7 | open (Verdicts IN-FLIGHT 4b) |
| B12 | `lowering/Repr` (FLOAT_PRINT, Repr x2), `lowering/Scalars` Repr x2 (format's `%r` arm: step 2b) | legend-pure's representation | E-Java (sentence table), then F (2b) | CSG | IN-FLIGHT (E-Java #28) |
| B13 | `testdatagen` `COMPAT` (TestDataPlanner:97 x3), `EngineTextDialect` (TestDataGenerator x3, TestDataGenerationNatives x2) | engine-shaped test-data text | D | CSG D1.7/D1.9 | open |
| B14 | `parser/EngineQuirks` (legend-engine parser bugs reproduced on purpose, "byte-identity with the engine's wire") | compat inside the ONE parser | **the user** (a product row, or the edge's) | **NONE** | needs a ruling |
| B15 | `sql/dialect/H2` `ENGINE_LEXICON` x3, `SET NON_KEYWORDS` | ruled product row (§10 item 9) | - | CSG RULED | done (ruled) |
| B16 | STAYS, product semantics sharing a word: `builtin/EngineHandlers` (name resolution by engine's handler registry), `builtin/TdsLegacy` (the legacy TDS vocabulary), `lowering/DateFormats` +0000 (the Z pattern letter), `protocol/ts/TsShow` +0000, `compiler/element/type/PlatformTypes` & `sql/SqlExpr` `TDS_NULL_CELL` (the #TDS literal's input token), `values` `toEngineString` (the literal body: a name only) | language semantics / input, not output compat | - | CSG STAYS/D1.9 rows | recorded only |

Not compat (excluded after reading): `protocol/sql/iq` `IqDialect.Engine`, `IqFunctions.EngineRefusals`, `IqParser.EngineCast` (Sybase IQ's engine, the SQL island).

## C. Test side: product test infrastructure holding compat behaviour

| # | Member (evidence) | Owner | Guard | State |
|---|---|---|---|---|
| C1 | `TestPlan.JudgingRules.COMPILED_PURE_COMPAT` (testexecutionplan/TestPlan.java:378) and its branches: `testexec/Equality`, `HostJudgments`, `SqlTextRuns`; `testexecutionplan/JudgeJson`, `TestPlanJson` x2; `testplan/AssertVerdicts`, `DatabaseJudge` x2, `PlanningOptions` x2, `TestPlanner`:51 | E-Java | CSG `COMPAT` row on TestPlanner only; **the rest NONE** | open |
| C2 | The referee and appeals in product packages (G6: "the appeals and the referee are allowed only at the edge"): `testexec/SqlReplayReferee`, `Appeals`, `CanonicalDivergence`; `testexecutionplan/Appeal`, `PlannedAppeals`; `testplan/AppealsByVerdict`, `EngineTextAppeals` | E-Java | CSG `EngineText` row on PlannedAppeals only; **no rule keeps them at the edge** | open |
| C3 | Engine-text verdict records and their readers: `testexecutionplan/HostJudgment.EngineText` (record), `HostJson` x4, `Verdicts` x1; `testexec/HostJudgments`, `AssertRuns`, `SqlTextRuns`; `testplan/AgreementAsserts` x1, `AssertVerdicts` x3 | E-Java | CSG D1.7 rows | IN-FLIGHT (4a moved the arm; the records remain) |
| C4 | The engine-text facade: `testplan/EngineTestText`, `EngineTextPlanning`, `PlanStyleFacets` | E-Java | AT (one architecture row, S2c), PSF | ruled seam (4a) |
| C5 | The judges' legend-pure canons and messages: `lowering/Repr`, `VerdictSql` (Repr x3, TDS_NULL_CELL), `CanonicalRenderSql`, `testexec/CanonicalForm` (+0000), `SideShapes` (TDS_NULL_CELL; "Cannot cast a collection of size" x3) | E-Java | CSG JUDGE rows | IN-FLIGHT (4b) |
| C6 | The fused agreement path (`testplan/AgreementAsserts`, `testexec/AgreementArm`, `AgreementRuns`, `testexecutionplan/Agreement`): judge parity (G2), NOT compat, apart from C3's one EngineText read | - | - | recorded only |
| C7 | GoldenText / TextJudge: none in the product; the golden-text judging is `edge/SqlTextVerdicts` | - | - | done |

## D. Counts

| | lines | guarded | partly | unguarded | IN-FLIGHT |
|---|---|---|---|---|---|
| A build | 6 | 2 (A1, A2) | - | 4 (A3, A4, A5, A6) | 2 (A1, A5) |
| B code | 16 (2 are records only: B15 ruled, B16 STAYS) | 8 (B1-B5, B9, B11-B13) | 2 (B7 layers only; B8 FLOAT_PRINT only) | 3 (B6, B10, B14) | 7 (B2-B5, B6 half, B7, B12) |
| C test side | 7 (2 records only: C6, C7) | 2 (C4, C5) | 3 (C1, C2, C3: one row each) | 0 | 3 (C3, C4 ruled, C5) |

The unguarded lines a guard should pin now, whatever their owner (cheap, mechanical; F can build them): A3 (add
`//query-store:page_entry` to the jars the hosts' rule imports, or its own rule); B8 (a census test: the seven
edge-only SqlFn members, emitted only at their listed sites and refused by the product's renderer); B10 (`TDSNull` into
CSG's vocabulary, the rows registered with their owners); C1 (`COMPILED_PURE_COMPAT` into CSG's symbols); C2 (a rule:
referee and appeal classes reachable only from the edge or the test door). Rulings needed: B14 (EngineQuirks), A4 (the
shipped binary's root).

## E. The work queue: owner and size per line (S < half a day, M about a day, L more)

| Line | Owner | Size | Note |
|---|---|---|---|
| A1 wasm:boundary -> edge | D | M | with D-1 (clients onto lite's own routes) |
| A2 native:entry -> edge_store_words | D | S | the query store's words: a seam, or the edge's own native target |
| A3 query-store:page_entry guard | F | S | add QueriesPage to the hosts' rule |
| A4 the shipped root (EdgeServer) | the user | S | a ruling |
| A5 core_tests_lib / test_cases off the edge | E-Java | M | in flight (Phase 7 build half) |
| A6 seam_benchmark, data_delivery_matrix_postgres_test | E-Java | S | with A5 |
| B1 PlanStyle members review | D | M | Phase 7 guard review: each a ruled seam or behind `facet` |
| B2-B5 engine plan text (UpstreamPlans, EngineTextDialect, RawSqlBoundary, withEngineText, tdsBuilder, executionTraceID) | E-SQL | L | in flight (Phase 6) |
| B6 PurePrint (E-SQL, Phase 6), UpstreamRelationType (D) | E-SQL / D | S each | + a guard row each |
| B7 PureV1Api / :pure_v1 out of the product's plan side | D | L | in flight (pure/v1 build) |
| B8 the seven edge-only SqlFn nodes: guard now | F | S | a census test (emitting sites, product refusal) |
| B8 the same: their final home (edge lowering variants vs refused nodes) | D | M | after 4b/5 |
| B9 renderer hooks and print passes | D | M | FloatPrintText/DateTimePrintText go after 4b/5 |
| B10 TDSNull literals: guard now | F | S | into CSG with owner rows |
| B10 the same: moved | E-Java (judges) / D (Render, Scalars) | M | |
| B11 +0000 (LiteralSpelling x7, TestDataPlanner, Verdicts) | D / E-Java | M | |
| B12 Repr / format %r (2b) | E-Java then F | S (F's part) | waits on the sentence table |
| B13 testdatagen COMPAT / EngineTextDialect | D | M | |
| B14 EngineQuirks | the user | S | a ruling, then S-M |
| C1 COMPILED_PURE_COMPAT branches: guard now | F | S | into CSG's symbols |
| C1 the same: moved to the edge | E-Java | L | 10 files |
| C2 referee and appeals: guard now | F | S | an AT rule: reachable only from the edge or the test door |
| C2 the same: moved to the edge | E-Java | L | G6 |
| C3 engine-text verdict records | E-Java | M | in flight (4a follow-on) |
| C5 judges' canons and messages | E-Java | L | in flight (4b) |

F's own queue from this list (all S, all guards): A3, B8, B10, C1, C2.

## F. Log

- 2026-11-17, the orchestrator's rulings: **A4** the shipped `//core:server` stays rooted at `EdgeServer` (the edge
  depends on the product, never the reverse: G5) -- recorded, closed. **B14** `parser/EngineQuirks` STAYS as product
  semantics (legend-pure 5.99.0 and legend-engine 4.145.0 parse `1 < 2 + 3 * 4` alike: how Pure parses, not an engine
  quirk; F35 corrects the class's comment) -- recorded, closed.
- 2026-11-17, F's five guards, `claude/total-wall-f-guards` @ dc903476 (each its own commit; guardrails and census
  green, `--nobuild //...` green):
  - **A3** `//query-store:page_entry` joins `authored_door_jars`; the hosts' doors rule reads `QueriesPage`, whose
    `edge.StoreWords` reach is a named, dated entry. Proven to bite.
  - **B8** `EdgeOnlyNodesCensusTest`: the seven edge-only SqlFn members pinned to their emitting sites (LiteralSpelling,
    Scalars, Lowerer, Repr, TaggedLists), and on every relational product dialect the product refuses legend-pure's
    four texts by name and writes its own form for TEXT_FORMS and TRACE_TEXT.
  - **B10** `'TDSNull'` in CompatSpellingGuardTest's vocabulary: 11 rows (6 STAYS: input and language; 4 D1.9: the
    judges'; 1 D1.3: Render's relation text). Wider than the census's first list: the compiler's #TDS token sites
    (TdsChecker, TdsNullForms, Typer, PlatformTypes) and VerdictQueries, found by the scan.
  - **C1** `COMPILED_PURE_COMPAT` in its symbols: 10 files registered, D1.9.
  - **C2** `RefereeAppealsCensusTest`: the six referee/appeal classes, each pinned to its exact product reachers (the
    named entries, 26 in all), shrink-only until the move.

- 2026-11-17, later (each on its branch, the count of record `EdgeSeparationTest.linesLeft`):
  - **A1** done by E-P5 (`claude/total-wall-p7-a1` @ 1c18e97f4): BUILD empty; A1's last code entries relabelled B6
    and B7. Its follow-up recorded as **A7** (record only).
  - **A2, A3** moved to the edge (F, `claude/total-wall-f-guards` @ 885e2c36): `SavedQueriesHome` to
    `//native:edge_entry`, `QueriesPage` to `//query-store:edge_page_entry` (A4 extended).
  - **B17** opened and moved (F, `claude/total-wall-f-b17` @ 6b605d1e): a relation's legend-pure text
    (`toCSV`, a TDS's `csv`, `toString`) to `edge.RelationTextLowering` through the lowering facet `RelationTexts`;
    the product refuses at LOWER. Open at 4: the hook, until its static forwarders meet ruling (a).
  - **Hooks are terminal** (the orchestrator's ruling): **B1, B3** closed HOOK-RULED (`EngineTextDialect`),
    PlanStyleFacets HOOK-RULED; `HookRulingTest` pins (a)-(d).
  - **B8 (i)** began: `FORMAT_PADDED` left SqlFn (`PureOnlyTexts` facet, HOOK-RULED) @ 95442a1f.

## G. Recount after the guards (2026-11-17)

| | lines | guarded | partly | unguarded | closed |
|---|---|---|---|---|---|
| A build | 6 | 3 (A1, A2, A3) | - | 2 (A5, A6: E-Java's Phase 7 rule) | 1 (A4 ruled) |
| B code | 16 | 12 (B1-B5, B8, B9, B10, B11-B13, B15/B16 records) | 1 (B7: layers only) | 1 (B6: PurePrint, UpstreamRelationType) | 1 (B14 ruled) |
| C test side | 7 | 5 (C1, C2, C4, C5, C3) | - | 0 | 2 records (C6, C7) |

Unguarded now: A5/A6 (the product tests' edge dependency: E-Java's `productTestsReachNoEdge`), B6 (two `plan` classes:
an S guard row each, owner E-SQL/D), and B7 partly (`:pure_v1` in the plan side: D's pure/v1 build).
Next for F: the MOVES C2 (the referee and appeals into the edge package) and B10 (TDSNull out of product lowering).
