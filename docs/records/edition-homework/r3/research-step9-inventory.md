# Step 9 inventory: test machinery out of the product (read 2026-11-26 on claude/edit-ahead 585adf88)

*A read-only researcher's report, set out as it was handed back. Nothing was edited and no build was run; every claim about who calls what cites a path and line.*

I read the code, built a name-reference graph of the 84 edge classes (comments and strings stripped), and grepped every caller in `core`, `spec`, `pct`, `wasm`, `native`, `tools` and `parser-equivalence`. Nothing was edited and no Bazel was run.

**The finding that shapes everything.** The edge is not test-only. `//core:edge` ships:
- `//core:server` takes `:edge_server` at runtime (core/BUILD.bazel:572-576), and `:edge_server` depends on `:edge` (:519).
- `//wasm:edge_boundary` and `//native:edge_entry` also depend on it.

So "out of the product" means two things: a class leaves `core/src/main`, and it leaves the shipped `//core:edge` jar for a `testonly` library.

The product roots are `EdgeServer`, `EngineMount`, `PureV1Routes`, `HostAnswers`, `EngineEnvelope` (wasm `EdgeBoundary`) and `StoreWords` (native). From them, 61 edge classes are reachable. The other 23 are reached only by harnesses and tests.

The SQL-text arm is reachable only through one link. `PureText.UPSTREAM.facet(EngineTestText)` returns `EdgeEngineTestText.INSTANCE` (PureText.java:112-114), and that drags in `SqlTextVerdicts`, `SqlTextReferee`, `ReplayGates`, `NotAConstant`, `Appeal` and `EngineTextJudgment`. `RawSqlBoundary` and `StableScanOrder` are held the same way, through PureText's dialect subclasses (:386-398, :444-452).

## A. The move list

Paths are under `core/src/main/java/com/legend/`. "Target" is today's Bazel target. **H** means a new testonly library, `//core:edge_harness`, modelled on `//core:test_backends` (core/BUILD.bazel:2602-2614: testonly, visible to //pct, //spec, //tools/census). Its sources would go under a new root, e.g. `core/src/edge_harness/java/com/legend/edge/`. Keeping the package name means no guard needs its class names changed. Do not put it under `src/test/java`: `core_tests_lib` globs that tree.

### MOVE AS IS (no conflicts)

| Class | Lines | What it does | Main callers | Test callers | Target → dest |
|---|---|---|---|---|---|
| edge/AppealRun | 34 | the corpus's/PCT's `EdgeRun`: hears appeals | none in edge | spec MinimalCorpus:622, pct ChannelB:428, core EdgeProgram, HookRulingTest, MixedRunRefusalTest +5 | edge → H |
| edge/Appeals | 324 | a body's held appeals, decided after the run (`EdgeHearing`) | AppealRun, SqlTextRuns | HookRulingTest | edge → H |
| edge/SqlTextRuns | 273 | the text-vs-golden decision after the run, rows to the referee | Appeals | — | edge → H |
| edge/SqlReplayReferee | 173 | the referee's SPI | AppealRun, Appeals, SqlTextRuns | spec harness ReplayReferee:29, RefereeVerify, PlanReplay, BodyRouterParityTest | edge → H |
| edge/CompatEdge | 157 | the `testexecutionplan.Edge` recorder (ledger texts, referee copies) | none in edge | MinimalCorpus:612, ChannelB:436, EdgeProgram, HookRulingTest:450 +6 | edge → H |
| edge/RawLedger | 141 | the referee's raw-SQL ledger | CompatEdge | MinimalCorpus:539-603, spec ReplayReferee, ChannelB +7 | edge → H |
| edge/RefereeLoad | 34 | a seed's referee copy as an `Extension` | CompatEdge, EdgeCodec | EdgeRefereeLoadTest, TestPlanJsonRoundTripTest | edge → H |
| edge/EffectTexts | 28 | an effect's ledger texts as an `Extension` | CompatEdge, EdgeCodec | HookRulingTest:242, TestPlanJsonRoundTripTest | edge → H |
| edge/EdgeCodec | 101 | `ExtensionCodec` for the edge's records. Found by ServiceLoader (core/src/main/edge/META-INF/services/…ExtensionCodec, loaded at TestPlanJson.java:330); **the services file moves with it** | none | ServiceLoaderSitesTest, EdgeRefereeLoadTest | edge → H |
| edge/AppealJson | 255 | Appeal JSON | EdgeCodec | — | edge → H |
| edge/PlannedAppeals | 56 | appeals read from the plan by number | Appeals | — | edge → H |
| edge/CompiledPureHostForms | 49 | legend-pure's host forms (2-ULP rule); implements `testexec.HostForms` | Appeals | spec RefereeVerify, PlantedCompatReach, EdgeSeparationTest | edge → H. Low risk: `HostForms` is a step-6 neighbour |
| edge/ServiceTestRunner | 618 | runs legend-engine service tests (census hard case 12) | none | CreateClassTest, DataDeliveryMatrixTest, EmbeddedDataKindsTest, ServiceTestProvisionKeyTest, StressSuites, StressTool | `edge_service_tests` (already outside every shipped artifact) → same target made testonly, or H |
| edge/TdsObjectsWriter | 93 | PURE_TDSOBJECT writer | ServiceTestRunner | ArchitectureTest:2050-2059 | same. Check with step 8: research-step8-presentation.md:135 names it |
| edge/EqualToJson | 132 | the service-test comparator | ServiceTestRunner | ArchitectureTest:4089 | same |

**Total MOVE AS IS: 15 classes, 2,468 lines** (the run side: 12 classes, 1,625; the service tests: 3 classes, 843).

### MOVE AFTER STEP 6, and after the orchestrator releases PureText

These need PureText.java:99-114 edited: the harness wraps the style to answer `EngineTestText` itself.

| Class | Lines | What it does | Callers | Note |
|---|---|---|---|---|
| edge/EdgeEngineTestText | 160 | the `EngineTestText` facet | PureText:113, SqlTextVerdicts | ArchitectureTest PLANNER_ENTRIES rows |
| edge/SqlTextVerdicts | 1,609 | plans each SQL-text assert and its appeal | EdgeEngineTestText | reads `testplan.EngineTextPlanning`, the facade over AssertVerdicts and BodyDriver, which step 6 rewrites |
| edge/SqlTextReferee | 257 | the golden's strip, the diff, bindings | SqlTextVerdicts, SqlTextRuns | `fail` is package-private (:179), so keep the package or make it public |
| edge/ReplayGates | 121 | the referee's facts | SqlTextVerdicts | OpenClassMembersTest |
| edge/NotAConstant | 23 | a refusal kind | EdgeEngineTestText, SqlTextVerdicts | FailureKindsTest |
| edge/Appeal | 173 | the appeal record | SqlTextVerdicts, plus 6 run-side classes | spec ReplayReferee |
| edge/EngineTextJudgment | 18 | a text judgment record | SqlTextVerdicts, plus 4 run-side classes | |

**Total: 7 classes, 2,361 lines.**

### MOVE AFTER a PureText edit (orchestrator conflict, not step 6 or 8)

| Class | Lines | What it does | Callers |
|---|---|---|---|
| edge/RawSqlBoundary | 221 | translates legend-engine's H2 text for DuckDB and Postgres, through `SqlDialect.raw`. Its only product caller is `testplan/EffectPlanner.java:106` (test bodies), so pure/v1 never reaches it | PureText:388,446; CompatEdge; spec EngineH2VersionPinTest, ReplayRefereeFileReadTest |
| edge/StableScanOrder | 598 | the corpus's scan order, behind `-Dlegend.exec.engineScanOrder`. That property is set only by spec/corpus.bzl:13, CorpusOn:25 and CorpusOne:30 | PureText:396 |

**Total: 2 classes, 819 lines.** PureText's dialect classes are `final` (:380, :415, :438), so the harness cannot subclass them today.

### MOVE AFTER STEP 8, or dissolved by it

| Class | Lines | What it does | Note |
|---|---|---|---|
| edge/UpstreamPrints | 337 | the print lift | Callers: MinimalCorpus:635, ChannelB:419. It is step 8's file |
| edge/PrintedRelations, PureRelationText, PureRelationTextWriter, PureValueText | 175 + 223 + 103 + 130 = 631 | Java printers | Product roots do not reach them today (callers: UpstreamPrints, pct PctPlanRoute:163). Step 8 decides whether they become the door's presenters. Do not move them |

### AFTER STEPS 4 AND 5 (orchestrator), or dissolved into the edition

`UpstreamTestBodies` 519, `EngineSubstitutes` 50, `PureComparisons` 100, together 669 lines. This is the test-body door. Callers: MinimalCorpus:628, LeanSqlCorpusTest:61, ChannelB:411, PctPlanRoute:147. `UpstreamTestBodies` was edited on 2026-11-26 (20a315c1).

### Product hooks: deleted, not moved (after step 6; the b3 design's S3-S5)

- testplan: `EngineTestText` 53, `EngineTextGates` 82, `EngineTextPlanning` 204, `TextCollector` 39, `TextAsk` 27, `TextPlanned` 13, `NumberedExtensions` 55. Total 473.
- testexecutionplan: `Edge` 42, `NoEdge` 48, `Extension` 15, `ExtensionCodec` 23, `NamedExtensions` 135. Total 263.
- testexec: `EdgeRun` 25, `EdgeHearing` 65, `NoEdgeRun` 75, `HostForms` 24. Total 189. These wait on S5: `HostForms.caught` still exists (HostForms.java:23).

Their callers are step 6's files: AssertVerdicts:146-190, 319, 405, 813-924; DatabaseAsserts:619; JudgePlans:163; BodyDriver:52-187, 756; TestPlanner:132.

### STAYS, and why

- **Plan-text natives** (25 classes, 5,341 lines: EnginePlans, PlanText, EngineSeedText, TestDataFetches, AggAwareActivities, EngineResultEnvelopes, EngineExecutedValues, …). Census T, but `PURE_V1.facet` and `twoForm` delegate to UPSTREAM (PureText:256-275), `TestDataRoutes` calls `EnginePlans.testDataTimestampText` (TestDataRoutes:67,117), and PureV1Routes:442 calls `PlanSupportFunctions`. Whether pure/v1 promises `toSQLString` or `planToString` inside a program is a ruling I could not read off the code.
- **test/PureTestRunner (438) and PureTests (130).** Its javadoc says product surface: "a user with `<<test.Test>>` functions is another" caller. It ships in `//core:server` through the umbrella.
- **test/Audience (65) and TestObserver (77).** Runner seams; they go with S5.
- **planner/UpstreamPlans, ActivityTexts, RoutedQueries, ExecutedValues, PlanTextRoutines** (166). Hooks for the natives above.

### Totals

| Mark | Classes | Lines |
|---|---|---|
| MOVE AS IS | 15 | 2,468 |
| AFTER 6 (arm) | 7 | 2,361 |
| AFTER PureText edit | 2 | 819 |
| AFTER 8 | 1 (+4 printers step 8 decides) | 337 (+631) |
| AFTER 4/5 | 3 | 669 |
| Hooks to delete | 16 | 925 |
| STAYS | ~37 | ~6,290 |

Moving everything out comes to about 44 classes and about 7,600 lines. The plan's "about 60 classes, 3,400 lines" does not match any cut I can make. MOVE AS IS plus the arm is 22 classes and 4,829 lines.

## B. The SQL-text check today

**Product code it forces to exist:**
- `PlanStyle.judgesEngineText()` (database/PlanStyle.java:76; PRODUCT returns false at :94) and the facet mechanism.
- `PlanStyleFacets.KINDS`'s EngineTestText row.
- The testplan hooks listed above, plus `BodyDriver.ExecEnv`'s `engineText`/`appealOn`/`appeals` fields (BodyDriver:63-187).
- In AssertVerdicts: `gateOf`'s dual-golden branch (:183-190), the dual golden at :405 and :856-860, and `holdsEdgeJudged` (:1418).
- `compiler/spec/VerdictQueries.assertEqualsOf` (:47; it is now in compiler/spec, the orchestrator's package).
- `PlatformTypes.ASSERT_SAME_SQL` and `ASSERT_EQUALS_DUAL_GOLDEN` (compiler/element/type/PlatformTypes.java:607,611).
- `HostJudgment.EdgeJudged` (:301), `TestPlan.extensionsByStep`/`extensionsByVerdict` (:43), `RootLoop.NO_APPEAL` (:31), `AssertListener.declined`/`refereed` (:29,42).
- `ExecuteOptions.recording`/`edge()` (:42,82).
- `ProgramRunner`'s sink `seeded`/`effectRan` (exec/ProgramRunner.java:133-137, called at :542,556).
- `StatementOrigin.REFEREE_OURS`/`REFEREE_GOLDEN`/`MIRROR_SEED` (:36-47).
- The referee copies: `Databases.REPLAY_REFEREE` (Databases.java:31), EffectPlanner:62, TestKinds:393, `setup/CsvSeed.Planned.referee` (:85-164), and `planner/LoadPlanner`'s `referee` parameter (:444).

**Edge pieces.** The planning arm and the run side, as listed in A. The text producers are EnginePlans and friends.

**Spec pieces.** `harness/ReplayReferee`, `RefereeVerify`, `PlanReplay`, and MinimalCorpus's attempt bracket and `textDecided` (:507-731). The pins are `PLATFORM_STRENGTH {990,22,27}` and `REFEREE_STRENGTH {944,22,27}` (MinimalCorpusTest:616,649), unchanged.

**Dead code found.** `PureText.ENGINE_TEST_DATABASE` (PureText.java:30) is private and never read. The live constant is `EngineText.ENGINE_TEST_DATABASE` (EngineText.java:26), read by EnginePlans.

**What b3-step6-design.md still gets right:**
- the lift and marker idea;
- the run halves;
- the Q16c rewrite at legend-engine's door;
- the referee seed strategy (re-plan for H2);
- the S3/S4 deletion lists, which mostly still exist.

**What the code has made stale:**
1. File paths point to `/home/user/s2` at 50b33970.
2. The corpus now plans under `ENGINE_TESTS` (MinimalCorpus:616). PCT plans under `UPSTREAM` with CompatEdge and `AppealRun.with(null)` (ChannelB:428-448). S2's "`UPSTREAM.judgesEngineText()` false" therefore lands on PCT too. `EngineTextGates.isRoot` asks the facet for every helper call (EngineTextGates:26-33, AssertVerdicts:319,813), so PCT is affected.
3. Line numbers: the dual golden is now at AssertVerdicts:405 and :856-860; DatabaseAsserts:312 is now :619; HostAsserts:231 has no engine-text reference left; TestPlanner keeps only `NumberedExtensions` (:132).
4. `VerdictQueries` moved to compiler/spec, and `CsvSeed` and `LoadPlanner` moved to setup/ and planner/.
5. The `Audience.read`/UpstreamPrints part and the §7 lift-SQL cost now belong to step 8.
6. S5 is still blocked: `HostForms.caught` is still in testexec.
7. S1's beside-the-old harness (a second mirror, a TSV, SqlTextChecks, MirrorSeeds, LiftedVerdicts) is new harness code. The user's §7 item 4 ("wouldn't spend time building yet another test harness") was about SQL capture, but it weighs against S1. A pure move (section E) needs none of it, because the byte-pinned corpus rosters are the gate.

## C. Guards the MOVE AS IS draft will trip

The rule to follow: add the new library's jar to `edge_jars` (core/BUILD.bazel:2788-2802; read as `legend.edge.jars`). Then every jar-reading guard sees the same classes and no jar pin moves. Without it, HookRulingTest, ServiceLoaderSitesTest and OpenClassMembersTest fail, and `thePlannerEntriesAreTheMeasuredOnesExactly` too once the arm moves.

| Guard | Why it trips | Direction |
|---|---|---|
| tools/deps/core-layers.txt:138 (`edge`) | After the move no staying edge class imports `com.legend.exec` or `testexec` (only the 12 run-side classes and ServiceTestRunner do), and `diagnostics` only CompiledPureHostForms. `:edge` can drop them (BUILD :500-505) | DOWN. A real G4 gain: the shipped edge reaches no runtime library |
| core-layers.txt:146 (`edge_service_tests`) and `not_layers` (core/BUILD.bazel:3628-3647) | A testonly library is no layer: delete the line, add a not_layers entry for it and for `edge_harness` | entry moved |
| JavaEvalLedgerTest `theFunnelPackagesAreClosedRegisters` (edge set, :1871-2133) | 15 files "GONE — delete its register row" | DOWN |
| JavaEvalLedgerTest EVICT_SIZE SqlTextRuns (:1075, 199) | "EVICTED WHOLE" | DOWN |
| JavaEvalLedgerTest SESSION_ONLY_FILES (:2327-2329: Appeals, SqlTextRuns, SqlReplayReferee) | `SourceFiles.file` of a missing path fails | delete the rows (the connection ban stops covering them), or re-point if the new root is in the declared sources |
| ServiceLoaderSitesTest PROVIDERS (:65-66, `libedge.jar!/…ExtensionCodec`) | the services file moves jar | entry moved |
| HookRulingTest:550-552 (implementers of EdgeHearing, EdgeRun, Edge) | read from the edge jars | unchanged if the jar is in edge_jars |
| WriterCarriageGuardTest:318-320 | `SourceFiles.file` paths of ServiceTestRunner and TdsObjectsWriter | entry moved (path) |
| ErrorShapeGuardrailTest:59 (ServiceTestRunner 4), :96 (SqlTextRuns 1), `CATCH_RETURNS_VALUE` (:274) | per-file catch pins over core/src/main | DOWN, re-pin |
| CodeShapeGuardrailTest:68, 121, 125, 246, 253 (Appeals.*, ServiceTestRunner.*, TdsObjectsWriter.* mutable fields) | allowlist rows | DOWN; whether stale rows fail was not determined |
| CompatSpellingGuardTest (token counts per file, :110, :347-348, :400-401) | counts in moved files leave the scan | DOWN |
| EdgeSeparationTest:451 (PlantedCompatReach → CompiledPureHostForms) | compile dependency only | `core_edge_tests_lib` (:783-807) needs a dep on H |
| _EDGE_TARGETS / _EDGE_TARGET_LIST (core/BUILD.bazel:2819, 2903) | not a failure (no root depends on H) | add H for honesty |
| ArchitectureTest rules over CORE_AND_EDGE_CLASSES and coreAndTheHostsDoors (:3782-3801), :2050-2059 writer rule, :4089 | jar-based | unchanged with edge_jars |
| PLANNER_ENTRIES (:1162-1547) | slice 1 classes are not callers. The arm's ~37 rows (:1189-1276) stay measured if H's jar is scanned; they go DOWN only with the b3 lift | none now |
| RefereeAppealsCensusTest | scans non-edge product files only | no trip |
| VerdictChannelRegisterTest:99 (one Math.ulp home) | CompiledPureHostForms leaves the main scan; no ulp site remains | no trip |
| GuardNamesExistTest | reads the source, edge and test jars | likely no trip if H is in edge_jars; the path strings above are fixed anyway |
| DialectLeakRatchetTest, DatabaseOwners, EdgeOnlyDataTest, InterimEdgeSitesTest, SqlFnSortedTest | name no slice-1 class | none |

Later slices:
- **RawSqlBoundary and StableScanOrder:** DatabaseOwners:115,263; DialectOwnerExclusivityTest:310; RawSqlLedgerTest; SqlTextRatchetTest; TestLaneOrderGuardrailTest:52-90 (it pins the property switch inside PureText's DuckDb, so an entry moves); ArchitectureTest:4205 (`StableScanOrder.FIRINGS`); PureTextOverridesTest (pinned overrides).
- **The arm:** EVICT_SIZE SqlTextVerdicts 1089 (:1044), VERDICT_FILES (:2258-2262), SESSION_ONLY (:2319, :2331), OpenClassMembersTest, EdgeOnlyMembersTest, ErrorShapeGuardrailTest:95, PlanStyleFacetsTest, PlanStyleSurfaceTest.

## D. Conflicts

- **(i) The orchestrator.**
  - `PureText.java` blocks the arm (:99-114) and the corpus dialect hooks (:380-452, plus the `final` keywords).
  - `compiler/spec/VerdictQueries.assertEqualsOf` blocks b3's S3.
  - `UpstreamTestBodies`, `EngineSubstitutes` and `PureComparisons` are under edit in steps 4 and 5.
  - No move-list file touches EngineTable, NamedRule(s) or Scalars.
- **(ii) Step 8.**
  - UpstreamPrints and the four printers belong to step 8.
  - TdsObjectsWriter appears in step 8's research (research-step8-presentation.md:135).
  - Appeals, SqlTextRuns, CompiledPureHostForms and ServiceTestRunner only read `PureMessages`/`PureV1Writer`/`Presenter`; they do not edit them.
- **(iii) Step 6.**
  - The testplan and testexec hooks, and the arm's facade `EngineTextPlanning`.
  - research-step6-equality.md:494-504 says not to run step 6 and step 9 in parallel on AssertVerdicts, VerdictQueries, DatabaseAsserts, TestPlanner and JudgePlans.
- **Free of all of these:** the 11 run-side classes without CompiledPureHostForms, plus ServiceTestRunner and EqualToJson. None has been touched since 2026-11-21 to 11-24. CompiledPureHostForms and TdsObjectsWriter carry the low risks noted above.
- **Shared files every slice edits:** core/BUILD.bazel, spec/BUILD.bazel (`spec_tests_lib`, :120-141), pct/BUILD.bazel (`pct_tests_lib`, :108, and :1153). Textual conflicts are possible but trivial.

## E. Proposed order

1. **The appeals' run side and the service tests.**
   - Classes: the 15 MOVE AS IS. Create `//core:edge_harness` (testonly), move EdgeCodec's services file into it, make `edge_service_tests` testonly with its sources moved, add H to edge_jars, and drop :exec, :testexec and :diagnostics from :edge.
   - Moves 2,468 lines out of core/src/main.
   - Guards: everything in C, all DOWN or moved entries.
   - Risk: EdgeCodec must be on the classpath of every lane that decodes a plan holding appeals (the corpus lanes run `-Dlegend.diagnostics=decoded-plan`, spec/BUILD.bazel:193, plus PCT and the core edge tests). A missing dependency shows up as moved verdicts, so the corpus lanes are the check.
2. **The corpus's dialect hooks, once PureText is free.**
   - Classes: RawSqlBoundary and StableScanOrder. A harness style wraps `ENGINE_TESTS` and its dialect overrides `raw` and `scanOrderPasses`; PureText drops those overrides and its `final` keywords.
   - Moves 819 lines.
   - Guards: the "later slices" list in C.
   - Risk: every lane's dialect must be wrapped (DuckDB, H2, Postgres for `corpus_on_postgres`), and PureTextOverridesTest's pins move.
3. **The SQL-text arm's planning half, after step 6.**
   - Classes: the 7 AFTER-6 classes. A harness wrapper answers `judgesEngineText` and `facet(EngineTestText)`; PureText loses :99-114.
   - Moves 2,361 lines.
   - Guards: the arm's JavaEvalLedgerTest rows (SESSION_ONLY_FILES :2319, :2331), OpenClassMembersTest, EdgeOnlyMembersTest, ErrorShapeGuardrailTest:95, PlanStyleFacetsTest, PlanStyleSurfaceTest. PLANNER_ENTRIES is unchanged.
   - Risk: PCT (ChannelB:448, PctPlanRoute:98,226, PctBackend:73) plans under `UPSTREAM`, and `isRoot` asks the facet for every helper call. PCT must be wrapped too, or `UPSTREAM.judgesEngineText` is made false and PCT channel B is measured.
4. **Optional, a ruling: the b3 lift and the hooks' deletion.**
   - About 925 hook lines, plus routing code in AssertVerdicts and BodyDriver.
   - Guards: PLANNER_ENTRIES ~37 rows DOWN, EVICT_SIZE `EngineTextPlanning` (:1043).
   - Risk: it builds the harness pieces the user said not to build (§7 item 4), and it collides with step 6's files.
5. **After step 8 and steps 4/5: UpstreamPrints and the test-body door.**
   - About 1,006 lines, or dissolved by those steps.
   - Risk: ownership. Do it only when those sessions are done.

## What I could not determine by reading

- Whether pure/v1 promises the plan-text natives inside a program. This decides whether 5,341 lines are product or test machinery.
- Whether CodeShapeGuardrailTest and ErrorShapeGuardrailTest fail on stale rows, or just undercount.
- Whether `//docs:all` checks the file paths named in docs. About 200 doc mentions name the moved classes.
- Whether `tools/census` counts edge-jar classes.
- Whether any body in PCT channel B actually reaches the SQL-text arm.

Reflection: I found none. No main or harness code names `com.legend.edge.X` as a string; the ServiceLoader lookup at TestPlanJson:330 is the only dynamic reach. The reference graph is name-based, so a class reached only by reflection from a Bazel target I did not scan would be missed.

My graph and caller scripts are in `/tmp/claude-0/-home-user-legend-lite/37e604d0-f16d-5c80-a65e-84a7c2121e8e/scratchpad/` (`graph.py`, `ext.py`, `callers.py`, `g.json`, `ext.json`).
