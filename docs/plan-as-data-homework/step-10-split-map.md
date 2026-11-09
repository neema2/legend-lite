> **Read this first (2026-11-08, after the user's review).** This is the read-only map a planning agent produced at
> `07258c9e` for step 10. It is kept verbatim below as evidence. Its class-by-class destinations and its cut list held
> up: the class-level cuts made from it in the first attempt left **zero planning-to-run and zero run-to-planning
> references**, measured with `jdeps` (the patch is `step-10-cuts-draft.patch.txt`).
>
> **Where it departs from the design** (docs/PLAN_AS_DATA_2026_11_07.md 2.9, ruling 8), and must not be followed:
> - **It routes `SystemDatabase` and the router's sequencing to "the test door" as machinery.** The design has no such
>   package. The first attempt built one (`com.legend.testdoor`), which is a renamed driver, so it was abandoned. The
>   router's sequencing belongs in a thin front door that calls one `testplan` entry and one `testexec` entry and holds no
>   logic. The system store's sessions are replaced by the catalog design (docs/SYSTEM_CATALOG_DESIGN_2026_11_08.md).
> - **It leaves `testplan -> testdatagen -> exec/testexec` as a pin.** Strict separation means splitting the generator's
>   planning entry from its run, and moving `PureAsserts.repr` below both, instead.
>
> See PLAN_AS_DATA section 10, "Step 10's first attempt".

# Step 10 split map: `driver/` and `judge/` into testplan, testexec, the edge and the doors

I read everything read-only, at `07258c9e`. Paths are relative to `core/src/main/java/com/legend/` unless they start with `core/`.

## 1. Where each class goes

**Shared types go to `test_plan` (`com.legend.testexecutionplan`).** Both halves (and the edge) use these, and `test_plan` is the only library testplan and testexec share. The 8a decision put `Edge` there for the same reason.
- `judge/TypedSide`, `judge/Column`, `judge/Row`.
- `judge/AssertListener`. The planning env carries it (`BodyDriver.ExecEnv.assertListener`, `SqlTextVerdicts.declined`/`refereed`); testexec, the edge and the harness use it too.
- `judge/Failures`, as Failure's own `of`/`raised`. This needs cut 6 in section 2 first.

### driver/

| Class | Destination |
|---|---|
| AgreementArm | testexec (delete the unused imports of `SpecCompiler`/`TypedSpec`, lines 6-7) |
| AgreementAsserts | testplan (delete the unused `PlanRunner` import, line 12) |
| AgreementRuns | testexec |
| AssertBranch, BodyDriver, DatabaseAsserts, EngineTextAppeals, HostAsserts (delete unused `Sessions` import, line 13), JudgmentFacts, LiteralFold, SeedSqlForms, VerdictArm | testplan |
| RunReader | deleted: the edge's decision gets explicit services instead (section 3) |
| SystemDatabase | the test door. It needs `ModelContext.derived`, the model's `DatabaseType` and `planner.ConnectionPlans`, which exec may not name. It goes to exec only when the held 2.6/8c question is answered. |
| TestRun | testexec (it becomes the entry) |
| package-info | deleted |

**SPLIT classes:**
- **Appeals**
  - testexec: `OPEN`, `Held`, `Report`, `Keeping`, `listener()`, `open()`, `hold`, `listenerNow`, `writing`, `afterFailure`, `told`, the `decide` loop (ordering, the `wroteAfterHold` refusal, telling the kept reports), `failed`, `tell`, `Scope`. `Decider` becomes testexec's `AppealJudge` (section 3).
  - edge: the per-appeal body of `decide`: the `own` reader over `PlanRunner.rows` (lines 178-187), the `RunScope.judging` scope, and the decision itself.
- **AppealsByVerdict**
  - testplan: `of(TestPlan, EngineTextAppeals)`, `walk`, `add`, `numbers`.
  - test_plan (as data reads): `byNumber`, `of(Map,int)`, `stores`.
- **AssertErrorNative**
  - testplan: `Args`, `args`×2, `optionalInt`, `caught`.
  - testexec: `judge`, `judged`.
- **AssertVerdicts**
  - testexec: `enter`, `familyEntered`, `sqlTextRouted`, `judge(Assertion,…)`, `judgedOrFellThrough`, `judgedAtItsStep`, `judgedRowAtItsStep`, `HostHeld`, `heldAtItsStep`, `agreedAtItsStep`, `deferredTextOrigin`, `excerpt`, `firstTextDiff`, `structured`, `jsonSideTextOf`, `decodeSide`, `decodeSideValues`, `one`, `cells`, `concat`, `ok`, `fail`.
  - testplan: everything else (`planDatabase`, `DatabasePlan`, `DatabaseGate`, `SpliceHook`, `HostAssert`, `planHost`, `planArm`, the quantified plans, `holdsEngineText`, `oneTextOrigin` comparing origin names as strings, `kindKey`, …).
  - `HostAssert.judge`/`enter` are dead and can be deleted.
- **BodyRouter**
  - testplan: `lifted` and the classify-and-plan block of `execute` (lines 69-145): BodyClass, `bodyEnv`, system routing, the `*Asserts.plan` calls, `TestDoor.planned`, `AppealsByVerdict.of`. This becomes one testplan entry returning a `TestPlan`.
  - test door: the sequencing in `execute` (round trip, guard 3, system sessions, run, `appeals.decide`, answer), `planned`, `runPlanned`, `sessionsByTarget`.
  - testexec: `Moved`, whole, as a public sink/judges class.
- **DatabaseJudge**
  - testexec: `verdictOf`, `unjudged`×2. Planning also calls `unjudged`; copy it, it is 3 lines.
  - testplan: everything else.
- **HostJudge**
  - testexec: `finish`×2, `columnKinds`, `tdsRowValuesVerdict`, `tdsHostMessage`, `tdsRowValuesSameElements`, `allWireType`, `restrictToKeys`/`One`/`Nested`, `SideFetch`, `typedSide`, `structuredSide`, `sideCanonOf`, `summarize`, `judge`×3, `decided`, `restrictKeys`, and every `*Of`: `tdsEquivalentOf`, `gridPairOf`, `gridText`, `sizeOf`, `containsOf`, `toleranceOf`, `messagedOf`, `conditionOf`, `subsetOf`, `emptyOf`, `eqOf`, `instanceOfOf`, `isOf`, `sameElementsOf`, `unrolledOf`, `allRowsOf`, `rowsOf`, `literal`, `pairedOf`, `rowArgValue`, `number`, `equalsOf`, `equalsMessage`, `jsonOf`, `renderedOf`.
  - testplan: `statementAsk`, `valueAsk`, `identityAsk`, `canonAsk`, `facts`, every `plan*`, `messaged`, `rowArg`, `pairing`, `zipArms`.
- **HostSides**
  - testplan: `Ask`, `Check`, `PlannedCheck`, `planChecked`, `withoutTheMessage`, `Planned`, `plan`.
  - testexec: `shapedBase`, `message`.
- **JudgeJson**
  - product (`test_plan`, since planning reads goldens too, `DatabaseJudge.java:1028`): `read` with the strict LITE branch.
  - edge: `JsonSimple`, behind a seam (section 3).
- **JudgePlans**
  - testexec: `shape(SideShape,Rows)`, `rowsOf`, `pivotColumnType`, `valuesShape(Rows,SideShape)`, `host`, `hostColumn`, `hostRow`, `holding`, `Cardinality`.
  - testplan: `target`, `rowsPlan`×2, `sideOf`, `withFrames`, `runner`, `evalSide`, `PlannedSide`, `plannedSide`, `plannedOf`, `constantRows`, `shape(PlannedSide,…)`, `shapeOf`, `sideShape`, `shapeColumn`, `objectsPlan`/`Shape`, `tabularPlanned`/`Shape`, `reportedType`, `oneCarrier`, `keyedShape`, `valueLane`, `valuesShape(Rows,WrappedSide)`, `constantSide`/`Text`/`Value`, `decodeConstant`, `sameConstants`.
- **SqlTextVerdicts**
  - testexec: `judge(EngineText,…)`, `rowAppealed`, `actualText` (rewritten, cut 2), `textsDiffer`, `declinedNow`, `declinedTo`, `scalarString`, `stripChainedPlanWarning`, `CHAINED_PLAN_WARNING`, `isPopulationGolden`, `NAMED_IN_TEMP`, `ok`, `fail`. Copy the small ones planning also uses.
  - edge: `decideAppeal`, `rowsLegNow`, `read(Appeal.Read)`, `InListSeed.temps`, `NUMBERED_IN_TEMP`.
  - testplan: everything else (`Collector`, `Deferring`, `NotAConstant`, `Collecting`, `BatchTexts`, `plan`, `armOf`, `decide`, the `tryArm*` arms, `rowsLegAndVerdict`×3, `rowsAhead`, `sideAhead`, `inListSeed` producing `Appeal.InListSeed`, `storesOf`, `declined`, `refereed`, …).

### judge/

| Class | Destination |
|---|---|
| CallerMessage, CanonicalForm, Equality, SideReader, TdsCompare, SqlTypeCensus, PrimitiveCensus (only tests and pct read it) | testexec |
| PureAsserts | testexec (cut 3) |
| RootLoops | testexec. `fallback`/`attribute` (Census alone) go to `diagnostics`; `VerdictBatch.Judge` and `.Appeal` move in here. |
| CanonicalDivergence | SPLIT. To `diagnostics`: the `sql*` counters (`sqlRoute`, `sqlRaised`, `sqlFellThrough`, `sqlEnter`, `sqlJudgedInDatabase`, `sqlUnjudged`, `sqlFamily`, `sqlUlpPolicy` and their counts, `SQL_CENSUS`), because planning counts them. To testexec: `probe*`, `v7*`, samples, `keyOf`, `kindClass`, the reports. |
| VerdictBatch | testplan, minus `Judge`/`Appeal`. Drop the dead `trace` field (`ExecutionTrace`) and move `PrepTrace` to `diagnostics`. |
| VerdictIds | testplan |
| SqlReplayReferee | edge |
| AssertListener, TypedSide, Column, Row, Failures | test_plan (see the shared types above) |

### The root doors
- `Execution` is SPLIT:
  - product door `//core:execution`: `run`×5, `Source`, `Establish`, `planned`, `roundTrip`.
  - a new test-door class and target: `runProgram`×3, `PlannedProgram`, `runPlanned`, `planProgram`, `dialectOf`.
  - `Answer` is declared in testexec (cut 16).
- `ExecuteOptions` stays at the test door. It names `platform.Feature`, `model.DatabaseType` and `exec.Sessions.Opener`. testplan gets a narrow planning-options record instead (judgeMode, rules, features, resources, edge); testexec takes plain inputs, as `Moved`'s constructor already does.

## 2. What breaks the testexec rule, and how to cut each

**Compiler and lowering:**
1. `BodyRouter.java:561` uses `compiler.element.type.Multiplicity.Bounded(...).text()`. Add `text()` to `ExecutionPlan.Multiplicity`, the same 3 lines.
2. `SqlTextVerdicts.java:517-525` (`actualText`) reads `lowering.VerdictSql.schema()`/`ACTUAL`. Read position 2, as `DatabaseJudge.verdictOf` (429-447) already does. Name it a `test_plan` constant, with a planner-side test pinning `VerdictSql.schema`'s order.
3. `HostJudge.java:155,217,691` and `PureAsserts.java:250,256` read `ClassLayouts.SYNTHETIC_TYPE`/`SYNTHETIC_ID` ("__type"/"__id"). Put the constants in a lower library and make `ClassLayouts` alias them, as `PlatformTypes.TDS_NULL_CELL` aliases `SqlExpr`.
4. `HostJudge.java:592` calls `compiler.spec.AssertMessages.unevaluated`. Move the text to `error` (2.9's "asserts' message texts"), or store the full text in `HostJudgment.Messaged.unplanned` at `HostSides.withoutTheMessage`.
5. `JudgePlans.java:517,537,544,546` use `Type.RelationType.presentPivotName` and `PIVOT_SEPARATOR`. Move the rule to `sql` (or onto `SideShape`) and make `Type` delegate. `JudgePlans.java:580` uses `PlatformTypes.TDS_NULL_CELL`: use `sql.SqlExpr.TDS_NULL_CELL` directly.
6. `Failures.java:56,57,63` build `compiler.spec.TypeInferenceException`, `SchemaInvariantException` and `sql.dialect.DialectCapability`. Move the three classes to `com.legend.error`, as section 10 already decided. That is a sed over 71+8+57 files. Also update the binary names in `Failure.Kind` (`Failure.java:57-60`): `NAMED` lookup depends on them; the JSON writes the enum name, so it is unchanged.
7. `AgreementArm.java:6-7`: unused compiler imports. Delete them.

**java.sql and exec:**
8. `TestRun.java:218-219` and `AssertErrorNative.java:156` name `exec.RaisedErrors.Positioned`, which extends `java.sql.SQLException`. Add an exec accessor `RaisedErrors.position(Throwable)` returning a nullable `Position(line, column)` record (the step-3 decision).

**testplan:**
9. `RootLoops.java:95-96,143-144,176`, `AgreementRuns.java:32,36,87,90,164` and `BodyRouter.java:281` use `VerdictBatch.Judge`/`Appeal`. Declare both in `RootLoops`.
10. The run reads `AppealsByVerdict.byNumber`, `of` and `stores` (`BodyRouter.java:170,173,340-341`). Move them to `test_plan` as plain reads of the plan.
11. Helpers planning also uses: `AssertVerdicts.ok`, `fail`, `excerpt`, `firstTextDiff` and `DatabaseJudge.unjudged`. Copy them (2-5 lines each), or put `ok`/`fail` beside `TypedSide`.

**The root door and the model:**
12. `Execution.Answer` is used at `BodyRouter.java:275,326,542` and `JudgePlans.java:105`. Declare `Answer` in testexec.
13. `ExecuteOptions` and `SystemDatabase`: the sessions function stays at the door (`BodyRouter.java:164-166,244-247`). testexec takes `Function<Database.System, Session>`, the rules and the edge.

**Not testexec breaks:** `SqlReplayReferee.java:62,121,141` (`ModelContext`) leaves with the class for the edge. `VerdictBatch`'s `DialectCapability` and `PrepTrace` uses go to testplan.

**Edges into testplan.** These break 2.9's "testplan may not depend on exec or testexec" rather than your hard rule:
- `StatementOrigin` marks at planning: `SqlTextVerdicts.java:141,176-177,637,1309`, `BodyDriver.java:722`, `AssertVerdicts.oneTextOrigin`. The origin is already a String in `EngineText.oursOrigin`; give planning its own mark.
- `PlanRunner.Rows` in `JudgePlans`' constant check (392-405, 758-783, 879): move the record to `execution_plan`.
- `ExecutionTrace` in `BodyDriver.ExecEnv` (73,95): drop it.
- `BodyDriver.java:92` calls `testdatagen`, which reaches exec and `judge.PureAsserts.repr` (`TestDataGenerator.java:1034`): split the generator's planning entry and move `repr` down.

## 3. Where the product would depend on the edge, and how to invert it

1. **The referee.**
   - **Where it is named:** `Execution.java:179,192,245` (the `runProgram` parameter), `BodyRouter.java:50`, `BodyDriver.java:47,71,156` (`ExecEnv.replayReferee`), `SqlTextVerdicts` null checks at 651, 783, 901, 1035, 1317, 1496, `com.legend.test` (`TestObserver.java:8,50` and `PureTestRunner.java:12,479-606`). `test` is listed in `PRODUCT_LIBRARIES`, so it counts as product.
   - **The inversion:** drop the parameter everywhere. The referee rides inside the per-test edge the harness already builds per test (`MinimalCorpus.java:603-608`): `new CompatEdge(recorder, referee)`.
   - **Planning:** the "no referee: wall" check becomes "no edge handed", or is left to the decision, which already refuses at `SqlTextVerdicts.java:1845`.
   - **Planning's referee records:** `SqlReplayReferee.ReplayFacts` and `TempTable` (`AssertVerdicts.java:1890`, `SqlTextVerdicts.java:1083,1187,1534-1541,1649-1700,2039-2097`) are replaced by the existing `Appeal.Rows` fields and `Appeal.TempTable`.
2. **The decision after the run.** `BodyRouter.java:174-175` builds the `Decider` from `SqlTextVerdicts.decideAppeal(…, referee, typed.context(), l)`.
   - **The inversion:** testexec declares `AppealJudge`: decide(Appeal, the recorded texts, AppealRun, AssertListener). `AppealRun` offers `read(Appeal.Read)` (a plan of its own on the sessions, then `JudgePlans.shape` and `Failure.raised`) and `session(Target)`. This replaces RunReader's thread-locals.
   - **How it reaches the run:** `CompatEdge` implements both `test_plan.Edge` and `AppealJudge`. `ExecuteOptions.recording` takes `<E extends Edge & AppealJudge>` (or two fields), and the test door hands it to testexec. A ServiceLoader seam does not fit here: the recorder and the referee are per test.
   - **`ModelContext`:** the referee uses it only for enum decode and properties (`ReplayReferee.java:721-755`). Either bind the model at the harness (`referee(t)` at `PureTestRunner.java:479`) and drop `ctx` from `verify*`, or have the harness build the edge per body with the model. In neither case does the product hand the edge a compiler type.
   - **Edge library deps:** + error, exec, testexec, and compiler if `ctx` stays.
3. **The compatibility JSON grammar.** `HostJudge.java:1091,1096` (run) and `DatabaseJudge.java:1028` (planning) call `JudgeJson.read`.
   - **The inversion:** `test_plan` declares a `CompatJson` service, found by ServiceLoader like `ExtensionCodec`. The edge registers `JsonSimple` in `core/src/main/edge/META-INF/services`. A compatibility run with no provider is refused by name.
   - **Why ServiceLoader here:** compatibility rules run without any edge in pct `ChannelB.java:276`, `AssertBarTest`, `DecimalKindsTest` and `RenderedTextTest`.
   - **Build:** add `//core:edge` as a runtime dependency of the pct lanes and `core_tests_lib`.
4. **Optional:** `SqlTextVerdicts.judge`'s compatibility branch (stripping the upstream warning, the population/in-temp refusal, lines 382-390) could move behind `AppealJudge.hear`. It can also stay as rule-switched handling of text data.

## 4. Guards and tests to update

**ArchitectureTest:**
- `PRODUCT_LIBRARIES` (lines 75, 100): replace `judge` and `driver` with `testexec`, the test door and `execution`. The edge stays off the list.
- Rules naming the old packages:
  - `phasesNeverDependOnTheDriverLayer` (221, 231)
  - `engineStyleRendererIsQuarantinedToTheRootLayer` (250)
  - `engineTextDialectIsQuarantinedToTheRootLayerAndThePlanPrinter` (282)
  - `theDriverHasOneCaller` (573-581)
  - `plannerIsExecutionFree` (600)
  - `theTestPlanningLibraryIsTheDriversAlone` (618, 633)
  - `thePlanRunSplitInReportMode` (1128-1135)
  - `SPLIT_CROSSINGS` (1183-1196)
  - `theBodyDriverAndTheJudgesHoldNoJdbc` (1391-1392)
  - `theCompatibilityJsonReaderIsTheJudgesAlone` (1511-1517)
  - `hostVerdictIsReachableOnlyFromTheVerdictSeam` (1524-1565)
  - `staticCollectionStateIsImmutableOrRegistered` (1642-1706: the `judge.CanonicalDivergence.*`, `SqlTypeCensus.*` and `PrimitiveCensus.*` names)
- **New hard rules:** testexec reaches no compiler, lowering, resolver, planner or testplan; no product class reaches `com.legend.edge`; R1-R4.

**JavaEvalLedgerTest:**
- `import com.legend.driver.BodyDriver` (line 6).
- Line pins at 299, 303 (judge Equality, PureAsserts), 578, 579, 582 (HostJudge, DatabaseJudge, AssertVerdicts), 830 (BodyDriver), 1010 (SqlTextVerdicts), 1035 (TdsCompare). Re-path them and re-split the counts.
- `JUDGE_CLASSES` (1381), `ROOT_CLASSES` (1425: add the test-door class), `DRIVER_CLASSES` (1499).
- `FUNNEL_PACKAGE_REGISTERS` (1649): drop driver and judge; add testexec and testplan; add the edge if it is to be closed too.
- `SESSION_ONLY_FILES` (1787-1797).

**CodeShapeGuardrailTest** (keyed `Class.field` by simple name): `BodyRouter.opened`, `.lastVerdict`, `.reader` (these are `Moved`'s fields), `JudgePlans.rows` (`Cardinality`), `JudgeJson.at`/`.kind`/`.value` (move to the edge class). These follow renames. The others (`VerdictBatch.current`, `RootLoops.counted`, `Appeals.*`, `VerdictIds.last`, `TestRun.reading`, `EngineTextAppeals.last`, `AgreementRuns.*`) stay if the names are kept.

**ErrorShapeGuardrailTest** per-file catch counts move with each split: `SqlTextVerdicts` 3 (part to the edge), `AgreementArm` 1, `BodyDriver` 3, `HostAsserts` 6, `BodyRouter` 1, `AssertVerdicts` 3, `HostSides` 1, `DatabaseAsserts` 6, `SqlTypeCensus` 1; and the global `CATCH_RETURNS_VALUE = 30` (line 242).

**Other source guards (paths):**
- HarnessDisciplineTest:286, 289
- SqlTextRatchetTest:84
- VerdictChannelRegisterTest:36-56
- TestLaneOrderGuardrailTest:99 (to the planning half of `JudgePlans`)
- NativeRegistryGovernanceTest:5, 34, 42 (`BodyDriver.registered*Keys`)

**No live entries:** JdbcSurfaceCensusTest, DatabaseOwners and DialectBoundaryTest only mention these packages in prose. PlanTypesAreDataTest already lists `com.legend.edge` as a data package; it needs a change only if non-data classes in the edge become reachable from an `Extension`.

**BUILD and data:**
- `_CORE_TEST_PACKAGES`: replace `driver` and `judge` with `testexec` (and `edge`).
- `_CORE_TARGETS` and `core-layers.txt`: drop `driver` and `judge`; add `testexec`, `execution` and the test door; `edge` grows.
- `server_lib`: `:driver` becomes `:execution`. The `test` library and testdatagen switch to the new targets.
- Move the test sources with their classes (package-private access): `core/src/test/java/com/legend/driver/*` (11 files) and `judge/*` (9).
- Harness imports: spec `ReplayReferee`, `PlanReplay`, `RefereeVerify`, `MinimalCorpus`; pct `ChannelB*` and `PctCensusGate` (census class names).

## 5. Order of moves (each step compiles on its own)

1. **Cuts in place (small surgery):** items 1-5 and 7-8 of section 2, `Multiplicity.text`, deleting the dead `HostAssert.judge`/`enter` and the unused imports.
2. **Exceptions to `error` (mechanical):** about 136 files plus `Failure.Kind`. Corpus/pct ledgers print simple names, which are unchanged; the lanes confirm.
3. **Census and `VerdictBatch` (mechanical):** the `CanonicalDivergence` `sql*` split, `RootLoops.fallback`, `PrepTrace` to `diagnostics`, `Judge`/`Appeal` into `RootLoops`.
4. **Shared types to `test_plan` (mechanical rename):** `TypedSide`, `Column`, `Row`, `AssertListener`, `Failures`. About 45 files including spec and pct.
5. **Split the mixed classes inside `com.legend.driver` (real surgery, the largest step):** `HostJudge`, `AssertVerdicts`, `DatabaseJudge`, `SqlTextVerdicts`, `JudgePlans`, `HostSides`, `AssertErrorNative`, `AppealsByVerdict`; `Moved` becomes top-level; the planning-options record. `thePlanRunSplitInReportMode` can then classify every class.
6. **The edge inversion (real surgery):** `CompatJson` SPI, `AppealJudge`/`AppealRun`, referee into the edge, delete `RunReader`, drop the `runProgram` referee parameter (12 door callers plus the harness).
7. **Create `//core:testexec` (mechanical):** package rename; delete `//core:judge`.
8. **Planning classes into `com.legend.testplan` (mechanical, plus the testplan-to-exec cuts):** pin any left with a dated note if not cut.
9. **Split the doors and delete `//core:driver`:** mostly mechanical; the door class split touches about 12 callers. Rewrite `core-layers.txt` and make R1-R4 hard.

Roughly 60% of the diff is mechanical (steps 2-4 and 7-9). The risk sits in steps 5 and 6. Run the DuckDB corpus on both judges after each of them.

### Critical Files for Implementation
- /home/user/legend-lite/core/src/main/java/com/legend/driver/BodyRouter.java
- /home/user/legend-lite/core/src/main/java/com/legend/driver/SqlTextVerdicts.java
- /home/user/legend-lite/core/src/main/java/com/legend/driver/Appeals.java
- /home/user/legend-lite/core/src/main/java/com/legend/driver/HostJudge.java
- /home/user/legend-lite/core/BUILD.bazel (plus /home/user/legend-lite/tools/deps/core-layers.txt and /home/user/legend-lite/core/src/test/java/com/legend/ArchitectureTest.java)