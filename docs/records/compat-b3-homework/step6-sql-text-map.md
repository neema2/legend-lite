# B3 step 6: the SQL-text fallback as test tooling -- the map (read-only research, 2026-11-24, on c152f9b0)

Prepared while step 2b's round ran; the plan of record is compat-stage-b-plan-2026-11-20.md (§B3, Q16b, Q16c; H13 §27-28,
quoted in the program log at eafc8272:3279-3367). On H2 today's appeal decides 1,053 row MATCHes; the public door rebuilds
986 (93.6%); the 67 left: 15 plan-text, 7 shapes `from()` cannot spell (4 post-processor runtimes, 3 inline `^Mapping`s),
20 other shapes, 32 "unaligned" (the 20 and 32 named in no file here: to be read).

## Today's path
- **Planning:** `testplan/AssertVerdicts.java:796` routes `assertSameSQL`/`assertEqualsH2Compatible` (and assertEquals that
  `reachesUpstreamText`, :894-907) to `EngineTextGates.armFor` (:824) when the style `judgesEngineText` (the edge's
  `PureText` only) -> `edge/EdgeEngineTestText.plan` -> `SqlTextVerdicts.plan` (:206-260): each text a constant or a
  deferred side, the check `HostJudgment.EdgeJudged(new EngineTextJudgment(...), appeal, ...)`; the arms `tryArm` (:416,
  toSQLString; a foreign dialect a CONTRACT appeal), `tryArmSameSql` (:597), `tryArmDualGolden` (:660), `tryArmExecRead`
  (:788), `tryArmPlanText` (:1055); `rowsLegAndVerdict` (:1366-1405) plans the appeal's rows (ours, a paged chain's
  population, the in-list temp seed, enum decodes, `ReplayGates` facts, the replay target) into `Appeal.Rows`, carried by
  verdict (`TestPlanner.java:130-215`). The dual-golden decline is rewritten BY THE PRODUCT PLANNER to
  `assertEquals(upgraded, actual)` (`AssertVerdicts.java:839-851`, `gateOf` :180) -- Q16c moves it to the door.
- **Run:** `TestRunner.java:103` opens the hook (`AppealRun.with(referee)`); at the verdict step `HostJudgments.java:310` ->
  `BodyRun.judged` -> `Appeals.judged` (:97) -> `SqlTextRuns.judge` (:29-87): equal texts hold; otherwise the appeal is HELD
  and the assert holds provisionally, later reports kept back. After the run (`TestRunner.java:124` -> `Appeals.decide`,
  :191-227): refused if the body wrote after the hold; else each planned side runs as a plan of its own (`REFEREE_OURS`)
  and `SqlTextRuns.decideAppeal` asks the referee (`verify`/`verifyPlan`: MATCH holds; DIVERGED/FAULT fail; DECLINED keeps
  the text); the first failure drops what follows (:229-262).
- **The referee's goldens:** `spec/.../harness/ReplayReferee` replays legend-engine's golden SQL on an H2 2.1.214 mirror per
  session, seeded from the `RawLedger.Recorder` ledger the product's `testexecutionplan.Edge` hook writes
  (`TestDoor.java:144-148` plans `EffectTexts` and `RefereeLoad` -- the referee-dialect copies of each load;
  `TestRunner.java:434-474` reports them to `CompatEdge`); rows compared by `RefereeVerify` (spec, 1,409 lines).

## Counts
Edge-net goldens: H2 assertSameSQL 419 pass / 7 fail, assertEqualsH2Compatible 298 / 4 (701 tests); DuckDB 445/8, 313/4
(736). Failing details: H2 58 (oracle declined 28, rows underivable 20, ROW DIVERGED 10), DuckDB 59 (28, 20, 11).
`MinimalCorpusTest`: the differential floor (referee MATCH) DuckDB 990, H2 944 (:616, :644); text-decided ceilings
(:805-833). H13: 1,075 SQL-text asserts in 1,013 tests on H2; 199 through assertEqualsH2Compatible (Q16c).

## The replacement
The door LIFTS each SQL-text assert out (not substituted: our text failing inside the body would stop it); the body runs as
lite; AFTER the body the harness runs `from(<the producer lambda's last statement>, mapping, runtime)` (lets inlined,
`execute` bound: `UpstreamPrints.readBody`) on the body's session through the public test door, asks the referee for the
golden rows, compares, and puts the verdict back at the assert's index with first-failure sequencing. AFTER, not before:
48 DuckDB appealing bodies write (Appeals.java:30-31); H13 §28.5 found 0 writes after a lifted SQL-text assert, so the rows
after the body are the rows at the assert. Keep the by-name refusals (an assert followed by a write; a body that failed
after writing). The harness must keep reporting the routes (`listener.declined`/`refereed`) the ceilings read.
Not rebuilt by the public door: 15 plan-text (the referee's plan replay); 4 post-processor runtimes (R1: maybe fixed --
`BodyPlanner.java:245-258` merges a `from()`'s tableReplace; measure); 3 inline `^Mapping`s (MATCH as today: the lambda
binds its mapping); `testQualifierQueryWithOr` (needs the `extentSubset` fact); 20 shapes and 32 unaligned (to read).

## The deletion (core main, lines)
edge/: SqlTextVerdicts 1609, Appeals 324, SqlTextRuns 273, SqlTextReferee 257, AppealJson 255, Appeal 173,
EdgeEngineTestText 160, CompatEdge 157, RawLedger 141, EdgeCodec 101, PlannedAppeals 56, AppealRun 34, RefereeLoad 34,
EffectTexts 28, NotAConstant 23, EngineTextJudgment 18 (SqlReplayReferee 173 and ReplayGates 121 move to the harness;
SqlTextInputs stays). testplan/: EngineTextPlanning 204, EngineTextGates 82, NumberedExtensions 55, EngineTestText 53,
TextCollector 39, TextAsk 27, TextPlanned 13; ExecEnv's engineText/appealOn/appeals; AssertVerdicts' arm routing,
`holdsEdgeJudged`, the dual-golden rewrite, `gateOf`; HostAsserts:231, DatabaseAsserts:312, TestPlanner:130-215,
TestDoor:144-148, JudgePlans:163, PlanStyleFacets.KINDS, PlanStyle.judgesEngineText. testexec/testexecutionplan: EdgeRun,
EdgeHearing, NoEdgeRun (whole only if HostForms.caught moves to B2), BodyRun.judged/writing, HostJudgments:310,
AssertRuns:44, HostJudgment.EdgeJudged and its HostJson arms, TestPlan.extensionsByVerdict, RootLoop.NO_APPEAL,
testexecutionplan.Edge/NoEdge/Extension. exec/root: ProgramSink.seeded/effectRan, StatementOrigin.REFEREE_*,
ExecuteOptions.recording, TestExecution's EdgeRun parameters, Audience.edgeRun. Guards: ArchitectureTest (~38
EngineTextPlanning PLANNER_ENTRIES rows :1175-1263, RUN_API :2289, CALLER_INTERFACES :2336, theListenerIsOutputOnly
:2866/:2893-2897, the Appeals.reader row :4833-4836, FACET_PLANTS/PlantedEngineTestText :5054-5069, :5147),
JavaEvalLedgerTest (EVICT_SIZE AssertVerdicts 1329, EngineTextPlanning 125, SqlTextVerdicts 1089, SqlTextRuns 199;
:1494, :1736, :1745, :1784, :1871-2127, :2244, :2302-2314), HookRulingTest, HookLambdasTest, HookRulingsAgreeTest,
RefereeAppealsCensusTest (whole), GuardNamesExistTest:67, and re-pins elsewhere. Registers: S380, the differences page;
rows for Q16c's rule and R1.

## Risks
- **The referee's seeds are the biggest gap:** deleting CompatEdge/RefereeLoad removes the referee-dialect copies of each
  load (CSV reads DuckDB's statements cannot replay on H2). The harness needs its own seeding of the mirror (each setup
  planned for the H2 referee through the public door, run on the mirror), or every rows verdict declines or faults.
- Product code holding compatibility logic: the dual-golden rewrite (AssertVerdicts), PlatformTypes.ASSERT_SAME_SQL /
  ASSERT_EQUALS_DUAL_GOLDEN, the exec sink hooks and StatementOrigin values, the public door signatures.
- Lanes that move: both corpora where the 67 go unplaced; the strength floors and text ceilings unless the harness
  re-reports; PCT channel B and EdgeProgram (`AppealRun.with(null)`, CompiledPureHostForms' `caught` wording unless B2
  takes it); the held appeal `testToSqlGenerationFirstDayOfWeek`.
- Report order after a lifted assert fails: upstream stops at that assert -- the harness drops the reports after it.
